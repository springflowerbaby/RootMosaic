"""Owner-bound local attempt journal and write-ahead recovery orchestration.

Only local filesystem I/O is implemented here. Resource operations are supplied
by a compare-and-swap adapter; importing this module performs no I/O. A fake
adapter can prove offline orchestration, never live restoration or sample QC.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import threading
from typing import Any, Callable, Protocol
import uuid

from .contract import ContractError, RunContract, canonical_json, canonical_sha256

JOURNAL_SCHEMA = "rq4-collect/attempt-journal-v1"
_ZERO = "0" * 64
_PHASES = ("PREFLIGHT", "WARMUP", "PRE", "INJECTION_TRANSITION", "DURING",
           "RECOVERY_TRANSITION", "POST", "VERIFYING")
_LEGACY_NAMES = {"k8s_pilot", "_delivery", "_archive", "traditional_v2_lite", "agentfault_k8s"}
_FAILURE_EVENTS = {"failed", "apply_failed", "recovery_blocked", "cleanup_blocked"}
_ARTIFACT_RECOVERY_EVENTS = {"recovery_round_begin", "artifact_recovered_complete", "artifact_quarantined"}
_OUTPUT_EVENTS = {"artifact_intent", "artifact_complete", "artifact_abandoned"} | _ARTIFACT_RECOVERY_EVENTS


class JournalError(RuntimeError):
    """Unusable journal/configuration; does not expose credential values."""


class BlockedDirty(JournalError):
    """Queue must stop until safe reconciliation is established."""


class OperationFailed(JournalError):
    """Apply failed after durable intent; reconciliation remains mandatory."""


class IncompleteArtifact(JournalError):
    """An interrupted artifact write (intent without completion) blocks the operation."""


def _check(ok: bool, message: str) -> None:
    if not ok:
        raise JournalError(message)


def _safe_json(value: Any) -> bytes:
    try:
        return canonical_json(value).encode("utf-8")
    except (ContractError, ValueError, TypeError):
        raise JournalError("non-JSON or credential-bearing journal value") from None


def _parse(raw: bytes) -> Any:
    def unique_pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, value in items:
            _check(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(JournalError("nonfinite JSON")))
        _safe_json(value)
        return value
    except (UnicodeError, ValueError, TypeError):
        raise JournalError("invalid journal JSON") from None


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _absolute(value: Path | str) -> Path:
    path = Path(value)
    _check(path.is_absolute() and ".." not in path.parts, "explicit absolute non-traversing path required")
    return path


def _identity(path: Path) -> tuple[int, int]:
    st = path.stat()
    _check(st.st_ino != 0, "filesystem object identity unavailable")
    return st.st_dev, st.st_ino


def _recovery_transition(artifacts, rounds, row, failed_seen):
    """One shared validator for full replay and its process-local cursor."""
    event, payload = row["event"], row["payload"]
    _check(failed_seen, "artifact recovery requires a failed attempt")
    if event == "recovery_round_begin":
        _check(set(payload) == {"round_id", "previous_round_hash", "contract_sha256", "source_fingerprints", "rules_sha256", "scope", "pending_artifact_ids"}, "recovery round schema invalid")
        expected = "round-%06d" % (len(rounds) + 1)
        _check(payload["round_id"] == expected and payload["scope"] == "prepared_auxiliary", "recovery round sequence/scope invalid")
        prior = next(reversed(rounds.values()))["event_hash"] if rounds else None
        _check(payload["previous_round_hash"] == prior, "recovery round predecessor mismatch")
        pending = sorted(aid for aid, value in artifacts.items() if not value["complete"] and not value["abandoned"])
        _check(payload["pending_artifact_ids"] == pending, "recovery round pending inventory mismatch")
        hashes = [payload["contract_sha256"], payload["rules_sha256"], *payload["source_fingerprints"].values()]
        _check(bool(payload["source_fingerprints"]) and all(type(v) is str and re.fullmatch(r"[0-9a-f]{64}", v) for v in hashes), "recovery round source/rules hash invalid")
        rounds[expected] = _parse(_safe_json(row))
        return
    _check(set(payload) == {"artifact_id", "round_id", "round_begin_hash", "reason_code", "expected", "observed"}, "artifact recovery schema invalid")
    aid = payload["artifact_id"]
    _check(aid in artifacts and not artifacts[aid]["complete"] and not artifacts[aid]["abandoned"], "artifact recovery requires pending intent")
    _check(payload["round_id"] in rounds and payload["round_id"] == next(reversed(rounds))
           and payload["round_begin_hash"] == rounds[payload["round_id"]]["event_hash"], "artifact recovery round mismatch")
    expected, observed = payload["expected"], payload["observed"]
    _check(expected == artifacts[aid]["spec"] and type(observed) is dict and set(observed) == {"relative_path", "sha256", "bytes"}, "artifact recovery expectation mismatch")
    _check(observed["relative_path"] == expected["relative_path"] and type(observed["bytes"]) is int and observed["bytes"] >= 0
           and type(observed["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", observed["sha256"]), "artifact residue evidence invalid")
    _check(type(payload["reason_code"]) is str and payload["reason_code"].isidentifier(), "artifact recovery reason invalid")
    equal = all(observed[k] == expected[k] for k in ("sha256", "bytes", "relative_path"))
    _check(equal == (event == "artifact_recovered_complete"), "artifact recovery disposition contradicts bytes")
    artifacts[aid]["complete" if equal else "abandoned"] = True
    artifacts[aid]["recovery_disposition"] = _parse(_safe_json(row))


@dataclass(frozen=True)
class OutputPolicy:
    """Caller-approved root plus mandatory old-data boundaries, resolved once.

    The source repository is mandatory and its entire datasets tree is always
    protected, even if an extra protected_roots list omits it. Empty/relative
    extra protection lists are rejected as configuration mistakes.
    """
    repo_root: Path
    source_repo_root: Path
    approved_root: Path
    protected_roots: tuple[Path, ...]
    approved_real: Path

    @classmethod
    def define(cls, *, repo_root: Path | str, source_repo_root: Path | str, approved_root: Path | str,
               protected_roots: tuple[Path | str, ...]) -> OutputPolicy:
        repo = _absolute(repo_root)
        source = _absolute(source_repo_root).resolve()
        _check(source.is_dir(), "existing source repository required")
        root = _absolute(approved_root)
        _check(bool(protected_roots), "explicit old-data protected roots required")
        protected = tuple(_absolute(p).resolve() for p in protected_roots)
        protected += ((source / "datasets").resolve(),)
        # OutputPolicy is part of RunSettings' persisted fingerprint. Keep
        # this tuple deterministic across Python processes (set iteration is
        # salted by PYTHONHASHSEED), or a valid prepared recovery can appear
        
        # same.
        protected += tuple((repo / "datasets" / name).resolve() for name in sorted(_LEGACY_NAMES))
        policy = cls(repo.resolve(), source, root, protected, root.resolve())
        policy.check_root()
        return policy

    def check_root(self) -> None:
        actual = self.approved_root.resolve()
        _check(bool(self.protected_roots), "explicit protected roots required")
        _check(actual == self.approved_real, "approved output root changed resolution")
        _check(actual != self.repo_root and not _within(self.repo_root, actual), "output root contains repository")
        _check(not _LEGACY_NAMES.intersection(part.lower() for part in actual.parts), "legacy data output forbidden")
        for protected in (*self.protected_roots, self.source_repo_root / "datasets"):
            resolved = protected.resolve()
            _check(not _within(actual, resolved) and not _within(resolved, actual),
                   "output/protected root overlap")

    def attempt_path(self, contract: RunContract) -> Path:
        self.check_root()
        data = contract.to_dict()
        context = data["context"]
        _check(Path(context["repo_root"]).resolve() == self.repo_root, "repository identity mismatch")
        _check(Path(context["output_root"]).resolve() == self.approved_real, "unapproved output root")
        expected = self.approved_real / data["purpose"] / context["run_id"] / context["attempt_id"]
        supplied = Path(context["evidence_root"]).resolve()
        _check(supplied == expected and _within(supplied, self.approved_real), "attempt path escapes approved root")
        return expected


@dataclass(frozen=True)
class Target:
    kind: str
    name: str
    scope: str
    uid: str | None

    def to_dict(self) -> dict[str, Any]:
        for value in (self.kind, self.name, self.scope):
            _check(isinstance(value, str) and bool(value.strip()), "target identity missing")
        _check(self.uid is None or (isinstance(self.uid, str) and bool(self.uid)), "target UID invalid")
        return {"kind": self.kind, "name": self.name, "scope": self.scope, "uid": self.uid}


@dataclass(frozen=True)
class ResourceState:
    exists: bool
    uid: str | None
    ownership: str | None
    fields: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        _check(type(self.exists) is bool and isinstance(self.fields, dict), "resource state invalid")
        if self.exists:
            _check(isinstance(self.uid, str) and bool(self.uid), "existing resource UID unknown")
            _check(self.ownership is None or (isinstance(self.ownership, str) and bool(self.ownership)), "ownership unknown")
        else:
            _check(self.uid is None and self.ownership is None and not self.fields, "absent state must be empty")
        value = {"exists": self.exists, "uid": self.uid, "ownership": self.ownership, "fields": self.fields}
        return _parse(_safe_json(value))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ResourceState:
        _check(isinstance(value, dict) and set(value) == {"exists", "uid", "ownership", "fields"}, "resource state fields invalid")
        result = cls(**value)
        result.to_dict()
        return result


class ResourceAdapter(Protocol):
    """Adapters MUST atomically compare expected UID/ownership/fields on change.

    observe returns ONLY declared controlled fields, not full secret-bearing
    config. Each external suboperation gets its own execute_change call.
    """
    evidence_kind: str  # "fake" for offline; "live" only for a real adapter.

    def observe(self, target: Target, field_names: tuple[str, ...]) -> ResourceState: ...

    def compare_and_apply(self, target: Target, expected: ResourceState,
                          desired_fields: dict[str, Any], ownership: str) -> None: ...

    def compare_and_restore(self, target: Target, expected: ResourceState,
                            original: ResourceState) -> None: ...


@dataclass(frozen=True)
class RecoveryReport:
    status: str
    evidence_kind: str
    restored_intents: tuple[str, ...]
    blockers: tuple[str, ...]


def _lock_file(path: Path) -> int:
    # OS locks are released on process exit; an existing stale lock *file* is
    # not treated as proof of an active writer or deleted to bypass one.
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0), 0o600)
    try:
        if os.fstat(fd).st_size == 0:
            os.write(fd, b"0")
            os.fsync(fd)
        os.lseek(fd, 0, os.SEEK_SET)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except OSError:
        os.close(fd)
        raise JournalError("attempt already has an active writer") from None


def _unlock_file(fd: int) -> None:
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


class _ReplayCursor:
    """Incremental semantic replay state for exactly one journal row at a time.

    Ports the per-row checks of ``AttemptJournal._replay`` and
    ``AttemptJournal._artifact_states`` so a cached chain can validate one new
    row without re-reading the whole log. Threat-model boundary: the cursor is
    process-private, is built only from rows that already passed a full replay
    (or a successful incremental append), and is never persisted or shared
    across processes. It therefore detects in-process continuation errors
    (duplicate intent, phase order, unknown event, artifact reuse, ...), not
    external on-disk tampering after the cache was built; that stronger
    guarantee stays with every full verification path (``records()``,
    ``open_for_reconcile`` and all ``_replay`` consumers keep whole-file
    replay semantics). Payload-derived state is stored as canonical copies so
    later caller-side mutation of appended payload objects cannot drift the
    cursor away from the durable bytes.
    """

    def __init__(self) -> None:
        self.count = 0
        self.phase = -1
        self.failed_seen = False
        self.intents: dict[str, dict[str, Any]] = {}
        self.artifacts: dict[str, dict[str, Any]] = {}
        self.artifact_paths: set[str] = set()
        self.recovery_rounds: dict[str, dict[str, Any]] = {}

    def apply(self, row: dict[str, Any]) -> None:
        event, payload = row["event"], row["payload"]
        _check(isinstance(payload, dict), "event payload invalid")
        if event == "created":
            _check(self.count == 0, "created event out of order")
        elif event == "phase":
            self.phase += 1
            _check(self.phase < len(_PHASES) and payload == {"phase": _PHASES[self.phase]}, "phase order invalid")
        elif event == "intent":
            iid = payload.get("intent_id")
            _check(isinstance(iid, str) and iid not in self.intents, "duplicate/invalid intent")
            self.intents[iid] = {"intent": _parse(_safe_json(payload)), "status": "pending", "applied": None}
        elif event in {"applied", "apply_failed", "restored", "recovery_blocked"}:
            iid = payload.get("intent_id")
            _check(iid in self.intents and self.intents[iid]["status"] != "restored", "operation event without open intent")
            if event == "applied":
                _check(self.intents[iid]["status"] == "pending", "duplicate applied event")
                self.intents[iid]["applied"] = _parse(_safe_json(payload["observed"]))
            self.intents[iid]["status"] = event
        elif event == "subset_cleanup_complete":
            selected = payload.get("fault_instance_ids")
            _check(isinstance(selected, list) and bool(selected) and len(set(selected)) == len(selected), "subset scope invalid")
            _check(all(item["status"] == "restored" for item in self.intents.values()
                       if item["intent"]["fault_instance_id"] in selected), "subset cleanup has unresolved selected intents")
            remaining = sorted(iid for iid, item in self.intents.items() if item["status"] != "restored")
            _check(payload.get("remaining_intent_ids") == remaining, "subset remaining intents mismatch")
        elif event in _OUTPUT_EVENTS:
            pass  # The independent artifact transition chain is checked below.
        elif event in {"cleanup_complete", "cleanup_blocked", "failed"}:
            if event == "failed":
                self.failed_seen = True
            if event == "cleanup_complete":
                _check(all(item["status"] == "restored" for item in self.intents.values()), "cleanup complete with unresolved intents")
        else:
            raise JournalError("unknown journal event")
        if self.count == 0:
            _check(event == "created", "journal missing creation")
        if event == "artifact_intent":
            _check(set(payload) == {"artifact_id", "relative_path", "sha256", "bytes", "media_type"}, "artifact intent schema invalid")
            aid, path = payload["artifact_id"], payload["relative_path"]
            _check(isinstance(aid, str) and bool(aid) and aid not in self.artifacts and path not in self.artifact_paths,
                   "artifact identity/path reused")
            _check(isinstance(payload["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", payload["sha256"]) is not None
                   and type(payload["bytes"]) is int and payload["bytes"] >= 0, "artifact hash/size invalid")
            self.artifacts[aid] = {"spec": _parse(_safe_json(payload)), "complete": False, "abandoned": False}
            self.artifact_paths.add(path)
        elif event == "artifact_complete":
            aid = payload.get("artifact_id")
            _check(aid in self.artifacts and not self.artifacts[aid]["complete"]
                   and not self.artifacts[aid]["abandoned"] and payload == self.artifacts[aid]["spec"],
                   "artifact completion without matching intent")
            self.artifacts[aid]["complete"] = True
        elif event == "artifact_abandoned":
            aid = payload.get("artifact_id")
            _check(set(payload) == {"artifact_id", "reason_code"} and isinstance(payload["reason_code"], str)
                   and payload["reason_code"].isidentifier(), "artifact abandon schema invalid")
            _check(aid in self.artifacts and not self.artifacts[aid]["complete"]
                   and not self.artifacts[aid]["abandoned"] and self.failed_seen,
                   "artifact abandon requires an interrupted artifact of an already failed attempt")
            self.artifacts[aid]["abandoned"] = True
        elif event in _ARTIFACT_RECOVERY_EVENTS:
            _recovery_transition(self.artifacts, self.recovery_rounds, row, self.failed_seen)
        self.count += 1


class _VerifiedChain:
    """Process-private cached head of an already verified event chain.

    Holds the durable head (last row, its event_hash, verified row count) plus
    the ``_ReplayCursor`` reached by applying those rows, so ``_append`` can
    validate only the new row. Threat-model boundary: this defends against
    in-process continuation errors, and the per-append re-check of the small
    durable anchors (owner.json/contract.json digests, head.json) keeps
    external tampering of every O(1)-sized file as detectable as before. The
    events.jsonl *body* below the cached head is the one thing the
    incremental path no longer re-reads: mid-session external mutation of old
    event lines is invisible to it by construction and stays detected by the
    unchanged whole-file replay in ``records()``/``open_for_reconcile`` and
    every ``_replay`` consumer (open-time and any-load-time verification,
    never cached across processes). The cache itself is trusted process-private
    state, exactly like every other in-memory journal field:
    ``head_consistent`` is a cheap integrity self-check of the cached head, not
    a defense against arbitrary in-memory mutation. Any incremental
    inconsistency falls back to a full replay before the append is allowed.
    """

    __slots__ = ("count", "last_event_hash", "last_row", "cursor")

    def __init__(self, rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> None:
        cursor = _ReplayCursor()
        for row in rows:
            cursor.apply(row)
        self.cursor = cursor
        self.count = len(rows)
        self.last_row = rows[-1] if rows else None
        self.last_event_hash = rows[-1]["event_hash"] if rows else _ZERO

    def head_consistent(self, metadata: dict[str, Any]) -> bool:
        if self.count == 0:
            return self.last_row is None and self.last_event_hash == _ZERO
        row = self.last_row
        if not isinstance(row, dict) or row.get("sequence") != self.count\
                or row.get("event_hash") != self.last_event_hash\
                or self.cursor.count != self.count:
            return False
        if any(row.get(key) != metadata[key] for key in ("run_id", "attempt_id", "ownership")):
            return False
        try:
            return canonical_sha256({k: v for k, v in row.items() if k != "event_hash"}) == self.last_event_hash
        except (ContractError, ValueError, TypeError):
            return False

    def commit(self, row: dict[str, Any]) -> None:
        self.count += 1
        self.last_row = row
        self.last_event_hash = row["event_hash"]


class AttemptJournal:
    """Single writer with immutable owner/contract and append-only operation log."""

    def __init__(self, contract: RunContract, policy: OutputPolicy, path: Path, lock_fd: int,
                 metadata: dict[str, Any]) -> None:
        self.contract = contract
        self.policy = policy
        self.path = path
        self.metadata = _parse(_safe_json(metadata))
        self._metadata_hash = _digest(_safe_json(metadata))
        self._directory_identity = _identity(path)
        self._lock_fd = lock_fd
        self._closed = False
        self._poisoned = False
        self._reconciled_in_session = False
        self._verified: _VerifiedChain | None = None
        self._mutex = threading.RLock()

    @classmethod
    def create(cls, contract: RunContract, policy: OutputPolicy) -> AttemptJournal:
        path = policy.attempt_path(contract)
        # Every ancestor is resolved before and after creation. This assumes
        # an OS-protected workspace, not an adversarial directory-swap attack.
        for parent in (policy.approved_real, path.parent.parent, path.parent):
            policy.check_root()
            _check(_within(parent.resolve(), policy.approved_real), "output ancestor escaped")
            parent.mkdir(parents=True, exist_ok=True)
            _check(parent.resolve() == parent, "output ancestor redirected")
        _check(path.resolve() == path, "attempt path redirected")
        try:
            path.mkdir()
        except FileExistsError:
            raise JournalError("attempt already exists; retries require a new attempt ID") from None
        lock_fd = _lock_file(path / "writer.lock")
        context = contract.context.to_dict()
        metadata = {"schema": JOURNAL_SCHEMA, "run_id": context["run_id"], "attempt_id": context["attempt_id"],
                    "owner_id": context["owner_id"], "ownership": uuid.uuid4().hex,
                    "purpose": contract.to_dict()["purpose"], "contract_sha256": contract.sha256,
                    "scenario": contract.to_dict()["scenario"], "profile_id": context["profile_id"]}
        journal = cls(contract, policy, path, lock_fd, metadata)
        try:
            journal._write_new("contract.json", contract.to_json().encode("utf-8"))
            journal._write_new("owner.json", _safe_json(metadata))
            journal._write_new("events.jsonl", b"")
            journal._replace_head({"sequence": 0, "event_hash": _ZERO, "metadata_sha256": journal._metadata_hash})
            journal._append("created", {"metadata_sha256": journal._metadata_hash})
        except BaseException:
            journal.close()
            raise
        return journal

    @classmethod
    def inspect_records(cls, contract: RunContract, policy: OutputPolicy) -> dict[str, Any]:
        """Read verified identity/records under writer exclusion; append nothing."""
        path = policy.attempt_path(contract)
        _check(path.is_dir() and path.resolve() == path and (path / "writer.lock").is_file()
               and (path / "writer.lock").resolve() == path / "writer.lock", "inspection journal unavailable")
        lock_fd = _lock_file(path / "writer.lock")
        journal = None
        try:
            for name in ("owner.json", "contract.json"):
                _check((path / name).resolve() == path / name, "inspection identity redirected")
            metadata = _parse((path / "owner.json").read_bytes())
            context = contract.context.to_dict()
            _check(metadata.get("contract_sha256") == contract.sha256 and _digest((path / "contract.json").read_bytes()) == contract.sha256
                   and all(metadata.get(k) == context[k] for k in ("run_id", "attempt_id", "owner_id", "profile_id")), "inspection attempt differs")
            journal = cls(contract, policy, path, lock_fd, metadata)
            rows = journal.records()
            return {"metadata": metadata, "records": rows, "artifacts": cls._artifact_states(rows)}
        finally:
            if journal is not None:
                journal.close()
            else:
                _unlock_file(lock_fd)

    @classmethod
    def open_for_reconcile(cls, contract: RunContract, policy: OutputPolicy) -> AttemptJournal:
        path = policy.attempt_path(contract)
        _check(path.is_dir() and path.resolve() == path, "attempt directory unavailable or redirected")
        # Avoid following a hostile lock-file alias before acquiring the lock.
        _check((path / "writer.lock").resolve() == path / "writer.lock", "lock file redirected")
        lock_fd = _lock_file(path / "writer.lock")
        try:
            for name in ("owner.json", "contract.json"):
                _check((path / name).resolve() == path / name, "attempt identity file redirected")
            metadata = _parse((path / "owner.json").read_bytes())
            _check(metadata.get("contract_sha256") == contract.sha256
                   and _digest((path / "contract.json").read_bytes()) == contract.sha256,
                   "resume contract differs from immutable attempt")
            context = contract.context.to_dict()
            for name in ("run_id", "attempt_id", "owner_id", "profile_id"):
                _check(metadata.get(name) == context[name], "resume identity mismatch")
            journal = cls(contract, policy, path, lock_fd, metadata)
            rows = journal.records()
            intents = journal._replay(rows)
            pending_artifacts = [aid for aid, item in journal._artifact_states(rows).items()
                                 if not item["complete"] and not item["abandoned"]]
            if pending_artifacts:
                journal._append("failed", {"reason_code": "interrupted_artifact", "artifact_ids": pending_artifacts})
            referenced = {item["intent"]["snapshot"]["path"] for item in intents.values()}
            orphans = []
            for candidate in path.glob("snapshot-*.json"):
                if candidate.name not in referenced:
                    raw = journal._file(candidate.name).read_bytes()
                    orphans.append({"path": candidate.name, "sha256": _digest(raw)})
            if orphans:
                # No durable intent means our callback could not have run, but
                # an interrupted/failed persistence attempt cannot be resumed
                # as a fresh planned experiment. Preserve all partial bytes.
                journal._append("failed", {"reason_code": "orphaned_snapshot", "snapshots": orphans})
            if any(item["status"] != "restored" for item in intents.values()):
                journal._append("failed", {"reason_code": "interrupted_attempt"})
            return journal
        except BaseException:
            _unlock_file(lock_fd)
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            _unlock_file(self._lock_fd)

    def __enter__(self) -> AttemptJournal:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _guard(self) -> None:
        _check(not self._closed and not self._poisoned, "journal closed or persistence uncertain")
        self.policy.check_root()
        _check(self.policy.attempt_path(self.contract) == self.path and self.path.resolve() == self.path,
               "attempt real path changed")
        _check(_identity(self.path) == self._directory_identity, "attempt directory identity changed")

    def _file(self, name: str) -> Path:
        self._guard()
        relative = Path(name)
        _check(not relative.is_absolute() and ".." not in relative.parts, "journal relative path invalid")
        path = self.path / relative
        _check(path.resolve() == path and _within(path, self.path), "journal file/parent redirected")
        return path

    def _write_new(self, name: str, raw: bytes) -> None:
        try:
            path = self._file(name)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            # Leave partial files for audit; no retry ever overwrites them.
            self._poisoned = True
            raise

    def _replace_head(self, value: dict[str, Any]) -> None:
        name = "head-" + uuid.uuid4().hex + ".tmp"
        self._write_new(name, _safe_json(value))
        os.replace(self._file(name), self._file("head.json"))

    def records(self) -> tuple[dict[str, Any], ...]:
        """Verify identity, every line/hash, monotonic sequence and durable head.

        A complete-line tail truncation is detected by the separate head, as is
        an incomplete final line. This is integrity checking, not a signature
        against an attacker able to replace the entire approved workspace.
        """
        self._guard()
        try:
            _check(_digest(self._file("owner.json").read_bytes()) == self._metadata_hash, "owner metadata changed")
            _check(_digest(self._file("contract.json").read_bytes()) == self.contract.sha256, "contract file changed")
            raw = self._file("events.jsonl").read_bytes()
            _check(not raw or raw.endswith(b"\n"), "journal truncated line")
            head = _parse(self._file("head.json").read_bytes())
            previous = _ZERO
            result = []
            for seq, line in enumerate(raw.splitlines(), 1):
                row = _parse(line)
                _check(isinstance(row, dict) and set(row) == {"schema", "sequence", "previous_hash", "event_hash",
                        "run_id", "attempt_id", "ownership", "recorded_at_utc", "event", "payload"}, "journal row schema invalid")
                event_hash = row["event_hash"]
                body = {k: v for k, v in row.items() if k != "event_hash"}
                _check(row["schema"] == JOURNAL_SCHEMA and type(row["sequence"]) is int and row["sequence"] == seq
                       and row["previous_hash"] == previous and canonical_sha256(body) == event_hash, "journal hash/sequence invalid")
                for key in ("run_id", "attempt_id", "ownership"):
                    _check(row[key] == self.metadata[key], "journal owner mismatch")
                previous = event_hash
                result.append(row)
            _check(head == {"sequence": len(result), "event_hash": previous, "metadata_sha256": self._metadata_hash},
                   "journal head mismatch or truncated tail")
            self._replay(result)
            return tuple(result)
        except (OSError, KeyError, TypeError, ContractError) as exc:
            raise BlockedDirty("journal unavailable or invalid; reconciliation blocked") from None

    @staticmethod
    def _artifact_states(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, dict[str, Any]]:
        artifacts: dict[str, dict[str, Any]] = {}
        paths = set()
        failed_seen = False
        rounds = {}
        for row in rows:
            event, payload = row["event"], row["payload"]
            if event == "failed":
                failed_seen = True
            if event == "artifact_intent":
                _check(set(payload) == {"artifact_id", "relative_path", "sha256", "bytes", "media_type"}, "artifact intent schema invalid")
                aid, path = payload["artifact_id"], payload["relative_path"]
                _check(isinstance(aid, str) and bool(aid) and aid not in artifacts and path not in paths,
                       "artifact identity/path reused")
                _check(isinstance(payload["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", payload["sha256"]) is not None
                       and type(payload["bytes"]) is int and payload["bytes"] >= 0, "artifact hash/size invalid")
                artifacts[aid] = {"spec": payload, "complete": False, "abandoned": False}
                paths.add(path)
            elif event == "artifact_complete":
                aid = payload.get("artifact_id")
                _check(aid in artifacts and not artifacts[aid]["complete"] and not artifacts[aid]["abandoned"]
                       and payload == artifacts[aid]["spec"], "artifact completion without matching intent")
                artifacts[aid]["complete"] = True
            elif event == "artifact_abandoned":
                aid = payload.get("artifact_id")
                _check(set(payload) == {"artifact_id", "reason_code"} and isinstance(payload["reason_code"], str)
                       and payload["reason_code"].isidentifier(), "artifact abandon schema invalid")
                _check(aid in artifacts and not artifacts[aid]["complete"]
                       and not artifacts[aid]["abandoned"] and failed_seen,
                       "artifact abandon requires an interrupted artifact of an already failed attempt")
                artifacts[aid]["abandoned"] = True
            elif event in _ARTIFACT_RECOVERY_EVENTS:
                _recovery_transition(artifacts, rounds, row, failed_seen)
        return artifacts

    @staticmethod
    def _replay(rows: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, dict[str, Any]]:
        intents: dict[str, dict[str, Any]] = {}
        phase = -1
        for index, row in enumerate(rows):
            event, payload = row["event"], row["payload"]
            _check(isinstance(payload, dict), "event payload invalid")
            if event == "created":
                _check(index == 0, "created event out of order")
            elif event == "phase":
                phase += 1
                _check(phase < len(_PHASES) and payload == {"phase": _PHASES[phase]}, "phase order invalid")
            elif event == "intent":
                iid = payload.get("intent_id")
                _check(isinstance(iid, str) and iid not in intents, "duplicate/invalid intent")
                intents[iid] = {"intent": payload, "status": "pending", "applied": None}
            elif event in {"applied", "apply_failed", "restored", "recovery_blocked"}:
                iid = payload.get("intent_id")
                _check(iid in intents and intents[iid]["status"] != "restored", "operation event without open intent")
                if event == "applied":
                    _check(intents[iid]["status"] == "pending", "duplicate applied event")
                    intents[iid]["applied"] = payload["observed"]
                intents[iid]["status"] = event
            elif event == "subset_cleanup_complete":
                selected = payload.get("fault_instance_ids")
                _check(isinstance(selected, list) and bool(selected) and len(set(selected)) == len(selected), "subset scope invalid")
                _check(all(item["status"] == "restored" for item in intents.values()
                           if item["intent"]["fault_instance_id"] in selected), "subset cleanup has unresolved selected intents")
                remaining = sorted(iid for iid, item in intents.items() if item["status"] != "restored")
                _check(payload.get("remaining_intent_ids") == remaining, "subset remaining intents mismatch")
            elif event in _OUTPUT_EVENTS:
                pass  # The independent artifact transition chain is checked below.
            elif event in {"cleanup_complete", "cleanup_blocked", "failed"}:
                if event == "cleanup_complete":
                    _check(all(item["status"] == "restored" for item in intents.values()), "cleanup complete with unresolved intents")
            else:
                raise JournalError("unknown journal event")
        _check(not rows or rows[0]["event"] == "created", "journal missing creation")
        AttemptJournal._artifact_states(rows)
        return intents

    def _persist_event(self, row: dict[str, Any]) -> None:
        try:
            with self._file("events.jsonl").open("ab") as stream:
                stream.write(_safe_json(row) + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            self._replace_head({"sequence": row["sequence"], "event_hash": row["event_hash"],
                                "metadata_sha256": self._metadata_hash})
        except BaseException:
            self._poisoned = True
            raise

    def _durable_anchors_match(self, cached: _VerifiedChain) -> bool:
        """O(1) per-append re-check of every small durable anchor file.

        owner.json/contract.json digests and the durable head must still equal
        the cached chain, so external tampering of these files is detected on
        the very next append exactly as in the whole-replay implementation.
        Only the events.jsonl body below the cached head is not re-read here.
        Called right after ``_guard``, so fixed-name reads skip the repeated
        full guard and only keep the per-file redirect check.
        """
        try:
            for name in ("owner.json", "contract.json", "head.json"):
                path = self.path / name
                if path.resolve() != path:  # same redirect check as _file, minus the repeated full guard
                    return False
            if _digest((self.path / "owner.json").read_bytes()) != self._metadata_hash:
                return False
            if _digest((self.path / "contract.json").read_bytes()) != self.contract.sha256:
                return False
            head = _parse((self.path / "head.json").read_bytes())
        except (OSError, JournalError):
            return False
        return head == {"sequence": cached.count, "event_hash": cached.last_event_hash,
                        "metadata_sha256": self._metadata_hash}

    def _append(self, event: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._mutex:
            cached = self._verified
            if cached is not None:
                self._guard()
                if cached.head_consistent(self.metadata) and self._durable_anchors_match(cached):
                    body = {"schema": JOURNAL_SCHEMA, "sequence": cached.count + 1,
                            "previous_hash": cached.last_event_hash,
                            "run_id": self.metadata["run_id"], "attempt_id": self.metadata["attempt_id"],
                            "ownership": self.metadata["ownership"], "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                            "event": event, "payload": payload}
                    row = dict(body, event_hash=_digest(_safe_json(body)))
                    _check(row["sequence"] == cached.count + 1 and row["previous_hash"] == cached.last_event_hash
                           and canonical_sha256(body) == row["event_hash"], "incremental chain continuation invalid")
                    try:
                        cached.cursor.apply(row)
                    except BaseException as exc:
                        # Invalidate the whole cache (its cursor may be partially
                        # mutated) and fall back to a full replay verification;
                        # only non-JournalError escapes propagate unchanged.
                        self._verified = None
                        if not isinstance(exc, JournalError):
                            raise
                    else:
                        try:
                            self._persist_event(row)
                        except BaseException:
                            self._verified = None
                            raise
                        cached.commit(row)
                        return row
            # Full-replay path: first append in this process, an invalidated
            # cache, or any incremental inconsistency. Identical verification
            # and bytes as the pre-incremental implementation; on success the
            # verified chain (also cross-checked against _replay by the cursor
            # pass) seeds the incremental cache.
            rows = self.records()
            body = {"schema": JOURNAL_SCHEMA, "sequence": len(rows) + 1,
                    "previous_hash": rows[-1]["event_hash"] if rows else _ZERO,
                    "run_id": self.metadata["run_id"], "attempt_id": self.metadata["attempt_id"],
                    "ownership": self.metadata["ownership"], "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                    "event": event, "payload": payload}
            row = dict(body, event_hash=_digest(_safe_json(body)))
            self._replay([*rows, row])
            verified = _VerifiedChain([*rows, row])
            try:
                self._persist_event(row)
            except BaseException:
                self._verified = None
                raise
            self._verified = verified
            return row

    def enter_phase(self, phase: str) -> None:
        rows = self.records()
        _check(all(item["complete"] or item["abandoned"] for item in self._artifact_states(rows).values()),
               "incomplete artifact blocks phase advance")
        intents = self._replay(rows)
        _check(not any(v["status"] in {"pending", "apply_failed", "recovery_blocked"} for v in intents.values())
               and not any(r["event"] in _FAILURE_EVENTS for r in rows)
               and rows[-1]["event"] != "cleanup_blocked", "phase advance blocked by failed/uncertain attempt")
        self._append("phase", {"phase": phase})

    def mark_failed(self, reason_code: str) -> None:
        _check(isinstance(reason_code, str) and reason_code.isidentifier(), "reason must be a non-sensitive code")
        self._append("failed", {"reason_code": reason_code})
        self._reconciled_in_session = False

    @staticmethod
    def _adapter_kind(adapter: ResourceAdapter) -> str:
        kind = getattr(adapter, "evidence_kind", None)
        _check(kind in {"fake", "live"}, "adapter evidence kind required")
        return kind

    def _observe(self, adapter: ResourceAdapter, target: Target, fields: tuple[str, ...]) -> ResourceState:
        state = adapter.observe(target, fields)
        _check(isinstance(state, ResourceState), "adapter must return ResourceState")
        state = ResourceState.from_dict(state.to_dict())
        _check(not state.exists or set(state.fields) == set(fields), "readback controlled fields incomplete")
        return state

    def execute_change(self, *, intent_id: str, fault_instance_id: str, target: Target,
                       desired_fields: dict[str, Any], adapter: ResourceAdapter,
                       checkpoint: Callable[[str], None] | None = None) -> None:
        """Persist controlled snapshot + intent + head before any apply callback."""
        with self._mutex:
            self._adapter_kind(adapter)
            self._reconciled_in_session = False
            rows = self.records()
            _check(all(item["complete"] or item["abandoned"] for item in self._artifact_states(rows).values()),
                   "incomplete artifact blocks new mutations")
            state = self._replay(rows)
            _check(not any(row["event"] in _FAILURE_EVENTS for row in rows)
                   and rows[-1]["event"] != "cleanup_blocked", "failed/blocked attempt cannot apply new changes")
            _check(not any(v["status"] in {"pending", "apply_failed", "recovery_blocked"} for v in state.values()),
                   "unresolved operation blocks further applies")
            _check(isinstance(intent_id, str) and intent_id.isidentifier() and intent_id not in state, "intent ID invalid/reused")
            _check(fault_instance_id in {f.fault_instance_id for f in self.contract.faults}, "fault instance not in contract")
            target.to_dict()
            desired = _parse(_safe_json(desired_fields))
            _check(isinstance(desired, dict) and bool(desired), "controlled fields required")
            fields = tuple(sorted(desired))
            before = self._observe(adapter, target, fields)
            _check((target.uid is None and not before.exists) or (before.exists and before.uid == target.uid),
                   "target baseline UID/existence mismatch")
            _check(before.ownership in {None, self.metadata["ownership"]}, "resource owned by another attempt")
            if checkpoint:
                checkpoint("before_intent")
            snapshot_name = "snapshot-" + intent_id + ".json"
            snapshot = _safe_json(before.to_dict())
            self._write_new(snapshot_name, snapshot)
            intent = {"intent_id": intent_id, "fault_instance_id": fault_instance_id, "target": target.to_dict(),
                      "controlled_fields": desired, "snapshot": {"path": snapshot_name, "sha256": _digest(snapshot)},
                      "restore_action": "restore_fields" if before.exists else "delete_owned_created",
                      "ownership": self.metadata["ownership"], "evidence_kind": self._adapter_kind(adapter)}
            self._append("intent", intent)
            if checkpoint:
                checkpoint("after_intent")
            try:
                adapter.compare_and_apply(target, ResourceState.from_dict(before.to_dict()),
                                          _parse(_safe_json(desired)), self.metadata["ownership"])
                if checkpoint:
                    checkpoint("after_apply")
                observed = self._observe(adapter, target, fields)
                _check(observed.exists and observed.ownership == self.metadata["ownership"]
                       and observed.fields == desired and (target.uid is None or observed.uid == target.uid),
                       "apply readback does not match intended owned state")
                if checkpoint:
                    checkpoint("before_complete")
                self._append("applied", {"intent_id": intent_id, "observed": observed.to_dict(),
                                         "evidence_kind": self._adapter_kind(adapter)})
            except Exception as exc:
                self._append("apply_failed", {"intent_id": intent_id, "error_type": type(exc).__name__})
                raise OperationFailed("apply uncertain; durable intent requires reconciliation") from None

    def reconcile(self, adapter: ResourceAdapter | None,
                  checkpoint: Callable[[str], None] | None = None, *,
                  fault_instance_ids: tuple[str, ...] | None = None) -> RecoveryReport:
        """Reverse owned changes; an explicit subset never certifies full cleanup."""
        with self._mutex:
            self._reconciled_in_session = False
            rows = self.records()
            intents = self._replay(rows)
            selected = None
            if fault_instance_ids is not None:
                known = {fault.fault_instance_id for fault in self.contract.faults}
                _check(type(fault_instance_ids) is tuple and bool(fault_instance_ids)
                       and all(isinstance(fid, str) for fid in fault_instance_ids)
                       and len(set(fault_instance_ids)) == len(fault_instance_ids)
                       and set(fault_instance_ids) <= known, "invalid selected fault-instance scope")
                selected = set(fault_instance_ids)
                keys = lambda item: tuple(item["intent"]["target"][k] for k in ("kind", "scope", "name"))
                selected_keys = {keys(item) for item in intents.values() if item["intent"]["fault_instance_id"] in selected}
                conflicts = sorted(iid for iid, item in intents.items() if item["intent"]["fault_instance_id"] not in selected
                                   and item["status"] != "restored" and keys(item) in selected_keys)
                if conflicts:
                    self._append("cleanup_blocked", {"reason_code": "subset_resource_dependency", "intent_ids": conflicts,
                                                     "fault_instance_ids": sorted(selected)})
                    return RecoveryReport("BLOCKED_DIRTY", getattr(adapter, "evidence_kind", "none"), (), tuple(conflicts))
                intents = {iid: item for iid, item in intents.items() if item["intent"]["fault_instance_id"] in selected}
            if adapter is None:
                self._append("cleanup_blocked", {"reason_code": "adapter_missing"})
                return RecoveryReport("BLOCKED_DIRTY", "none", (), ("adapter_missing",))
            kind = self._adapter_kind(adapter)
            restored = []
            blockers = []
            for iid, item in reversed(tuple(intents.items())):
                if item["status"] == "restored":
                    continue
                intent = item["intent"]
                try:
                    _check(intent["ownership"] == self.metadata["ownership"] and intent["evidence_kind"] == kind,
                           "intent ownership/evidence kind mismatch")
                    raw = self._file(intent["snapshot"]["path"]).read_bytes()
                    _check(_digest(raw) == intent["snapshot"]["sha256"], "snapshot hash mismatch")
                    original = ResourceState.from_dict(_parse(raw))
                    target = Target(**intent["target"])
                    desired = intent["controlled_fields"]
                    fields = tuple(sorted(desired))
                    current = self._observe(adapter, target, fields)
                    if current.to_dict() != original.to_dict():
                        _check(current.exists and current.ownership == self.metadata["ownership"]
                               and current.fields == desired, "foreign or changed controlled fields")
                        expected_uid = target.uid or (item["applied"] or {}).get("uid")
                        _check(expected_uid is None or current.uid == expected_uid, "resource UID changed")
                        # For a create interrupted before readback, a concrete
                        # current UID plus the exact owned name/scope/fields is
                        # the only supported recovery identity; no blind delete.
                        if checkpoint:
                            checkpoint("during_cleanup")
                        adapter.compare_and_restore(target, current, original)
                        if checkpoint:
                            checkpoint("after_restore")
                        verified = self._observe(adapter, target, fields)
                        _check(verified.to_dict() == original.to_dict(), "restore readback mismatch")
                    self._append("restored", {"intent_id": iid, "evidence_kind": kind,
                                              "original_state_sha256": canonical_sha256(original.to_dict())})
                    restored.append(iid)
                except Exception as exc:
                    blockers.append(iid)
                    self._append("recovery_blocked", {"intent_id": iid, "error_type": type(exc).__name__})
                    # Earlier operations may depend on this later change; stop
                    # reverse cleanup rather than clobber their shared fields.
                    break
            if blockers:
                self._append("cleanup_blocked", {"reason_code": "unresolved_intent", "intent_ids": blockers})
                return RecoveryReport("BLOCKED_DIRTY", kind, tuple(restored), tuple(blockers))
            # Re-check one final baseline per target, including already-restored
            # intents after resume. Intermediate snapshots of a multi-step
            # change need not equal the final earliest baseline.
            baselines: dict[tuple[str, str, str], dict[str, Any]] = {}
            for iid, item in intents.items():
                intent = item["intent"]
                target = Target(**intent["target"])
                key = (target.kind, target.scope, target.name)
                try:
                    raw = self._file(intent["snapshot"]["path"]).read_bytes()
                    _check(_digest(raw) == intent["snapshot"]["sha256"], "snapshot hash mismatch")
                    original = ResourceState.from_dict(_parse(raw))
                    if key not in baselines:
                        baselines[key] = {"iid": iid, "target": target, "original": original.to_dict(), "fields": set()}
                    baseline = baselines[key]
                    baseline["fields"].update(intent["controlled_fields"])
                    if baseline["original"]["exists"]:
                        for name, value in original.fields.items():
                            baseline["original"]["fields"].setdefault(name, value)
                except Exception:
                    self._append("cleanup_blocked", {"reason_code": "baseline_readback_failed", "intent_ids": [iid]})
                    return RecoveryReport("BLOCKED_DIRTY", kind, tuple(restored), (iid,))
            for baseline in baselines.values():
                try:
                    current = self._observe(adapter, baseline["target"], tuple(sorted(baseline["fields"])))
                    _check(current.to_dict() == baseline["original"], "final original fields not all verified")
                except Exception:
                    iid = baseline["iid"]
                    self._append("cleanup_blocked", {"reason_code": "baseline_readback_failed", "intent_ids": [iid]})
                    return RecoveryReport("BLOCKED_DIRTY", kind, tuple(restored), (iid,))
            if selected is not None:
                remaining = sorted(iid for iid, item in self._replay(self.records()).items() if item["status"] != "restored")
                self._append("subset_cleanup_complete", {"evidence_kind": kind, "fault_instance_ids": sorted(selected),
                                                          "remaining_intent_ids": remaining})
                return RecoveryReport("SUBSET_CLEAN_LIVE" if kind == "live" else "SUBSET_CLEAN_OFFLINE", kind, tuple(restored), ())
            self._append("cleanup_complete", {"evidence_kind": kind})
            self._reconciled_in_session = True
            return RecoveryReport("CLEAN_LIVE" if kind == "live" else "CLEAN_OFFLINE", kind, tuple(restored), ())

    def assert_queue_safe(self, *, require_live: bool = True) -> None:
        rows = self.records()
        _check(self._reconciled_in_session, "queue blocked: fresh reconciliation required in this session")
        intents = self._replay(rows)
        _check(all(item["complete"] or item["abandoned"] for item in self._artifact_states(rows).values()),
               "queue blocked: incomplete artifact")
        operational = [row for row in rows if row["event"] not in _OUTPUT_EVENTS]
        _check(bool(operational) and operational[-1]["event"] == "cleanup_complete"
               and all(v["status"] == "restored" for v in intents.values()), "queue blocked: recovery not complete")
        if require_live:
            _check(operational[-1]["payload"]["evidence_kind"] == "live", "queue blocked: only offline restoration evidence")

    def pending_artifact_ids(self) -> tuple[str, ...]:
        """Artifact IDs whose write is unresolved (neither completed nor abandoned)."""
        return tuple(sorted(aid for aid, item in self._artifact_states(self.records()).items()
                            if not item["complete"] and not item["abandoned"]))

    def begin_recovery_round(self, *, source_fingerprints: dict[str, str], rules_sha256: str) -> dict[str, Any]:
        """Reserve one prepared recovery round even while output is pending.

        This is a control event, not an artifact; callers persist its exact hash
        in the physical lease before mutation or disposition of pending bytes.
        """
        with self._mutex:
            self._guard()
            rows = self.records()
            _check(any(row["event"] == "failed" for row in rows), "recovery round requires failed attempt")
            prior = [row for row in rows if row["event"] == "recovery_round_begin"]
            payload = {"round_id": "round-%06d" % (len(prior) + 1), "previous_round_hash": prior[-1]["event_hash"] if prior else None,
                       "contract_sha256": self.contract.sha256, "source_fingerprints": source_fingerprints,
                       "rules_sha256": rules_sha256, "scope": "prepared_auxiliary", "pending_artifact_ids": list(self.pending_artifact_ids())}
            self._append("recovery_round_begin", payload)
            return self.records()[-1]

    def audit_incomplete_artifact(self, artifact_id: str, *, round_id: str, round_begin_hash: str,
                                  reason_code: str, max_bytes: int) -> dict[str, Any]:
        """Confirm exact bytes now or quarantine partial bytes in their place.

        Never overwrites/truncates/moves a file. Absent bytes still use the old
        abandonment API. A valid owner/contract/head/event chain is mandatory.
        """
        with self._mutex:
            self._guard()
            states = self._artifact_states(self.records())
            _check(artifact_id in states and not states[artifact_id]["complete"] and not states[artifact_id]["abandoned"], "audit requires pending artifact")
            _check(type(max_bytes) is int and max_bytes > 0, "artifact audit bound required")
            spec = states[artifact_id]["spec"]
            path = self._file(spec["relative_path"])
            _check(path.is_file() and path.stat().st_size <= max_bytes, "artifact audit requires bounded existing bytes")
            identity = _identity(path)
            with path.open("rb") as stream:
                raw = stream.read(max_bytes + 1)
            _check(len(raw) <= max_bytes and _identity(path) == identity and path.resolve() == path, "artifact changed during audit")
            observed = {"relative_path": spec["relative_path"], "sha256": _digest(raw), "bytes": len(raw)}
            exact = observed["sha256"] == spec["sha256"] and observed["bytes"] == spec["bytes"]
            event = "artifact_recovered_complete" if exact else "artifact_quarantined"
            self._append(event, {"artifact_id": artifact_id, "round_id": round_id, "round_begin_hash": round_begin_hash,
                                 "reason_code": reason_code, "expected": spec, "observed": observed})
            return self.records()[-1]

    def verify_recovery_residue(self) -> None:
        """Re-read each audited residue before terminal qualification."""
        with self._mutex:
            for row in self.records():
                if row["event"] not in {"artifact_recovered_complete", "artifact_quarantined"}:
                    continue
                observed = row["payload"]["observed"]
                path = self._file(observed["relative_path"])
                _check(path.stat().st_size == observed["bytes"], "audited artifact residue size changed")
                identity = _identity(path)
                with path.open("rb") as stream:
                    raw = stream.read(observed["bytes"] + 1)
                _check(_identity(path) == identity and len(raw) == observed["bytes"] and _digest(raw) == observed["sha256"], "audited artifact residue changed")

    def abandon_incomplete_artifact(self, artifact_id: str, *, reason_code: str) -> dict[str, Any]:
        """Terminal disposition for exactly one interrupted artifact of a failed attempt.

        Append-only: the artifact's intent row and its reserved identity/path
        stay untouched; nothing is deleted, completed or retried. Refuses
        unknown artifacts, completed artifacts, repeated abandonment, any
        attempt not already carrying a failure event, and any artifact whose
        bytes are provably present on disk (residue of a torn write or a
        readback-mismatched file must stay un dispositioned for explicit
        audit), so the abandoned state can only ever describe an interrupted
        write that provably never landed and that this failed attempt will
        never finish.
        """
        with self._mutex:
            self._guard()
            _check(isinstance(artifact_id, str) and bool(artifact_id), "artifact ID required")
            _check(isinstance(reason_code, str) and reason_code.isidentifier(), "reason must be a non-sensitive code")
            rows = self.records()
            states = self._artifact_states(rows)
            _check(any(row["event"] == "failed" for row in rows), "abandon requires an already failed attempt")
            _check(artifact_id in states, "unknown artifact")
            _check(not states[artifact_id]["complete"], "completed artifact cannot be abandoned")
            _check(not states[artifact_id]["abandoned"], "artifact already abandoned")
            target = self._file(states[artifact_id]["spec"]["relative_path"])
            _check(not target.exists() and not target.is_symlink(),
                   "artifact bytes present on disk; abandonment requires provably absent bytes")
            self._append("artifact_abandoned", {"artifact_id": artifact_id, "reason_code": reason_code})
            return {"artifact_id": artifact_id, "reason_code": reason_code, "status": "abandoned",
                    "run_id": self.metadata["run_id"], "attempt_id": self.metadata["attempt_id"],
                    "contract_sha256": self.contract.sha256}

    def write_artifact(self, *, artifact_id: str, relative_path: str, raw: bytes, media_type: str) -> dict[str, Any]:
        """Durable exclusive output with an intent/completion pair and byte hash.

        Unfinished writes survive process failure in the main journal. Resume
        marks the attempt failed before resource reconciliation; no partial
        artifact is retried or overwritten in the old attempt. A failed
        attempt's interrupted artifact may receive one explicit terminal
        ``artifact_abandoned`` disposition (see ``abandon_incomplete_artifact``);
        only then may further artifacts be written, e.g. the recovery
        qualification evidence of that same attempt.
        """
        with self._mutex:
            self._guard()
            _check(isinstance(artifact_id, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", artifact_id) is not None,
                   "artifact ID invalid")
            _check(type(raw) is bytes and isinstance(media_type, str) and bool(media_type.strip()), "artifact bytes/media type required")
            relative = Path(relative_path)
            _check(not relative.is_absolute() and not relative.drive and len(relative.parts) > 1
                   and relative.parts[0] == "artifacts", "artifacts must use their exclusive subtree")
            reserved = {"CON", "PRN", "AUX", "NUL", *("COM" + str(i) for i in range(1, 10)), *("LPT" + str(i) for i in range(1, 10))}
            for part in relative.parts:
                _check(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", part) is not None and not part.endswith(".")
                       and part.upper().split(".")[0] not in reserved, "artifact path component invalid")
            normalized = relative.as_posix()
            records = self.records()
            current = self._artifact_states(records)
            if any(not item["complete"] and not item["abandoned"] for item in current.values()):
                raise IncompleteArtifact("incomplete artifact blocks additional output")
            _check(artifact_id not in current and all(item["spec"]["relative_path"] != normalized for item in current.values()),
                   "artifact ID/path already used")
            parent = self.path
            for part in relative.parts[:-1]:
                parent = parent / part
                _check(parent.resolve() == parent and _within(parent, self.path), "artifact parent redirected")
                parent.mkdir(exist_ok=True)
                _check(parent.resolve() == parent, "artifact parent redirected after creation")
            payload = {"artifact_id": artifact_id, "relative_path": normalized, "sha256": _digest(raw),
                       "bytes": len(raw), "media_type": media_type}
            self._append("artifact_intent", payload)
            self._write_new(normalized, raw)
            try:
                actual = self._file(normalized).read_bytes()
            except BaseException:
                self._poisoned = True
                raise
            if len(actual) != len(raw) or _digest(actual) != payload["sha256"]:
                self._poisoned = True
                raise BlockedDirty("written artifact readback mismatch")
            self._append("artifact_complete", payload)
            return {**payload, "status": "written", "run_id": self.metadata["run_id"],
                    "attempt_id": self.metadata["attempt_id"], "contract_sha256": self.contract.sha256}

    @property
    def state(self) -> str:
        rows = self.records()
        intents = self._replay(rows)
        if any(not item["complete"] and not item["abandoned"] for item in self._artifact_states(rows).values()):
            return "CLEANUP_REQUIRED"
        rows = tuple(row for row in rows if row["event"] not in _OUTPUT_EVENTS)
        if rows[-1]["event"] == "cleanup_blocked" or any(v["status"] == "recovery_blocked" for v in intents.values()):
            return "BLOCKED_DIRTY"
        if rows[-1]["event"] == "cleanup_complete":
            return "FAILED_CLEAN" if any(r["event"] in _FAILURE_EVENTS for r in rows) else "CLEAN"
        if any(v["status"] in {"pending", "apply_failed"} for v in intents.values()) or any(r["event"] in _FAILURE_EVENTS for r in rows):
            return "CLEANUP_REQUIRED"
        phases = [row["payload"]["phase"] for row in rows if row["event"] == "phase"]
        return phases[-1] if phases else "PLANNED"
