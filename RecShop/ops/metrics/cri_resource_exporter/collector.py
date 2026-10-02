"""Target resolution, per-tick collection and the bounded fixed tick loop.

Honesty rules encoded here (per RESOURCE-CADENCE-DECISION-20260917.json):
- exactly ONE ContainerStats RPC attempt per target per tick; failures are
  recorded, never retried within the tick;
- skipped tick slots stay skipped (no catch-up burst after an overrun);
- a container identity change (new container id) starts a new sample history;
  the previous container's last sample is never carried over;
- a stats payload whose ``attributes.id`` does not match the requested id is
  rejected (identity mismatch) rather than attributed to the target;
- nothing computes rates; nothing interpolates; nothing zero-fills.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from . import cri_client
from . import __version__

RUNNING_STATE = "CONTAINER_RUNNING"


@dataclass(frozen=True)
class IdTarget:
    """Explicit container id; never re-resolved by name."""
    container_id: str

    @property
    def key(self) -> str:
        return "id:" + self.container_id[:12]


@dataclass(frozen=True)
class NameTarget:
    """namespace/pod-name/container-name target, resolved via ListContainers."""
    namespace: str
    pod_name: str
    container_name: str

    @property
    def key(self) -> str:
        return "pod:{}/{}/{}".format(self.namespace, self.pod_name, self.container_name)


@dataclass
class TargetState:
    target: object
    resolved_id: Optional[str] = None
    # last id this target ever resolved to; survives no_match gaps so that a
    # replacement appearing after a gap still counts as an identity change (F3)
    ever_resolved_id: Optional[str] = None
    last_sample: Optional[dict] = None
    last_collected_ns: Optional[int] = None
    identity_changes: int = 0
    last_error: Optional[str] = None          # most recent stats-path error
    last_stats_status: Optional[str] = None
    last_resolve_status: Optional[str] = None  # most recent resolution outcome


@dataclass
class TickReport:
    tick_index: int
    duration_s: float
    resolve_notes: List[tuple] = field(default_factory=list)
    stats_outcomes: List[dict] = field(default_factory=list)


class ResourceCollector:
    def __init__(self, targets: Sequence, *, node: str = "", node_uid: str = "",
                 interval_s: float = 2.0, rpc_timeout_s: float = 1.0,
                 max_sample_age_s: float = 6.0, resolve_every_ticks: int = 1,
                 concurrency: int = 4, now_fn: Callable[[], int] = time.time_ns):
        self.targets = list(targets)
        if not self.targets:
            raise ValueError("at least one target is required")
        keys = [target.key for target in self.targets]
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        if duplicates:
            # OBS-1 guard: two states sharing one exposition key cannot be
            # represented honestly - the exposition would emit duplicate
            # series per key while the collection path dedups by resolved id,
            # leaving the losing state to inflate cri_exporter_stale_targets
            # even though collection succeeds. Refuse; never guess which
            # state keeps the data (same policy as ambiguous resolution).
            raise ValueError(
                "duplicate target key(s) %s: each configured target must have "
                "a unique key (id:<12-hex prefix> or pod:<ns>/<pod>/<container>)"
                % duplicates)
        self.node = node
        self.node_uid = node_uid
        self.interval_s = float(interval_s)
        self.rpc_timeout_s = float(rpc_timeout_s)
        self.max_sample_age_ns = int(float(max_sample_age_s) * 1_000_000_000)
        self.resolve_every_ticks = max(1, int(resolve_every_ticks))
        self._now = now_fn
        self._lock = threading.Lock()
        self._states = []
        for target in self.targets:
            state = TargetState(target)
            if isinstance(target, IdTarget):
                state.resolved_id = target.container_id  # explicit ids resolve instantly, never re-resolved
            self._states.append(state)
        self._pool: Optional[ThreadPoolExecutor] = None
        self._concurrency = max(1, int(concurrency))
        self._tick_index = 0
        self.counters: Dict[str, object] = {
            "ticks_total": 0,
            "missed_ticks_total": 0,
            "tick_overruns_total": 0,
            "last_tick_duration_s": 0.0,
            "cpu_cumulative_decreases_total": 0,
            "rpc_total": {},           # "kind|status" -> count
            "rpc_last_duration_s": {},  # kind -> seconds
            "resolve_total": {},        # status -> count (name targets)
        }

    # ------------------------------------------------------------------ util

    @property
    def interval_ns(self) -> int:
        return int(self.interval_s * 1_000_000_000)

    def _bump(self, key, amount=1):
        with self._lock:
            self.counters[key] += amount

    def _bump_rpc(self, kind, status, duration_s):
        with self._lock:
            key = "{}|{}".format(kind, status)
            self.counters["rpc_total"][key] = self.counters["rpc_total"].get(key, 0) + 1
            self.counters["rpc_last_duration_s"][kind] = duration_s

    def record_missed_ticks(self, count: int):
        with self._lock:
            self.counters["missed_ticks_total"] += count

    def close(self):
        with self._lock:
            pool = self._pool
            self._pool = None
        if pool is not None:
            pool.shutdown(wait=False)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "version": __version__,
                "node": self.node,
                "node_uid": self.node_uid,
                "interval_s": self.interval_s,
                "rpc_timeout_s": self.rpc_timeout_s,
                "max_sample_age_ns": self.max_sample_age_ns,
                "tick_index": self._tick_index,
                "counters": {
                    "ticks_total": self.counters["ticks_total"],
                    "missed_ticks_total": self.counters["missed_ticks_total"],
                    "tick_overruns_total": self.counters["tick_overruns_total"],
                    "last_tick_duration_s": self.counters["last_tick_duration_s"],
                    "cpu_cumulative_decreases_total": self.counters["cpu_cumulative_decreases_total"],
                    "rpc_total": dict(self.counters["rpc_total"]),
                    "rpc_last_duration_s": dict(self.counters["rpc_last_duration_s"]),
                    "resolve_total": dict(self.counters["resolve_total"]),
                },
                "states": [
                    {
                        "target_kind": "id" if isinstance(s.target, IdTarget) else "names",
                        "target_key": s.target.key,
                        "resolved_id": s.resolved_id,
                        "last_sample": dict(s.last_sample) if s.last_sample else None,
                        "last_collected_ns": s.last_collected_ns,
                        "identity_changes": s.identity_changes,
                        "last_error": s.last_error,
                        "last_stats_status": s.last_stats_status,
                        "last_resolve_status": s.last_resolve_status,
                    }
                    for s in self._states
                ],
            }

    # -------------------------------------------------------------- resolving

    def _resolve(self, client, name_states: List[TargetState]) -> List[tuple]:
        notes = []
        started = self._now()
        listing = None
        status = "ok"
        try:
            listing = client.list_containers(None, timeout_s=self.rpc_timeout_s)
        except Exception as exc:  # bounded: classified, never retried here
            status = cri_client.classify_rpc_error(exc)
        self._bump_rpc("list_containers", status, (self._now() - started) / 1e9)
        if listing is None:
            for st in name_states:
                with self._lock:
                    st.last_resolve_status = "rpc_" + status
                    self.counters["resolve_total"]["rpc_" + status] = \
                        self.counters["resolve_total"].get("rpc_" + status, 0) + 1
            notes.append(("resolve", status))
            return notes

        containers = list(cri_client.iter_containers(listing))
        for st in name_states:
            target = st.target
            matches = [
                c for c in containers
                if c.get("container_name") == target.container_name
                and c.get("pod_name") == target.pod_name
                and (not target.namespace or c.get("pod_namespace") == target.namespace)
                and c.get("state") == RUNNING_STATE
            ]
            with self._lock:
                if not matches:
                    st.last_resolve_status = "no_match"
                    st.resolved_id = None  # container gone: stop sampling it; old sample ages out
                    notes.append((target.key, "resolve_no_match"))
                elif len(matches) > 1:
                    st.last_resolve_status = "ambiguous"  # never pick silently
                    notes.append((target.key, "resolve_ambiguous"))
                else:
                    st.last_resolve_status = "ok"
                    new_id = matches[0].get("id")
                    if new_id and new_id != st.resolved_id:
                        # Identity change = resolved id differs from the last
                        # id this target EVER resolved to. Covers both direct
                        # replacement and replacement after a no_match gap;
                        # the same id returning after a gap is not a change.
                        if st.ever_resolved_id is not None and new_id != st.ever_resolved_id:
                            st.identity_changes += 1
                        st.resolved_id = new_id
                        st.ever_resolved_id = new_id
                        st.last_sample = None  # identity boundary: no carryover
                        notes.append((target.key, "resolved:" + new_id[:12]))
                self.counters["resolve_total"][st.last_resolve_status] = \
                    self.counters["resolve_total"].get(st.last_resolve_status, 0) + 1
        return notes

    # ------------------------------------------------------------- collection

    def _collect_one(self, client, container_id: str) -> dict:
        """One stats attempt for one container. Never raises (review F2):
        any failure becomes a classified outcome string that tick() counts."""
        started = self._now()
        outcome = {
            "container_id": container_id,
            "status": "error",
            "duration_s": 0.0,
            "sample": None,
        }
        try:
            try:
                raw = client.container_stats(container_id, timeout_s=self.rpc_timeout_s)
            except Exception as exc:  # single attempt; classified below
                outcome["status"] = cri_client.classify_rpc_error(exc)
                return outcome
            try:
                sample = cri_client.normalize_stats(raw)
            except Exception:
                outcome["status"] = "unparseable_stats"
                return outcome
            if sample is None:
                outcome["status"] = "unparseable_stats"
            elif sample.get("container_id") != container_id:
                outcome["status"] = "identity_mismatch"
            else:
                outcome["status"] = "ok"
                outcome["sample"] = sample
            return outcome
        finally:
            outcome["duration_s"] = (self._now() - started) / 1e9

    def tick(self, client) -> TickReport:
        started = self._now()
        with self._lock:
            self._tick_index += 1
            tick_index = self._tick_index
            self.counters["ticks_total"] += 1

        name_states = [s for s in self._states if isinstance(s.target, NameTarget)]
        resolve_notes: List[tuple] = []
        if name_states and ((tick_index - 1) % self.resolve_every_ticks) == 0:
            resolve_notes = self._resolve(client, name_states)

        with self._lock:
            pool = self._pool
            if pool is None:
                pool = ThreadPoolExecutor(
                    max_workers=self._concurrency, thread_name_prefix="cri-stats")
                self._pool = pool
            to_collect = {s.resolved_id: s for s in self._states if s.resolved_id}

        if to_collect:
            futures = [(cid, pool.submit(self._collect_one, client, cid))
                       for cid in to_collect]
            outcomes = [future.result() for _, future in futures]
        else:
            outcomes = []

        collected_ns = self._now()
        with self._lock:
            for outcome in outcomes:
                state = to_collect.get(outcome["container_id"])
                if state is None:
                    continue
                key = "container_stats|{}".format(outcome["status"])
                self.counters["rpc_total"][key] = self.counters["rpc_total"].get(key, 0) + 1
                self.counters["rpc_last_duration_s"]["container_stats"] = outcome["duration_s"]
                state.last_stats_status = outcome["status"]
                if outcome["status"] == "ok":
                    sample = outcome["sample"]
                    previous = state.last_sample
                    if (previous is not None
                            and previous.get("usage_core_nano_seconds") is not None
                            and sample.get("usage_core_nano_seconds") is not None
                            and sample["usage_core_nano_seconds"]
                            < previous["usage_core_nano_seconds"]):
                        # Recorded but not corrected: cumulative CPU decreased
                        # within one container id (unexpected; live signal).
                        self.counters["cpu_cumulative_decreases_total"] += 1
                    state.last_sample = sample
                    state.last_collected_ns = collected_ns
                    state.last_error = None
                else:
                    state.last_error = outcome["status"]  # keep last sample; it ages out

        duration_s = (self._now() - started) / 1e9
        with self._lock:
            self.counters["last_tick_duration_s"] = duration_s
            if duration_s * 1e9 > self.interval_ns:
                self.counters["tick_overruns_total"] += 1
        return TickReport(tick_index=tick_index, duration_s=duration_s,
                          resolve_notes=resolve_notes, stats_outcomes=outcomes)


# ------------------------------------------------------------------ scheduler


def next_deadline_ns(start_ns: int, interval_ns: int, now_ns: int) -> int:
    """First slot boundary strictly after now_ns."""
    if now_ns < start_ns:
        return start_ns
    elapsed = now_ns - start_ns
    slots = elapsed // interval_ns + 1
    return start_ns + slots * interval_ns


def run_collection_loop(collector: ResourceCollector, client, *,
                        interval_s: float,
                        now_fn: Callable[[], int] = time.time_ns,
                        sleep_fn: Callable[[float], None] = time.sleep,
                        stop_fn: Optional[Callable[[], bool]] = None,
                        max_ticks: Optional[int] = None) -> int:
    """Bounded fixed-interval loop. Late or overlong ticks skip the slots they
    missed; skipped slots are counted, never re-run (no catch-up burst)."""
    interval_ns = int(float(interval_s) * 1_000_000_000)
    stop = stop_fn or (lambda: False)
    start_ns = now_fn()
    slot = 1
    executed = 0
    while not stop():
        if max_ticks is not None and executed >= max_ticks:
            break
        deadline = start_ns + slot * interval_ns
        now = now_fn()
        if now < deadline:
            sleep_fn((deadline - now) / 1e9)
            continue
        collector.tick(client)
        executed += 1
        after = now_fn()
        skipped = 0
        while start_ns + (slot + 1) * interval_ns <= after:
            slot += 1
            skipped += 1
        if skipped:
            collector.record_missed_ticks(skipped)
        slot += 1
    return executed
