"""Collection primitives db implementation. Runtime identity and source checks remain explicit."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import re
from typing import Any, Protocol
import uuid

from .contract import FaultInstanceSpec, canonical_json, canonical_sha256
from .journal import AttemptJournal, ResourceState, Target
from .primitives import (MECHANISM_VERSION, PrimitiveError, UnsupportedPrimitive, _check, _copy, _fields,
                         _positive)

DB_MECHANISM = "mysql_lock_table"
DB_TARGET_KIND = "MySqlTableLock"
# Per-mechanism sub-identity for the DbCommandClient protocol surface
# (connect/lock/unlock/confirm-release/checksum and its evidence record
# shapes). The global primitives MECHANISM_VERSION stays untouched: it marks
# the primitive protocol/adapter shape, and adding fault types under the same
# version keeps every existing embedded contract rebuildable (recovery
# qualification and release gates depend on that).
DB_CLIENT_PROTOCOL = "m1-db-lock-client-v1"
# Entity -> locked table whitelist. The collection catalog atom is mysql_items_lock;
# every other entity must be rejected instead of guessed by table name.
DB_LOCK_TABLE_BY_ENTITY = {"mysql_items_lock": "items"}
DB_LOCK_MODES = frozenset({"WRITE"})
CHECKSUM_TABLES = ("items", "inventory")
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# metadata_locks LOCK_TYPE of a table write lock acquired via LOCK TABLES ... WRITE
TABLE_WRITE_LOCK_TYPE = "SHARED_NO_READ_WRITE"


@dataclass(frozen=True)
class DbUnlockRecord:
    """Outcome of the four-fold release chain for one lock session.

    Fold 3 (server-side ``wait_timeout`` idle disconnect) is configured at
    connect time and has no per-call outcome; it is the abandoned-session
    backstop when folds 1/2/4 cannot run (process killed).
    """
    connection_id: int
    unlock_executed: bool      # fold 1: UNLOCK TABLES on the lock session
    session_final_unlock: bool  # fold 2: final UNLOCK retry on the same session
    session_closed: bool
    residual_kill_attempted: bool  # fold 4: KILL CONNECTION via an independent session
    residual_kill_confirmed: bool


@dataclass(frozen=True)
class DbReleaseConfirmation:
    table: str
    released: bool  # independent SELECT 1 FROM `t` LIMIT 1 completed inside the deadline
    timed_out: bool
    examined_connection_ids: tuple[int, ...]
    # Every currently granted table-lock holder discovered on the table
    # ({"connection_id": int, "table": str, "lock_type": str}); empty rows plus
    # released=True is the verified clean state required by Q2 evidence.
    active_owned_locks: tuple[dict[str, Any], ...]


class DbCommandClient(Protocol):
    """Command surface for the table-lock primitive (SQL, not argv).

    connect/lock/unlock/confirm-release/checksum. Lock identity is the MySQL
    connection_id; the lock session is never reused for checksums or probes.
    """
    evidence_kind: str

    def connect_lock_session(self, *, idle_timeout_s: float) -> int: ...
    def lock_table(self, connection_id: int, table: str, mode: str) -> None: ...
    def unlock_table(self, connection_id: int, table: str) -> DbUnlockRecord: ...
    def confirm_release(self, table: str, *, connection_ids: tuple[int, ...], timeout_s: float) -> DbReleaseConfirmation: ...
    def checksum_tables(self, tables: tuple[str, ...]) -> dict[str, str]: ...


def _table_name(table: Any) -> str:
    _check(isinstance(table, str) and _IDENTIFIER.fullmatch(table) is not None, "database table identifier invalid")
    return table


def _connection_id(value: Any) -> int:
    _check(type(value) is int and value > 0, "lock session identity invalid")
    return value


class LiveDbCommandClient:
    """Bounded synchronous DB I/O; a timeout is never a successful drain.

    No query daemon and no KILL are used. Missing original handles can only
    recover after a fresh identity-bound read proves the old session absent.
    The injectable connector is for offline tests and is labelled fake.
    """
    def __init__(self, *, enabled=False, env_prefix="DB_", connect_timeout_s=10.0,
                 statement_timeout_s=30.0, socket_timeout_s=5.0,
                 expected_server_uuid=None, expected_database=None,
                 environ=None, connector_factory=None):
        for value, label in ((connect_timeout_s, "connect timeout"),
                             (statement_timeout_s, "operation timeout"),
                             (socket_timeout_s, "socket timeout")):
            _positive(value, label)
        self.enabled, self.env_prefix = enabled is True, str(env_prefix)
        self.connect_timeout_s = connect_timeout_s
        self.statement_timeout_s, self.socket_timeout_s = statement_timeout_s, socket_timeout_s
        self.expected_server_uuid, self.expected_database = expected_server_uuid, expected_database
        self.environ, self.factory = environ, connector_factory
        self.evidence_kind = "fake" if connector_factory is not None else "live"
        self._sessions, self._identities, self._unconfirmed = {}, {}, set()
        self._connection_ids, self._journal = {}, None
        self._active_timers = set()

    def bind_journal(self, journal):
        _check(isinstance(journal, AttemptJournal), "database journal required")
        if self._journal is not None:
            _check(self._journal.path == journal.path and self._journal.metadata == journal.metadata,
                   "database client cannot cross attempts")
        self._journal = journal

    def _prior_sessions(self):
        if self._journal is None:
            return set(self._connection_ids.values())
        paths = sorted(self._journal.path.glob("db-client-session-*.json"))
        _check(len(paths) <= 256, "database session evidence budget exceeded")
        result = set()
        for path in paths:
            _check(path.stat().st_size <= 4096, "database session evidence too large")
            data = json.loads(path.read_bytes())
            _check(set(data) == {"server_uuid", "database", "connection_id", "ownership"}
                   and data["ownership"] == self._journal.metadata["ownership"], "database session evidence identity mismatch")
            self.validate_session_identity({key: data[key] for key in ("server_uuid", "database", "connection_id")},
                                           data["connection_id"])
            result.add(data["connection_id"])
        return result

    def _require_enabled(self):
        _check(self.enabled is True, "real database client disabled")
        _check(type(self.expected_server_uuid) is str
               and str(uuid.UUID(self.expected_server_uuid)) == self.expected_server_uuid,
               "explicit expected database server identity required")
        _table_name(self.expected_database)
        _check(self.evidence_kind != "live" or self._journal is not None,
               "live database sessions require the attempt journal")

    def _config(self):
        import os
        environment = os.environ if self.environ is None else self.environ
        values = {key: environment.get(self.env_prefix + key) for key in ("HOST", "PORT", "USER", "PASSWORD", "NAME")}
        _check(all(type(value) is str and value for value in values.values()), "database connection reference missing")
        _check(values["NAME"] == self.expected_database, "database configuration differs from bound schema")
        port = int(values["PORT"])
        _check(0 < port < 65536, "database port invalid")
        return {"host": values["HOST"], "port": port, "user": values["USER"],
                "password": values["PASSWORD"], "database": values["NAME"], "use_pure": True}

    @staticmethod
    def _remaining(deadline):
        import time
        remaining = deadline - time.monotonic()
        _check(remaining > 0, "database total deadline exceeded")
        return remaining

    def _deadline(self, timeout=None):
        import time
        self._require_enabled()
        return time.monotonic() + (self.statement_timeout_s if timeout is None else timeout)

    @staticmethod
    def _socket(connection):
        sock = getattr(getattr(connection, "_socket", None), "sock", None)
        _check(sock is not None and callable(getattr(sock, "settimeout", None)),
               "pure connector socket deadline unavailable")
        return sock

    @staticmethod
    def _abort(connection):
        import socket
        sock = getattr(getattr(connection, "_socket", None), "sock", None)
        if sock is not None:
            for name, args in (("shutdown", (socket.SHUT_RDWR,)), ("close", ())):
                try:
                    getattr(sock, name)(*args)
                except Exception:
                    pass

    def _bounded(self, connection, deadline, operation, call):
        import threading
        expired = threading.Event()
        remaining = self._remaining(deadline)
        self._socket(connection).settimeout(min(self.socket_timeout_s, remaining))
        def abort():
            expired.set()
            self._abort(connection)
        timer = threading.Timer(remaining, abort)
        self._active_timers.add(timer)
        timer.start()
        try:
            result = call()
            _check(not expired.is_set(), "database total deadline exceeded")
            self._remaining(deadline)
            return result
        except BaseException:
            cid = self._connection_ids.get(id(connection))
            if cid is not None:
                self._unconfirmed.add(cid)
            self._abort(connection)
            raise PrimitiveError("database operation unconfirmed: " + operation) from None
        finally:
            timer.cancel()
            timer.join(timeout=1.0)
            _check(not timer.is_alive(), "database deadline worker not drained")
            self._active_timers.discard(timer)

    def _query(self, connection, deadline, sql, args=(), *, rows=True, label="query"):
        def execute():
            cursor = connection.cursor()
            try:
                cursor.execute(sql, args)
                values = [tuple(row) for row in cursor.fetchmany(257)] if rows else None
                _check(values is None or len(values) <= 256, "database row budget exceeded")
                return values
            finally:
                cursor.close()
        return self._bounded(connection, deadline, label, execute)

    def _close(self, connection, deadline):
        try:
            self._bounded(connection, deadline, "close", connection.close)
        except BaseException:
            self._abort(connection)
            raise

    def _open(self, deadline):
        config = self._config()
        remaining = self._remaining(deadline)
        _check(remaining >= 1, "insufficient database connect budget")
        config["connection_timeout"] = min(5, max(1, int(min(self.connect_timeout_s, remaining))))
        factory = self.factory
        if factory is None:
            import mysql.connector
            factory = mysql.connector.connect
        connection = None
        try:
            connection = factory(**config)
            self._remaining(deadline)
            identity = self._query(connection, deadline,
                "SELECT @@server_uuid, DATABASE(), CONNECTION_ID(), @@performance_schema", label="identity")
            _check(len(identity) == 1 and identity[0][0] == self.expected_server_uuid
                   and identity[0][1] == self.expected_database and identity[0][3] == 1,
                   "database physical identity mismatch")
            cid = _connection_id(identity[0][2])
            self._connection_ids[id(connection)] = cid
            if self._journal is not None:
                self._journal._write_new("db-client-session-" + uuid.uuid4().hex + ".json", canonical_json({
                    "server_uuid": self.expected_server_uuid, "database": self.expected_database,
                    "connection_id": cid, "ownership": self._journal.metadata["ownership"]}).encode("utf-8"))
            return connection, cid
        except BaseException:
            if connection is not None:
                self._abort(connection)
            raise PrimitiveError("database connection or identity unconfirmed") from None

    def session_identity(self, connection_id):
        cid = _connection_id(connection_id)
        _check(cid in self._identities, "database session identity unavailable")
        return dict(self._identities[cid])

    def lock_session_ids(self):
        """Connection ids of every lock session opened through this client.

        Read-only registry view for the collection post_recovery release-evidence
        probe: each id came from a real connect_lock_session on this
        attempt's client and stays registered after unlock/close, so the
        closure over the client yields exactly the attempt's own lock
        sessions. Nothing here invents or zero-fills an id.
        """
        return tuple(sorted(self._identities))

    def validate_session_identity(self, identity, connection_id):
        self._require_enabled()
        _check(identity == {"server_uuid": self.expected_server_uuid, "database": self.expected_database,
                            "connection_id": _connection_id(connection_id)}, "durable database identity mismatch")

    def connect_lock_session(self, *, idle_timeout_s):
        _positive(idle_timeout_s, "idle timeout")
        deadline = self._deadline()
        connection, cid = self._open(deadline)
        self._sessions[cid] = connection
        self._identities[cid] = {"server_uuid": self.expected_server_uuid,
                                 "database": self.expected_database, "connection_id": cid}
        try:
            self._query(connection, deadline, "SET SESSION wait_timeout = %s",
                        (int(math.ceil(max(60.0, idle_timeout_s))),), rows=False, label="idle_timeout")
            return cid
        except BaseException:
            self._abort(connection)
            raise

    def lock_table(self, connection_id, table, mode):
        deadline = self._deadline()
        cid = _connection_id(connection_id)
        _check(table == "items" and mode == "WRITE", "only items WRITE lock is supported")
        _check(cid in self._sessions and cid not in self._unconfirmed, "owned lock session unavailable")
        self._query(self._sessions[cid], deadline, "LOCK TABLES `items` WRITE", rows=False, label="lock_table")

    def _process_ids(self, connection, deadline, connection_ids):
        if not connection_ids:
            return set()
        marks = ",".join(["%s"] * len(connection_ids))
        rows = self._query(connection, deadline,
            "SELECT ID FROM information_schema.processlist WHERE ID IN (" + marks + ")",
            tuple(connection_ids), label="processlist")
        return {_connection_id(row[0]) for row in rows}

    def _holders(self, connection, deadline, table):
        _check(table == "items", "only items lock observation supported")
        instrumentation = self._query(connection, deadline,
            "SELECT ENABLED,TIMED FROM performance_schema.setup_instruments WHERE NAME='wait/lock/metadata/sql/mdl'",
            label="lock_instrumentation")
        _check(instrumentation == [("YES", "YES")], "metadata lock instrumentation unavailable")
        rows = self._query(connection, deadline,
            "SELECT t.PROCESSLIST_ID, m.OBJECT_NAME, m.LOCK_TYPE "
            "FROM performance_schema.metadata_locks m "
            "JOIN performance_schema.threads t ON m.OWNER_THREAD_ID = t.THREAD_ID "
            "WHERE m.OBJECT_TYPE = 'TABLE' AND m.LOCK_STATUS = 'GRANTED' "
            "AND m.OBJECT_SCHEMA = DATABASE()", label="metadata_locks")
        holders = []
        for processlist_id, object_name, lock_type in rows:
            if object_name != table or lock_type in {"SHARED_READ", "SHARED_WRITE"}:
                continue
            _check(lock_type == TABLE_WRITE_LOCK_TYPE, "unknown target metadata lock mode")
            holders.append({"connection_id": _connection_id(processlist_id), "table": table, "lock_type": lock_type})
        return holders

    def unlock_table(self, connection_id, table):
        deadline = self._deadline()
        cid = _connection_id(connection_id)
        _check(table == "items", "only items unlock supported")
        session = self._sessions.get(cid)
        unlock = final_unlock = closed = False
        if session is not None:
            for step in range(2):
                try:
                    self._query(session, deadline, "UNLOCK TABLES", rows=False, label="unlock_table")
                    if step == 0:
                        unlock = True
                    else:
                        final_unlock = True
                except PrimitiveError:
                    break
            try:
                self._close(session, deadline)
                closed = True
            except PrimitiveError:
                self._abort(session)
        admin, _ = self._open(deadline)
        try:
            present = self._process_ids(admin, deadline, (cid,))
            _check(not present, "original database session still present; no unproven KILL permitted")
            self._unconfirmed.discard(cid)
            self._sessions.pop(cid, None)
        finally:
            self._close(admin, deadline)
        return DbUnlockRecord(cid, unlock, final_unlock, closed, False, False)

    def confirm_release(self, table, *, connection_ids, timeout_s):
        _positive(timeout_s, "release confirmation timeout")
        _check(type(connection_ids) is tuple and all(type(cid) is int and cid > 0 for cid in connection_ids),
               "connection ids invalid")
        deadline = self._deadline(min(timeout_s, self.statement_timeout_s))
        connection, observation_cid = self._open(deadline)
        try:
            holders = self._holders(connection, deadline, table)
            examined = tuple(sorted(set(connection_ids) | {row["connection_id"] for row in holders}))
            if holders:
                return DbReleaseConfirmation(table, False, False, examined, tuple(holders))
            pending = tuple(sorted((set(connection_ids) | self._unconfirmed | self._prior_sessions()) - {observation_cid}))
            _check(not self._process_ids(connection, deadline, pending), "prior database session not drained")
            self._query(connection, deadline, "SELECT 1 FROM `items` LIMIT 1", label="release_probe")
            fresh = self._holders(connection, deadline, table)
            _check(not fresh, "database lock reappeared during release readback")
            self._unconfirmed.difference_update(pending)
            _check(not self._active_timers, "database timers not drained")
            return DbReleaseConfirmation(table, True, False, examined, ())
        finally:
            self._close(connection, deadline)

    def checksum_tables(self, tables):
        _check(type(tables) is tuple and bool(tables) and set(tables) <= set(CHECKSUM_TABLES), "checksum table whitelist required")
        _check(not self._unconfirmed and not self._sessions, "checksum blocked by unresolved database sessions")
        deadline = self._deadline()
        connection, _ = self._open(deadline)
        try:
            _check(not self._holders(connection, deadline, "items"), "checksum blocked by table lock")
            values = {}
            for name in tables:
                rows = self._query(connection, deadline, "CHECKSUM TABLE `" + name + "`", label="checksum")
                _check(len(rows) == 1 and rows[0][1] is not None, "checksum unavailable")
                values[name] = str(rows[0][1])
            return values
        finally:
            self._close(connection, deadline)

@dataclass(frozen=True)
class DbBinding:
    """Logical database lock binding; no Kubernetes identity involved."""
    database: str
    table: str

    def __post_init__(self) -> None:
        for value in (self.database, self.table):
            _check(isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None,
                   "database/table identifier invalid")


@dataclass(frozen=True, repr=False)
class PreparedDbLock:
    spec: FaultInstanceSpec
    binding: DbBinding
    target: Target
    desired_json: str
    mode: str

    def __post_init__(self) -> None:
        _check(self.mode == "mysql_table_lock", "unknown database lock mode")

    @property
    def desired_fields(self) -> dict[str, Any]:
        return json.loads(self.desired_json)


def prepare_db_lock(spec: FaultInstanceSpec, binding: DbBinding, *, ownership: str) -> PreparedDbLock:
    """Pure preparation of the table-lock primitive; contacts no database."""
    data = spec.to_dict()
    if data["fault_type"] != "db_table_lock":
        raise UnsupportedPrimitive("primitives_db prepares only db_table_lock faults")
    _check(data["mechanism_version"] == MECHANISM_VERSION, "unsupported primitive mechanism version")
    _check(data["mechanism"] == DB_MECHANISM, "db_table_lock requires the mysql_lock_table mechanism")
    entity = data["normalized_root_entity"]
    _check(entity in DB_LOCK_TABLE_BY_ENTITY, "unsupported db_table_lock root entity")
    _check(isinstance(ownership, str) and re.fullmatch(r"[a-zA-Z0-9-]+", ownership) is not None, "ownership marker invalid")
    raw = data["raw_target"]
    _check(raw["kind"] == DB_TARGET_KIND and raw["name"] == binding.table
           and raw["scope"] == binding.database and raw["selector"] == {}, "fault/binding database target mismatch")
    params = data["parameters"]
    _fields(params, {"table", "mode"})
    _check(params["table"] == binding.table == DB_LOCK_TABLE_BY_ENTITY[entity], "locked table does not match the root entity")
    _check(params["mode"] == "WRITE" and params["mode"] in DB_LOCK_MODES, "only WRITE table locks are implemented")
    target = Target(DB_TARGET_KIND, binding.table, binding.database, None)
    return PreparedDbLock(spec, binding, target,
                          canonical_json({"table": binding.table, "mode": params["mode"]}), "mysql_table_lock")


class DatabaseLockAdapter:
    """Journal-bound CAS adapter for the owned MySQL table lock.

    Controlled state: one held WRITE table lock per target. exists=True with
    uid=str(lock connection_id) while held; the absent empty state is the only
    original. Ownership is proven by the in-memory held session or by durable
    session/release evidence in the attempt directory (crash resume); an
    unproven holder is foreign and blocks apply.
    """

    def __init__(self, journal: AttemptJournal, client: DbCommandClient, prepared: tuple[PreparedDbLock, ...], *,
                 confirm_timeout_s: float = 15.0, idle_margin_s: float = 60.0,
                 checksum_tables: tuple[str, ...] = CHECKSUM_TABLES) -> None:
        _positive(confirm_timeout_s, "release confirmation timeout")
        _positive(idle_margin_s, "idle margin")
        _check(type(checksum_tables) is tuple and bool(checksum_tables)
               and len(set(checksum_tables)) == len(checksum_tables), "checksum table set invalid")
        self.guarded_tables = tuple(_table_name(t) for t in checksum_tables)
        _check(client.evidence_kind in {"fake", "live"}, "client evidence kind required")
        self.journal, self.client = journal, client
        bind = getattr(client, "bind_journal", None)
        if callable(bind):
            bind(journal)
        self.evidence_kind = client.evidence_kind
        self.confirm_timeout_s, self.idle_margin_s = confirm_timeout_s, idle_margin_s
        self._held: dict[tuple[str, str, str], int] = {}
        self._prepared: dict[tuple[str, str, str], PreparedDbLock] = {}
        for item in prepared:
            _check(item == prepare_db_lock(item.spec, item.binding, ownership=journal.metadata["ownership"]),
                   "prepared primitive differs from validated mechanism/owner")
            _check(any(f.to_dict() == item.spec.to_dict() for f in journal.contract.faults),
                   "primitive not bound to this contract")
            _check(item.binding.table in self.guarded_tables,
                   "locked table must be covered by the checksum guard")
            key = (item.target.kind, item.target.name, item.target.scope)
            _check(key not in self._prepared, "duplicate prepared database target")
            self._prepared[key] = item
        self.evidence: list[dict[str, Any]] = []

    # ---- identity and durable lock-session evidence ----

    def _item(self, target: Target) -> PreparedDbLock:
        item = self._prepared.get((target.kind, target.name, target.scope))
        _check(item is not None and item.target == target, "unapproved database target")
        return item

    @staticmethod
    def _key(target: Target) -> tuple[str, str, str]:
        return (target.kind, target.name, target.scope)

    def _record(self, operation: str, payload: dict[str, Any]) -> None:
        evidence = dict(payload)
        evidence.update(operation=operation, recorded_at_utc=datetime.now(timezone.utc).isoformat(),
                        evidence_kind=self.evidence_kind)
        self.evidence.append(evidence)
        self.journal._write_new("primitive-evidence-" + uuid.uuid4().hex + ".json",
                                canonical_json(evidence).encode("utf-8"))

    def _write_session_evidence(self, target: Target, connection_id: int, ownership: str) -> None:
        payload = {"operation": "db_lock_session", "target": target.to_dict(), "connection_id": _connection_id(connection_id),
                   "ownership": ownership, "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                   "evidence_kind": self.evidence_kind}
        identity = getattr(self.client, "session_identity", None)
        if callable(identity):
            payload["server_identity"] = identity(connection_id)
        self.evidence.append(payload)
        self.journal._write_new("db-lock-session-" + uuid.uuid4().hex + ".json",
                                canonical_json(payload).encode("utf-8"))

    def _write_release_evidence(self, target: Target, connection_id: int) -> None:
        payload = {"operation": "db_lock_release", "target": target.to_dict(), "connection_id": _connection_id(connection_id),
                   "ownership": self.journal.metadata["ownership"], "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                   "evidence_kind": self.evidence_kind}
        self.evidence.append(payload)
        self.journal._write_new("db-lock-release-" + uuid.uuid4().hex + ".json",
                                canonical_json(payload).encode("utf-8"))

    def _lock_evidence(self, operation: str) -> list[dict[str, Any]]:
        rows = []
        for path in sorted(self.journal.path.glob("db-lock-" + operation + "-*.json")):
            try:
                data = json.loads(path.read_bytes().decode("utf-8"))
            except (OSError, ValueError, UnicodeDecodeError):
                raise PrimitiveError("durable lock evidence unreadable") from None
            _check(isinstance(data, dict)
                   and set(data) in ({"operation", "target", "connection_id", "ownership", "recorded_at_utc", "evidence_kind"},
                                    {"operation", "target", "connection_id", "ownership", "recorded_at_utc", "evidence_kind", "server_identity"})
                   and data.get("operation") == "db_lock_" + operation and isinstance(data.get("target"), dict)
                   and type(data.get("connection_id")) is int, "durable lock evidence invalid")
            if operation == "session" and ("server_identity" in data or data["evidence_kind"] == "live"):
                validate = getattr(self.client, "validate_session_identity", None)
                _check(callable(validate) and "server_identity" in data, "legacy live session lacks bound server identity")
                validate(data["server_identity"], data["connection_id"])
            rows.append(data)
        return rows

    def _owned_connection_ids(self, target: Target) -> set[int]:
        ownership = self.journal.metadata["ownership"]
        key = self._key(target)
        sessions = {row["connection_id"] for row in self._lock_evidence("session")
                    if self._key(Target(**row["target"])) == key and row["ownership"] == ownership}
        released = {row["connection_id"] for row in self._lock_evidence("release")
                    if self._key(Target(**row["target"])) == key and row["ownership"] == ownership}
        return sessions - released

    def _unreleased_owned_sessions(self) -> bool:
        ownership = self.journal.metadata["ownership"]
        sessions = {(self._key(Target(**row["target"])), row["connection_id"])
                    for row in self._lock_evidence("session") if row["ownership"] == ownership}
        released = {(self._key(Target(**row["target"])), row["connection_id"])
                    for row in self._lock_evidence("release") if row["ownership"] == ownership}
        return bool(sessions - released)

    def _require_durable_intent(self, target: Target, desired: dict[str, Any], *, restoring: bool) -> None:
        states = self.journal._replay(self.journal.records())
        for item in reversed(tuple(states.values())):
            intent = item["intent"]
            if intent["target"] != target.to_dict() or item["status"] == "restored":
                continue
            _check(intent["ownership"] == self.journal.metadata["ownership"], "journal ownership mismatch")
            if restoring:
                _check(desired == intent["controlled_fields"], "recovery expected state differs from durable intent")
            else:
                _check(item["status"] == "pending" and desired == intent["controlled_fields"],
                       "mutation lacks current durable intent")
            return
        raise PrimitiveError("mutation blocked without durable write-ahead intent")

    # ---- ResourceAdapter protocol ----

    def observe(self, target: Target, field_names: tuple[str, ...]) -> ResourceState:
        item = self._item(target)
        _check(set(field_names) == set(item.desired_fields), "unexpected controlled fields")
        confirmation = self.client.confirm_release(target.name, connection_ids=tuple(sorted(self._owned_connection_ids(target))),
                                                   timeout_s=self.confirm_timeout_s)
        holders = confirmation.active_owned_locks
        if confirmation.released and not holders:
            for cid in sorted(self._owned_connection_ids(target)):
                self._write_release_evidence(target, cid)
                self._held.pop(self._key(target), None)
            return ResourceState(False, None, None, {})
        if not holders:
            raise PrimitiveError("table lock state unresolvable: probe blocked without a visible holder")
        _check(len(holders) == 1 and all(type(row.get("connection_id")) is int and row.get("table") == target.name
                                         and isinstance(row.get("lock_type"), str) for row in holders),
               "multiple or malformed table lock holders")
        cid = holders[0]["connection_id"]
        owned = cid == self._held.get(self._key(target)) or cid in self._owned_connection_ids(target)
        return ResourceState(True, str(cid), self.journal.metadata["ownership"] if owned else None,
                             _copy(item.desired_fields))

    def compare_and_apply(self, target: Target, expected: ResourceState,
                          desired_fields: dict[str, Any], ownership: str) -> None:
        item = self._item(target)
        data = item.spec.to_dict()
        _check(desired_fields == item.desired_fields and ownership == self.journal.metadata["ownership"],
               "apply ownership/spec mismatch")
        self._require_durable_intent(target, desired_fields, restoring=False)
        _check(not expected.exists and not expected.fields, "table lock apply requires an absent baseline")
        _check(self._key(target) not in self._held, "owned table lock already held by this adapter")
        window = item.spec.planned_window
        cid = self.client.connect_lock_session(idle_timeout_s=window.end - window.start + self.idle_margin_s)
        # Durable lock identity lands before the lock itself: a crash between
        # connect and the journal "applied" event must stay recoverable.
        self._write_session_evidence(target, cid, ownership)
        try:
            self.client.lock_table(cid, target.name, desired_fields["mode"])
        except Exception:
            self._release_lock_session(target, cid)
            raise
        self._held[self._key(target)] = cid
        state = self.observe(target, tuple(sorted(desired_fields)))
        _check(state.exists and state.uid == str(cid) and state.ownership == ownership
               and state.fields == desired_fields, "table lock readback mismatch")
        self._record("db_lock_inject", {"instance_id": data["fault_instance_id"], "action": "inject",
                                        "db_client_protocol": DB_CLIENT_PROTOCOL,
                                        "mechanism": data["mechanism"], "mechanism_version": data["mechanism_version"],
                                        "raw_target": data["raw_target"], "effective_parameters": data["parameters"],
                                        "resource_uid": str(cid), "desired_fields": desired_fields,
                                        "observed_fields": state.fields, "lock_connection_ids": [cid], "return_code": 0})

    def compare_and_restore(self, target: Target, expected: ResourceState, original: ResourceState) -> None:
        item = self._item(target)
        self._require_durable_intent(target, expected.fields, restoring=True)
        _check(not original.exists and not original.fields, "table lock restore expects an absent original")
        _check(expected.exists and expected.fields == item.desired_fields, "recovery expected state mismatch")
        _check(isinstance(expected.uid, str) and expected.uid.isdigit(), "lock session identity missing")
        cid = int(expected.uid)
        _check(cid == self._held.get(self._key(target)) or cid in self._owned_connection_ids(target),
               "refusing to release a table lock not owned by this attempt")
        record = self.client.unlock_table(cid, target.name)
        confirmation = self.client.confirm_release(target.name, connection_ids=(cid,),
                                                   timeout_s=self.confirm_timeout_s)
        _check(confirmation.released and not confirmation.active_owned_locks,
               "table lock release unconfirmed or still held")
        self._held.pop(self._key(target), None)
        self._record("db_lock_recover", {"instance_id": item.spec.fault_instance_id, "action": "recover",
                                         "db_client_protocol": DB_CLIENT_PROTOCOL,
                                         "resource_uid": str(cid),
                                         "unlock": {"unlock_executed": record.unlock_executed,
                                                    "session_final_unlock": record.session_final_unlock,
                                                    "session_closed": record.session_closed,
                                                    "residual_kill_attempted": record.residual_kill_attempted,
                                                    "residual_kill_confirmed": record.residual_kill_confirmed},
                                         "examined_connection_ids": list(confirmation.examined_connection_ids),
                                         "active_owned_locks": [dict(row) for row in confirmation.active_owned_locks],
                                         "return_code": 0})
        state = self.observe(target, tuple(sorted(expected.fields)))
        _check(not state.exists, "table lock still observed after a confirmed release")
        # observe wrote the release only after fresh absence proof.

    def _release_lock_session(self, target: Target, cid: int) -> None:
        """Bounded best-effort release after a failed partial start."""
        confirmed = False
        try:
            self.client.unlock_table(cid, target.name)
            confirmation = self.client.confirm_release(target.name, connection_ids=(cid,),
                                                       timeout_s=self.confirm_timeout_s)
            confirmed = confirmation.released and not confirmation.active_owned_locks
        except Exception:
            confirmed = False
        if confirmed:
            self._held.pop(self._key(target), None)
            self._write_release_evidence(target, cid)
        self._record("db_lock_partial_start_release", {"target": target.to_dict(), "connection_id": cid,
                                                       "released": confirmed})

    # ---- checksum guard (iron rule) ----

    def checksum_tables(self, tables: tuple[str, ...] | None = None) -> dict[str, str]:
        names = self.guarded_tables if tables is None else tables
        _check(type(names) is tuple and bool(names) and len(set(names)) == len(names), "checksum tables required")
        names = tuple(_table_name(t) for t in names)
        _check(not self._held and not self._unreleased_owned_sessions(),
               "checksum refused while an owned table lock is held (CHECKSUM TABLE would block on the WRITE lock)")
        values = self.client.checksum_tables(names)
        _check(isinstance(values, dict) and set(values) == set(names)
               and all(isinstance(v, str) and v for v in values.values()), "checksum result malformed")
        self._record("db_checksum", {"tables": values})
        return values

    def guard_checksums(self, expected: dict[str, str]) -> dict[str, str]:
        _check(isinstance(expected, dict) and bool(expected)
               and all(isinstance(k, str) and _IDENTIFIER.fullmatch(k) and isinstance(v, str) for k, v in expected.items()),
               "expected checksums malformed")
        values = self.checksum_tables(tuple(sorted(expected)))
        drift = {table: {"observed": values.get(table), "expected": expected[table]}
                 for table in sorted(expected) if values.get(table) != expected[table]}
        if drift:
            self._record("db_checksum_drift", {"drift": drift})
            raise PrimitiveError("checksum drift: business tables changed; collection must stop")
        return values


class DatabaseLockExecutor:
    """Applies the same intent/verify discipline as PrimitiveExecutor for DB locks."""

    def __init__(self, journal: AttemptJournal, adapter: DatabaseLockAdapter) -> None:
        _check(adapter.journal is journal, "adapter belongs to another journal")
        self.journal, self.adapter = journal, adapter

    def apply(self, prepared: PreparedDbLock, *, intent_id: str) -> dict[str, Any]:
        self.journal.execute_change(intent_id=intent_id, fault_instance_id=prepared.spec.fault_instance_id,
                                    target=prepared.target, desired_fields=prepared.desired_fields,
                                    adapter=self.adapter)
        return self.verify(prepared)

    def verify(self, prepared: PreparedDbLock) -> dict[str, Any]:
        try:
            state = self.adapter.observe(prepared.target, tuple(prepared.desired_fields))
            _check(state.exists and state.fields == prepared.desired_fields
                   and state.ownership == self.journal.metadata["ownership"],
                   "primitive controlled state no longer matches")
            return {"fault_instance_id": prepared.spec.fault_instance_id, "controlled_state_matches": True,
                    "state_sha256": canonical_sha256(state.to_dict()), "evidence_kind": self.adapter.evidence_kind,
                    "admission_spec_matches": None, "injection_effectiveness": "not_assessed",
                    "physical_recovery": "not_assessed"}
        except Exception:
            self.journal.mark_failed("primitive_verify_failed")
            raise

    def recover(self):
        return self.journal.reconcile(self.adapter)
