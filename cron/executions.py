"""Profile-local durable audit ledger for cron execution attempts.

The ledger records what is known about each attempt; it is not a retry queue.
Interrupted attempts become ``unknown`` only after their exact owner process is
proved gone. Terminal states are immutable.
"""

from __future__ import annotations

import datetime
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

from hermes_constants import get_hermes_home
from hermes_time import now as _hermes_now

EXECUTIONS_FILE = get_hermes_home().resolve() / "cron" / "executions.db"
MAX_TERMINAL_EXECUTIONS = 1000
_TERMINAL_STATES = ("completed", "failed", "unknown")
_lock = threading.RLock()
_PROCESS_ID = uuid.uuid4().hex


def _connect() -> sqlite3.Connection:
    EXECUTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(EXECUTIONS_FILE, timeout=5)


def _initialize_schema(conn: sqlite3.Connection) -> None:
    from hermes_state import apply_wal_with_fallback

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    apply_wal_with_fallback(conn, db_label="cron/executions.db")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS executions (
             id TEXT PRIMARY KEY,
             job_id TEXT NOT NULL,
             source TEXT NOT NULL,
             process_id TEXT NOT NULL,
             pid INTEGER NOT NULL,
             process_started_at INTEGER,
             status TEXT NOT NULL CHECK(status IN
               ('claimed','running','completed','failed','unknown')),
             claimed_at TEXT NOT NULL,
             started_at TEXT,
             finished_at TEXT,
             error TEXT
           )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_job_claimed "
        "ON executions(job_id, claimed_at DESC, id DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_status_claimed "
        "ON executions(status, claimed_at DESC, id DESC)"
    )
    # pending_deliveries：api_server 目标（薄转发架构，无法 push）的 cron 触发内容，
    # 由 Luma 轮询拉取后 chat_sync 唤起。独立于 jobs.json 存活——一次性 job 移除后
    # 记录仍在，唤醒不丢。不是重试队列：claim 后由 Luma ack。
    conn.execute(
        """CREATE TABLE IF NOT EXISTS pending_deliveries (
             id           TEXT PRIMARY KEY,
             execution_id TEXT NOT NULL,
             job_id       TEXT NOT NULL,
             platform     TEXT NOT NULL,
             chat_id      TEXT NOT NULL,
             content      TEXT NOT NULL,
             status       TEXT NOT NULL DEFAULT 'pending',
             created_at   TEXT NOT NULL,
             claimed_at   TEXT,
             acked_at     TEXT,
             UNIQUE (execution_id, chat_id)
           )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pd_status "
        "ON pending_deliveries(status, claimed_at)"
    )


@contextmanager
def _transaction() -> Iterator[sqlite3.Connection]:
    """Open a connection, commit/rollback on exit, always close.

    ``sqlite3.Connection.__enter__``/``__exit__`` only commit or roll back
    the transaction; it does not close the connection. Relying on that alone
    leaks a connection (and its WAL/SHM file descriptors) on every call,
    since closing then depends on the garbage collector. Schema init runs
    inside the ``try`` too, so a PRAGMA/DDL failure after a successful
    ``connect()`` still closes the connection instead of leaking it.
    """
    with _lock:
        conn = _connect()
        try:
            _initialize_schema(conn)
            with conn:
                yield conn
        finally:
            conn.close()


def _record(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    return dict(row) if row is not None else None


def _emit_execution_state(
    record: Optional[Dict[str, Any]], *, delivery_outcome: Optional[str] = None
) -> None:
    """Project durable state to monitoring without affecting ledger behavior."""
    try:
        from agent.monitoring.cron_health import emit_execution_state

        emit_execution_state(record, delivery_outcome=delivery_outcome)
    except Exception:
        pass


def _process_start_time(pid: int) -> Optional[int]:
    try:
        from gateway.status import get_process_start_time
        return get_process_start_time(pid)
    except Exception:
        return None


def _owner_is_live(pid: int, started_at: Optional[int]) -> bool:
    try:
        from gateway.status import _pid_exists
        if not _pid_exists(pid):
            return False
    except Exception:
        return True  # fail safe: inability to prove death must not rewrite state
    if started_at is None:
        return pid == os.getpid()
    current = _process_start_time(pid)
    return current is not None and current == started_at


def _prune_unlocked(conn: sqlite3.Connection) -> None:
    limit = max(0, int(MAX_TERMINAL_EXECUTIONS))
    conn.execute(
        """DELETE FROM executions WHERE id IN (
             SELECT id FROM executions
             WHERE status IN ('completed','failed','unknown')
             ORDER BY claimed_at DESC, id DESC LIMIT -1 OFFSET ?
           )""",
        (limit,),
    )


def create_execution(job_id: str, *, source: str) -> Dict[str, Any]:
    """Persist a claimed attempt before executor/provider dispatch."""
    now = _hermes_now().isoformat()
    execution_id = uuid.uuid4().hex
    pid = os.getpid()
    with _transaction() as conn:
        conn.execute(
            """INSERT INTO executions
               (id, job_id, source, process_id, pid, process_started_at,
                status, claimed_at)
               VALUES (?, ?, ?, ?, ?, ?, 'claimed', ?)""",
            (execution_id, str(job_id), str(source), _PROCESS_ID, pid,
             _process_start_time(pid), now),
        )
        row = conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone()
    record = _record(row)
    _emit_execution_state(record)
    return record  # type: ignore[return-value]


def mark_execution_running(execution_id: str) -> Optional[Dict[str, Any]]:
    """Transition one claimed attempt to running exactly once."""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET status='running', started_at=?
               WHERE id=? AND status='claimed'""",
            (now, execution_id),
        )
        if cur.rowcount != 1:
            return None
        record = _record(conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone())
    _emit_execution_state(record)
    return record


def finish_execution(
    execution_id: str, *, success: bool, error: Optional[str] = None,
    delivery_outcome: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Write a terminal result once; terminal attempts cannot be rewritten."""
    now = _hermes_now().isoformat()
    status = "completed" if success else "failed"
    detail = None if success else (str(error) if error else "unknown failure")
    with _transaction() as conn:
        cur = conn.execute(
            """UPDATE executions SET status=?, finished_at=?, error=?
               WHERE id=? AND status IN ('claimed','running')""",
            (status, now, detail, execution_id),
        )
        if cur.rowcount != 1:
            return None
        _prune_unlocked(conn)
        record = _record(conn.execute(
            "SELECT * FROM executions WHERE id=?", (execution_id,)
        ).fetchone())
    _emit_execution_state(record, delivery_outcome=delivery_outcome)
    return record


def recover_interrupted_executions() -> int:
    """Mark provably abandoned attempts unknown without scheduling retries."""
    now = _hermes_now().isoformat()
    changed = 0
    recovered: List[Dict[str, Any]] = []
    with _transaction() as conn:
        rows = conn.execute(
            """SELECT id, process_id, pid, process_started_at FROM executions
               WHERE status IN ('claimed','running')"""
        ).fetchall()
        for row in rows:
            if row["process_id"] == _PROCESS_ID:
                continue
            if _owner_is_live(int(row["pid"]), row["process_started_at"]):
                continue
            cur = conn.execute(
                """UPDATE executions SET status='unknown', finished_at=?, error=?
                   WHERE id=? AND status IN ('claimed','running')""",
                (now,
                 "Scheduler restarted after this execution's owner exited before a durable "
                 "terminal state; whether side effects ran is unknown.",
                 row["id"]),
            )
            changed += cur.rowcount
            if cur.rowcount:
                record = _record(conn.execute(
                    "SELECT * FROM executions WHERE id=?", (row["id"],)
                ).fetchone())
                if record is not None:
                    recovered.append(record)
        if changed:
            _prune_unlocked(conn)
    for record in recovered:
        _emit_execution_state(record)
    return changed


def list_executions(
    *, job_id: Optional[str] = None, limit: int = 50,
    before_claimed_at: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return indexed, newest-first execution history with cursor pagination."""
    clauses: List[str] = []
    params: List[Any] = []
    if job_id is not None:
        clauses.append("job_id=?")
        params.append(str(job_id))
    if before_claimed_at is not None:
        clauses.append("claimed_at < ?")
        params.append(str(before_claimed_at))
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(max(1, min(int(limit), 500)))
    with _transaction() as conn:
        rows = conn.execute(
            "SELECT * FROM executions" + where
            + " ORDER BY claimed_at DESC, id DESC LIMIT ?",
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def latest_execution(job_id: str) -> Optional[Dict[str, Any]]:
    rows = list_executions(job_id=job_id, limit=1)
    return rows[0] if rows else None


def latest_executions(job_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Load latest execution for many jobs in one indexed query."""
    clean = [str(job_id) for job_id in dict.fromkeys(job_ids) if job_id]
    if not clean:
        return {}
    placeholders = ",".join("?" for _ in clean)
    with _transaction() as conn:
        rows = conn.execute(
            f"""SELECT e.* FROM executions e
                WHERE e.job_id IN ({placeholders})
                  AND e.id=(SELECT e2.id FROM executions e2
                            WHERE e2.job_id=e.job_id
                            ORDER BY e2.claimed_at DESC, e2.id DESC LIMIT 1)""",
            clean,
        ).fetchall()
    return {row["job_id"]: dict(row) for row in rows}


# ══════════════════════════════════════════════════════════════
# pending_deliveries：api_server 目标（薄转发，无法 push）的 cron 触发内容。
# Luma 轮询 claim → chat_sync 唤起 → ack；claim 后超 TTL 自动翻回（at-least-once）。
# ══════════════════════════════════════════════════════════════


def _iso_minus(iso_str: str, seconds: int) -> str:
    try:
        dt = datetime.datetime.fromisoformat(iso_str)
        return (dt - datetime.timedelta(seconds=seconds)).isoformat()
    except Exception:
        return iso_str


def enqueue_pending_delivery(
    execution_id: Optional[str],
    job_id: str,
    platform: str,
    chat_id: str,
    content: str,
) -> Dict[str, Any]:
    """入队一条待 Luma 拉取的投递（幂等：同一 execution+chat_id 只入一条）。"""
    now = _hermes_now().isoformat()
    delivery_id = uuid.uuid4().hex
    with _transaction() as conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO pending_deliveries
               (id, execution_id, job_id, platform, chat_id, content,
                status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)""",
            (delivery_id, str(execution_id or ""), str(job_id), str(platform),
             str(chat_id), str(content), now),
        )
        inserted = cur.rowcount == 1
        row = conn.execute(
            "SELECT * FROM pending_deliveries WHERE id=?", (delivery_id,)
        ).fetchone()
    record = _record(row)
    return record if record is not None else {"id": delivery_id, "inserted": False}


def claim_pending_deliveries(
    limit: int = 50,
    stale_ttl_seconds: int = 600,
) -> List[Dict[str, Any]]:
    """原子认领待投递：先把超 TTL 的 claimed 翻回 pending（崩溃恢复），
    再返回 up to limit 条 pending 并标记 claimed（claim-on-read）。"""
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        conn.execute(
            "UPDATE pending_deliveries SET status='pending', claimed_at=NULL "
            "WHERE status='claimed' AND claimed_at IS NOT NULL AND claimed_at < ?",
            (_iso_minus(now, max(1, int(stale_ttl_seconds))),),
        )
        rows = conn.execute(
            "SELECT * FROM pending_deliveries WHERE status='pending' "
            "ORDER BY created_at ASC LIMIT ?",
            (max(1, min(int(limit), 200)),),
        ).fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"UPDATE pending_deliveries SET status='claimed', claimed_at=? "
                f"WHERE id IN ({placeholders})",
                (now, *ids),
            )
    return [dict(r) for r in rows]


def ack_pending_deliveries(ids: Optional[List[str]]) -> int:
    """确认投递完成（Luma chat_sync 成功后调用）→ acked，随后清理过期 acked。"""
    clean = [str(i) for i in (ids or []) if str(i).strip()]
    if not clean:
        return 0
    now = _hermes_now().isoformat()
    with _transaction() as conn:
        placeholders = ",".join("?" for _ in clean)
        cur = conn.execute(
            f"UPDATE pending_deliveries SET status='acked', acked_at=? "
            f"WHERE id IN ({placeholders}) AND status='claimed'",
            (now, *clean),
        )
        acked = cur.rowcount
    prune_acked_pending_deliveries(older_than_days=7)
    return acked


def prune_acked_pending_deliveries(older_than_days: int = 7) -> int:
    """清理已 ack 超过 N 天的记录（表有界）。"""
    cutoff = _iso_minus(_hermes_now().isoformat(), max(1, int(older_than_days)) * 86400)
    with _transaction() as conn:
        cur = conn.execute(
            "DELETE FROM pending_deliveries WHERE status='acked' "
            "AND acked_at IS NOT NULL AND acked_at < ?",
            (cutoff,),
        )
        return cur.rowcount
