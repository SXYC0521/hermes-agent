"""cron pending_deliveries 存储 + scheduler 入队钩子测试。

薄转发架构：api_server 目标无法被 push（send() 永远失败），Hermes scheduler
到点把触发内容落进 pending_deliveries，由 Luma 轮询拉取后 chat_sync 唤起。

覆盖：
  - 存储四函数：入队幂等 / claim-on-read / 超 TTL 翻回（at-least-once）/ ack
  - _deliver_result 对 api_server 目标入队一条、非 api_server 不入队
"""

from __future__ import annotations

import sqlite3

import pytest

from cron import executions


@pytest.fixture
def pd_db(tmp_path, monkeypatch):
    """把 executions.db 指向临时文件，隔离 pending_deliveries 表。"""
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", tmp_path / "executions.db")
    return executions


def _chat_rows() -> list[dict]:
    conn = sqlite3.connect(str(executions.EXECUTIONS_FILE))
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM pending_deliveries ORDER BY created_at, id",
        ).fetchall()]
    finally:
        conn.close()


# ── 存储函数 ──


def test_enqueue_idempotent(pd_db):
    executions.enqueue_pending_delivery(
        "exec-1", "job-1", "api_server", "luma_xia-yizhou_2026-08-08", "内容A",
    )
    executions.enqueue_pending_delivery(
        "exec-1", "job-1", "api_server", "luma_xia-yizhou_2026-08-08", "内容B",
    )
    rows = executions.claim_pending_deliveries()
    assert len(rows) == 1  # 幂等：同 execution+chat 只入一条
    assert rows[0]["content"] == "内容A"


def test_claim_marks_and_returns(pd_db):
    executions.enqueue_pending_delivery("e1", "j1", "api_server", "c1", "a")
    executions.enqueue_pending_delivery("e2", "j2", "api_server", "c2", "b")
    claimed = executions.claim_pending_deliveries()
    assert len(claimed) == 2
    # 已 claim → 再取拿不到
    assert executions.claim_pending_deliveries() == []


def test_claim_stale_reclaim(pd_db):
    executions.enqueue_pending_delivery("e1", "j1", "api_server", "c1", "a")
    executions.claim_pending_deliveries()  # 状态 → claimed
    conn = sqlite3.connect(str(executions.EXECUTIONS_FILE))
    conn.execute("UPDATE pending_deliveries SET claimed_at='2000-01-01T00:00:00+00:00'")
    conn.commit()
    conn.close()
    # 超 TTL → 翻回 pending 并重新 claim（at-least-once 恢复）
    rows = executions.claim_pending_deliveries(stale_ttl_seconds=600)
    assert len(rows) == 1
    assert rows[0]["chat_id"] == "c1"


def test_ack_marks_acked(pd_db):
    executions.enqueue_pending_delivery("e1", "j1", "api_server", "c1", "a")
    rec = executions.claim_pending_deliveries()[0]
    assert executions.ack_pending_deliveries([rec["id"]]) == 1
    assert executions.claim_pending_deliveries() == []
    # ack 未认领/不存在的 id → 0
    assert executions.ack_pending_deliveries(["nonexistent"]) == 0


# ── scheduler 入队钩子 ──
# 注意：_deliver_result 是同步函数（内部独立投递用 asyncio.run），不 await。


def test_deliver_result_enqueues_api_server_target(monkeypatch, tmp_path):
    import cron.scheduler as sched

    monkeypatch.setattr(executions, "EXECUTIONS_FILE", tmp_path / "executions.db")
    # 绕过真实目标解析对 job 字段的依赖：直接给 api_server 目标
    monkeypatch.setattr(
        sched, "_resolve_delivery_targets",
        lambda job: [{"platform": "api_server", "chat_id": "luma_xia-yizhou_2026-08-08", "thread_id": None}],
    )
    # 独立投递快速失败（不发网络）
    import tools.send_message_tool as smt

    async def _fake_send(*a, **k):
        return {"error": "test-only failure"}

    monkeypatch.setattr(smt, "_send_to_platform", _fake_send)

    job = {
        "id": "job-x", "name": "测试任务", "prompt": "收鱼",
        "deliver": "origin", "repeat": {"times": 1, "completed": 0},
    }
    err = sched._deliver_result(
        job, "去收鱼", adapters=None, loop=None, execution_id="exec-1",
    )
    assert err  # 投递必然失败（api_server 无法 send）

    rows = executions.claim_pending_deliveries()
    assert len(rows) == 1
    assert rows[0]["chat_id"] == "luma_xia-yizhou_2026-08-08"
    assert rows[0]["execution_id"] == "exec-1"
    assert "收鱼" in rows[0]["content"]  # 触发内容完整入队，一次 job 移除后仍在


def test_deliver_result_skips_non_api_server(monkeypatch, tmp_path):
    import cron.scheduler as sched

    monkeypatch.setattr(executions, "EXECUTIONS_FILE", tmp_path / "executions.db")
    monkeypatch.setattr(
        sched, "_resolve_delivery_targets",
        lambda job: [{"platform": "telegram", "chat_id": "12345", "thread_id": None}],
    )
    import tools.send_message_tool as smt

    async def _fake_send(*a, **k):
        return {"error": "test-only failure"}

    monkeypatch.setattr(smt, "_send_to_platform", _fake_send)

    job = {
        "id": "job-x", "name": "测试", "prompt": "x",
        "deliver": "origin", "repeat": {"times": 1, "completed": 0},
    }
    sched._deliver_result(job, "hello", adapters=None, loop=None, execution_id="exec-1")
    assert executions.claim_pending_deliveries() == []  # telegram 不入队
