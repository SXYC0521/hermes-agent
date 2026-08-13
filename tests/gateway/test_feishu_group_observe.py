"""方案 B：feishu 群消息观察 + @ 触发注入全群上下文。

群里所有消息（含未 @ 的）记入观察缓存；@ 触发时把最近讨论注入 Agent 上下文。
"""

from __future__ import annotations

from types import SimpleNamespace

from plugins.platforms.feishu.adapter import FeishuAdapter



import asyncio

def _observe_sync(a, msg, sender):
    asyncio.run(a._observe_group_message(msg, sender))

def _make_adapter() -> FeishuAdapter:
    # 测试基建不走 __init__（object.__new__），方法内 getattr 惰性初始化兜底
    return FeishuAdapter.__new__(FeishuAdapter)


def _msg(text: str, mid: str, chat_id: str = "oc_group", chat_type: str = "group") -> SimpleNamespace:
    return SimpleNamespace(
        message_id=mid, chat_id=chat_id, chat_type=chat_type,
        content=f'{{"text":"{text}"}}',
    )


def _sender(open_id: str = "ou_1") -> SimpleNamespace:
    return SimpleNamespace(sender_id=SimpleNamespace(open_id=open_id))


def test_observe_records_group_message():
    a = _make_adapter()
    _observe_sync(a, _msg("你好", "m1"), _sender("ou_1"))
    _observe_sync(a, _msg("项目讨论", "m2"), _sender("ou_2"))
    ctx = a._group_context("oc_group")
    assert "你好" in ctx
    assert "项目讨论" in ctx


def test_observe_skips_p2p():
    a = _make_adapter()
    _observe_sync(a, _msg("私聊", "m1", chat_type="p2p"), _sender("ou_1"))
    assert a._group_context("oc_group") == ""


def test_observe_skips_empty_text():
    a = _make_adapter()
    _observe_sync(a, 
        SimpleNamespace(message_id="m1", chat_id="oc_group", chat_type="group", content="{}"),
        _sender("ou_1"),
    )
    assert a._group_context("oc_group") == ""


def test_context_excludes_current_message():
    a = _make_adapter()
    _observe_sync(a, _msg("之前讨论", "m_prev"), _sender("ou_1"))
    _observe_sync(a, _msg("当前@消息", "m_cur"), _sender("ou_2"))
    ctx = a._group_context("oc_group", exclude_mid="m_cur")
    assert "之前讨论" in ctx
    assert "当前@消息" not in ctx


def test_context_bounded_to_max():
    a = _make_adapter()
    for i in range(30):
        _observe_sync(a, _msg(f"msg{i}", f"m{i}"), _sender("ou_1"))
    ctx = a._group_context("oc_group")
    # 最多 20 条（_GROUP_OBSERVE_MAX），最早的消息被挤出
    assert "msg0" not in ctx
    assert "msg29" in ctx


def test_include_sender_name():
    a = _make_adapter()
    _observe_sync(a, _msg("hi", "m1"), _sender("ou_abc123"))
    ctx = a._group_context("oc_group")
    assert "成员…abc123" in ctx  # 无缓存名字时用 open_id 尾部
    assert "hi" in ctx
