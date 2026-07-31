"""异步提案执行器 — 应用 bg-review 生成的 skill 修改提案。

bg-review 的 review_agent 现在只输出提案（proposal-only 模式），不直接
修改技能文件。本执行器负责把这些提案异步落地：

    读取 ~/.hermes/pending_proposals/proposal_*.md
      → 解析提案（target_skill / change_type / old_content_hint / new_content）
      → 定位 skill 的 SKILL.md
      → 验证 old_content_hint 匹配当前文件
      → 应用 new_content
      → 成功删除提案 / 失败移入 .failed/

纯文件操作，无需 LLM。由 Luma 侧的后台任务周期调用。
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional


def _hermes_root() -> Path:
    """返回 Hermes 根目录（不是 profile 子目录）。

    Luma 会把 HERMES_HOME 设成 ``<root>/profiles/<agent>``，但提案目录
    必须全局唯一（写和读用同一位置），所以取 ``/profiles/`` 之前的根。
    """
    try:
        from hermes_cli.config import get_hermes_home
        s = str(Path(get_hermes_home()))
    except Exception:
        s = str(Path.home() / ".hermes")
    if "/profiles/" in s:
        s = s.split("/profiles/")[0]
    return Path(s)


def proposals_dir() -> Path:
    return _hermes_root() / "pending_proposals"


def skills_root() -> Path:
    return _hermes_root() / "skills"


def proposals_dir() -> Path:
    return _hermes_root() / "pending_proposals"


def skills_root() -> Path:
    return _hermes_root() / "skills"


def parse_proposal(content: str) -> Optional[Dict]:
    """解析提案文本为结构化 dict，缺必填字段返回 None。"""
    lines = content.strip().splitlines()
    parsed: Dict[str, str] = {}
    in_new = False
    new_lines = []
    for line in lines:
        if line.startswith("target_skill:"):
            parsed["target_skill"] = line.split(":", 1)[1].strip()
        elif line.startswith("change_type:"):
            parsed["change_type"] = line.split(":", 1)[1].strip()
        elif line.startswith("section_path:"):
            parsed["section_path"] = line.split(":", 1)[1].strip()
        elif line.startswith("old_content_hint:"):
            parsed["old_content_hint"] = line.split(":", 1)[1].strip()
        elif line.startswith("reason:"):
            parsed["reason"] = line.split(":", 1)[1].strip()
        elif line.startswith("new_content:"):
            in_new = True
        elif in_new:
            new_lines.append(line)
    if in_new:
        parsed["new_content"] = "\n".join(new_lines).strip()
    if not all(k in parsed for k in ("target_skill", "change_type", "new_content")):
        return None
    return parsed


def find_skill_md(skill_name: str) -> Optional[Path]:
    """在 skills 目录下按技能名找到 SKILL.md。"""
    root = skills_root()
    if not root.exists():
        return None
    for md in root.rglob("SKILL.md"):
        if md.parent.name == skill_name:
            return md
    return None


def apply_proposal(parsed: Dict, current: str) -> Optional[str]:
    """按提案内容应用修改，返回新文件内容；无法应用返回 None。"""
    hint = parsed.get("old_content_hint")
    new_content = parsed["new_content"]
    if hint and hint in current:
        return current.replace(hint, new_content, 1)
    return None


def _move_failed(pf: Path, error: str) -> None:
    failed_dir = proposals_dir() / ".failed"
    failed_dir.mkdir(exist_ok=True)
    try:
        (failed_dir / pf.name).write_bytes(pf.read_bytes())
        pf.unlink()
    except Exception:
        pass
    try:
        (failed_dir / f"{pf.stem}_error.txt").write_text(str(error), encoding="utf-8")
    except Exception:
        pass


def execute_proposals() -> Dict[str, int]:
    """执行所有待处理提案，返回报告 {applied, failed, skipped}。"""
    report = {"applied": 0, "failed": 0, "skipped": 0}
    pdir = proposals_dir()
    if not pdir.exists():
        return report

    proposals = sorted(pdir.glob("proposal_*.md"))
    for pf in proposals:
        try:
            content = pf.read_text(encoding="utf-8")
            parsed = parse_proposal(content)
            if not parsed:
                report["failed"] += 1
                _move_failed(pf, "无法解析提案")
                continue

            md = find_skill_md(parsed["target_skill"])
            if md is None:
                report["failed"] += 1
                _move_failed(pf, f"找不到技能 '{parsed['target_skill']}'")
                continue

            current = md.read_text(encoding="utf-8")
            new_content_full = apply_proposal(parsed, current)
            if new_content_full is None:
                report["failed"] += 1
                _move_failed(pf, "old_content_hint 未在文件中找到（文件可能已变化）")
                continue

            md.write_text(new_content_full, encoding="utf-8")
            pf.unlink()
            report["applied"] += 1
        except Exception as e:  # noqa: BLE001 — 单提案失败不影响其余
            report["failed"] += 1
            _move_failed(pf, str(e))

    return report
