"""数据装载：把虚构边明细以批次方式导入引擎，并执行守恒校验。"""

from __future__ import annotations

from typing import Any, Optional

from .engine import Engine
from .samples import get_sample
from .store import Store


def load_sample(
    store: Store, name: str, reset: bool = True,
    apply_demo_batch: bool = True,
) -> dict[str, Any]:
    """导入一个内置样例。

    reset=True 时先清空库，保证从空目录复现；首批边明细整体作为一个批次，
    样例自带的演示批次作为第二个批次提交。
    """
    sample = get_sample(name)
    if reset:
        store.wipe()

    engine = Engine(store)

    # 节点先按给定群组登记（边明细只引用节点，群组属于节点属性）
    for node_id, grp in sample["groups"].items():
        store.ensure_node(node_id, grp)
        engine.groups[node_id] = grp
        engine.adj.setdefault(node_id, set())

    initial_changes = [
        {"u": u, "v": v, "source": src, "action": "add_source"}
        for u, v, src in sample["edges"]
    ]
    first = engine.apply_batch(initial_changes, note=f"样例 {name}：首批导入")

    demo = None
    if apply_demo_batch and sample["batch"]["changes"]:
        demo = engine.apply_batch(
            sample["batch"]["changes"],
            note=sample["batch"].get("note") or f"样例 {name}：演示批次",
        )

    conservation = engine.verify_conservation()
    return {
        "sample": name,
        "note": sample["note"],
        "first_batch": first,
        "demo_batch": demo,
        "expect": sample.get("expect"),
        "conservation": conservation,
        "state": engine.state_view(),
    }


def load_edge_rows(
    engine: Engine, rows: list[dict[str, Any]], note: Optional[str] = None
) -> dict[str, Any]:
    """通用入口：导入任意来源级边明细行 {u,v,source,action?}。"""
    changes = []
    for r in rows:
        changes.append({
            "u": r["u"], "v": r["v"], "source": r["source"],
            "action": r.get("action", "add_source"),
        })
    result = engine.apply_batch(changes, note=note)
    result["conservation"] = engine.verify_conservation()
    return result
