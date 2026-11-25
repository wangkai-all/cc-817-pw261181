"""虚构边明细样例。

每个样例包含：
- groups:  node_id -> 初始群组
- edges:   业务边明细 [(u, v, source), ...]，同一条边允许出现多个来源
- batch:   首批导入后立即演示的一个批次（可空），刻意覆盖
  「一个三角的三条边在同一批都变化」与「批内临时三角」场景
- note:    人工核对说明
所有数据均为虚构。
"""

from __future__ import annotations

from typing import Any

SAMPLES: dict[str, dict[str, Any]] = {
    # 1) 星形：高阶节点 H，无闭合三角——定向邻接的压力样例
    "star": {
        "note": "星形图：H 度=5，无闭合三角；叶子边 L1-L2 制造唯一三角 H-L1-L2。",
        "groups": {
            "H": "g1", "L1": "g1", "L2": "g1", "L3": "g1", "L4": "g2", "L5": "g2",
        },
        "edges": [
            ("H", "L1", "src-1"),
            ("H", "L2", "src-1"),
            ("H", "L3", "src-1"),
            ("H", "L4", "src-1"),
            ("H", "L5", "src-1"),
            ("L1", "L2", "src-7"),
        ],
        "batch": {"note": "", "changes": []},
        "expect": {"nodes": 6, "edges": 6, "triangles": 1},
    },

    # 2) 完全图 K4（同组 4 个三角）+ 跨组 K3（不计入任何群组指标）
    "complete": {
        "note": "K4 完全图：6 条边、4 个闭合三角；另有跨组 K3 只进全局事实不进群组指标。",
        "groups": {"a": "alpha", "b": "alpha", "c": "alpha", "d": "alpha",
                   "x": "beta", "y": "beta", "z": "alpha"},
        "edges": [
            ("a", "b", "s1"), ("a", "c", "s1"), ("a", "d", "s1"),
            ("b", "c", "s1"), ("b", "d", "s1"), ("c", "d", "s1"),
            ("x", "y", "s2"), ("y", "z", "s2"), ("x", "z", "s2"),
        ],
        "batch": {"note": "", "changes": []},
        "expect": {"nodes": 7, "edges": 9, "triangles": 5, "triangles_in_alpha": 4,
                   "triangles_in_beta": 0},
    },

    # 3) 重复来源：一条边两个来源，重复导入不产生新三角；逐来源撤回
    "duplicate_sources": {
        "note": "边 A-B 有两个来源；重复加来源仍是一条边、一个三角，"
                "撤回一个来源边还在，撤回最后一个来源边与三角才删除。",
        "groups": {"A": "g1", "B": "g1", "C": "g1"},
        "edges": [
            ("A", "B", "feed-1"),
            ("A", "B", "feed-2"),   # 重复来源：同一条业务边
            ("B", "C", "feed-1"),
            ("A", "C", "feed-3"),
        ],
        "batch": {
            "note": "再重复导入一次 feed-1（幂等），并撤回 feed-2（边保留）",
            "changes": [
                {"u": "A", "v": "B", "source": "feed-1", "action": "add_source"},
                {"u": "A", "v": "B", "source": "feed-2", "action": "withdraw_source"},
            ],
        },
        "expect": {"nodes": 3, "edges": 3, "triangles": 1},
    },

    # 4) 批内加删：K4 上一个批次同时新增/删除相邻边，
    #    三角 pqr 的三条边在本批都变化；逐条顺序应用会出现临时三角 qrs
    "batch_add_remove": {
        "note": "K4 上单批次：删 pq、删 rs、给 pr 加来源。逐条顺序应用会产生"
                "临时三角 qrs（最终不存在）；正式查询只看到整批净结果（4→0 个三角，剩 4-环）。",
        "groups": {"p": "g1", "q": "g1", "r": "g1", "s": "g1"},
        "edges": [
            ("p", "q", "e1"),
            ("p", "r", "e1"),
            ("p", "s", "e1"),
            ("q", "r", "e1"),
            ("q", "s", "e1"),
            ("r", "s", "e1"),
        ],
        "batch": {
            "note": "同批：withdraw(pq,e1) + add_source(pr,e9) + withdraw(rs,e1)",
            "changes": [
                {"u": "p", "v": "q", "source": "e1", "action": "withdraw_source"},
                {"u": "p", "v": "r", "source": "e9", "action": "add_source"},
                {"u": "r", "v": "s", "source": "e1", "action": "withdraw_source"},
            ],
        },
        "expect": {"nodes": 4, "edges": 4, "triangles": 0,
                   "triangles_before_batch": 4,
                   "transient_triangles_sequential": ["qrs"]},
    },
}


def sample_names() -> list[str]:
    return list(SAMPLES.keys())


def get_sample(name: str) -> dict[str, Any]:
    if name not in SAMPLES:
        raise KeyError(f"未知样例 {name}，可选：{sample_names()}")
    return SAMPLES[name]
