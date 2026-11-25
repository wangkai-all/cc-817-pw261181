"""无状态图算法：规范化边、定向邻接、分区交集与三角枚举。

倾斜控制策略（详见 README「倾斜回退」）：
1. 定向邻接：按 (度数, 节点 id) 给每条边唯一定向，秩小的一端指向秩大的一端，
   形成 DAG；每个三角只在其秩最小节点处被枚举一次，总工作量 O(m^1.5)。
2. 分区交集：高基数邻域求交时把较小集合分块（chunk），用大集合做哈希探测，
   避免一次性拷贝/嵌套扫描，并按工作单元计数。
3. 预算回退：工作单元超过预算即抛 WorkBudgetExceeded，engine 转全量重建，
   保证最坏情况下结果仍然正确，且回退本身可在结果中显式标注。
"""

from __future__ import annotations

from typing import Iterable, Iterator, Sequence

# 默认参数。单次增量候选的工作预算约为「分块数 × 探测数」上限。
DEFAULT_CHUNK = 1024
DEFAULT_WORK_BUDGET = 2_000_000


class WorkBudgetExceeded(RuntimeError):
    def __init__(self, work: int, budget: int, detail: str = ""):
        self.work = work
        self.budget = budget
        self.detail = detail
        super().__init__(f"交集工作单元 {work} 超过预算 {budget} {detail}".strip())


def canon(u: str, v: str) -> tuple[str, str]:
    """边的规范化表示：字典序小端在前，同一无向边只有一个 key。"""
    return (u, v) if u < v else (v, u)


def build_adj(edges: Iterable[tuple[str, str]]) -> dict[str, set[str]]:
    adj: dict[str, set[str]] = {}
    for u, v in edges:
        u, v = canon(u, v)
        adj.setdefault(u, set()).add(v)
        adj.setdefault(v, set()).add(u)
    return adj


def rank(adj: dict[str, set[str]], x: str) -> tuple[int, str]:
    return (len(adj[x]), x)


def orient(adj: dict[str, set[str]]) -> dict[str, list[str]]:
    """定向邻接表：弧 x->y 当且仅当 (deg(x), x) < (deg(y), y)。

    每条无向边恰好产生一条弧，结果是 DAG；三角 (a,b,c) 的最小秩节点
    同时指向另外两个节点，因此枚举时每个三角恰好出现一次。
    """
    out: dict[str, list[str]] = {x: [] for x in adj}
    for u, nbrs in adj.items():
        ru = rank(adj, u)
        for v in nbrs:
            if ru < rank(adj, v):
                out[u].append(v)
    for u in out:
        out[u].sort()
    return out


def common_neighbors(
    a: str,
    b: str,
    adj: dict[str, set[str]],
    chunk: int = DEFAULT_CHUNK,
    budget: int = DEFAULT_WORK_BUDGET,
) -> Iterator[str]:
    """求 N(a) ∩ N(b) 的分区（分块哈希探测）生成器。

    以较小邻域为驱动集合分块，在较大邻域上做 O(1) 成员探测；
    work 计数探测次数，超预算立即中止，由上层回退全量重建。
    """
    na = adj.get(a, set())
    nb = adj.get(b, set())
    if len(na) > len(nb):
        na, nb = nb, na
    work = 0
    drive = sorted(na)
    for i in range(0, len(drive), chunk):
        block = drive[i : i + chunk]
        for w in block:
            work += 1
            if work > budget:
                raise WorkBudgetExceeded(work, budget, f"common_neighbors({a},{b})")
            if w in nb:
                yield w


def _has_edge(adj: dict[str, set[str]], u: str, v: str) -> bool:
    return v in adj.get(u, ())


def enumerate_triangles(
    adj: dict[str, set[str]],
    chunk: int = DEFAULT_CHUNK,
    budget: int = DEFAULT_WORK_BUDGET,
) -> set[tuple[str, str, str]]:
    """全量三角枚举。返回排序三元组集合，每个闭合三角恰好一个。

    复杂度 O(Σ out_deg(u)^2) ≤ O(m^1.5)，高阶节点因定向后出度很小而代价受限。
    预算保护同样作用于出邻域两两配对的探测工作量。
    """
    out = orient(adj)
    triangles: set[tuple[str, str, str]] = set()
    work = 0
    for u, outs in out.items():
        k = len(outs)
        # 配对探测工作量计数
        work += k * (k - 1) // 2
        if work > budget:
            raise WorkBudgetExceeded(work, budget, f"enumerate_at({u}) out_deg={k}")
        for i in range(k):
            v = outs[i]
            for j in range(i + 1, k):
                w = outs[j]
                if _has_edge(adj, v, w):
                    triangles.add(tuple(sorted((u, v, w))))  # type: ignore[arg-type]
    return triangles


def triangles_through_edge(
    u: str, v: str, adj: dict[str, set[str]], **kw
) -> set[tuple[str, str, str]]:
    """边 (u,v) 参与的全部三角：w ∈ N(u)∩N(v)，每个 w 一个。"""
    return {tuple(sorted((u, v, w))) for w in common_neighbors(u, v, adj, **kw)}


def triangle_group(nodes: Sequence[str], groups: dict[str, str]) -> str:
    """三角归属：三节点同组返回组名，否则返回 None（调用方记跨组哨兵）。"""
    g = groups[nodes[0]]
    if all(groups[x] == g for x in nodes[1:]):
        return g
    return None  # type: ignore[return-value]


def clustering_coefficient(degree: int, tri_count: int) -> float:
    """局部聚集系数：3T / C(k,2)，度 < 2 时定义为 0.0。"""
    if degree < 2:
        return 0.0
    return round((3.0 * tri_count) / (degree * (degree - 1) / 2.0), 6)
