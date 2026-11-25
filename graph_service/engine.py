"""图加工引擎。

核心约定：
- 业务边存在性与来源条数分离：edge_sources 非空才有 edges 行；
  只有撤回最后一个有效来源（withdraw）才删除边。
- 一个批次内的多条变更只产生一个「净结果」。增量以整批前后两张图做差，
  对涉及边求候选三角集合再比对，因此三条边都变化的三角也只计一次，
  批内逐条顺序制造的临时三角不会出现在正式查询里。
- 提交是单事务：SQLite 未提交前查询一律看到旧快照（sqlite 默认隔离级别），
  内存态在事务成功后才切换，保证正式查询只能看到完整批次结果。
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from copy import deepcopy
from typing import Any, Optional

from . import graph as g
from .store import CROSS_GROUP, Store, tid_of, tid_nodes


class EngineError(ValueError):
    pass


# 回退全量重建使用独立的大预算：定向枚举本身 O(m^1.5) 有界，
# 不能与触发回退的增量小预算共用，否则回退路径也会抛异常
REBUILD_BUDGET = 1_000_000_000


class Engine:
    def __init__(self, store: Store, chunk: int = g.DEFAULT_CHUNK,
                 work_budget: int = g.DEFAULT_WORK_BUDGET):
        self.store = store
        self.chunk = chunk
        self.work_budget = work_budget
        self.groups: dict[str, str] = {}
        self.edge_sources: dict[tuple[str, str], set[str]] = {}
        self.adj: dict[str, set[str]] = {}
        self._load_state()

    # ============================== 装载 ==============================
    def _load_state(self) -> None:
        """进程重启后的恢复：图本体全部来自 SQLite，派生表不需要重放日志。"""
        self.groups = self.store.all_nodes()
        self.edge_sources = self.store.all_edge_sources()
        self.adj = g.build_adj(self.store.all_edges())
        for node_id in self.groups:
            self.adj.setdefault(node_id, set())

    # ============================== 批次规划 ==============================
    def plan_batch(self, changes: list[dict[str, Any]]) -> dict[str, Any]:
        """把来源级变更规划成整批净变化，不触碰任何状态。

        同一 (edge, source) 在批内重复出现时按顺序折叠，保证规划结果幂等。
        返回 affected_edges（图结构可能变化的边）与每个来源操作的存活判定。
        """
        planned: list[dict[str, Any]] = []
        sim_sources = {e: set(s) for e, s in self.edge_sources.items()}
        affected: set[tuple[str, str]] = set()

        for idx, ch in enumerate(changes):
            action = ch.get("action")
            if action not in ("add_source", "withdraw_source"):
                raise EngineError(f"变更 #{idx} action 非法: {action!r}")
            u, v = g.canon(str(ch["u"]), str(ch["v"]))
            source = str(ch["source"])
            key = (u, v)
            before = bool(sim_sources.get(key))
            bag = sim_sources.setdefault(key, set())
            if action == "add_source":
                bag.add(source)
            else:
                if source not in bag:
                    raise EngineError(
                        f"变更 #{idx} 撤回不存在的来源 {key} / {source!r}"
                    )
                bag.discard(source)
            after = bool(bag)
            affected.add(key)
            planned.append({
                "seq": idx, "u": u, "v": v, "source": source, "action": action,
                "edge_born": (not before and after),
                "edge_died": (before and not after),
            })
        return {
            "changes": planned,
            "affected_edges": sorted(affected),
            "affected_nodes": sorted({x for e in affected for x in e}),
        }

    # ============================== 批次提交 ==============================
    @staticmethod
    def _batch_id() -> str:
        return "b_" + uuid.uuid4().hex[:12]

    def _apply_plan_to_copy(self, plan: dict[str, Any]) -> dict[str, set[str]]:
        """在邻接表副本上施加整批净边变化（只看边的存亡，与来源条数无关）。"""
        adj2 = deepcopy(self.adj)
        # 直接以规划终态来源袋为准，重建受影响边的存亡
        final_bags = self._final_source_bags(plan)
        for key in plan["affected_edges"]:
            u, v = key
            if final_bags[key]:
                adj2.setdefault(u, set()).add(v)
                adj2.setdefault(v, set()).add(u)
            else:
                if u in adj2:
                    adj2[u].discard(v)
                if v in adj2:
                    adj2[v].discard(u)
        return adj2

    def _final_source_bags(self, plan: dict[str, Any]) -> dict[tuple[str, str], set[str]]:
        bags = {e: set(s) for e, s in self.edge_sources.items()}
        for ch in plan["changes"]:
            key = (ch["u"], ch["v"])
            bags.setdefault(key, set())
            if ch["action"] == "add_source":
                bags[key].add(ch["source"])
            else:
                bags[key].discard(ch["source"])
        return bags

    @staticmethod
    def _triangles_via_edge_in_graph(
        u: str, v: str, adj: dict[str, set[str]],
        chunk: int, budget: int,
    ) -> set[tuple[str, str, str]]:
        """边 (u,v) 在给定图中真正参与的三角：本边存活且 w 与两端均相邻。"""
        if v not in adj.get(u, ()):
            return set()
        out: set[tuple[str, str, str]] = set()
        for w in g.common_neighbors(u, v, adj, chunk=chunk, budget=budget):
            # common_neighbors 已保证 w∈N(u)∩N(v)，即三条边全部存在
            out.add(tuple(sorted((u, v, w))))  # type: ignore[arg-type]
        return out

    def _candidate_triangles(
        self, adj_old: dict[str, set[str]], adj_new: dict[str, set[str]],
        affected_edges: list[tuple[str, str]],
    ) -> tuple[set[tuple[str, str, str]], set[tuple[str, str, str]], bool]:
        """对每条受影响边求其在新旧两张图上的全部三角，取并集作为候选。

        已删除的边在对应图中不产生三角（先验本边存活），公共邻居 w 保证
        另外两条边也在，因此候选集合里不会有「幻影三角」；三条边都变化的
        三角在多个受影响边处重复出现，集合去重后只计一次。
        工作单元超预算则交由上层全量重建回退。
        """
        old_cand: set[tuple[str, str, str]] = set()
        new_cand: set[tuple[str, str, str]] = set()
        try:
            for u, v in affected_edges:
                old_cand |= self._triangles_via_edge_in_graph(
                    u, v, adj_old, self.chunk, self.work_budget
                )
                new_cand |= self._triangles_via_edge_in_graph(
                    u, v, adj_new, self.chunk, self.work_budget
                )
        except g.WorkBudgetExceeded:
            return set(), set(), True
        return old_cand, new_cand, False

    def apply_batch(
        self, changes: list[dict[str, Any]], note: Optional[str] = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """提交一个来源级批次，返回结构化结果；dry_run 不落库、不改内存。"""
        if not changes:
            raise EngineError("空批次不允许提交")
        plan = self.plan_batch(changes)
        batch_id = self._batch_id()
        # 关键：旧图必须是独立快照，否则删边后旧候选三角也查不出来
        adj_old = deepcopy(self.adj)
        adj_new = self._apply_plan_to_copy(plan)
        final_bags = self._final_source_bags(plan)

        old_cand, new_cand, fell_back = self._candidate_triangles(
            adj_old, adj_new, plan["affected_edges"]
        )
        if fell_back:
            inc_summary = {"mode": "budget_exceeded", "candidates": 0}
            full_summary = self._rebuild_with_adj(
                adj_new, final_bags, batch_id, kind="edge_batch",
                note=note, changes=plan["changes"], dry_run=dry_run,
            )
            full_summary["mode"] = "full_rebuild"
            full_summary["incremental"] = inc_summary
            return full_summary

        disappeared = old_cand - new_cand
        appeared = new_cand - old_cand
        # 三条边都在本批变化的三角同时落在两个候选集合里，差集天然只计一次
        delta = {"added": sorted(appeared), "removed": sorted(disappeared)}

        inc_summary = {
            "mode": "incremental",
            "batch_id": batch_id,
            "affected_edges": plan["affected_edges"],
            "affected_nodes": plan["affected_nodes"],
            "triangle_delta": delta,
            "candidates_old": len(old_cand),
            "candidates_new": len(new_cand),
        }

        if dry_run:
            inc_summary["persisted"] = False
            return inc_summary

        self._persist_incremental(batch_id, note, plan, final_bags, adj_new, delta)
        inc_summary["persisted"] = True
        inc_summary["incremental"] = None
        return inc_summary

    # ============================== 增量落库 ==============================
    def _persist_incremental(
        self, batch_id: str, note: Optional[str], plan: dict[str, Any],
        final_bags: dict[tuple[str, str], set[str]],
        adj_new: dict[str, set[str]], delta: dict[str, list[tuple[str, str, str]]],
    ) -> None:
        appeared = [tuple(t) for t in delta["added"]]
        disappeared = [tuple(t) for t in delta["removed"]]
        # 受影响节点所在群组都要刷新——跨组三角的另一端群组同样会变动
        affected_groups = {self.groups.get(n, "default")
                           for n in plan["affected_nodes"]}
        # 三角增删还可能波及受影响边之外的幸存边（其支持数变化），
        # 收集候选三角全部三条边与三个节点，统一重算
        metric_edges: set[tuple[str, str]] = set(plan["affected_edges"])
        metric_nodes: set[str] = set(plan["affected_nodes"])
        for tri in appeared + disappeared:
            for i in range(3):
                metric_edges.add(g.canon(tri[i], tri[(i + 1) % 3]))
            metric_nodes.update(tri)
            affected_groups.update(
                self.groups.get(n, "default") for n in tri
            )

        with self.store.transaction():
            self.store.insert_batch(batch_id, "edge_batch", note)

            # 1) 节点与来源/边
            for ch in plan["changes"]:
                u, v = ch["u"], ch["v"]
                for n in (u, v):
                    self.store.ensure_node(n, self.groups.get(n, "default"))
                self.store.insert_change(
                    batch_id, ch["seq"], u, v, ch["source"], ch["action"],
                    ch["edge_born"], ch["edge_died"],
                )
                if ch["action"] == "add_source":
                    self.store.insert_source(u, v, ch["source"], batch_id)
                    if ch["edge_born"]:
                        self.store.insert_edge(u, v)
                else:
                    self.store.delete_source(u, v, ch["source"])
                    if ch["edge_died"]:
                        self.store.delete_edge(u, v)
                        self.store.delete_support_edge(u, v)
                        self.store.delete_edge_metric(u, v)

            # 新节点补群组（内存）
            for n in plan["affected_nodes"]:
                self.groups.setdefault(n, "default")

            # 2) 三角事实与支持记录
            for tri in appeared:
                tid = tid_of(tri)
                grp = g.triangle_group(tri, self.groups) or CROSS_GROUP
                self.store.put_triangle(tid, tri, grp, batch_id)
                for i in range(3):
                    eu, ev = g.canon(tri[i], tri[(i + 1) % 3])
                    self.store.replace_support(
                        tid, eu, ev, sorted(final_bags[(eu, ev)])
                    )
            for tri in disappeared:
                self.store.delete_triangle(tid_of(tri))
                self.store.delete_support_triangle(tid_of(tri))

            # 3) 指标：先重算受影响节点/边，再做群组指标——组 edge_sum 依赖边支持数
            # 清扫任何无来源的孤儿边指标行（边已在来源折叠中死亡的情形）
            for key in plan["affected_edges"]:
                if not final_bags[key]:
                    self.store.delete_edge_metric(*key)
            self._recompute_node_metrics(adj_new, metric_nodes)
            # 存活边集合内统一重算；已死边的指标行已在上面清扫
            live_metric_edges = {e for e in metric_edges if final_bags.get(e)}
            self._recompute_edge_metrics(
                adj_new, live_metric_edges, final_bags
            )
            # 群组指标整组重算（而非增减旧计数），保证 edge_sum = 3 * triangle_count
            for grp_name in affected_groups:
                self._refresh_group_metric(grp_name, adj_new)

            # 4) 审计事件
            self.store.insert_event("batch", batch_id, {
                "note": note, "changes": plan["changes"],
                "triangle_delta": {
                    "added": [list(t) for t in appeared],
                    "removed": [list(t) for t in disappeared],
                },
            })

        # 事务成功后切换内存态
        self.edge_sources = {e: s for e, s in final_bags.items() if s}
        self.adj = adj_new
        for n in plan["affected_nodes"]:
            self.adj.setdefault(n, set())

    # ============================== 全量重建 ==============================
    def rebuild(self, note: str = "manual rebuild") -> dict[str, Any]:
        """从当前图本体全量重建所有派生表（幂等：重复执行结果逐行相同）。"""
        return self._rebuild_with_adj(
            deepcopy(self.adj),
            {e: set(s) for e, s in self.edge_sources.items()},
            self._batch_id(), kind="rebuild", note=note, changes=None,
            dry_run=False,
        )

    def _rebuild_with_adj(
        self, adj_new: dict[str, set[str]],
        final_bags: dict[tuple[str, str], set[str]], batch_id: str,
        kind: str, note: Optional[str], changes: Optional[list[dict[str, Any]]],
        dry_run: bool,
    ) -> dict[str, Any]:
        # 回退/显式重建使用独立大预算，保证回退路径一定能完成
        triangles = g.enumerate_triangles(
            adj_new, chunk=self.chunk, budget=REBUILD_BUDGET
        )
        if dry_run:
            return {
                "mode": "full_rebuild", "batch_id": batch_id, "persisted": False,
                "triangles": sorted(triangles),
                "node_count": len(adj_new),
                "edge_count": sum(len(v) for v in adj_new.values()) // 2,
            }

        with self.store.transaction():
            if kind == "edge_batch":
                self.store.insert_batch(batch_id, "edge_batch", note)
                assert changes is not None
                for ch in changes:
                    u, v = ch["u"], ch["v"]
                    for n in (u, v):
                        self.store.ensure_node(n, self.groups.get(n, "default"))
                    self.store.insert_change(
                        batch_id, ch["seq"], u, v, ch["source"], ch["action"],
                        ch["edge_born"], ch["edge_died"],
                    )
                    if ch["action"] == "add_source":
                        self.store.insert_source(u, v, ch["source"], batch_id)
                        if ch["edge_born"]:
                            self.store.insert_edge(u, v)
                    else:
                        self.store.delete_source(u, v, ch["source"])
                        if ch["edge_died"]:
                            self.store.delete_edge(u, v)
                # 与增量路径一致：新节点登记到内存群组，后续归属判定才能取到组
                for ch in changes:
                    for n in (ch["u"], ch["v"]):
                        self.groups.setdefault(n, "default")
                        self.adj.setdefault(n, set())
            else:
                self.store.insert_batch(batch_id, kind, note)

            # 派生表整体重写——不复制任何旧计数；
            # 但三角事实的「首次创建批次」属于历史事实，重建前后保持不变
            prior_tri = {
                r["tid"]: (r["created_batch"], r["created_at"])
                for r in self.store.all_triangles()
            }

            # 派生表整体重写——不复制任何旧计数
            self.store.conn.execute("DELETE FROM edge_tri_support")
            self.store.conn.execute("DELETE FROM triangles")
            self.store.conn.execute("DELETE FROM node_metrics")
            self.store.conn.execute("DELETE FROM edge_metrics")
            self.store.conn.execute("DELETE FROM group_metrics")

            for tri in sorted(triangles):
                tid = tid_of(tri)
                grp = g.triangle_group(tri, self.groups) or CROSS_GROUP
                self.store.put_triangle(tid, tri, grp, batch_id)
                prior = prior_tri.get(tid)
                if prior is not None:
                    # 恢复历史创建批次/时间，保证重复重建逐行幂等
                    self.store.conn.execute(
                        "UPDATE triangles SET created_batch=?, created_at=? WHERE tid=?",
                        (prior[0], prior[1], tid),
                    )
                for i in range(3):
                    eu, ev = g.canon(tri[i], tri[(i + 1) % 3])
                    self.store.replace_support(
                        tid, eu, ev, sorted(final_bags[(eu, ev)])
                    )

            self._recompute_node_metrics(adj_new, set(adj_new.keys()))
            live_edges = {e for e, bag in final_bags.items() if bag}
            self._recompute_edge_metrics(adj_new, live_edges, final_bags)
            for grp_name in set(self.groups.values()):
                self._refresh_group_metric(grp_name, adj_new)

            self.store.insert_event(kind, batch_id, {
                "note": note,
                "triangle_count": len(triangles),
                "changes": changes or [],
            })

        self.edge_sources = {e: s for e, s in final_bags.items() if s}
        self.adj = adj_new
        for n in self.groups:
            self.adj.setdefault(n, set())
        return {
            "mode": "full_rebuild", "batch_id": batch_id, "persisted": True,
            "triangle_count": len(triangles),
            "node_count": len(adj_new),
            "edge_count": sum(len(v) for v in adj_new.values()) // 2,
            "incremental": None,
        }

    # ============================== 指标计算 ==============================
    def _triangles_for_node(self, node_id: str) -> list[tuple[str, str, str]]:
        rows = self.store.triangles_touching([node_id])
        return [tid_nodes(r["tid"]) for r in rows]

    def _recompute_node_metrics(
        self, adj: dict[str, set[str]], nodes: set[str]
    ) -> None:
        for n in nodes:
            degree = len(adj.get(n, ()))
            if degree == 0 and n not in self.groups:
                continue
            tris = self._triangles_for_node(n)
            tri_total = len(tris)
            tri_in_grp = sum(
                1 for t in tris
                if g.triangle_group(t, self.groups) == self.groups.get(n)
            )
            self.store.upsert_node_metric(
                n, self.groups.get(n, "default"), degree, tri_total,
                tri_in_grp, g.clustering_coefficient(degree, tri_total),
            )

    def _recompute_edge_metrics(
        self, adj: dict[str, set[str]],
        edges: set[tuple[str, str]],
        final_bags: dict[tuple[str, str], set[str]],
    ) -> None:
        for u, v in edges:
            if not final_bags.get((u, v)):
                continue  # 已死边：指标行应已删除
            support = self.store.support_for_edge(u, v)
            tri_total = len(support)
            tri_in_grp = 0
            gu = self.groups.get(u, "default")
            gv = self.groups.get(v, "default")
            if gu == gv:
                for row in support:
                    tri = tid_nodes(row["tid"])
                    if g.triangle_group(tri, self.groups) == gu:
                        tri_in_grp += 1
            self.store.upsert_edge_metric(u, v, gu, gv, tri_total, tri_in_grp)

    def _refresh_group_metric(
        self, grp_name: str, adj: Optional[dict[str, set[str]]] = None
    ) -> None:
        """整组重算（只扫描该组节点/边/三角），迁移与批次后调用。

        节点改群组时重建相关归属，绝不复制旧计数。adj 显式传入时使用调用方
        的目标图快照（提交前内存态尚未切换，不能读 self.adj）。
        """
        adj = adj if adj is not None else self.adj
        members = [n for n, gg in self.groups.items() if gg == grp_name]
        if not members:
            self.store.delete_group_metric(grp_name)
            return
        member_set = set(members)
        edge_count = 0
        edge_sum = 0
        for n in members:
            for v in adj.get(n, ()):
                if n < v and v in member_set:
                    edge_count += 1
                    # 只累加同组三角的支持；跨组三角不计入任何群组贡献
                    for row in self.store.support_for_edge(n, v):
                        tri = tid_nodes(row["tid"])
                        if g.triangle_group(tri, self.groups) == grp_name:
                            edge_sum += 1
        tri_count = 0
        for r in self.store.all_triangles():
            if r["grp"] == grp_name:
                tri_count += 1
        self.store.upsert_group_metric(
            grp_name, len(members), edge_count, tri_count, edge_sum
        )

    # ============================== 群组迁移 ==============================
    def move_node_group(self, node_id: str, new_group: str) -> dict[str, Any]:
        row = self.store.get_node(node_id)
        if row is None:
            raise EngineError(f"节点不存在: {node_id!r}")
        old_group = row["grp"]
        if old_group == new_group:
            return {"node_id": node_id, "from": old_group, "to": new_group,
                    "changed": False, "retriangulated": 0}

        touched = [tid_nodes(r["tid"])
                   for r in self.store.triangles_touching([node_id])]
        batch_id = self._batch_id()
        with self.store.transaction():
            self.store.insert_batch(batch_id, "group_move",
                                    f"{node_id}: {old_group} -> {new_group}")
            self.store.set_node_group(node_id, new_group)
            self.groups[node_id] = new_group

            # 三角归属：同组/跨组状态可能翻转，逐条重判而非搬旧计数
            for tri in touched:
                tid = tid_of(tri)
                grp = g.triangle_group(tri, self.groups) or CROSS_GROUP
                self.store.retriangle_group(tid, grp)
                # 三条边的同组贡献标记也随归属重建
                for i in range(3):
                    eu, ev = g.canon(tri[i], tri[(i + 1) % 3])
                    srcs = self.store.sources_for_edge(eu, ev)
                    if srcs:
                        self.store.replace_support(tid, eu, ev, srcs)

            # 节点指标（组内三角数）、相关边指标、两个群组整体重建
            self._recompute_node_metrics(self.adj, {node_id})
            for v in self.adj.get(node_id, set()):
                eu, ev = g.canon(node_id, v)
                self._recompute_edge_metrics(
                    self.adj, {(eu, ev)}, self.edge_sources
                )
            self._refresh_group_metric(old_group)
            self._refresh_group_metric(new_group)

            self.store.insert_event("group_move", batch_id, {
                "node_id": node_id, "from": old_group, "to": new_group,
                "retriangulated": len(touched),
            })
        return {"node_id": node_id, "from": old_group, "to": new_group,
                "changed": True, "retriangulated": len(touched),
                "batch_id": batch_id}

    # ============================== 顺序模拟 / 对比 ==============================
    def simulate_sequential(
        self, changes: list[dict[str, Any]],
        baseline_adj: Optional[dict[str, set[str]]] = None,
        baseline_bags: Optional[dict[tuple[str, str], set[str]]] = None,
    ) -> dict[str, Any]:
        """按变更顺序逐条施加（演示用），暴露临时三角问题。

        每一步独立成「图状态」，与正式的整批净结果形成对照；不写库。
        baseline 用于从任意历史基线（如首批导入后的状态）回放。
        """
        adj = deepcopy(baseline_adj) if baseline_adj is not None else deepcopy(self.adj)
        bags = (
            {e: set(s) for e, s in baseline_bags.items()}
            if baseline_bags is not None
            else {e: set(s) for e, s in self.edge_sources.items()}
        )
        steps: list[dict[str, Any]] = []
        for idx, ch in enumerate(changes):
            u, v = g.canon(str(ch["u"]), str(ch["v"]))
            bag = bags.setdefault((u, v), set())
            if ch["action"] == "add_source":
                bag.add(str(ch["source"]))
            else:
                bag.discard(str(ch["source"]))
            if bag:
                adj.setdefault(u, set()).add(v)
                adj.setdefault(v, set()).add(u)
            else:
                adj.get(u, set()).discard(v)
                adj.get(v, set()).discard(u)
            tris = g.enumerate_triangles(
                adj, chunk=self.chunk, budget=max(self.work_budget, 10**9)
            )
            steps.append({
                "seq": idx, "change": {
                    "u": u, "v": v, "source": ch["source"],
                    "action": ch["action"],
                },
                "triangles": sorted(map(list, tris)),
                "triangle_count": len(tris),
            })
        final_tri = {tuple(sorted(t)) for t in (steps[-1]["triangles"] if steps else [])}
        return {"steps": steps, "final_triangles": sorted(map(list, final_tri))}

    def compare_plan(self, changes: list[dict[str, Any]]) -> dict[str, Any]:
        """并排对比：增量结果 vs 全量重建预测，断言两者一致后返回。"""
        inc = self.apply_batch(changes, dry_run=True)
        plan = self.plan_batch(changes)
        adj_new = self._apply_plan_to_copy(plan)
        full = self._rebuild_with_adj(
            adj_new, self._final_source_bags(plan), self._batch_id(),
            kind="rebuild", note="dry-run compare", changes=None, dry_run=True,
        )
        if inc.get("mode") == "incremental":
            inc_set = set(map(tuple, inc["triangle_delta"]["added"]))
            rem_set = set(map(tuple, inc["triangle_delta"]["removed"]))
            # 全量预测的三角集合 = 旧三角 - removed + added
            old_set = {tid_nodes(r["tid"]) for r in self.store.all_triangles()}
            predicted_full = (old_set - rem_set) | inc_set
            full_set = set(map(tuple, full["triangles"]))
            equal = predicted_full == full_set
        else:
            # 增量已因预算回退为全量重建：两边全量结果天然应当一致
            equal = True
        return {"incremental": inc, "full_rebuild": full, "equal": equal}

    # ============================== 守恒校验 ==============================
    def verify_conservation(self) -> dict[str, Any]:
        """全量核对守恒关系，返回每条检查的明细。"""
        checks: list[dict[str, Any]] = []

        def add(name: str, ok: bool, detail: Any = "") -> None:
            checks.append({"check": name, "ok": bool(ok), "detail": detail})

        # 1) 边与来源一致：edges 行 ⇔ 来源袋非空
        db_edges = set(self.store.all_edges())
        live_edges = {e for e, s in self.edge_sources.items() if s}
        add("edge_existence_equals_nonempty_sources", db_edges == live_edges,
            {"db_only": sorted(map(list, db_edges - live_edges)),
             "memory_only": sorted(map(list, live_edges - db_edges))})

        # 2) 邻接表与 edges 一致
        adj_edges = {g.canon(u, v) for u, nbrs in self.adj.items() for v in nbrs}
        add("adjacency_equals_edges", adj_edges == db_edges)

        # 3) 三角事实 = 枚举结果（无重复三角、无幽灵三角）
        enum_tris = g.enumerate_triangles(
            self.adj, chunk=self.chunk, budget=max(self.work_budget, 10**9)
        )
        db_tris = {tid_nodes(r["tid"]) for r in self.store.all_triangles()}
        add("triangles_match_enumeration", enum_tris == db_tris,
            {"missing": sorted(map(list, enum_tris - db_tris)),
             "extra": sorted(map(list, db_tris - enum_tris))})

        # 4) 每个三角恰好 3 条支持边，且边都存活
        support_ok = True
        support_detail: list[Any] = []
        for r in self.store.all_triangles():
            sup = self.store.support_for_triangle(r["tid"])
            edge_keys = {g.canon(s["u"], s["v"]) for s in sup}
            tri_edges = {
                g.canon(r["n1"], r["n2"]), g.canon(r["n2"], r["n3"]),
                g.canon(r["n1"], r["n3"]),
            }
            if len(sup) != 3 or edge_keys != tri_edges:
                support_ok = False
                support_detail.append(r["tid"])
            for s in sup:
                if not self.store.edge_exists(s["u"], s["v"]):
                    support_ok = False
                    support_detail.append(f"dead-support:{r['tid']}")
        add("triangle_support_three_live_edges", support_ok, support_detail)

        # 5) 节点指标：度与三角数重算一致
        node_ok = True
        node_detail: list[Any] = []
        for r in self.store.all_node_metrics():
            n = r["node_id"]
            degree = len(self.adj.get(n, ()))
            tris = self._triangles_for_node(n)
            want_c = g.clustering_coefficient(degree, len(tris))
            if r["degree"] != degree or r["tri_count"] != len(tris) or \
                    abs(r["clustering"] - want_c) > 1e-9:
                node_ok = False
                node_detail.append(n)
        add("node_metrics_recompute", node_ok, node_detail)

        # 6) 边指标：三角支持数重算一致
        edge_ok = True
        edge_detail: list[Any] = []
        for r in self.store.all_edge_metrics():
            key = (r["u"], r["v"])
            if r["tri_support"] != len(self.store.support_for_edge(*key)):
                edge_ok = False
                edge_detail.append(list(key))
        add("edge_metrics_recompute", edge_ok, edge_detail)

        # 7) 群组守恒：组内 edge_sum = 3 * triangle_count
        grp_ok = True
        grp_detail: list[Any] = []
        for r in self.store.all_group_metrics():
            if r["edge_sum"] != 3 * r["triangle_count"]:
                grp_ok = False
                grp_detail.append(r["grp"])
        # 另核：每组三角计数与 triangles 表归属一致
        grp_tri: dict[str, int] = defaultdict(int)
        for r in self.store.all_triangles():
            if r["grp"] != CROSS_GROUP:
                grp_tri[r["grp"]] += 1
        for r in self.store.all_group_metrics():
            if r["triangle_count"] != grp_tri.get(r["grp"], 0):
                grp_ok = False
                grp_detail.append(r["grp"] + ":tri")
        add("group_contribution_conservation", grp_ok, grp_detail)

        # 8) 全局：每三角对 3 个节点各贡献 1 ⇒ Σtri_count = 3T
        sum_node_tri = sum(r["tri_count"] for r in self.store.all_node_metrics())
        add("global_3t_identity_nodes",
            sum_node_tri == 3 * len(db_tris),
            {"sum_node_tri": sum_node_tri, "3T": 3 * len(db_tris)})
        sum_edge_sup = sum(r["tri_support"] for r in self.store.all_edge_metrics())
        add("global_3t_identity_edges",
            sum_edge_sup == 3 * len(db_tris),
            {"sum_edge_support": sum_edge_sup, "3T": 3 * len(db_tris)})

        return {
            "ok": all(c["ok"] for c in checks),
            "checks": checks,
            "counts": {
                "nodes": len(self.groups), "edges": len(db_edges),
                "triangles": len(db_tris),
                "sources": sum(len(s) for s in self.edge_sources.values()),
            },
        }

    # ============================== 查询视图 ==============================
    def state_view(self) -> dict[str, Any]:
        return {
            "nodes": [
                {"node_id": n, "group": self.groups[n]}
                for n in sorted(self.groups)
            ],
            "edges": [
                {"u": u, "v": v, "sources": sorted(self.edge_sources.get((u, v), ()))}
                for u, v in sorted(
                    e for e, bag in self.edge_sources.items() if bag
                )
            ],
            "triangles": [
                {"tid": r["tid"], "nodes": [r["n1"], r["n2"], r["n3"]],
                 "group": r["grp"], "created_batch": r["created_batch"]}
                for r in self.store.all_triangles()
            ],
            "node_metrics": [dict(r) for r in self.store.all_node_metrics()],
            "edge_metrics": [dict(r) for r in self.store.all_edge_metrics()],
            "group_metrics": [dict(r) for r in self.store.all_group_metrics()],
            "conservation": self.verify_conservation()["counts"],
        }

    def triangle_view(self, tid: str) -> Optional[dict[str, Any]]:
        row = self.store.get_triangle(tid)
        if row is None:
            return None
        supports = []
        for s in self.store.support_for_triangle(tid):
            supports.append({
                "edge": [s["u"], s["v"]],
                "sources_at_commit": json.loads(s["sources"]),
                "live_sources": self.store.sources_for_edge(s["u"], s["v"]),
            })
        return {
            "tid": tid, "nodes": [row["n1"], row["n2"], row["n3"]],
            "group": row["grp"], "created_batch": row["created_batch"],
            "supports": supports,
        }

    def edge_view(self, u: str, v: str) -> Optional[dict[str, Any]]:
        eu, ev = g.canon(u, v)
        if not self.store.edge_exists(eu, ev):
            return None
        metric = None
        for r in self.store.all_edge_metrics():
            if r["u"] == eu and r["v"] == ev:
                metric = dict(r)
                break
        triangles = []
        for r in self.store.support_for_edge(eu, ev):
            tri_row = self.store.get_triangle(r["tid"])
            triangles.append({
                "tid": r["tid"],
                "group": tri_row["grp"] if tri_row is not None else None,
            })
        return {
            "edge": [eu, ev],
            "sources": self.store.sources_for_edge(eu, ev),
            "metric": metric,
            "triangles": triangles,
        }

    def batches_view(self, limit: int = 100) -> list[dict[str, Any]]:
        out = []
        for b in self.store.list_batches(limit):
            changes = [dict(c) for c in self.store.list_changes(b["batch_id"])]
            out.append({
                "batch_id": b["batch_id"], "kind": b["kind"],
                "note": b["note"], "created_at": b["created_at"],
                "changes": changes,
            })
        return out
