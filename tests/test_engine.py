"""引擎核心语义：来源生命周期、批次原子性、临时三角、三边同变、回退与幂等。"""

from graph_service import graph as g
from graph_service.engine import Engine
from graph_service.store import CROSS_GROUP, Store, tid_of

from . import GraphCase


def add(u, v, source):
    return {"u": u, "v": v, "source": source, "action": "add_source"}


def withdraw(u, v, source):
    return {"u": u, "v": v, "source": source, "action": "withdraw_source"}


class SourceLifecycleTest(GraphCase):
    def _seed_triangle(self):
        self.engine.apply_batch(
            [add("A", "B", "f1"), add("B", "C", "f1"), add("A", "C", "f1")],
            note="seed triangle",
        )

    def test_duplicate_source_is_one_edge_one_triangle(self):
        self._seed_triangle()
        self.assertEqual(len(self.store.all_edges()), 3)
        self.assertEqual(len(self.store.all_triangles()), 1)
        # 同来源重复提交：幂等，无新边无新三角
        self.engine.apply_batch([add("A", "B", "f1")], note="dup same source")
        self.assertEqual(len(self.store.all_edges()), 3)
        self.assertEqual(len(self.store.all_triangles()), 1)
        # 第二个来源仍只是同一条业务边
        self.engine.apply_batch([add("A", "B", "f2")], note="second source")
        self.assertEqual(len(self.store.all_edges()), 3)
        self.assertEqual(len(self.store.all_triangles()), 1)
        self.assertEqual(
            self.store.sources_for_edge("A", "B"), ["f1", "f2"]
        )
        self.assertConservation()

    def test_withdraw_one_source_keeps_edge(self):
        self._seed_triangle()
        self.engine.apply_batch([add("A", "B", "f2")])
        self.engine.apply_batch([withdraw("A", "B", "f1")])
        self.assertTrue(self.store.edge_exists("A", "B"))
        self.assertEqual(self.store.sources_for_edge("A", "B"), ["f2"])
        self.assertEqual(len(self.store.all_triangles()), 1)  # 三角仍在
        self.assertConservation()

    def test_withdraw_last_source_deletes_edge_and_triangle(self):
        self._seed_triangle()
        self.engine.apply_batch([withdraw("A", "B", "f1")])
        self.assertFalse(self.store.edge_exists("A", "B"))
        self.assertEqual(self.store.sources_for_edge("A", "B"), [])
        self.assertEqual(len(self.store.all_triangles()), 0)
        # 支持记录与边指标一并清理
        self.assertEqual(self.store.support_for_edge("A", "B"), [])
        self.assertIsNone(
            next((r for r in self.store.all_edge_metrics()
                  if r["u"] == "A" and r["v"] == "B"), None)
        )
        self.assertConservation()

    def test_withdraw_unknown_source_rejected(self):
        self._seed_triangle()
        with self.assertRaises(Exception):
            self.engine.apply_batch([withdraw("A", "B", "ghost")])
        # 拒绝后状态不变
        self.assertEqual(len(self.store.all_triangles()), 1)
        self.assertConservation()


class BatchAtomicityTest(GraphCase):
    def _seed_k4(self):
        edges = [("p", "q"), ("p", "r"), ("p", "s"),
                 ("q", "r"), ("q", "s"), ("r", "s")]
        for n in {x for e in edges for x in e}:
            self.store.ensure_node(n, "g1")
            self.engine.groups[n] = "g1"
            self.engine.adj.setdefault(n, set())
        self.engine.apply_batch(
            [add(u, v, "e1") for u, v in edges], note="K4"
        )
        self.assertEqual(len(self.store.all_triangles()), 4)

    def test_transient_triangle_in_sequential_but_not_committed(self):
        self._seed_k4()
        changes = [
            withdraw("p", "q", "e1"),
            add("p", "r", "e9"),
            withdraw("r", "s", "e1"),
        ]
        # 顺序模拟：步骤 0/1 出现临时三角 qrs
        sim = self.engine.simulate_sequential(changes)
        sequential_sets = [set(map(tuple, s["triangles"])) for s in sim["steps"]]
        self.assertIn(("q", "r", "s"), sequential_sets[0])
        self.assertIn(("q", "r", "s"), sequential_sets[1])

        # 正式提交：整批净结果，临时三角从未落库，最终 4→0
        result = self.engine.apply_batch(changes, note="atomic batch")
        self.assertEqual(result["triangle_delta"]["added"], [])
        removed = {tuple(t) for t in result["triangle_delta"]["removed"]}
        self.assertEqual(len(removed), 4)
        # qrs 是批内临时三角：它作为「被删除的旧三角」出现，但从未作为新增落库
        self.assertIn(("q", "r", "s"), removed)
        self.assertEqual(len(self.store.all_triangles()), 0)
        self.assertConservation()

    def test_triangle_with_all_three_edges_changing_counts_once(self):
        # 三角 ABC 的三条边同批各加一个来源（边都已存在）：三角不消失不新增，只计 0 次变化
        self.engine.apply_batch(
            [add("A", "B", "x"), add("B", "C", "x"), add("A", "C", "x")]
        )
        before = self.triangle_ids()
        result = self.engine.apply_batch([
            add("A", "B", "y"), add("B", "C", "y"), add("A", "C", "y"),
        ], note="all three edges change")
        self.assertEqual(result["triangle_delta"]["added"], [])
        self.assertEqual(result["triangle_delta"]["removed"], [])
        self.assertEqual(self.triangle_ids(), before)
        # 每条边两个来源
        for u, v in [("A", "B"), ("B", "C"), ("A", "C")]:
            self.assertEqual(len(self.store.sources_for_edge(u, v)), 2)
        self.assertConservation()

    def test_mixed_add_remove_adjacent_edges_one_batch(self):
        self._seed_k4()
        # 删 pq（破 pqr/pqs），同时新增 r-s 的第二来源（边已在）：净变化只来自删边
        result = self.engine.apply_batch([
            withdraw("p", "q", "e1"), add("r", "s", "backup"),
        ])
        removed = {tuple(t) for t in result["triangle_delta"]["removed"]}
        self.assertEqual(removed, {("p", "q", "r"), ("p", "q", "s")})
        self.assertTrue(self.store.edge_exists("r", "s"))
        self.assertEqual(
            self.store.sources_for_edge("r", "s"), ["backup", "e1"]
        )
        self.assertConservation()

    def test_incremental_matches_full_rebuild(self):
        self._seed_k4()
        changes = [
            withdraw("p", "q", "e1"),
            add("p", "r", "e9"),
            withdraw("r", "s", "e1"),
        ]
        cmp = self.engine.compare_plan(changes)
        self.assertTrue(cmp["equal"])

    def test_dry_run_does_not_mutate(self):
        self._seed_k4()
        before = self.triangle_ids()
        dry = self.engine.apply_batch(
            [withdraw("p", "q", "e1")], dry_run=True
        )
        self.assertFalse(dry["persisted"])
        self.assertEqual(self.triangle_ids(), before)


class RebuildAndFallbackTest(GraphCase):
    def test_rebuild_idempotent_and_matches_incremental(self):
        result = self.load("complete")
        # 再做一个增量批次
        self.engine.apply_batch([add("a", "b", "extra-src"),
                                 withdraw("x", "y", "s2")])
        view_before = self.engine.state_view()
        self.engine.rebuild("rebuild 1")
        view_one = self.engine.state_view()
        self.engine.rebuild("rebuild 2")
        view_two = self.engine.state_view()
        # 两次重建逐行一致（幂等）
        self.assertEqual(_stable(view_one), _stable(view_two))
        # 重建不改变任何业务事实与指标
        for key in ("triangles", "edge_metrics", "node_metrics", "group_metrics"):
            self.assertEqual(_stable(view_one)[key], _stable(view_before)[key])
        self.assertConservation()

    def test_budget_fallback_produces_correct_state(self):
        tiny = Engine(self.store, work_budget=1)
        for n, grp in [("a", "g"), ("b", "g"), ("c", "g")]:
            self.store.ensure_node(n, grp)
            tiny.groups[n] = grp
            tiny.adj.setdefault(n, set())
        result = tiny.apply_batch([
            add("a", "b", "s"), add("b", "c", "s"), add("a", "c", "s"),
        ], note="budget forced fallback")
        self.assertEqual(result["mode"], "full_rebuild")
        self.assertEqual(result["triangle_count"], 1)
        self.assertEqual(len(self.store.all_triangles()), 1)
        self.assertTrue(tiny.verify_conservation()["ok"])

    def test_fallback_with_brand_new_nodes_registers_groups(self):
        # 全新节点（不预先登记）+ 预算 0 强制回退：不能因内存缺群组而 KeyError
        tiny = Engine(self.store, work_budget=0)
        result = tiny.apply_batch([
            add("n1", "n2", "s"), add("n2", "n3", "s"), add("n1", "n3", "s"),
        ], note="fallback with new nodes")
        self.assertEqual(result["mode"], "full_rebuild")
        self.assertEqual(result["triangle_count"], 1)
        # 新节点落入 default 组，三角为同组三角
        tid = tid_of(["n1", "n2", "n3"])
        self.assertEqual(self.store.get_triangle(tid)["grp"], "default")
        self.assertTrue(tiny.verify_conservation()["ok"])


class GroupMoveTest(GraphCase):
    def test_move_rebuilds_attribution_without_copying_counts(self):
        self.load("complete")
        tid_xyz = tid_of(["x", "y", "z"])
        self.assertEqual(self.store.get_triangle(tid_xyz)["grp"], CROSS_GROUP)

        before_beta = next(
            r for r in self.store.all_group_metrics() if r["grp"] == "beta"
        )
        self.assertEqual(before_beta["triangle_count"], 0)

        moved = self.engine.move_node_group("z", "beta")
        self.assertEqual(moved["retriangulated"], 1)
        # 归属重判：xyz 变为 beta 同组三角
        self.assertEqual(self.store.get_triangle(tid_xyz)["grp"], "beta")
        after_beta = next(
            r for r in self.store.all_group_metrics() if r["grp"] == "beta"
        )
        self.assertEqual(after_beta["triangle_count"], 1)
        self.assertEqual(after_beta["edge_sum"], 3)
        self.assertConservation()

        # 迁回去：归属与组指标完全恢复（重建而非复制旧计数的可恢复性）
        self.engine.move_node_group("z", "alpha")
        self.assertEqual(self.store.get_triangle(tid_xyz)["grp"], CROSS_GROUP)
        restored = next(
            r for r in self.store.all_group_metrics() if r["grp"] == "beta"
        )
        self.assertEqual(dict(restored), dict(before_beta))
        self.assertConservation()

    def test_move_keeps_global_triangle_count(self):
        self.load("complete")
        before = len(self.store.all_triangles())
        self.engine.move_node_group("a", "beta")
        self.assertEqual(len(self.store.all_triangles()), before)
        self.assertConservation()


def _stable(view: dict) -> dict:
    return {k: view[k] for k in
            ("triangles", "edges", "nodes", "edge_metrics",
             "node_metrics", "group_metrics")}
