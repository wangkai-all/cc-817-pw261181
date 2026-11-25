"""内置样例端到端核对：星形、完全图、重复来源、批内加删，以及重启恢复。"""

from graph_service.engine import Engine
from graph_service.store import Store, tid_of

from . import GraphCase


class SampleExpectationTest(GraphCase):
    def test_star(self):
        r = self.load("star")
        st = r["state"]
        self.assertEqual(len(st["nodes"]), 6)
        self.assertEqual(len(st["edges"]), 6)
        self.assertEqual(len(st["triangles"]), 1)
        # 高阶节点 H 的定向邻接不会制造伪三角
        h = next(m for m in st["node_metrics"] if m["node_id"] == "H")
        self.assertEqual(h["tri_count"], 1)
        self.assertEqual(h["degree"], 5)

    def test_complete_cross_group_excluded_from_group_stats(self):
        r = self.load("complete")
        st = r["state"]
        self.assertEqual(len(st["triangles"]), 5)
        groups = {g["grp"]: g for g in st["group_metrics"]}
        self.assertEqual(groups["alpha"]["triangle_count"], 4)
        self.assertEqual(groups["alpha"]["edge_sum"], 12)  # 3 × 4
        self.assertEqual(groups["beta"]["triangle_count"], 0)
        # 跨组三角仍在全局事实中
        xyz = next(t for t in st["triangles"]
                   if set(t["nodes"]) == {"x", "y", "z"})
        self.assertEqual(xyz["group"], "*cross-group*")

    def test_duplicate_sources_lifecycle(self):
        r = self.load("duplicate_sources")
        # 演示批次：重复加 feed-1（幂等）+ 撤回 feed-2（边保留）
        self.assertEqual(len(r["state"]["edges"]), 3)
        self.assertEqual(len(r["state"]["triangles"]), 1)
        self.assertEqual(self.store.sources_for_edge("A", "B"), ["feed-1"])
        # 再撤最后一个来源：边与三角消失
        self.engine.apply_batch([
            {"u": "A", "v": "B", "source": "feed-1",
             "action": "withdraw_source"},
        ])
        self.assertFalse(self.store.edge_exists("A", "B"))
        self.assertEqual(len(self.store.all_triangles()), 0)
        self.assertConservation()

    def test_batch_add_remove_sample(self):
        r = self.load("batch_add_remove")
        # 首批 K4 有 4 个三角，演示批次后剩 4-环、0 个三角
        self.assertEqual(r["expect"]["triangles"], 0)
        self.assertEqual(len(r["state"]["triangles"]), 0)
        self.assertEqual(len(r["state"]["edges"]), 4)
        # pr 现在有两个来源
        self.assertEqual(
            self.store.sources_for_edge("p", "r"), ["e1", "e9"]
        )
        # 顺序模拟能观察到临时三角 qrs：从首批 K4 基线回放演示批次
        from graph_service import graph as gutil
        from graph_service.samples import get_sample
        sample = get_sample("batch_add_remove")
        k4_edges = [(u, v) for u, v, _src in sample["edges"]]
        baseline_adj = gutil.build_adj(k4_edges)
        baseline_bags = {
            gutil.canon(u, v): {src} for u, v, src in sample["edges"]
        }
        sim = self.engine.simulate_sequential(
            sample["batch"]["changes"],
            baseline_adj=baseline_adj,
            baseline_bags=baseline_bags,
        )
        ever = {tuple(t) for s in sim["steps"] for t in s["triangles"]}
        self.assertIn(("q", "r", "s"), ever)
        self.assertConservation()


class RecoveryTest(GraphCase):
    def test_restart_recovers_from_tables_without_replay(self):
        self.load("complete")
        path = self.db_path
        self.store.close()
        # 新进程视角：重新打开库，仅靠持久化表恢复
        store2 = Store(path)
        engine2 = Engine(store2)
        report = engine2.verify_conservation()
        self.assertTrue(report["ok"])
        self.assertEqual(report["counts"]["triangles"], 5)
        # 三角支持记录可查询
        view = engine2.triangle_view(tid_of(["a", "b", "c"]))
        self.assertIsNotNone(view)
        self.assertEqual(len(view["supports"]), 3)
        # 批次日志仍可审计
        self.assertGreaterEqual(len(engine2.batches_view()), 1)
        store2.close()
