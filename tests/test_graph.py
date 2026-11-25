"""图算法层：规范化、定向邻接、分区交集、预算回退。"""

import unittest

from graph_service import graph as g


class GraphAlgorithmTest(unittest.TestCase):
    def test_canon_dedupes_undirected_edge(self):
        self.assertEqual(g.canon("b", "a"), ("a", "b"))
        self.assertEqual(g.canon("a", "b"), ("a", "b"))

    def test_orient_is_dag_with_one_arc_per_edge(self):
        edges = [("a", "b"), ("b", "c"), ("a", "c"), ("c", "d")]
        adj = g.build_adj(edges)
        out = g.orient(adj)
        arc_count = sum(len(v) for v in out.values())
        self.assertEqual(arc_count, len(edges))  # 每条无向边恰好一条弧
        # 弧总是从低秩指向高秩
        for u, outs in out.items():
            for v in outs:
                self.assertLess(g.rank(adj, u), g.rank(adj, v))

    def test_triangle_enumeration_k4(self):
        # K4：4 个三角，每个恰好枚举一次
        nodes = ["a", "b", "c", "d"]
        edges = [(x, y) for i, x in enumerate(nodes) for y in nodes[i + 1:]]
        tris = g.enumerate_triangles(g.build_adj(edges))
        self.assertEqual(len(tris), 4)
        self.assertEqual(len({t for t in tris}), 4)

    def test_star_has_no_triangle(self):
        edges = [("H", f"L{i}") for i in range(1, 6)]
        self.assertEqual(g.enumerate_triangles(g.build_adj(edges)), set())

    def test_common_neighbors_partitioned(self):
        adj = g.build_adj([("u", "w1"), ("v", "w1"), ("u", "w2"), ("v", "x")])
        got = set(g.common_neighbors("u", "v", adj, chunk=1, budget=100))
        self.assertEqual(got, {"w1"})

    def test_budget_raises_then_full_rebuild_possible(self):
        # 大星形 + 一条叶子边：预算 0 时任何探测都立即触发
        edges = [("H", f"L{i}") for i in range(1, 40)] + [("L1", "L2")]
        adj = g.build_adj(edges)
        with self.assertRaises(g.WorkBudgetExceeded):
            list(g.common_neighbors("H", "L1", adj, chunk=4, budget=0))
        with self.assertRaises(g.WorkBudgetExceeded):
            g.enumerate_triangles(adj, chunk=4, budget=0)
        # 放大预算后结果正确——回退路径的正确性基础
        tris = g.enumerate_triangles(adj, chunk=1024, budget=10_000_000)
        self.assertEqual(tris, {("H", "L1", "L2")})

    def test_clustering_coefficient(self):
        self.assertEqual(g.clustering_coefficient(0, 0), 0.0)
        self.assertEqual(g.clustering_coefficient(1, 0), 0.0)
        # 度 3、参与 1 个三角：3 / C(3,2) = 1.0
        self.assertEqual(g.clustering_coefficient(3, 1), 1.0)
        # 度 4、1 个三角：3 / 6 = 0.5
        self.assertEqual(g.clustering_coefficient(4, 1), 0.5)


if __name__ == "__main__":
    unittest.main()
