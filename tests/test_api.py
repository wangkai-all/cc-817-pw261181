"""HTTP API 端到端测试：服务在线程中启动，用 urllib 打真实端口。"""

import json
import tempfile
import threading
import urllib.error
import urllib.request
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from graph_service.api import Handler


def _request(url: str, payload=None, method="GET"):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        db = str(Path(cls._tmp.name) / "api.db")
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        from graph_service.api import ApiState
        cls.server.state = ApiState(db)
        cls.port = cls.server.server_address[1]
        cls.base = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.state.store.close()
        cls.server.server_close()
        cls._tmp.cleanup()

    def test_01_health_and_index(self):
        status, body = _request(self.base + "/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])
        with urllib.request.urlopen(self.base + "/", timeout=10) as resp:
            self.assertIn("text/html", resp.headers["Content-Type"])

    def test_02_load_sample_and_state(self):
        status, body = _request(self.base + "/api/load-sample",
                                {"sample": "complete"}, "POST")
        self.assertEqual(status, 200)
        self.assertTrue(body["conservation"]["ok"])
        status, state = _request(self.base + "/api/state")
        self.assertEqual(status, 200)
        self.assertEqual(len(state["triangles"]), 5)

    def test_03_batch_with_duplicate_sources(self):
        status, body = _request(self.base + "/api/batch", {
            "note": "api duplicate",
            "changes": [
                {"u": "a", "v": "b", "source": "s1",
                 "action": "add_source"},  # 已存在，幂等
                {"u": "a", "v": "b", "source": "s9",
                 "action": "add_source"},
            ],
        }, "POST")
        self.assertEqual(status, 200, body)
        self.assertTrue(body["conservation"]["ok"])
        self.assertEqual(body["triangle_delta"]["added"], [])

    def test_04_compare_and_simulate(self):
        changes = [
            {"u": "a", "v": "b", "source": "s9", "action": "withdraw_source"},
            {"u": "a", "v": "b", "source": "s10", "action": "add_source"},
        ]
        status, cmp_body = _request(self.base + "/api/compare",
                                    {"changes": changes}, "POST")
        self.assertEqual(status, 200)
        self.assertTrue(cmp_body["equal"])
        status, sim = _request(self.base + "/api/simulate",
                               {"changes": changes}, "POST")
        self.assertEqual(status, 200)
        self.assertEqual(len(sim["steps"]), 2)

    def test_05_triangle_and_edge_views(self):
        tid = json.dumps(["a", "b", "c"], ensure_ascii=False, separators=(",", ":"))
        status, tri = _request(
            self.base + "/api/triangle/" + urllib.parse.quote(tid, safe=""))
        self.assertEqual(status, 200)
        self.assertEqual(len(tri["supports"]), 3)
        status, edge = _request(self.base + "/api/edge/a/b")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(edge["metric"]["tri_support"], 1)
        status, missing = _request(self.base + "/api/triangle/%5B%22zzz%22%5D")
        self.assertEqual(status, 404)

    def test_06_move_node_and_rebuild(self):
        status, moved = _request(self.base + "/api/move-node",
                                 {"node_id": "d", "group": "beta"}, "POST")
        self.assertEqual(status, 200)
        self.assertTrue(moved["conservation"]["ok"])
        status, rebuilt = _request(self.base + "/api/rebuild",
                                   {"note": "api rebuild"}, "POST")
        self.assertEqual(status, 200)
        self.assertTrue(rebuilt["conservation"]["ok"])

    def test_07_verify_and_batches(self):
        status, report = _request(self.base + "/api/verify")
        self.assertEqual(status, 200)
        self.assertTrue(report["ok"])
        status, batches = _request(self.base + "/api/batches")
        self.assertEqual(status, 200)
        self.assertGreater(len(batches["batches"]), 0)
    def test_08_validation_errors(self):
        status, body = _request(self.base + "/api/batch",
                                {"changes": []}, "POST")
        self.assertEqual(status, 400)
        status, body = _request(self.base + "/api/batch", {
            "changes": [{"u": "a", "v": "b", "source": "ghost",
                         "action": "withdraw_source"}]
        }, "POST")
        self.assertEqual(status, 400)
        status, body = _request(self.base + "/api/unknown", {}, "POST")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
