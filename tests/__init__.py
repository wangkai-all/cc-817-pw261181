"""测试公共夹具。

所有测试用临时目录中的独立 SQLite 文件，测试结束自动清理；
测试代码与库代码都在仓库根下，以 `python -I -m unittest discover -s tests -t .`
从仓库根运行（-t . 把根目录加入 sys.path，-I 仍隔离环境污染）。
"""

import tempfile
import unittest
from pathlib import Path

from graph_service.engine import Engine
from graph_service.loader import load_sample
from graph_service.store import Store, tid_of


class GraphCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "test.db")
        self.store = Store(self.db_path)
        self.engine = Engine(self.store)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def load(self, name: str, demo: bool = True) -> dict:
        result = load_sample(self.store, name, apply_demo_batch=demo)
        # load_sample 内部新建了 Engine；同步本夹具的引擎句柄
        self.engine = Engine(self.store)
        self.assertTrue(
            result["conservation"]["ok"],
            msg=f"样例 {name} 守恒失败: "
                + str([c for c in result["conservation"]["checks"] if not c["ok"]]),
        )
        return result

    def assertConservation(self) -> None:
        report = self.engine.verify_conservation()
        self.assertTrue(
            report["ok"],
            msg="守恒失败: " + str([c for c in report["checks"] if not c["ok"]]),
        )

    def triangle_ids(self) -> set[str]:
        return {r["tid"] for r in self.store.all_triangles()}

    def tid(self, *nodes: str) -> str:
        return tid_of(nodes)
