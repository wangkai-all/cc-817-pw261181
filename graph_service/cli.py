"""命令行入口：启动服务 / 装载样例 / 跑核对。

示例：
  python -I -m graph_service.cli serve --db data/graph.db --port 8000
  python -I -m graph_service.cli load-sample --db data/demo.db --sample star
  python -I -m graph_service.cli verify --db data/demo.db
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .api import serve
from .engine import Engine
from .loader import load_sample
from .samples import sample_names
from .store import Store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="图数据加工服务")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="启动 HTTP 服务与网页")
    p_serve.add_argument("--db", required=True)
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)

    p_load = sub.add_parser("load-sample", help="清空并装载内置样例")
    p_load.add_argument("--db", required=True)
    p_load.add_argument("--sample", required=True, choices=sample_names())
    p_load.add_argument("--no-demo-batch", action="store_true")

    p_verify = sub.add_parser("verify", help="对现有库运行守恒核对")
    p_verify.add_argument("--db", required=True)

    p_batch = sub.add_parser("batch", help="从 JSON 文件提交一个来源级批次")
    p_batch.add_argument("--db", required=True)
    p_batch.add_argument("--file", required=True, help="JSON：{changes:[...], note?}")

    args = parser.parse_args(argv)
    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)

    if args.cmd == "serve":
        serve(args.db, args.host, args.port)
        return 0

    store = Store(args.db)
    try:
        if args.cmd == "load-sample":
            result = load_sample(
                store, args.sample,
                apply_demo_batch=not args.no_demo_batch,
            )
            print(json.dumps({
                "sample": result["sample"],
                "expect": result["expect"],
                "conservation": result["conservation"],
                "first_batch_mode": result["first_batch"]["mode"],
                "demo_batch_mode": result["demo_batch"]["mode"]
                if result["demo_batch"] else None,
            }, ensure_ascii=False, indent=2))
            return 0 if result["conservation"]["ok"] else 2

        engine = Engine(store)
        if args.cmd == "verify":
            print(json.dumps(engine.verify_conservation(), ensure_ascii=False, indent=2))
            return 0 if engine.verify_conservation()["ok"] else 2

        if args.cmd == "batch":
            with open(args.file, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            result = engine.apply_batch(payload["changes"],
                                        note=payload.get("note"))
            result["conservation"] = engine.verify_conservation()
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result["conservation"]["ok"] else 2
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
