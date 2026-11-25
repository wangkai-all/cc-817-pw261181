"""标准库 HTTP 服务：JSON API + 单页网页。

正式查询端点（state/triangle/edge/...）只读已提交数据；批次提交在单事务内
完成，因此并发查询永远看不到批内临时状态。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

from .engine import Engine, EngineError
from .loader import load_sample
from .samples import SAMPLES, sample_names
from .store import Store, tid_of

WEB_DIR = Path(__file__).parent / "web"


class ApiState:
    """单引擎共享状态；写操作串行化，读操作走 WAL 快照。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.store = Store(db_path)
        self.engine = Engine(self.store)
        self.write_lock = threading.Lock()


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: Any) -> None:
    payload = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


class Handler(BaseHTTPRequestHandler):
    server_version = "GraphService/1.0"

    # ---- 工具 ----
    @property
    def state(self) -> ApiState:
        return self.server.state  # type: ignore[attr-defined]

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise EngineError(f"请求体不是合法 JSON: {exc}")
        if not isinstance(data, dict):
            raise EngineError("请求体必须是 JSON 对象")
        return data

    def log_message(self, fmt: str, *args: Any) -> None:
        pass  # 静默；错误由响应体承载

    # ---- 路由 ----
    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            # self.path 仍是百分号编码的原始路径，先解码再路由
            path = unquote(parsed.path).rstrip("/") or "/"
            qs = parse_qs(parsed.query)
            if path == "/":
                return self._serve_index()
            if path == "/api/health":
                return _json_response(self, 200, {"ok": True})
            if path == "/api/state":
                return _json_response(self, 200, self.state.engine.state_view())
            if path == "/api/samples":
                return _json_response(self, 200, {"samples": sample_names(),
                                                  "details": SAMPLES})
            if path.startswith("/api/triangle/"):
                raw_tid = path.rsplit("/", 1)[1]
                # 容忍手工输入带空格的 JSON：解析后按 store 规则重新序列化
                try:
                    norm_tid = tid_of(json.loads(raw_tid))
                except json.JSONDecodeError:
                    norm_tid = raw_tid
                view = self.state.engine.triangle_view(norm_tid)
                return _json_response(self, 200 if view else 404,
                                      view or {"error": "三角不存在"})
            if path.startswith("/api/edge/"):
                parts = path.split("/")  # /api/edge/u/v
                if len(parts) != 5:
                    raise EngineError("边查询路径应为 /api/edge/<u>/<v>")
                view = self.state.engine.edge_view(parts[3], parts[4])
                return _json_response(self, 200 if view else 404,
                                      view or {"error": "边不存在"})
            if path == "/api/batches":
                limit = int(qs.get("limit", ["100"])[0])
                return _json_response(self, 200,
                                      {"batches": self.state.engine.batches_view(limit)})
            if path == "/api/verify":
                return _json_response(self, 200,
                                      self.state.engine.verify_conservation())
            _json_response(self, 404, {"error": f"未知路径 {path}"})
        except EngineError as exc:
            _json_response(self, 400, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - 兜底
            _json_response(self, 500, {"error": f"内部错误: {exc}"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/")
            data = self._read_body()
            if path == "/api/batch":
                changes = data.get("changes")
                if not isinstance(changes, list) or not changes:
                    raise EngineError("changes 必须是非空数组")
                note = data.get("note")
                with self.state.write_lock:
                    result = self.state.engine.apply_batch(changes, note=note)
                    result["conservation"] = self.state.engine.verify_conservation()
                return _json_response(self, 200, result)
            if path == "/api/compare":
                changes = data.get("changes")
                if not isinstance(changes, list) or not changes:
                    raise EngineError("changes 必须是非空数组")
                return _json_response(self, 200,
                                      self.state.engine.compare_plan(changes))
            if path == "/api/simulate":
                changes = data.get("changes")
                if not isinstance(changes, list) or not changes:
                    raise EngineError("changes 必须是非空数组")
                return _json_response(self, 200,
                                      self.state.engine.simulate_sequential(changes))
            if path == "/api/move-node":
                node_id = str(data["node_id"])
                new_group = str(data["group"])
                with self.state.write_lock:
                    result = self.state.engine.move_node_group(node_id, new_group)
                    result["conservation"] = self.state.engine.verify_conservation()
                return _json_response(self, 200, result)
            if path == "/api/rebuild":
                note = data.get("note", "manual rebuild via api")
                with self.state.write_lock:
                    result = self.state.engine.rebuild(note)
                    result["conservation"] = self.state.engine.verify_conservation()
                return _json_response(self, 200, result)
            if path == "/api/load-sample":
                name = str(data["sample"])
                with self.state.write_lock:
                    result = load_sample(
                        self.state.store, name,
                        apply_demo_batch=bool(data.get("apply_demo_batch", True)),
                    )
                    # 引擎持有的内存态已被 wipe/重建，重新装载
                    self.state.engine = Engine(self.state.store)
                    result["conservation"] = self.state.engine.verify_conservation()
                return _json_response(self, 200, _strip_heavy(result))
            _json_response(self, 404, {"error": f"未知路径 {path}"})
        except EngineError as exc:
            _json_response(self, 400, {"error": str(exc)})
        except KeyError as exc:
            _json_response(self, 400, {"error": f"缺少字段: {exc}"})
        except Exception as exc:  # pragma: no cover - 兜底
            _json_response(self, 500, {"error": f"内部错误: {exc}"})

    def _serve_index(self) -> None:
        index = WEB_DIR / "index.html"
        if not index.exists():
            return _json_response(self, 404, {"error": "网页缺失"})
        payload = index.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def _strip_heavy(result: dict[str, Any]) -> dict[str, Any]:
    """样例装载响应去掉完整 state（网页会另行拉取），保留核对信息。"""
    out = dict(result)
    out.pop("state", None)
    return out


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8000) -> None:
    server = ThreadingHTTPServer((host, port), Handler)
    server.state = ApiState(db_path)  # type: ignore[attr-defined]
    print(f"图数据加工服务已启动: http://{host}:{port}/  (db={db_path})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n关闭中…")
    finally:
        server.state.store.close()
        server.server_close()
