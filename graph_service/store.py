"""SQLite 持久层。

表结构说明（见 README「数据模型」）：
- nodes/edges/edge_sources     图本体，业务边与来源条数分开存储
- batches/edge_changes/events  批次与操作日志（恢复/审计）
- triangles/edge_tri_support   三角事实与每条边在三角中的支持记录
- node_metrics/edge_metrics/group_metrics  聚集指标
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

# 跨群组三角在 triangles.grp 中的哨兵值，不参与任何群组指标
CROSS_GROUP = "*cross-group*"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id     TEXT PRIMARY KEY,
    grp         TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS edges (
    u          TEXT NOT NULL,
    v          TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (u, v),
    CHECK (u < v)
);

CREATE TABLE IF NOT EXISTS edge_sources (
    u          TEXT NOT NULL,
    v          TEXT NOT NULL,
    source     TEXT NOT NULL,
    batch_id   TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (u, v, source)
);

CREATE TABLE IF NOT EXISTS batches (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL,              -- edge_batch | group_move
    note       TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS edge_changes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   TEXT NOT NULL,
    seq        INTEGER NOT NULL,           -- 批内顺序（仅审计，不代表对外可见性）
    u          TEXT NOT NULL,
    v          TEXT NOT NULL,
    source     TEXT NOT NULL,
    action     TEXT NOT NULL,              -- add_source | withdraw_source
    edge_born  INTEGER NOT NULL,           -- 该变更是否令业务边从无到有
    edge_died  INTEGER NOT NULL            -- 该变更是否令业务边被删除
);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL,              -- batch | group_move
    ref_id     TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS triangles (
    tid        TEXT PRIMARY KEY,           -- 排序后三节点的 JSON 数组
    n1         TEXT NOT NULL,
    n2         TEXT NOT NULL,
    n3         TEXT NOT NULL,
    grp        TEXT NOT NULL,              -- 同组为组名；跨组为 CROSS_GROUP 哨兵
    created_batch TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS edge_tri_support (
    tid        TEXT NOT NULL,
    u          TEXT NOT NULL,
    v          TEXT NOT NULL,
    sources    TEXT NOT NULL,              -- 提交时刻该边有效来源 JSON
    PRIMARY KEY (tid, u, v)
);

CREATE TABLE IF NOT EXISTS node_metrics (
    node_id           TEXT PRIMARY KEY,
    grp               TEXT NOT NULL,
    degree            INTEGER NOT NULL,
    tri_count         INTEGER NOT NULL,    -- 该节点参与的全部三角（含跨组）
    tri_count_in_grp  INTEGER NOT NULL,    -- 仅同组三角
    clustering        REAL NOT NULL        -- 局部聚集系数
);

CREATE TABLE IF NOT EXISTS edge_metrics (
    u             TEXT NOT NULL,
    v             TEXT NOT NULL,
    grp_u         TEXT NOT NULL,
    grp_v         TEXT NOT NULL,
    tri_support   INTEGER NOT NULL,        -- 该边参与的三角数（边的贡献）
    tri_in_grp    INTEGER NOT NULL,        -- 其中同组三角数
    PRIMARY KEY (u, v)
);

CREATE TABLE IF NOT EXISTS group_metrics (
    grp            TEXT PRIMARY KEY,
    node_count     INTEGER NOT NULL,
    edge_count     INTEGER NOT NULL,       -- 两端均在本组的边
    triangle_count INTEGER NOT NULL,       -- 三节点均在本组的闭合三角
    edge_sum       INTEGER NOT NULL        -- 组内边三角贡献之和 = 3 * triangle_count
);

CREATE INDEX IF NOT EXISTS idx_sources_edge ON edge_sources(u, v);
CREATE INDEX IF NOT EXISTS idx_tri_n1 ON triangles(n1);
CREATE INDEX IF NOT EXISTS idx_tri_n2 ON triangles(n2);
CREATE INDEX IF NOT EXISTS idx_tri_n3 ON triangles(n3);
CREATE INDEX IF NOT EXISTS idx_support_edge ON edge_tri_support(u, v);
CREATE INDEX IF NOT EXISTS idx_support_tid ON edge_tri_support(tid);
CREATE INDEX IF NOT EXISTS idx_changes_batch ON edge_changes(batch_id);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def tid_of(nodes: Iterable[str]) -> str:
    """三角主键：节点排序后紧凑 JSON 序列化，天然去重，重复关系不会产生新三角。"""
    return json.dumps(sorted(nodes), ensure_ascii=False, separators=(",", ":"))


def tid_nodes(tid: str) -> tuple[str, str, str]:
    a, b, c = json.loads(tid)
    return a, b, c


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        # isolation_level=None：关闭驱动隐式事务，事务边界完全由 transaction() 控制
        # check_same_thread=False：ThreadingHTTPServer 下多线程复用连接，
        # 写串行化由 ApiState.write_lock 保证，读走 WAL 快照
        self.conn = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA foreign_keys=OFF")
        self.conn.executescript(SCHEMA)
        self.conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '1')"
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ---- 事务由 engine 用 with store.transaction() 包裹 ----
    class _Tx:
        def __init__(self, conn: sqlite3.Connection):
            self.conn = conn

        def __enter__(self):
            self.conn.execute("BEGIN IMMEDIATE")
            return self

        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            return False

    def transaction(self) -> "_Tx":
        return self._Tx(self.conn)

    # ---- 节点 ----
    def upsert_node(self, node_id: str, grp: str) -> None:
        ts = now_iso()
        self.conn.execute(
            "INSERT INTO nodes(node_id, grp, created_at, updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(node_id) DO UPDATE SET grp=excluded.grp, updated_at=excluded.updated_at",
            (node_id, grp, ts, ts),
        )

    def ensure_node(self, node_id: str, grp: str) -> None:
        """不存在才插入（边明细引用节点时使用，不覆盖既有群组）。"""
        self.conn.execute(
            "INSERT OR IGNORE INTO nodes(node_id, grp, created_at, updated_at) VALUES(?,?,?,?)",
            (node_id, grp, now_iso(), now_iso()),
        )

    def set_node_group(self, node_id: str, grp: str) -> None:
        self.conn.execute(
            "UPDATE nodes SET grp=?, updated_at=? WHERE node_id=?",
            (grp, now_iso(), node_id),
        )

    def get_node(self, node_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()

    def all_nodes(self) -> dict[str, str]:
        return {r["node_id"]: r["grp"] for r in self.conn.execute("SELECT node_id, grp FROM nodes")}

    # ---- 边与来源 ----
    def insert_edge(self, u: str, v: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO edges(u, v, created_at) VALUES(?,?,?)",
            (u, v, now_iso()),
        )

    def delete_edge(self, u: str, v: str) -> None:
        self.conn.execute("DELETE FROM edges WHERE u=? AND v=?", (u, v))

    def edge_exists(self, u: str, v: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM edges WHERE u=? AND v=?", (u, v)
        ).fetchone() is not None

    def all_edges(self) -> list[tuple[str, str]]:
        return [(r["u"], r["v"]) for r in self.conn.execute("SELECT u, v FROM edges")]

    def insert_source(self, u: str, v: str, source: str, batch_id: str) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO edge_sources(u, v, source, batch_id, created_at) "
            "VALUES(?,?,?,?,?)",
            (u, v, source, batch_id, now_iso()),
        )
        return cur.rowcount > 0

    def delete_source(self, u: str, v: str, source: str) -> bool:
        cur = self.conn.execute(
            "DELETE FROM edge_sources WHERE u=? AND v=? AND source=?",
            (u, v, source),
        )
        return cur.rowcount > 0

    def sources_for_edge(self, u: str, v: str) -> list[str]:
        return [
            r["source"]
            for r in self.conn.execute(
                "SELECT source FROM edge_sources WHERE u=? AND v=? ORDER BY source", (u, v)
            )
        ]

    def all_edge_sources(self) -> dict[tuple[str, str], set[str]]:
        out: dict[tuple[str, str], set[str]] = {}
        for r in self.conn.execute("SELECT u, v, source FROM edge_sources"):
            out.setdefault((r["u"], r["v"]), set()).add(r["source"])
        return out

    # ---- 批次 / 日志 ----
    def insert_batch(self, batch_id: str, kind: str, note: Optional[str]) -> None:
        self.conn.execute(
            "INSERT INTO batches(batch_id, kind, note, created_at) VALUES(?,?,?,?)",
            (batch_id, kind, note, now_iso()),
        )

    def insert_change(
        self, batch_id: str, seq: int, u: str, v: str, source: str,
        action: str, edge_born: bool, edge_died: bool,
    ) -> None:
        self.conn.execute(
            "INSERT INTO edge_changes(batch_id, seq, u, v, source, action, edge_born, edge_died) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (batch_id, seq, u, v, source, action, int(edge_born), int(edge_died)),
        )

    def insert_event(self, kind: str, ref_id: str, payload: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO events(kind, ref_id, payload, created_at) VALUES(?,?,?,?)",
            (kind, ref_id, json.dumps(payload, ensure_ascii=False), now_iso()),
        )

    def list_batches(self, limit: int = 100) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM batches ORDER BY seq DESC LIMIT ?", (limit,)
            )
        )

    def list_changes(self, batch_id: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM edge_changes WHERE batch_id=? ORDER BY seq", (batch_id,)
            )
        )

    # ---- 三角事实 ----
    def put_triangle(
        self, tid: str, nodes: tuple[str, str, str], grp: str, batch_id: str
    ) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO triangles(tid, n1, n2, n3, grp, created_batch, created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (tid, nodes[0], nodes[1], nodes[2], grp, batch_id, now_iso()),
        )

    def delete_triangle(self, tid: str) -> None:
        self.conn.execute("DELETE FROM triangles WHERE tid=?", (tid,))

    def get_triangle(self, tid: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM triangles WHERE tid=?", (tid,)
        ).fetchone()

    def all_triangles(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM triangles ORDER BY tid"))

    def triangles_touching(self, node_ids: Iterable[str]) -> list[sqlite3.Row]:
        nodes = list(node_ids)
        if not nodes:
            return []
        marks = ",".join("?" for _ in nodes)
        q = (
            f"SELECT * FROM triangles WHERE n1 IN ({marks}) OR n2 IN ({marks}) "
            f"OR n3 IN ({marks})"
        )
        return list(self.conn.execute(q, nodes * 3))

    def retriangle_group(self, tid: str, grp: str) -> None:
        self.conn.execute("UPDATE triangles SET grp=? WHERE tid=?", (grp, tid))

    # ---- 三角支持记录 ----
    def replace_support(self, tid: str, u: str, v: str, sources: list[str]) -> None:
        self.conn.execute(
            "INSERT INTO edge_tri_support(tid, u, v, sources) VALUES(?,?,?,?) "
            "ON CONFLICT(tid, u, v) DO UPDATE SET sources=excluded.sources",
            (tid, u, v, json.dumps(sorted(sources), ensure_ascii=False)),
        )

    def delete_support_triangle(self, tid: str) -> None:
        self.conn.execute("DELETE FROM edge_tri_support WHERE tid=?", (tid,))

    def delete_support_edge(self, u: str, v: str) -> None:
        self.conn.execute(
            "DELETE FROM edge_tri_support WHERE u=? AND v=?", (u, v)
        )

    def support_for_triangle(self, tid: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM edge_tri_support WHERE tid=? ORDER BY u, v", (tid,)
            )
        )

    def support_for_edge(self, u: str, v: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT * FROM edge_tri_support WHERE u=? AND v=? ORDER BY tid", (u, v)
            )
        )

    # ---- 指标 ----
    def upsert_node_metric(
        self, node_id: str, grp: str, degree: int, tri_count: int,
        tri_in_grp: int, clustering: float,
    ) -> None:
        self.conn.execute(
            "INSERT INTO node_metrics(node_id, grp, degree, tri_count, tri_count_in_grp, clustering) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET "
            "grp=excluded.grp, degree=excluded.degree, tri_count=excluded.tri_count, "
            "tri_count_in_grp=excluded.tri_count_in_grp, clustering=excluded.clustering",
            (node_id, grp, degree, tri_count, tri_in_grp, clustering),
        )

    def delete_node_metric(self, node_id: str) -> None:
        self.conn.execute("DELETE FROM node_metrics WHERE node_id=?", (node_id,))

    def all_node_metrics(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM node_metrics ORDER BY node_id"))

    def upsert_edge_metric(
        self, u: str, v: str, grp_u: str, grp_v: str, tri_support: int, tri_in_grp: int
    ) -> None:
        self.conn.execute(
            "INSERT INTO edge_metrics(u, v, grp_u, grp_v, tri_support, tri_in_grp) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(u, v) DO UPDATE SET grp_u=excluded.grp_u, "
            "grp_v=excluded.grp_v, tri_support=excluded.tri_support, tri_in_grp=excluded.tri_in_grp",
            (u, v, grp_u, grp_v, tri_support, tri_in_grp),
        )

    def delete_edge_metric(self, u: str, v: str) -> None:
        self.conn.execute("DELETE FROM edge_metrics WHERE u=? AND v=?", (u, v))

    def all_edge_metrics(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM edge_metrics ORDER BY u, v"))

    def upsert_group_metric(
        self, grp: str, node_count: int, edge_count: int,
        triangle_count: int, edge_sum: int,
    ) -> None:
        self.conn.execute(
            "INSERT INTO group_metrics(grp, node_count, edge_count, triangle_count, edge_sum) "
            "VALUES(?,?,?,?,?) ON CONFLICT(grp) DO UPDATE SET node_count=excluded.node_count, "
            "edge_count=excluded.edge_count, triangle_count=excluded.triangle_count, "
            "edge_sum=excluded.edge_sum",
            (grp, node_count, edge_count, triangle_count, edge_sum),
        )

    def delete_group_metric(self, grp: str) -> None:
        self.conn.execute("DELETE FROM group_metrics WHERE grp=?", (grp,))

    def all_group_metrics(self) -> list[sqlite3.Row]:
        return list(self.conn.execute("SELECT * FROM group_metrics ORDER BY grp"))

    def adjust_group_metric(self, grp: str, d_edges: int, d_tri: int) -> None:
        """增量调整群组指标；组消失（无节点）时由 engine 删除整行。"""
        self.conn.execute(
            "UPDATE group_metrics SET edge_count=edge_count+?, "
            "triangle_count=triangle_count+?, edge_sum=edge_sum+? WHERE grp=?",
            (d_edges, d_tri, 3 * d_tri, grp),
        )

    def wipe(self) -> None:
        """清空全部业务数据（保留表结构），用于重新导入样例。"""
        for tbl in (
            "edge_tri_support", "triangles", "edge_changes", "edge_sources",
            "edges", "node_metrics", "edge_metrics", "group_metrics",
            "events", "batches", "nodes",
        ):
            self.conn.execute(f"DELETE FROM {tbl}")
        self.conn.execute("DELETE FROM sqlite_sequence")
        self.conn.commit()
