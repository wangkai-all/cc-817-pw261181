"""图数据加工服务：多来源边、批次化增量三角统计与节点聚集指标。

模块划分：
- store:   SQLite 持久化（边/来源/批次日志/三角事实/指标）
- graph:   无状态图算法：规范化边、定向邻接、分区交集、三角枚举
- engine:  核心引擎：批次规划与原子提交、全量重建、群组迁移、守恒校验、全量 vs 增量对比
- samples: 虚构边明细样例
- api:     标准库 HTTP 服务 + 静态网页
"""

__version__ = "1.0.0"

# 跨群组三角在 triangles.grp 中的哨兵值，不参与任何群组指标
from .store import CROSS_GROUP

__all__ = ["CROSS_GROUP"]
