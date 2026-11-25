#!/usr/bin/env python3 -I
"""仓库根启动脚本：在隔离模式下把仓库根加入 sys.path 后转交 CLI。

用法（从仓库根或任意目录均可）：
  python3 -I run.py serve --db data/graph.db
  python3 -I run.py load-sample --db data/demo.db --sample star
  python3 -I run.py verify --db data/demo.db
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_service.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
