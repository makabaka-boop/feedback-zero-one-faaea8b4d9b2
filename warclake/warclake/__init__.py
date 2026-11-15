"""warclake：WARC 1.1 归档入湖后端。

公开接口：

* :class:`IngestError` —— 带文件下标与字节起点的归档错误；
* :func:`build_archive` —— 从有序 WARC 文件列表还原捕获 JSON 数组；
* :func:`write_atomic` —— 成功后原子替换输出文件；
* :class:`BatchStore` —— SQLite 分批入湖：``submit(paths)`` / ``query(n)``，
  支持引用迟到、跨批引用、批级依赖裁决与重启恢复。
"""

from .errors import IngestError
from .ingest import build_archive, write_atomic
from .store import BatchStore

__all__ = [
    "IngestError",
    "build_archive",
    "write_atomic",
    "BatchStore",
]
