#!/usr/bin/env python3
"""显式入口脚本：在 ``python -I`` 隔离模式下也能定位到本地包。

用法::

    python3 -I run_warclake.py -o 输出.json 输入1.warc 输入2.warc ...

正常安装后也可以直接用 ``python3 -m warclake``。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from warclake.__main__ import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
