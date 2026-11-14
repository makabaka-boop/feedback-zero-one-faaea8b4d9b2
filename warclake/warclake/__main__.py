"""命令行入口：

    python -m warclake -o 输出.json 输入1.warc 输入2.warc ...

成功：退出码 0，输出文件被原子替换。
失败：退出码 1，stderr 打印“文件零基下标 + 记录零基字节起点 + 原因”，
旧输出保持不变。无输入文件时输出 []。
"""

import argparse
import sys

from .errors import IngestError
from .ingest import build_archive, write_atomic


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="warclake",
        description="把有序未压缩 WARC 1.1 文件归档为捕获 JSON 数组",
    )
    parser.add_argument(
        "-o", "--output", required=True, help="输出 JSON 文件路径"
    )
    parser.add_argument(
        "inputs", nargs="*", help="按归档顺序排列的未压缩 WARC 文件"
    )
    args = parser.parse_args(argv)

    try:
        records = build_archive(args.inputs)
        write_atomic(args.output, records)
    except IngestError as exc:
        if exc.file_index is None:
            sys.stderr.write("拒绝: %s\n" % exc.reason)
        else:
            sys.stderr.write(
                "拒绝: 文件下标=%d 字节起点=%d 原因=%s\n"
                % (exc.file_index, exc.offset, exc.reason)
            )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
