"""端到端演练：手工造字节 -> CLI 入湖 -> 核对 JSON / 错误坐标 / 旧输出保留。

运行（项目根）：
    python3 -I demo/end_to_end_demo.py
所有输入 WARC 与输出都写到独立临时目录，脚本本身不在该目录内。
"""

import json
import os
import subprocess
import sys
import tempfile

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "tests"))

from test_warclake import (  # noqa: E402
    PROFILE,
    WarcFileBuilder,
    b32_sha1,
    digest_header,
    http_block,
    warc_record,
)


def run_cli(output_path, *inputs):
    return subprocess.run(
        [
            sys.executable, "-I",
            os.path.join(PROJECT_ROOT, "run_warclake.py"),
            "-o", output_path, *inputs,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def main():
    work = tempfile.mkdtemp(prefix="warclake-demo-")
    print("工作目录:", work)

    body = b"AB"

    # 文件 0：先放一个 revisit（空块），引用“后存”的 response。
    f0 = WarcFileBuilder()
    v_off = f0.add(warc_record(
        "revisit", "v-late", b"",
        extra_headers=[
            ("WARC-Target-URI", "http://v.example/"),
            ("WARC-Profile", PROFILE),
            ("WARC-Refers-To", "<r-main>"),
            ("WARC-Truncated", "length"),
            digest_header(body),
        ],
    ))

    # 文件 1：一个普通 response + 一个带前缀块的 revisit。
    f1 = WarcFileBuilder()
    r_off = f1.add(warc_record(
        "response", "r-main", http_block(body),
        extra_headers=[
            ("WARC-Target-URI", "http://r.example/"),
            digest_header(body),
        ],
    ))
    prefix = http_block(b"A").replace(
        b"Content-Length: 1", b"Content-Length: 2", 1
    )
    vp_off = f1.add(warc_record(
        "revisit", "v-prefix", prefix,
        extra_headers=[
            ("WARC-Target-URI", "http://p.example/"),
            ("WARC-Profile", PROFILE),
            ("WARC-Refers-To", "<r-main>"),
            ("WARC-Truncated", "length"),
            digest_header(body),
        ],
    ))

    p0 = os.path.join(work, "0.warc")
    p1 = os.path.join(work, "1.warc")
    open(p0, "wb").write(f0.data)
    open(p1, "wb").write(f1.data)
    print("记录起点: v-late@%d, r-main@%d, v-prefix@%d"
          % (v_off, r_off, vp_off))

    out = os.path.join(work, "capture.json")
    proc = run_cli(out, p0, p1)
    assert proc.returncode == 0, proc.stderr.decode()
    data = json.loads(open(out, encoding="utf-8").read())
    print("成功输出:")
    print(json.dumps(data, ensure_ascii=False, indent=2))
    assert [d["id"] for d in data] == ["v-late", "r-main", "v-prefix"]
    assert all(d["data"] == "QUI=" for d in data)
    assert all(d["source"] == "r-main" for d in data)

    # 制造违规：文件 1 的第二条记录 Block-Digest 改错。
    bad_builder = WarcFileBuilder()
    bad_builder.add(warc_record(
        "response", "r-main", http_block(body),
        extra_headers=[
            ("WARC-Target-URI", "http://r.example/"),
            digest_header(body),
        ],
    ))
    bad_v = warc_record(
        "revisit", "v-prefix", prefix,
        extra_headers=[
            ("WARC-Target-URI", "http://p.example/"),
            ("WARC-Profile", PROFILE),
            ("WARC-Refers-To", "<r-main>"),
            ("WARC-Truncated", "length"),
            digest_header(body),
        ],
        block_digest="sha1:" + "Z" * 32,
    )
    bad_v_off = bad_builder.add(bad_v)
    bad_path = os.path.join(work, "1-bad.warc")
    open(bad_path, "wb").write(bad_builder.data)

    proc = run_cli(out, p0, bad_path)
    assert proc.returncode == 1
    print("\n违规批次被拒绝，stderr:")
    print(proc.stderr.decode().strip())
    expected = "文件下标=1 字节起点=%d" % bad_v_off
    assert expected in proc.stderr.decode(), proc.stderr.decode()

    kept = json.loads(open(out, encoding="utf-8").read())
    assert kept == data, "失败后旧输出必须原样保留"
    print("旧输出已保留，条目数:", len(kept))

    # 空输入 -> []
    proc = run_cli(out)
    assert proc.returncode == 0
    assert json.loads(open(out, encoding="utf-8").read()) == []
    print("\n空输入输出: []")
    print("\n全部端到端断言通过。")


if __name__ == "__main__":
    main()
