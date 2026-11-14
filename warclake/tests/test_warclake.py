"""独立字节验收测试。

特点：
* 所有 WARC 字节都在本文件内手工拼装，不调用任何生产代码的构造逻辑；
* 摘要用 hashlib/base64 现场独立计算，避免“用被测代码给被测代码出数据”；
* 既覆盖成功路径的精确字节断言，也覆盖全部拒绝路径的
  （文件零基下标, 记录零基字节起点）断言；
* 含 CLI 子进程与“失败保留旧输出、成功原子替换”验收。

运行（在项目根目录）：
    python -I -m unittest -v tests.test_warclake
"""

import base64
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest

# 允许在不安装包的情况下从项目根目录直接运行（python -I 时也生效）。
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from warclake import IngestError, build_archive, write_atomic  # noqa: E402

PROFILE = (
    "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest"
)
CRLF = b"\r\n"


# --------------------------------------------------------------------------
# 独立字节工具
# --------------------------------------------------------------------------

def b32_sha1(data: bytes) -> str:
    return base64.b32encode(hashlib.sha1(data).digest()).decode().rstrip("=")


def http_block(body: bytes, extra_headers=()) -> bytes:
    head = b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n" % len(body)
    for name, value in extra_headers:
        head += name.encode() + b": " + value.encode() + b"\r\n"
    return head + b"\r\n" + body


def warc_record(rec_type, rec_id, block, extra_headers=(),
                block_digest=None) -> bytes:
    headers = [
        ("WARC-Type", rec_type),
        ("WARC-Record-ID", "<%s>" % rec_id),
        ("Content-Length", str(len(block))),
        (
            "WARC-Block-Digest",
            block_digest if block_digest is not None
            else "sha1:" + b32_sha1(block),
        ),
    ]
    headers.extend(extra_headers)
    raw = b"WARC/1.1\r\n"
    for name, value in headers:
        raw += name.encode() + b": " + value.encode() + b"\r\n"
    raw += b"\r\n" + block + b"\r\n\r\n"
    return raw


def digest_header(payload):
    return ("WARC-Payload-Digest", "sha1:" + b32_sha1(payload))


class WarcFileBuilder:
    """在内存中拼 WARC 文件，同时记录每条记录的零基字节起点。"""

    def __init__(self):
        self.data = b""
        self.offsets = []

    def add(self, raw: bytes):
        self.offsets.append(len(self.data))
        self.data += raw
        return self.offsets[-1]

    def write(self, directory, name):
        path = os.path.join(directory, name)
        with open(path, "wb") as fh:
            fh.write(self.data)
        return path


class BuildTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="warclake-test-")

    def paths(self, *blobs):
        result = []
        for i, blob in enumerate(blobs):
            path = os.path.join(self.tmp, "in-%d.warc" % i)
            with open(path, "wb") as fh:
                fh.write(blob)
            result.append(path)
        return result

    def assertRejected(self, paths, file_index, offset):
        with self.assertRaises(IngestError) as ctx:
            build_archive(paths)
        self.assertEqual(ctx.exception.file_index, file_index)
        self.assertEqual(ctx.exception.offset, offset)


# --------------------------------------------------------------------------
# 成功路径
# --------------------------------------------------------------------------

class TestHappyPath(BuildTestBase):
    def test_empty_input_returns_empty_list(self):
        self.assertEqual(build_archive([]), [])

    def test_basic_response_exact_bytes(self):
        body = b"AB"
        block = http_block(body)
        raw = warc_record(
            "response", "r1", block,
            extra_headers=[
                ("WARC-Target-URI", "http://a.example/"),
                digest_header(body),
            ],
        )
        out = build_archive(self.paths(raw))
        self.assertEqual(
            out,
            [{
                "id": "r1",
                "target": "http://a.example/",
                "data": "QUI=",
                "source": "r1",
            }],
        )
        # 独立核验 Base64 带填充。
        self.assertEqual(base64.b64decode(out[0]["data"], validate=True), body)

    def test_revisit_empty_block_and_prefix_block_share_payload(self):
        body = b"AB"
        r_block = http_block(body)
        r_raw = warc_record(
            "response", "r1", r_block,
            extra_headers=[
                ("WARC-Target-URI", "http://a.example/"),
                digest_header(body),
            ],
        )
        # V1：空块（Block-Digest 即空字节摘要），引用后存的 R。
        v1_raw = warc_record(
            "revisit", "v1", b"",
            extra_headers=[
                ("WARC-Target-URI", "http://b.example/v1"),
                ("WARC-Profile", PROFILE),
                ("WARC-Refers-To", "<r1>"),
                ("WARC-Truncated", "length"),
                digest_header(body),
            ],
        )
        # V2：完整 HTTP 头 + 所引正文前缀 A（Content-Length 仍为 2）。
        prefix_block = http_block(b"A")
        # 手工把 Content-Length 改成完整正文长度 2。
        prefix_block = prefix_block.replace(
            b"Content-Length: 1", b"Content-Length: 2", 1
        )
        v2_raw = warc_record(
            "revisit", "v2", prefix_block,
            extra_headers=[
                ("WARC-Target-URI", "http://c.example/v2"),
                ("WARC-Profile", PROFILE),
                ("WARC-Refers-To", "<r1>"),
                ("WARC-Truncated", "length"),
                digest_header(body),
            ],
        )
        out = build_archive(self.paths(v1_raw, r_raw + v2_raw))
        self.assertEqual([item["id"] for item in out], ["v1", "r1", "v2"])
        for item in out:
            self.assertEqual(item["data"], "QUI=")
            self.assertEqual(item["source"], "r1")
        self.assertEqual(out[0]["target"], "http://b.example/v1")
        self.assertEqual(out[2]["target"], "http://c.example/v2")

    def test_revisit_chain_across_uris_resolves_to_final_r(self):
        body = b"hello"
        r_raw = warc_record(
            "response", "r0", http_block(body),
            extra_headers=[
                ("WARC-Target-URI", "http://a/"),
                digest_header(body),
            ],
        )
        vx_raw = warc_record(
            "revisit", "vx", b"",
            extra_headers=[
                ("WARC-Target-URI", "http://b/"),
                ("WARC-Profile", PROFILE),
                ("WARC-Refers-To", "<r0>"),
                ("WARC-Truncated", "length"),
                digest_header(body),
            ],
        )
        vy_raw = warc_record(
            "revisit", "vy", b"",
            extra_headers=[
                ("WARC-Target-URI", "http://c/"),
                ("WARC-Profile", PROFILE),
                ("WARC-Refers-To", "<vx>"),
                ("WARC-Truncated", "length"),
                digest_header(body),
            ],
        )
        # V 在 R 之前出现（后存引用）。
        out = build_archive(self.paths(vy_raw, vx_raw, r_raw))
        by_id = {item["id"]: item for item in out}
        self.assertEqual(by_id["vy"]["source"], "r0")
        self.assertEqual(by_id["vx"]["source"], "r0")
        self.assertEqual(
            base64.b64decode(by_id["vy"]["data"]), body
        )

    def test_segmented_response_assembled_out_of_order_across_files(self):
        # 完整 HTTP：头在首段，正文 ABC 切成 A / BC，跨文件乱序到达。
        body = b"ABC"
        head = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n"
        seg1_block = head + b"A"
        seg2_block = b"BC"
        total = len(seg1_block) + len(seg2_block)

        def seg(rec_id, segno, block, origin, with_total):
            extra = [
                ("WARC-Target-URI", "http://seg/"),
                ("WARC-Segment-Number", str(segno)),
                ("WARC-Segment-Origin-ID", "<%s>" % origin),
            ]
            if with_total:
                extra.append(("WARC-Segment-Total-Length", str(total)))
            if segno == 1:
                # R 必带 Payload-Digest，针对拼块后解出的完整正文。
                extra.append(digest_header(body))
            return warc_record(
                "response" if segno == 1 else "continuation",
                rec_id, block, extra_headers=extra,
            )

        first = seg("rseg", 1, seg1_block, "rseg", False)
        second = seg("c2", 2, seg2_block, "rseg", True)
        # 文件 0 放 continuation，文件 1 放首段：跨文件且乱序。
        out = build_archive(self.paths(second, first))
        self.assertEqual(len(out), 1)
        item = out[0]
        self.assertEqual(item["id"], "rseg")
        self.assertEqual(item["source"], "rseg")
        self.assertEqual(base64.b64decode(item["data"]), body)

    def test_output_ordering_by_file_then_offset(self):
        b1, b2, b3 = b"one", b"two", b"three"
        def r(rid, body, uri):
            return warc_record(
                "response", rid, http_block(body),
                extra_headers=[
                    ("WARC-Target-URI", uri), digest_header(body)
                ],
            )
        f0 = r("z", b1, "u") + r("a", b2, "u")
        f1 = r("m", b3, "u")
        out = build_archive(self.paths(f0, f1))
        self.assertEqual([i["id"] for i in out], ["z", "a", "m"])


# --------------------------------------------------------------------------
# 分帧与基础头拒绝路径
# --------------------------------------------------------------------------

class TestFramingRejections(BuildTestBase):
    def test_empty_file_is_file_level_error_offset_zero(self):
        self.assertRejected(self.paths(b""), 0, 0)

    def test_bad_warc_version_reports_record_offset(self):
        body = b"AB"
        raw = warc_record(
            "response", "r1", http_block(body),
            extra_headers=[
                ("WARC-Target-URI", "u"), digest_header(body)
            ],
        )
        raw = raw.replace(b"WARC/1.1", b"WARC/1.0", 1)
        self.assertRejected(self.paths(raw), 0, 0)

    def test_truncated_block_is_file_level_error(self):
        body = b"AB"
        raw = warc_record(
            "response", "r1", http_block(body),
            extra_headers=[
                ("WARC-Target-URI", "u"), digest_header(body)
            ],
        )
        # 砍掉尾部若干字节（块尾 + CRLFCRLF 被截断）。
        self.assertRejected(self.paths(raw[:-5]), 0, 0)

    def test_duplicate_warc_header_rejected_at_record_offset(self):
        builder = WarcFileBuilder()
        body = b"AB"
        raw = warc_record(
            "response", "r1", http_block(body),
            extra_headers=[
                ("WARC-Target-URI", "u"), digest_header(body)
            ],
        )
        # 再插入一个重名 WARC-Type。
        raw = raw.replace(
            b"WARC-Type: response\r\n",
            b"WARC-Type: response\r\nWARC-Type: response\r\n",
            1,
        )
        off = builder.add(raw)
        self.assertRejected(self.paths(builder.data), 0, off)

    def test_second_file_error_carries_its_own_index_and_offset(self):
        good_body = b"ok"
        good = warc_record(
            "response", "g", http_block(good_body),
            extra_headers=[
                ("WARC-Target-URI", "u"), digest_header(good_body)
            ],
        )
        builder = WarcFileBuilder()
        # 文件 1 先放一条好记录，再放残帧，残记录偏移必须被报出。
        bad_body = b"x"
        bad = warc_record(
            "response", "bad", http_block(bad_body),
            extra_headers=[
                ("WARC-Target-URI", "u"), digest_header(bad_body)
            ],
        )[:-3]
        builder.add(
            warc_record(
                "response", "g2", http_block(b"y"),
                extra_headers=[
                    ("WARC-Target-URI", "u"), digest_header(b"y")
                ],
            )
        )
        bad_off = builder.add(bad)
        self.assertRejected(self.paths(good, builder.data), 1, 0)
        # 记录级错误示例：第二个文件里重复 ID，偏移为该记录起点。
        builder2 = WarcFileBuilder()
        dup = warc_record(
            "response", "g", http_block(b"z"),
            extra_headers=[
                ("WARC-Target-URI", "u"), digest_header(b"z")
            ],
        )
        dup_off = builder2.add(dup)
        self.assertRejected(
            self.paths(good, builder2.data), 1, dup_off
        )


# --------------------------------------------------------------------------
# 类型 / ID / 摘要拒绝路径
# --------------------------------------------------------------------------

class TestRecordValidation(BuildTestBase):
    def _r(self, rid="r1", body=b"AB", uri="u", **kw):
        return warc_record(
            "response", rid, http_block(body),
            extra_headers=[
                ("WARC-Target-URI", uri), digest_header(body)
            ],
            **kw,
        )

    def test_disallowed_record_type(self):
        raw = self._r()
        raw = raw.replace(b"WARC-Type: response", b"WARC-Type: metadata", 1)
        self.assertRejected(self.paths(raw), 0, 0)

    def test_duplicate_global_id(self):
        builder = WarcFileBuilder()
        builder.add(self._r("same"))
        off = builder.add(self._r("same"))
        self.assertRejected(self.paths(builder.data), 0, off)

    def test_id_missing_angle_brackets(self):
        raw = self._r()
        raw = raw.replace(b"<r1>", b"r1", 1)
        self.assertRejected(self.paths(raw), 0, 0)

    def test_block_digest_mismatch_reports_record_offset(self):
        builder = WarcFileBuilder()
        good = self._r("good")
        builder.add(good)
        bad = self._r("bad", block_digest="sha1:" + "A" * 32)
        off = builder.add(bad)
        self.assertRejected(self.paths(builder.data), 0, off)

    def test_digest_must_be_sha1_uppercase_base32(self):
        # md5 方案名拒绝。
        raw = self._r(block_digest="md5:" + "A" * 22)
        self.assertRejected(self.paths(raw), 0, 0)
        # 小写 base32 拒绝。
        raw2 = self._r(
            "r2",
            block_digest="sha1:" + b32_sha1(b"AB").lower(),
        )
        self.assertRejected(self.paths(raw2), 0, 0)

    def test_missing_block_digest(self):
        raw = self._r()
        raw = raw.replace(b"WARC-Block-Digest: sha1:",
                          b"WARC-X-Digest: sha1:", 1)
        self.assertRejected(self.paths(raw), 0, 0)

    def test_response_missing_payload_digest(self):
        raw = warc_record(
            "response", "r1", http_block(b"AB"),
            extra_headers=[("WARC-Target-URI", "u")],
        )
        self.assertRejected(self.paths(raw), 0, 0)

    def test_payload_digest_mismatch(self):
        raw = warc_record(
            "response", "r1", http_block(b"AB"),
            extra_headers=[
                ("WARC-Target-URI", "u"),
                ("WARC-Payload-Digest", "sha1:" + "B" * 32),
            ],
        )
        self.assertRejected(self.paths(raw), 0, 0)


# --------------------------------------------------------------------------
# HTTP 规则拒绝路径
# --------------------------------------------------------------------------

class TestHttpRejections(BuildTestBase):
    def _warc(self, block):
        return warc_record(
            "response", "r1", block,
            extra_headers=[("WARC-Target-URI", "u")],
        )

    def test_non_200_rejected(self):
        block = http_block(b"AB").replace(b"200 OK", b"404 No", 1)
        self.assertRejected(self.paths(self._warc(block)), 0, 0)

    def test_transfer_encoding_rejected(self):
        block = http_block(
            b"AB", extra_headers=[("Transfer-Encoding", "chunked")]
        )
        self.assertRejected(self.paths(self._warc(block)), 0, 0)

    def test_content_encoding_rejected(self):
        block = http_block(
            b"AB", extra_headers=[("Content-Encoding", "gzip")]
        )
        self.assertRejected(self.paths(self._warc(block)), 0, 0)

    def test_content_length_not_equal_body(self):
        block = http_block(b"AB").replace(
            b"Content-Length: 2", b"Content-Length: 9", 1
        )
        self.assertRejected(self.paths(self._warc(block)), 0, 0)

    def test_duplicate_http_header_rejected(self):
        block = http_block(b"AB")
        block = block.replace(
            b"Content-Length: 2\r\n",
            b"Content-Length: 2\r\nX-Dup: 1\r\nX-Dup: 2\r\n",
            1,
        )
        self.assertRejected(self.paths(self._warc(block)), 0, 0)


# --------------------------------------------------------------------------
# 分段规则拒绝路径
# --------------------------------------------------------------------------

class TestSegmentRejections(BuildTestBase):
    def _seg(self, rec_id, segno, block, origin, total=None,
             rec_type=None, uri="u"):
        if rec_type is None:
            rec_type = "response" if segno == 1 else "continuation"
        extra = [
            ("WARC-Target-URI", uri),
            ("WARC-Segment-Number", str(segno)),
            ("WARC-Segment-Origin-ID", "<%s>" % origin),
        ]
        if total is not None:
            extra.append(("WARC-Segment-Total-Length", str(total)))
        return warc_record(rec_type, rec_id, block, extra_headers=extra)

    def setUp(self):
        super().setUp()
        self.head = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n"
        self.b1 = self.head + b"A"
        self.b2 = b"BC"
        self.total = len(self.b1) + len(self.b2)

    def test_only_response_may_be_segmented(self):
        # 首段是 revisit -> 拒绝。
        raw = self._seg("v", 1, self.b1, "v", rec_type="revisit")
        self.assertRejected(self.paths(raw), 0, 0)

    def test_continuation_without_origin_group(self):
        raw = self._seg("c", 2, self.b2, "ghost", total=self.total)
        self.assertRejected(self.paths(raw), 0, 0)

    def test_segment_numbers_must_be_continuous_from_one(self):
        first = self._seg("r", 1, self.b1, "r")
        third = self._seg("c3", 3, self.b2, "r", total=self.total)
        self.assertRejected(self.paths(first + third), 0, 0)

    def test_total_length_only_on_last_segment(self):
        first = self._seg("r", 1, self.b1, "r", total=self.total)
        second = self._seg("c2", 2, self.b2, "r", total=self.total)
        self.assertRejected(self.paths(first + second), 0, 0)

    def test_total_length_must_equal_assembled_length(self):
        first = self._seg("r", 1, self.b1, "r")
        second = self._seg("c2", 2, self.b2, "r", total=self.total + 1)
        self.assertRejected(self.paths(first + second), 0, 0)

    def test_target_uri_must_match_across_segments(self):
        first = self._seg("r", 1, self.b1, "r", uri="u1")
        second = self._seg("c2", 2, self.b2, "r",
                           total=self.total, uri="u2")
        self.assertRejected(self.paths(first + second), 0,
                           len(first))

    def test_origin_id_must_equal_leader_record_id(self):
        first = self._seg("r", 1, self.b1, "other")
        self.assertRejected(self.paths(first), 0, 0)

    def test_missing_continuation_tail_rejected(self):
        # 只有首段 R，没有任何 continuation：分段不完整，拒绝。
        first = self._seg("r", 1, self.b1, "r")
        self.assertRejected(self.paths(first), 0, 0)

    def test_empty_body_response_uses_empty_base64(self):
        body = b""
        raw = warc_record(
            "response", "r-empty", http_block(body),
            extra_headers=[
                ("WARC-Target-URI", "u"), digest_header(body)
            ],
        )
        out = build_archive(self.paths(raw))
        self.assertEqual(out[0]["data"], "")
        self.assertEqual(
            base64.b64decode(out[0]["data"], validate=True), b""
        )


# --------------------------------------------------------------------------
# revisit 规则拒绝路径
# --------------------------------------------------------------------------

class TestRevisitRejections(BuildTestBase):
    def _r(self, rid="r1", body=b"AB", uri="u"):
        return warc_record(
            "response", rid, http_block(body),
            extra_headers=[
                ("WARC-Target-URI", uri), digest_header(body)
            ],
        )

    def _v(self, vid, block=b"", refers="r1", uri="uv",
            trunc="length", profile=PROFILE, payload=b"AB"):
        extra = [
            ("WARC-Target-URI", uri),
            ("WARC-Profile", profile),
            ("WARC-Refers-To", "<%s>" % refers),
            ("WARC-Truncated", trunc),
            digest_header(payload),
        ]
        return warc_record("revisit", vid, block, extra_headers=extra)

    def test_wrong_profile_rejected(self):
        raw = self._v("v1", profile="http://example/wrong-profile")
        self.assertRejected(self.paths(raw), 0, 0)

    def test_missing_refers_to(self):
        raw = self._v("v1").replace(
            b"WARC-Refers-To: <r1>\r\n", b"", 1
        )
        self.assertRejected(self.paths(raw), 0, 0)

    def test_truncated_must_be_length(self):
        raw = self._v("v1", trunc="time")
        self.assertRejected(self.paths(raw), 0, 0)

    def test_missing_reference_rejected(self):
        raw = self._v("v1", refers="nope")
        self.assertRejected(self.paths(raw), 0, 0)

    def test_reference_cycle_rejected(self):
        v1 = self._v("v1", refers="v2")
        v2 = self._v("v2", refers="v1")
        self.assertRejected(self.paths(v1 + v2), 0, 0)

    def test_self_cycle_rejected(self):
        raw = self._v("v1", refers="v1")
        self.assertRejected(self.paths(raw), 0, 0)

    def test_payload_digest_mismatch_against_referent(self):
        r = self._r("r1")
        v = self._v("v1", payload=b"DIFFERENT")
        self.assertRejected(self.paths(r + v), 0, len(r))

    def test_prefix_block_must_be_prefix(self):
        body = b"AB"
        r = self._r("r1", body)
        bad_prefix = http_block(b"X").replace(
            b"Content-Length: 1", b"Content-Length: 2", 1
        )
        v = self._v("v1", block=bad_prefix)
        self.assertRejected(self.paths(r + v), 0, len(r))

    def test_prefix_block_content_length_must_match_full_payload(self):
        body = b"AB"
        r = self._r("r1", body)
        # Content-Length 仍写本地长度 1，而不是完整正文长度 2。
        bad = http_block(b"A")
        v = self._v("v1", block=bad)
        self.assertRejected(self.paths(r + v), 0, len(r))

    def test_v_missing_payload_digest(self):
        raw = warc_record(
            "revisit", "v1", b"",
            extra_headers=[
                ("WARC-Target-URI", "uv"),
                ("WARC-Profile", PROFILE),
                ("WARC-Refers-To", "<r1>"),
                ("WARC-Truncated", "length"),
            ],
        )
        self.assertRejected(self.paths(raw), 0, 0)


# --------------------------------------------------------------------------
# 原子输出 / CLI 验收
# --------------------------------------------------------------------------

class TestOutputAndCli(BuildTestBase):
    LAUNCHER = os.path.join(PROJECT_ROOT, "run_warclake.py")

    def _run_cli(self, *args):
        # -I 隔离模式：入口脚本自行把项目根插入 sys.path，不依赖 cwd/PYTHONPATH。
        return subprocess.run(
            [sys.executable, "-I", self.LAUNCHER, *args],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

    def _read_json(self, path):
        with open(path, encoding="utf-8") as fh:
            return json.loads(fh.read())

    def test_write_atomic_replaces_on_success(self):
        out_path = os.path.join(self.tmp, "out.json")
        write_atomic(out_path, [{"old": True}])
        with open(out_path, "rb") as fh:
            before = fh.read()
        write_atomic(out_path, [])
        with open(out_path, "rb") as fh:
            after = fh.read()
        self.assertNotEqual(before, after)
        self.assertEqual(json.loads(after.decode()), [])

    def test_failure_keeps_old_output(self):
        out_path = os.path.join(self.tmp, "out.json")
        write_atomic(out_path, [{"sentinel": True}])
        bad = warc_record(
            "response", "r1", http_block(b"AB"),
            extra_headers=[("WARC-Target-URI", "u")],  # 缺 Payload-Digest
        )
        with self.assertRaises(IngestError):
            records = build_archive(self.paths(bad))
            write_atomic(out_path, records)
        with open(out_path, encoding="utf-8") as fh:
            self.assertEqual(
                json.loads(fh.read()),
                [{"sentinel": True}],
            )

    def test_cli_success_and_failure_and_empty(self):
        out_path = os.path.join(self.tmp, "cli.json")

        # 空输入 -> []
        proc = self._run_cli("-o", out_path)
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        self.assertEqual(self._read_json(out_path), [])

        # 成功写入
        body = b"AB"
        good = self.paths(warc_record(
            "response", "r1", http_block(body),
            extra_headers=[
                ("WARC-Target-URI", "u"), digest_header(body)
            ],
        ))[0]
        proc = self._run_cli("-o", out_path, good)
        self.assertEqual(proc.returncode, 0, proc.stderr.decode())
        data = self._read_json(out_path)
        self.assertEqual(data[0]["data"], "QUI=")

        # 失败：退出码 1，stderr 含下标与起点，旧输出保留。
        bad = self.paths(warc_record(
            "response", "r1", http_block(body),
            extra_headers=[("WARC-Target-URI", "u")],
        ))[0]
        proc = self._run_cli("-o", out_path, bad)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("文件下标=0", proc.stderr.decode())
        self.assertIn("字节起点=0", proc.stderr.decode())
        self.assertEqual(self._read_json(out_path), data)


if __name__ == "__main__":
    unittest.main(verbosity=2)
