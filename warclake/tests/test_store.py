"""SQLite 分批入湖（:class:`warclake.BatchStore`）独立字节验收。

覆盖题设全部裁决语义：

* 受理校验（分帧/ID 格式/全库 ID 重复/Block-Digest）抛 :class:`IngestError`、
  沿用原坐标、拒绝不占号；
* 批号从 1 连续，裁决失败的批号保留；
* 分段须批内完整；V 可跨批引用；缺失引用为 pending；
* 本批及传递依赖齐备、全部原校验通过、记录引用无环才 ready；
* 批级依赖成环但记录无环允许整组 ready；
* 已知违规或依赖 failed 使整批 failed（优先于 pending）并沿依赖传播；
  failed 批的合法 R 也不可用；
* query 的 {state, rows, error} 形状、根因三元组、未知批号 KeyError；
* 空批、重启不依赖原路径、多线程并发串行等价、强退后库可恢复。

运行（项目根）：
    python3 -I tests/test_store.py -v
"""

import base64
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from warclake import BatchStore, IngestError  # noqa: E402

# 复用独立字节构造工具（字节与摘要均不经过被测生产代码）。
from tests.test_warclake import (  # noqa: E402
    PROFILE,
    WarcFileBuilder,
    b32_sha1,
    digest_header,
    http_block,
    warc_record,
)


# --------------------------------------------------------------------------
# 字节构造小工具
# --------------------------------------------------------------------------

def r_record(rid, body=b"AB", uri="u"):
    return warc_record(
        "response", rid, http_block(body),
        extra_headers=[("WARC-Target-URI", uri), digest_header(body)],
    )


def v_record(vid, refers, body=b"AB", block=b"", uri="uv"):
    return warc_record(
        "revisit", vid, block,
        extra_headers=[
            ("WARC-Target-URI", uri),
            ("WARC-Profile", PROFILE),
            ("WARC-Refers-To", "<%s>" % refers),
            ("WARC-Truncated", "length"),
            digest_header(body),
        ],
    )


class StoreTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="warclake-store-")
        self.db = os.path.join(self.tmp, "lake.sqlite")
        self._seq = 0

    def write_warc(self, blob, name=None):
        self._seq += 1
        if name is None:
            name = "batch-%d.warc" % self._seq
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as fh:
            fh.write(blob)
        return path

    def store(self):
        return BatchStore(self.db)


# --------------------------------------------------------------------------
# 题设主例：V→r 先 pending，补交 R r 后两批均 ready，data 均为 QUI=
# --------------------------------------------------------------------------

class TestPendingThenReady(StoreTestBase):
    def test_late_response_resolves_pending_revisit(self):
        body = b"AB"
        with self.store() as store:
            # 第一批：V 引用尚不存在的 r。
            b1 = store.submit([self.write_warc(v_record("v1", "r1"))])
            self.assertEqual(b1, 1)
            q1 = store.query(1)
            self.assertEqual(q1, {"state": "pending", "rows": None,
                                  "error": None})

            # 第二批：迟到的正文 R r。
            b2 = store.submit([self.write_warc(r_record("r1", body))])
            self.assertEqual(b2, 2)

            q1 = store.query(1)
            self.assertEqual(q1["state"], "ready")
            self.assertEqual(
                q1["rows"],
                [{
                    "id": "v1",
                    "target": "uv",
                    "data": "QUI=",
                    "source": "r1",
                }],
            )
            q2 = store.query(2)
            self.assertEqual(q2["state"], "ready")
            self.assertEqual(
                q2["rows"],
                [{
                    "id": "r1",
                    "target": "u",
                    "data": "QUI=",
                    "source": "r1",
                }],
            )
            # 独立核验 Base64 带填充。
            self.assertEqual(
                base64.b64decode(q1["rows"][0]["data"], validate=True), body
            )

    def test_late_arrival_through_transitive_dependencies(self):
        # B1: v1 -> r-missing；B2: v2 -> v1（传递依赖 B1）。
        with self.store() as store:
            store.submit([self.write_warc(v_record("v1", "r-late"))])
            store.submit([self.write_warc(v_record("v2", "v1"))])
            self.assertEqual(store.query(1)["state"], "pending")
            self.assertEqual(store.query(2)["state"], "pending")

            # B3: 正文到达，前两批与本批全部 ready。
            store.submit([self.write_warc(r_record("r-late"))])
            for batch_id in (1, 2, 3):
                q = store.query(batch_id)
                self.assertEqual(q["state"], "ready", batch_id)
            self.assertEqual(store.query(1)["rows"][0]["source"], "r-late")
            self.assertEqual(store.query(2)["rows"][0]["source"], "r-late")

    def test_v_chain_across_batches_resolves_to_final_r(self):
        body = b"hello"
        with self.store() as store:
            store.submit([self.write_warc(v_record("vy", "vx", body=body))])
            store.submit([self.write_warc(v_record("vx", "r0", body=body))])
            store.submit([self.write_warc(r_record("r0", body))])
            for batch_id in (1, 2, 3):
                q = store.query(batch_id)
                self.assertEqual(q["state"], "ready", batch_id)
                self.assertEqual(
                    base64.b64decode(q["rows"][0]["data"]), body
                )
            self.assertEqual(store.query(1)["rows"][0]["source"], "r0")


# --------------------------------------------------------------------------
# 受理校验：抛 IngestError、沿用坐标、拒绝不占号
# --------------------------------------------------------------------------

class TestGateRejections(StoreTestBase):
    def test_framing_error_does_not_consume_batch_number(self):
        good = r_record("r1")
        bad = r_record("r2")[:-5]  # 残帧：文件级错误，偏移 0
        with self.store() as store:
            with self.assertRaises(IngestError) as ctx:
                store.submit([
                    self.write_warc(good, "a.warc"),
                    self.write_warc(bad, "b.warc"),
                ])
            self.assertEqual(ctx.exception.file_index, 1)
            self.assertEqual(ctx.exception.offset, 0)
            # 整批回滚，不占号；下一次受理成功仍是批号 1。
            nxt = store.submit([self.write_warc(good, "c.warc")])
            self.assertEqual(nxt, 1)
            with self.assertRaises(KeyError):
                store.query(0)
        # 库里没有任何失败/待决批次残留。
        with self.store() as store:
            self.assertEqual(store.query(1)["state"], "ready")

    def test_bad_id_format_uses_record_coordinates(self):
        raw = r_record("r1").replace(b"<r1>", b"r1", 1)
        with self.store() as store:
            with self.assertRaises(IngestError) as ctx:
                store.submit([self.write_warc(raw)])
            self.assertEqual((ctx.exception.file_index,
                              ctx.exception.offset), (0, 0))
            self.assertEqual(
                store.submit([self.write_warc(r_record("r1"))]), 1
            )

    def test_whole_lake_duplicate_id_rejected_with_late_coords(self):
        builder = WarcFileBuilder()
        off = builder.add(r_record("same"))
        with self.store() as store:
            first = store.submit([self.write_warc(builder.data, "f1.warc")])
            self.assertEqual(first, 1)
            builder2 = WarcFileBuilder()
            dup_off = builder2.add(r_record("same"))
            self.assertEqual(off, dup_off)
            with self.assertRaises(IngestError) as ctx:
                store.submit([self.write_warc(builder2.data, "f2.warc")])
            self.assertEqual((ctx.exception.file_index,
                              ctx.exception.offset), (0, dup_off))
            # 拒绝不占号：下一批号仍是 2，旧批保持 ready。
            self.assertEqual(
                store.submit([self.write_warc(r_record("other"))]), 2
            )
            self.assertEqual(store.query(1)["state"], "ready")
            self.assertEqual(store.query(2)["state"], "ready")

    def test_block_digest_mismatch_rejected_at_record_offset(self):
        builder = WarcFileBuilder()
        builder.add(r_record("good"))
        bad = warc_record(
            "response", "bad", http_block(b"AB"),
            extra_headers=[
                ("WARC-Target-URI", "u"),
                ("WARC-Payload-Digest", "sha1:" + b32_sha1(b"AB")),
            ],
            block_digest="sha1:" + "A" * 32,
        )
        off = builder.add(bad)
        with self.store() as store:
            with self.assertRaises(IngestError) as ctx:
                store.submit([self.write_warc(builder.data)])
            self.assertEqual((ctx.exception.file_index,
                              ctx.exception.offset), (0, off))

    def test_unreadable_file_raises_without_touching_store(self):
        with self.store() as store:
            missing = os.path.join(self.tmp, "nope.warc")
            with self.assertRaises(IngestError) as ctx:
                store.submit([missing])
            self.assertEqual(ctx.exception.file_index, 0)
            self.assertEqual(
                store.submit([self.write_warc(r_record("r1"))]), 1
            )

    def test_illegal_type_rejected_at_gate(self):
        raw = r_record("r1").replace(
            b"WARC-Type: response", b"WARC-Type: metadata", 1
        )
        with self.store() as store:
            with self.assertRaises(IngestError):
                store.submit([self.write_warc(raw)])
            self.assertEqual(
                store.submit([self.write_warc(r_record("r1"))]), 1
            )


# --------------------------------------------------------------------------
# 裁决失败：批号保留、坐标根因、优先于 pending、依赖传播
# --------------------------------------------------------------------------

class TestAdjudicationFailures(StoreTestBase):
    def test_failed_batch_keeps_its_number_and_reports_root(self):
        # 缺 Payload-Digest 的 R：受理（Block-Digest 合法）通过，裁决失败。
        bad_r = warc_record(
            "response", "r-bad", http_block(b"AB"),
            extra_headers=[("WARC-Target-URI", "u")],
        )
        with self.store() as store:
            failed_id = store.submit([self.write_warc(bad_r)])
            self.assertEqual(failed_id, 1)
            q = store.query(1)
            self.assertEqual(q["state"], "failed")
            self.assertIsNone(q["rows"])
            self.assertEqual(q["error"], [1, 0, 0])
            # 失败批 ID 保留：后续批号继续递增。
            self.assertEqual(
                store.submit([self.write_warc(r_record("r2"))]), 2
            )
            self.assertEqual(store.query(2)["state"], "ready")

    def test_violation_takes_priority_over_missing_reference(self):
        # 同批既有自身违规（Payload-Digest 错），V 又引用缺失 ID：
        # failed 优先于 pending。
        bad_r = warc_record(
            "response", "r-bad", http_block(b"AB"),
            extra_headers=[
                ("WARC-Target-URI", "u"),
                ("WARC-Payload-Digest", "sha1:" + "B" * 32),
            ],
        )
        with self.store() as store:
            store.submit([
                self.write_warc(bad_r + v_record("v1", "ghost")),
            ])
            self.assertEqual(store.query(1)["state"], "failed")
            self.assertEqual(store.query(1)["error"][0], 1)

    def test_failed_dependency_propagates_and_root_is_dependency(self):
        # B1: V v1 -> r1（迟到中，pending）。
        # B2: R r1 合法 + V v2 前缀块不是 r1 正文前缀 -> B2 failed。
        # B1 因依赖 failed 而 failed，根因是 B2 的 v2 坐标。
        body = b"AB"
        bad_prefix = http_block(b"X").replace(
            b"Content-Length: 1", b"Content-Length: 2", 1
        )
        with self.store() as store:
            store.submit([self.write_warc(v_record("v1", "r1"))])
            self.assertEqual(store.query(1)["state"], "pending")

            builder = WarcFileBuilder()
            r_off = builder.add(r_record("r1", body))
            v2_off = builder.add(v_record("v2", "r1", block=bad_prefix))
            store.submit([self.write_warc(builder.data, "b2.warc")])

            q2 = store.query(2)
            self.assertEqual(q2["state"], "failed")
            self.assertEqual(q2["error"], [2, 0, v2_off])

            q1 = store.query(1)
            self.assertEqual(q1["state"], "failed")
            # 传播来的根因仍指向真正违规的批次。
            self.assertEqual(q1["error"], [2, 0, v2_off])
            self.assertGreater(v2_off, r_off)

    def test_failed_batches_valid_r_is_unusable(self):
        # B1: R r1 合法 + 一条违规 V（自环）-> B1 failed，r1 不可用。
        # B2: V v9 -> r1：引用的 R 存在但所属批 failed -> B2 failed。
        with self.store() as store:
            builder = WarcFileBuilder()
            r_off = builder.add(r_record("r1"))
            vself_off = builder.add(v_record("vself", "vself"))
            store.submit([self.write_warc(builder.data, "b1.warc")])
            self.assertEqual(store.query(1)["error"], [1, 0, vself_off])

            store.submit([self.write_warc(v_record("v9", "r1"))])
            q2 = store.query(2)
            self.assertEqual(q2["state"], "failed")
            self.assertEqual(q2["error"], [1, 0, vself_off])
            self.assertGreater(vself_off, r_off)

    def test_record_cycle_fails_even_across_batches(self):
        # B1: v1 -> v2；B2: v2 -> v1。记录引用成环，两批都 failed。
        with self.store() as store:
            store.submit([self.write_warc(v_record("v1", "v2"))])
            self.assertEqual(store.query(1)["state"], "pending")
            builder = WarcFileBuilder()
            v2_off = builder.add(v_record("v2", "v1"))
            store.submit([self.write_warc(builder.data, "b2.warc")])
            self.assertEqual(store.query(1)["state"], "failed")
            self.assertEqual(store.query(2)["state"], "failed")
            # 两批根因各自落在本批环成员坐标上。
            self.assertEqual(store.query(1)["error"], [1, 0, 0])
            self.assertEqual(store.query(2)["error"], [2, 0, v2_off])


# --------------------------------------------------------------------------
# 分段：必须批内完整，跨批分段不被拼接
# --------------------------------------------------------------------------

class TestSegmentScope(StoreTestBase):
    def _segments(self):
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
                extra.append(
                    ("WARC-Segment-Total-Length", str(total))
                )
            if segno == 1:
                extra.append(digest_header(body))
            return warc_record(
                "response" if segno == 1 else "continuation",
                rec_id, block, extra_headers=extra,
            )

        first = seg("rseg", 1, seg1_block, "rseg", False)
        second = seg("c2", 2, seg2_block, "rseg", True)
        return first, second

    def test_complete_segmentation_within_one_batch_ready(self):
        first, second = self._segments()
        with self.store() as store:
            store.submit([
                self.write_warc(first, "s1.warc"),
                self.write_warc(second, "s2.warc"),
            ])
            q = store.query(1)
            self.assertEqual(q["state"], "ready")
            self.assertEqual(len(q["rows"]), 1)
            self.assertEqual(
                base64.b64decode(q["rows"][0]["data"]), b"ABC"
            )
            self.assertEqual(q["rows"][0]["id"], "rseg")

    def test_segment_split_across_batches_both_fail(self):
        first, second = self._segments()
        with self.store() as store:
            # B1 只有首段：缺 continuation 末段 -> failed（坐标为首段起点）。
            store.submit([self.write_warc(first)])
            self.assertEqual(store.query(1)["state"], "failed")
            self.assertEqual(store.query(1)["error"], [1, 0, 0])

            # B2 的 continuation 在本批找不到源首段 -> 同样 failed，
            # 不会与 B1 跨批拼接。
            c_off_builder = WarcFileBuilder()
            c_off = c_off_builder.add(second)
            store.submit([self.write_warc(c_off_builder.data, "b2.warc")])
            self.assertEqual(store.query(2)["state"], "failed")
            self.assertEqual(store.query(2)["error"], [2, 0, c_off])


# --------------------------------------------------------------------------
# 批依赖成环但记录无环：允许整组发布
# --------------------------------------------------------------------------

class TestBatchCycleRecordAcyclic(StoreTestBase):
    def test_mutual_batch_dependencies_both_ready(self):
        body = b"AB"
        with self.store() as store:
            # B1: R r1 + V v1 -> v2（v2 尚不存在，先 pending）。
            builder1 = WarcFileBuilder()
            builder1.add(r_record("r1", body))
            v1_off = builder1.add(v_record("v1", "v2", body=body))
            store.submit([self.write_warc(builder1.data, "b1.warc")])
            self.assertEqual(store.query(1)["state"], "pending")

            # B2: V v2 -> r1（r1 在 B1）。记录链 v1->v2->r1 终于 R，无环；
            # 批依赖 B1<->B2 成环，允许整组发布。
            builder2 = WarcFileBuilder()
            v2_off = builder2.add(v_record("v2", "r1", body=body))
            store.submit([self.write_warc(builder2.data, "b2.warc")])

            q1, q2 = store.query(1), store.query(2)
            self.assertEqual(q1["state"], "ready")
            self.assertEqual(q2["state"], "ready")
            rows1 = {row["id"]: row for row in q1["rows"]}
            self.assertEqual(rows1["v1"]["source"], "r1")
            self.assertEqual(rows1["v1"]["data"], "QUI=")
            self.assertEqual(q2["rows"][0]["id"], "v2")
            self.assertEqual(q2["rows"][0]["source"], "r1")


# --------------------------------------------------------------------------
# query 形状、空批、未知批号、批内排序
# --------------------------------------------------------------------------

class TestQueryContract(StoreTestBase):
    def test_unknown_batch_raises_keyerror(self):
        with self.store() as store:
            with self.assertRaises(KeyError) as ctx:
                store.query(42)
            self.assertEqual(ctx.exception.args[0], 42)
            # 受理被拒的编号同样查不到。
            with self.assertRaises(IngestError):
                store.submit([self.write_warc(b"")])  # 空文件 -> 分帧拒绝
            with self.assertRaises(KeyError):
                store.query(1)

    def test_empty_batch_is_ready_with_empty_rows(self):
        with self.store() as store:
            batch_id = store.submit([])
            self.assertEqual(batch_id, 1)
            self.assertEqual(
                store.query(1),
                {"state": "ready", "rows": [], "error": None},
            )
            # 空批也占号。
            self.assertEqual(
                store.submit([self.write_warc(r_record("r1"))]), 2
            )

    def test_rows_ordered_by_file_then_offset_within_batch(self):
        f0 = r_record("z", b"one") + r_record("a", b"two")
        f1 = r_record("m", b"three")
        with self.store() as store:
            store.submit([
                self.write_warc(f0, "f0.warc"),
                self.write_warc(f1, "f1.warc"),
            ])
            rows = store.query(1)["rows"]
            self.assertEqual([row["id"] for row in rows], ["z", "a", "m"])

    def test_revisit_with_prefix_block_emits_full_payload(self):
        body = b"AB"
        prefix = http_block(b"A").replace(
            b"Content-Length: 1", b"Content-Length: 2", 1
        )
        with self.store() as store:
            store.submit([self.write_warc(v_record("v1", "r1"))])
            builder = WarcFileBuilder()
            builder.add(r_record("r1", body))
            builder.add(v_record("v2", "r1", block=prefix))
            store.submit([self.write_warc(builder.data, "b2.warc")])
            for batch_id in (1, 2):
                q = store.query(batch_id)
                self.assertEqual(q["state"], "ready")
                for row in q["rows"]:
                    self.assertEqual(row["data"], "QUI=")
                    self.assertEqual(row["source"], "r1")


# --------------------------------------------------------------------------
# 迟到坏前缀：后到的违规把先前 pending 级联打成 failed
# --------------------------------------------------------------------------

class TestLateBadPrefix(StoreTestBase):
    def test_late_bad_prefix_cascades_to_pending_dependents(self):
        body = b"AB"
        bad_prefix = http_block(b"X").replace(
            b"Content-Length: 1", b"Content-Length: 2", 1
        )
        with self.store() as store:
            # B1: v1 -> r1，正文迟到 -> pending。
            store.submit([self.write_warc(v_record("v1", "r1"))])
            # B2: v2 -> v1，同样 pending（传递依赖缺失）。
            store.submit([self.write_warc(v_record("v2", "v1"))])
            self.assertEqual(store.query(1)["state"], "pending")
            self.assertEqual(store.query(2)["state"], "pending")

            # B3: r1 合法正文 + v3 带坏前缀 -> B3 failed。
            builder = WarcFileBuilder()
            builder.add(r_record("r1", body))
            v3_off = builder.add(v_record("v3", "r1", block=bad_prefix))
            store.submit([self.write_warc(builder.data, "b3.warc")])

            self.assertEqual(store.query(3)["error"], [3, 0, v3_off])
            # B1/B2 因传递依赖 failed 而 failed，根因都指向 B3 的坏前缀。
            for batch_id in (1, 2):
                q = store.query(batch_id)
                self.assertEqual(q["state"], "failed", batch_id)
                self.assertEqual(q["error"], [3, 0, v3_off])
                self.assertIsNone(q["rows"])

            # 再补交完全合法的新批，不受历史失败影响，正常 ready。
            store.submit([self.write_warc(r_record("r-clean", b"ok"))])
            q4 = store.query(4)
            self.assertEqual(q4["state"], "ready")
            self.assertEqual(
                base64.b64decode(q4["rows"][0]["data"]), b"ok"
            )


# --------------------------------------------------------------------------
# 重启：不依赖原路径，状态与原字节全部可恢复
# --------------------------------------------------------------------------

class TestRestartPersistence(StoreTestBase):
    def test_reopen_survives_and_ignores_deleted_source_paths(self):
        path1 = self.write_warc(v_record("v1", "r1"), name="keep-1.warc")
        with BatchStore(self.db) as store:
            self.assertEqual(store.submit([path1]), 1)
            self.assertEqual(store.query(1)["state"], "pending")

        # 删掉原始 WARC 路径后重开：裁决只依赖库内原字节。
        os.unlink(path1)
        with BatchStore(self.db) as store:
            self.assertEqual(store.query(1)["state"], "pending")
            path2 = self.write_warc(r_record("r1"), name="keep-2.warc")
            self.assertEqual(store.submit([path2]), 2)
            self.assertEqual(store.query(1)["state"], "ready")
            self.assertEqual(store.query(2)["state"], "ready")
        os.unlink(path2)

        # 再次重开，结果稳定，且批号序列延续。
        with BatchStore(self.db) as store:
            self.assertEqual(
                store.submit([self.write_warc(r_record("r3"))]), 3
            )
            self.assertEqual(store.query(1)["rows"][0]["data"], "QUI=")
            self.assertEqual(store.query(3)["rows"][0]["id"], "r3")

    def test_sqlite_file_contains_no_source_paths(self):
        path = self.write_warc(r_record("r1"), name="secret-name.warc")
        with BatchStore(self.db) as store:
            store.submit([path])
        with open(self.db, "rb") as fh:
            raw = fh.read()
        self.assertNotIn(b"secret-name", raw)


# --------------------------------------------------------------------------
# 并发：多线程提交串行等价，批号不重不跳
# --------------------------------------------------------------------------

class TestConcurrency(StoreTestBase):
    def test_parallel_submits_are_serial_equivalent(self):
        n_threads = 12
        errors = []

        def worker(idx):
            try:
                with BatchStore(self.db) as store:
                    blob = r_record("rid-%d" % idx, b"p%d" % idx)
                    path = self.write_warc(blob, name="t-%d.warc" % idx)
                    got = store.submit([path])
                    q = store.query(got)
                    if q["state"] != "ready":
                        errors.append((idx, got, q))
            except BaseException as exc:  # noqa: BLE001 - 测试内集中上报
                errors.append((idx, repr(exc), None))

        threads = [
            threading.Thread(target=worker, args=(idx,))
            for idx in range(n_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])

        with BatchStore(self.db) as store:
            ids = []
            for batch_id in range(1, n_threads + 1):
                q = store.query(batch_id)
                self.assertEqual(q["state"], "ready")
                ids.append(q["rows"][0]["id"])
            # 每条记录恰好出现一次，批号 1..N 连续无重复。
            self.assertEqual(sorted(ids), sorted(
                "rid-%d" % idx for idx in range(n_threads)
            ))
            with self.assertRaises(KeyError):
                store.query(n_threads + 1)


# --------------------------------------------------------------------------
# 强退：提交前 / 提交过程中被 SIGKILL，重启后库一致
# --------------------------------------------------------------------------

_DRIVER = r'''
import os
import sys
import time

sys.path.insert(0, "__PROJECT_ROOT__")
from warclake import BatchStore  # noqa: E402
from tests.test_warclake import digest_header, http_block, warc_record  # noqa

db_path, mode, warc_dir = sys.argv[1], sys.argv[2], sys.argv[3]
store = BatchStore(db_path)

if mode == "pre":
    # 只建库、不提交，等待被强退。
    time.sleep(5)

def blob(k):
    body = b"body-%d" % k
    return warc_record(
        "response", "crash-r-%d" % k, http_block(body),
        extra_headers=[("WARC-Target-URI", "u"), digest_header(body)],
    )

if mode == "mid":
    k = 0
    while True:
        k += 1
        path = os.path.join(warc_dir, "mid-%d.warc" % k)
        with open(path, "wb") as fh:
            fh.write(blob(k))
        store.submit([path])
        # 极小睡眠，让父进程有机会在任意时刻强退。
        time.sleep(0.002)
'''


class TestCrashRecovery(StoreTestBase):
    def _make_driver(self):
        script_dir = tempfile.mkdtemp(prefix="warclake-driver-")
        driver = os.path.join(script_dir, "driver.py")
        with open(driver, "w", encoding="utf-8") as fh:
            fh.write(_DRIVER.replace("__PROJECT_ROOT__", PROJECT_ROOT))
        data_dir = tempfile.mkdtemp(prefix="warclake-crash-data-")
        return driver, data_dir

    def test_kill_before_first_submit_leaves_empty_store(self):
        driver, data_dir = self._make_driver()
        proc = subprocess.Popen(
            [sys.executable, "-I", driver, self.db, "pre", data_dir]
        )
        # 等库文件被创建。
        deadline = 50
        while not os.path.exists(self.db) and deadline:
            time.sleep(0.05)
            deadline -= 1
        proc.kill()
        proc.wait()

        with BatchStore(self.db) as store:
            with self.assertRaises(KeyError):
                store.query(1)
            # 如同全新库：首个批号仍是 1。
            path = os.path.join(data_dir, "after.warc")
            with open(path, "wb") as fh:
                fh.write(r_record("r1"))
            self.assertEqual(store.submit([path]), 1)
            self.assertEqual(store.query(1)["state"], "ready")

    def test_kill_during_submits_leaves_consistent_prefix(self):
        driver, data_dir = self._make_driver()
        proc = subprocess.Popen(
            [sys.executable, "-I", driver, self.db, "mid", data_dir]
        )
        # 在提交循环中途强退（0.15s 通常已完成数十个小批次）。
        time.sleep(0.15)
        proc.kill()
        proc.wait()

        with BatchStore(self.db) as store:
            # 库内必须是 1..k 的连续前缀，每批都 ready，无半截文件行。
            k = 1
            while True:
                try:
                    q = store.query(k)
                except KeyError:
                    break
                self.assertEqual(q["state"], "ready")
                self.assertTrue(q["rows"])
                k += 1
            committed = k - 1
            self.assertGreater(committed, 0)

            # 文件表与批次表严格一致：无孤儿原字节。
            conn = store._conn  # 白盒验收存储一致性
            file_batches = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT batch_id FROM files"
                ).fetchall()
            }
            self.assertEqual(
                file_batches, set(range(1, committed + 1))
            )
            self.assertEqual(
                int(conn.execute(
                    "SELECT value FROM meta WHERE key='next_batch'"
                ).fetchone()[0]),
                committed + 1,
            )

            # 强退后继续提交，批号严格接续。
            path = os.path.join(data_dir, "after.warc")
            with open(path, "wb") as fh:
                fh.write(r_record("r-after"))
            new_id = store.submit([path])
            self.assertEqual(new_id, committed + 1)
            self.assertEqual(store.query(new_id)["state"], "ready")


if __name__ == "__main__":
    unittest.main(verbosity=2)
