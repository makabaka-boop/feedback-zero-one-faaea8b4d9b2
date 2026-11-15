"""SQLite 分批入湖接口：:class:`BatchStore` 的 ``submit(paths)`` / ``query(n)``。

与一次性 :func:`warclake.ingest.build_archive` 不同，本模块面向“WARC 分批
交回、引用正文可能迟到”的场景：

* ``submit(paths)`` 保存**原字节**到 SQLite，返回从 1 连续的批号；分帧、ID
  格式、全库 ID 重复等**受理校验**失败时抛 :class:`IngestError`，整批回滚、
  不占批号（沿用原坐标：零基文件号 + 记录零基字节起点）。
* 受理后的批次进入裁决：分段必须**批内完整**；revisit(V) 可跨批引用
  response(R)/V；引用暂时缺失为 ``pending``。
* 批依赖按 V 的直接引用（``WARC-Refers-To``）建立。本批及其传递依赖齐备、
  全部原校验通过、且记录引用无环才 ``ready``；**批级依赖成环但记录无环时
  允许整组发布**。
* 批次自身违规、或传递依赖 failed，则该批 ``failed``（优先于等待缺失引用），
  并沿依赖传播；failed 批中即使合法的 R 也不可用。
* ``query(n)`` 返回 ``{"state", "rows", "error"}``：``rows`` 仅 ready 时为
  原批捕获数组（否则 ``None``）；``error`` 仅 failed 时为根因三元组
  ``[批号, 零基文件号, 记录零基字节起点]``（否则 ``None``）；未知批号抛
  :class:`KeyError`。

持久化与并发：

* 每次提交的“受理 + 全库级联裁决”在**单个 SQLite 事务**内完成，进程内加
  互斥锁、事务用 ``BEGIN IMMEDIATE``，多线程/多进程并发等价于串行；
* 重启只需 SQLite 文件本身，不依赖任何原始输入路径（原字节已入库）。
"""

import base64
import json
import sqlite3
import threading

from .digests import sha1_base32, validate_warc_digest, verify_digest
from .errors import IngestError
from .httpmsg import parse_http
from .ingest import (
    _int_header,
    _require_header,
    _strip_angle_brackets,
    _target_of,
)
from .warc import parse_warc_file

_R = "response"
_V = "revisit"
_C = "continuation"
_ALLOWED_TYPES = {_R, _V, _C}

_PENDING = "pending"
_READY = "ready"
_FAILED = "failed"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id    INTEGER PRIMARY KEY,
    state       TEXT NOT NULL,
    root_batch  INTEGER,
    root_file   INTEGER,
    root_offset INTEGER,
    rows_json   TEXT
);
CREATE TABLE IF NOT EXISTS files (
    batch_id   INTEGER NOT NULL,
    file_index INTEGER NOT NULL,
    data       BLOB NOT NULL,
    PRIMARY KEY (batch_id, file_index)
);
"""


# --------------------------------------------------------------------------
# 内存中的湖内记录与违规
# --------------------------------------------------------------------------

class _LakeRec:
    """一条带批号的物理记录（块为原始字节，逻辑 ID 在受理校验后填入）。"""

    __slots__ = ("batch", "fidx", "off", "rec_type", "headers", "block", "lid")

    def __init__(self, batch, rec):
        self.batch = batch
        self.fidx = rec.file_index
        self.off = rec.offset
        self.rec_type = rec.rec_type
        self.headers = rec.headers
        self.block = rec.block
        self.lid = None

    def loc(self):
        # 坐标始终是“批内零基文件号 + 记录零基字节起点”。
        return self.fidx, self.off


class _Violation:
    """一条裁决违规：坐标 + 原因（原因仅用于诊断，不参与状态判定）。"""

    __slots__ = ("fidx", "off", "reason")

    def __init__(self, loc, reason):
        self.fidx, self.off = loc
        self.reason = reason

    def triple(self, batch_id):
        return batch_id, self.fidx, self.off


# --------------------------------------------------------------------------
# 受理校验（分帧已由 parse_warc_file 完成；此处做类型/ID/摘要/全库唯一）
# --------------------------------------------------------------------------

def _gate(all_recs):
    """全库受理校验。任何违规都抛 :class:`IngestError`，坐标落在违规记录上。

    全库 ID 重复时，后提交的重复记录必然在最新一批中，因此报错坐标天然
    属于被拒绝的新批；旧批记录在其提交时已通过同一校验，不会在此翻车。
    """
    seen_ids = set()
    for rec in all_recs:
        loc = rec.loc()
        if rec.rec_type not in _ALLOWED_TYPES:
            raise IngestError(
                "仅接受 response/revisit/continuation，实际类型: %s"
                % rec.rec_type,
                *loc,
            )
        raw_id = _require_header(rec.headers, "warc-record-id", loc)
        rec.lid = _strip_angle_brackets(raw_id, loc)
        if rec.lid in seen_ids:
            raise IngestError(
                "WARC-Record-ID 全库重复: %s" % rec.lid, *loc
            )
        seen_ids.add(rec.lid)
        block_digest = _require_header(
            rec.headers, "warc-block-digest", loc
        )
        verify_digest(block_digest, rec.block, "Block-Digest", loc)


# --------------------------------------------------------------------------
# 单批裁决：分段组装 + 独立 R/V 校验（违规收集，不立即抛出）
# --------------------------------------------------------------------------

def _evaluate_batch(batch_id, recs):
    """对单批记录跑原 ingest 第三~五阶段语义校验。

    Returns:
        (r_infos, v_descs, violations)：
        * ``r_infos``: logical_id -> {"target", "payload", "loc", "order"}；
        * ``v_descs``: revisit 描述符列表；
        * ``violations``: 本批违规坐标列表（缺失引用不在此列，跨批处理）。
    """
    violations = []
    groups = {}
    standalone = []

    def bad(loc, reason):
        violations.append(_Violation(loc, reason))

    # ---- 第三阶段：分段归组（作用域限定在本批，分段须批内完整）----
    for rec in recs:
        loc = rec.loc()
        h = rec.headers
        is_segmented = (
            "warc-segment-number" in h
            or "warc-segment-origin-id" in h
            or "warc-segment-total-length" in h
        )
        if not is_segmented:
            if rec.rec_type == _C:
                bad(loc, "continuation 记录缺少分段头")
            standalone.append(rec)
            continue

        try:
            segno = _int_header(h, "warc-segment-number", loc)
            if segno < 1:
                raise IngestError("WARC-Segment-Number 必须 >= 1", *loc)
            origin_id = _strip_angle_brackets(
                _require_header(h, "warc-segment-origin-id", loc), loc
            )
            has_total = "warc-segment-total-length" in h
            total = (
                _int_header(h, "warc-segment-total-length", loc)
                if has_total
                else None
            )
        except IngestError as exc:
            bad((exc.file_index, exc.offset), exc.reason)
            continue

        group = groups.setdefault(
            origin_id, {"leader": None, "parts": [], "totals": []}
        )

        if rec.rec_type == _R:
            if segno != 1:
                bad(loc, "分段首段 response 的段号必须为 1，实际 %d" % segno)
                continue
            if has_total:
                bad(loc, "仅末段允许携带 WARC-Segment-Total-Length")
                continue
            if origin_id != rec.lid:
                bad(loc, "首段的 Segment-Origin-ID 必须等于自身 Record-ID")
                continue
            if group["leader"] is not None:
                bad(loc, "分段源 ID 出现多个首段: %s" % origin_id)
                continue
            group["leader"] = rec
            group["parts"].append((segno, rec))
        elif rec.rec_type == _C:
            group["parts"].append((segno, rec))
            if has_total:
                group["totals"].append((total, rec))
        else:  # revisit 不允许分段
            bad(loc, "revisit 记录不允许参与分段")

    # ---- 第四阶段：组装每个分段组 ----
    r_infos = {}

    def register_r(logical_id, target, payload, order, loc):
        # 全库 ID 唯一已在受理阶段保证，此处重复只可能是内存装配失误。
        if logical_id in r_infos:
            bad(loc, "逻辑记录 ID 重复: %s" % logical_id)
            return
        r_infos[logical_id] = {
            "id": logical_id,
            "target": target,
            "payload": payload,
            "source": logical_id,
            "order": order,
            "loc": loc,
        }

    for origin_id, group in groups.items():
        leader = group["leader"]
        if leader is None:
            earliest = min(
                group["parts"],
                key=lambda item: (item[1].fidx, item[1].off),
            )[1]
            bad(
                earliest.loc(),
                "continuation 引用了不存在的分段源: %s" % origin_id,
            )
            continue

        loc = leader.loc()
        parts = sorted(group["parts"], key=lambda item: item[0])
        segnos = [segno for segno, _ in parts]

        if segnos[0] != 1 or segnos != list(range(1, len(segnos) + 1)):
            bad(loc, "分段 %s 的段号必须从 1 连续: %r" % (origin_id, segnos))
            continue
        if parts[0][1] is not leader:
            bad(loc, "分段 %s 的首段必须是 response" % origin_id)
            continue
        if len(parts) == 1:
            bad(loc, "分段 %s 只有首段、缺少 continuation 末段" % origin_id)
            continue

        if len(group["totals"]) != 1:
            bad(loc, "分段 %s 必须恰有一个末段携带 Total-Length" % origin_id)
            continue
        total_value, total_rec = group["totals"][0]
        last_segno, last_rec = parts[-1]
        if total_rec is not last_rec:
            bad(
                total_rec.loc(),
                "只有末段（段号 %d）可以携带 Total-Length" % last_segno,
            )
            continue

        try:
            target = _target_of(leader.headers, loc)
            for _, seg_rec in parts:
                other_target = _target_of(seg_rec.headers, seg_rec.loc())
                if other_target != target:
                    raise IngestError(
                        "分段 %s 各段 Target-URI 不一致" % origin_id,
                        *seg_rec.loc(),
                    )

            assembled = b"".join(seg_rec.block for _, seg_rec in parts)
            if total_value != len(assembled):
                raise IngestError(
                    "Total-Length=%d 不等于拼块长度 %d"
                    % (total_value, len(assembled)),
                    *loc,
                )

            payload_digest = _require_header(
                leader.headers, "warc-payload-digest", loc
            )
            _, _, body = parse_http(assembled, loc)
            verify_digest(payload_digest, body, "Payload-Digest", loc)
        except IngestError as exc:
            bad((exc.file_index, exc.offset), exc.reason)
            continue

        register_r(
            origin_id, target, body,
            (leader.fidx, leader.off), loc,
        )

    # ---- 第五阶段：独立 R/V ----
    v_descs = []
    for rec in standalone:
        loc = rec.loc()
        h = rec.headers
        try:
            target = _target_of(h, loc)
            if rec.rec_type == _R:
                payload_digest = _require_header(
                    h, "warc-payload-digest", loc
                )
                _, _, body = parse_http(rec.block, loc)
                verify_digest(payload_digest, body, "Payload-Digest", loc)
                register_r(
                    rec.lid, target, body,
                    (rec.fidx, rec.off), loc,
                )
                continue

            # revisit：本地头段校验；引用能否解析留到跨批阶段。
            profile = _require_header(h, "warc-profile", loc)
            if profile != (
                "http://netpreserve.org/warc/1.1/revisit/"
                "identical-payload-digest"
            ):
                raise IngestError(
                    "revisit 仅支持 identical-payload-digest profile", *loc
                )
            truncated = _require_header(h, "warc-truncated", loc)
            if truncated != "length":
                raise IngestError(
                    "revisit 必须带 WARC-Truncated: length，实际: %r"
                    % truncated,
                    *loc,
                )
            refers_to = _strip_angle_brackets(
                _require_header(h, "warc-refers-to", loc), loc
            )
            payload_digest = _require_header(h, "warc-payload-digest", loc)
            # 格式先校验，值的比对等解析出所引正文后再做。
            validate_warc_digest(payload_digest, "Payload-Digest", loc)
        except IngestError as exc:
            bad((exc.file_index, exc.offset), exc.reason)
            continue

        v_descs.append(
            {
                "batch": batch_id,
                "id": rec.lid,
                "target": target,
                "refers_to": refers_to,
                "payload_digest": payload_digest,
                "block": rec.block,
                "order": (rec.fidx, rec.off),
                "loc": loc,
            }
        )

    return r_infos, v_descs, violations


# --------------------------------------------------------------------------
# 全湖裁决
# --------------------------------------------------------------------------

def _adjudicate(lake):
    """对全湖记录跑跨批引用解析并裁决每批状态。

    Args:
        lake: ``[(batch_id, [_LakeRec, ...]), ...]``，按批号升序。

    Returns:
        ``{batch_id: (state, root_triple_or_None, rows_or_None)}``。
    """
    batch_ids = [batch_id for batch_id, _ in lake]

    # 单批校验
    r_by_id = {}
    v_by_id = {}
    own_violations = {batch_id: [] for batch_id in batch_ids}
    for batch_id, recs in lake:
        r_infos, v_descs, violations = _evaluate_batch(batch_id, recs)
        own_violations[batch_id].extend(violations)
        for info in r_infos.values():
            r_by_id[info["id"]] = (batch_id, info)
        for desc in v_descs:
            v_by_id[desc["id"]] = (batch_id, desc)

    # 物理归属：受理通过后，每个逻辑 ID 都唯一属于一个批。依赖边按
    # “V 直接引用了哪个物理记录”建立——即使被引记录后续裁决失败、未进入
    # r_by_id/v_by_id，依赖关系仍然成立，failed 才能沿批正确传播。
    owner_batch = {}
    owner_kind = {}
    for batch_id, recs in lake:
        for rec in recs:
            owner_batch[rec.lid] = batch_id
            owner_kind[rec.lid] = rec.rec_type
    # 裁决成功者的归属与物理归属一致（这里仅做一致性断言式覆盖）。
    for rid, (batch_id, _) in r_by_id.items():
        owner_batch[rid] = batch_id
    for vid, (batch_id, _) in v_by_id.items():
        owner_batch[vid] = batch_id

    # ---- 第六阶段：V 引用分类（ok / missing / dead / cycle）----
    # 沿 WARC-Refers-To 链行走：
    #   ok      链终于一个裁决成功的 R；
    #   missing 链上某个引用 ID 在全湖物理记录中不存在（引用可能迟到）；
    #   dead    引用 ID 存在但不可解析——continuation 物理 ID，或裁决失败
    #           而未进入 r_by_id/v_by_id 的 R/V（其所属批自身已违规，
    #           失败经依赖边传播，无需在此另记违规）；
    #   cycle   链上重复出现 V（记录引用成环，含自环）。
    # 带记忆化，均摊线性。
    classify_cache = {}      # vid -> ("ok", r_id) / ("missing", None) / ...
    v_status = {}            # 仅 V 节点的终局

    def classify(vid):
        if vid in classify_cache:
            return classify_cache[vid]
        path = []
        on_path = set()
        cur = vid
        result = None
        while True:
            if cur in classify_cache:
                result = classify_cache[cur]
                break
            if cur in r_by_id:
                result = ("ok", cur)
                break
            if cur not in owner_batch:
                # 全湖不存在的逻辑记录：引用可能迟到 -> pending。
                result = ("missing", None)
                break
            if cur not in v_by_id:
                # 物理记录存在但不是“可解析的 V”：continuation，或裁决
                # 失败的 R/V。
                result = ("dead", None)
                break
            if cur in on_path:
                result = ("cycle", None)
                break
            on_path.add(cur)
            path.append(cur)
            cur = v_by_id[cur][1]["refers_to"]
        # 同一条链上的节点共享终局；回填记忆。
        for node in path:
            classify_cache[node] = result
        classify_cache[vid] = result
        return result

    # 批 -> 直接依赖批集合（V 直接引用所落到物理记录的所属批）。
    direct_deps = {batch_id: set() for batch_id in batch_ids}
    for vid, (batch_id, desc) in v_by_id.items():
        status, terminal_r = classify(vid)
        v_status[vid] = (status, terminal_r)
        ref_id = desc["refers_to"]
        if ref_id in owner_batch:
            dep_batch = owner_batch[ref_id]
            if dep_batch != batch_id:
                direct_deps[batch_id].add(dep_batch)

    # 记录引用成环：环上及能走到环上的 V 均在本批坐标记违规。
    for vid, (status, _) in v_status.items():
        if status == "cycle":
            batch_id, desc = v_by_id[vid]
            own_violations[batch_id].append(
                _Violation(desc["loc"], "revisit 引用存在环: %s" % vid)
            )

    # 引用落到 continuation 物理 ID：continuation 不是可发布的逻辑记录，
    # 引用永远无法满足，记为引用方自身违规（原 ingest 中此类引用即报错）。
    for vid, (status, _) in v_status.items():
        if status != "dead":
            continue
        ref_id = v_by_id[vid][1]["refers_to"]
        if owner_kind.get(ref_id) == _C:
            batch_id, desc = v_by_id[vid]
            own_violations[batch_id].append(
                _Violation(
                    desc["loc"],
                    "revisit 引用的记录是 continuation，无法作为正文来源: %s"
                    % ref_id,
                )
            )

    # ok 的 V：此刻才能做前缀块与 Payload-Digest 比对（原 ingest 第六阶段）。
    for vid, (status, terminal_r) in v_status.items():
        if status != "ok":
            continue
        batch_id, desc = v_by_id[vid]
        ref_batch, ref_info = r_by_id[terminal_r]
        full_payload = ref_info["payload"]
        loc = desc["loc"]
        try:
            if desc["block"]:
                _, _, stored_body = parse_http(
                    desc["block"], loc,
                    expected_body_length=len(full_payload),
                )
                if not full_payload.startswith(stored_body):
                    raise IngestError("revisit 存储块不是所引正文的前缀", *loc)
            if sha1_base32(full_payload) != \
                    desc["payload_digest"][len("sha1:"):]:
                raise IngestError(
                    "revisit Payload-Digest 与所引完整正文不一致", *loc
                )
        except IngestError as exc:
            own_violations[batch_id].append(
                _Violation((exc.file_index, exc.offset), exc.reason)
            )

    # ---- 传递依赖闭包与“缺失引用”标记 ----
    # 批级依赖允许成环（记录无环即可发布），用定点扩张求可达集，天然兼容环。
    closure = {}

    def build_closure(batch_id):
        if batch_id in closure:
            return closure[batch_id]
        seen = set()
        frontier = set(direct_deps[batch_id])
        while frontier:
            dep = frontier.pop()
            if dep in seen:
                continue
            seen.add(dep)
            # 已算过闭包的节点直接并入，避免重复扩张。
            if dep in closure:
                seen.update(closure[dep])
            else:
                frontier.update(direct_deps.get(dep, ()))
        closure[batch_id] = seen
        return seen

    batch_missing = {batch_id: False for batch_id in batch_ids}
    for vid, (status, _) in v_status.items():
        if status == "missing":
            batch_missing[v_by_id[vid][0]] = True

    # ---- 失败定点：自身违规 -> 沿传递依赖传播，优先于 pending ----
    root = {}
    failed = set()
    for batch_id in batch_ids:
        if own_violations[batch_id]:
            failed.add(batch_id)
            first = min(
                own_violations[batch_id], key=lambda v: (v.fidx, v.off)
            )
            root[batch_id] = first.triple(batch_id)

    while True:
        changed = False
        for batch_id in batch_ids:
            if batch_id in failed:
                continue
            dep_failed = build_closure(batch_id) & failed
            if dep_failed:
                failed.add(batch_id)
                changed = True
                # 根因取传递依赖中最小的根因坐标（任一根因，确定化选取）。
                root[batch_id] = min(
                    root[dep] for dep in dep_failed
                )
        if not changed:
            break

    # ---- 汇总每批状态与捕获数组 ----
    result = {}
    for batch_id in batch_ids:
        dep_closure = build_closure(batch_id)
        missing = batch_missing[batch_id] or any(
            batch_missing[dep] for dep in dep_closure
        )
        if batch_id in failed:
            result[batch_id] = (_FAILED, root[batch_id], None)
            continue
        if missing:
            result[batch_id] = (_PENDING, None, None)
            continue

        # ready：组装原批捕获数组，按（批内文件号, 字节起点）升序。
        items = []
        for rid, (owner, info) in r_by_id.items():
            if owner != batch_id:
                continue
            items.append(
                (
                    info["order"],
                    {
                        "id": rid,
                        "target": info["target"],
                        "data": base64.b64encode(info["payload"]).decode(
                            "ascii"
                        ),
                        "source": info["source"],
                    },
                )
            )
        for vid, (owner, desc) in v_by_id.items():
            if owner != batch_id:
                continue
            status, terminal_r = v_status[vid]
            # 能走到 ready 的批不允许残留 missing/dead/cycle：dead 的依赖批
            # 必然 failed（传播），missing 会使本批 pending，cycle 是自身
            # 违规；三者在上面均已分流，此处只能是 ok。
            if status != "ok":
                raise RuntimeError(  # 防御性：理论不可达
                    "批 %d 的 revisit %s 状态异常: %s"
                    % (batch_id, vid, status)
                )
            _, ref_info = r_by_id[terminal_r]
            items.append(
                (
                    desc["order"],
                    {
                        "id": vid,
                        "target": desc["target"],
                        "data": base64.b64encode(
                            ref_info["payload"]
                        ).decode("ascii"),
                        "source": terminal_r,
                    },
                )
            )
        items.sort(key=lambda pair: pair[0])
        result[batch_id] = (_READY, None, [row for _, row in items])

    return result


# --------------------------------------------------------------------------
# SQLite 分批存储
# --------------------------------------------------------------------------

class BatchStore:
    """SQLite 支撑的分批入湖存储。

    Args:
        db_path: SQLite 文件路径；不存在则创建（目录需可写）。同一文件可被
            多个 :class:`BatchStore` 实例（甚至多进程）安全打开，写事务
            串行化，并发提交等价于某一种串行顺序。
    """

    def __init__(self, db_path):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)
        self._conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('next_batch', '1')"
        )

    # -- 上下文管理 / 关闭 -------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self):
        with self._lock:
            self._conn.close()

    # -- 内部：读全湖并裁决 ------------------------------------------------

    def _load_lake(self, conn, new_batch_id, new_blobs):
        """从库内原字节（含本批新字节）重新分帧，构造全湖内存视图。"""
        rows = conn.execute(
            "SELECT batch_id, file_index, data FROM files "
            "ORDER BY batch_id, file_index"
        ).fetchall()
        grouped = {}
        for batch_id, file_index, blob in rows:
            grouped.setdefault(batch_id, []).append((file_index, blob))
        # 空批没有任何文件字节，也必须进入裁决视图（立即 ready、rows=[]）。
        grouped.setdefault(new_batch_id, [])
        for file_index, blob in enumerate(new_blobs):
            grouped.setdefault(new_batch_id, []).append(
                (file_index, blob)
            )

        lake = []
        all_recs = []
        for batch_id in sorted(grouped):
            recs = []
            for file_index, blob in sorted(grouped[batch_id]):
                # 分帧错误在此抛出，file_index 为批内零基文件号。
                for rec in parse_warc_file(blob, file_index):
                    lake_rec = _LakeRec(batch_id, rec)
                    recs.append(lake_rec)
                    all_recs.append(lake_rec)
            lake.append((batch_id, recs))
        return lake, all_recs

    # -- 公开接口 ----------------------------------------------------------

    def submit(self, paths):
        """提交一批有序 WARC 文件，返回批号（从 1 连续）。

        原字节先读入内存再入库：读文件失败、分帧/ID 格式/全库 ID 重复/
        Block-Digest 等受理校验失败时抛 :class:`IngestError`，事务整体
        回滚、**不占批号**；受理成功则批号落库并参与全湖裁决（裁决失败
        也是合法受理，批号保留、状态为 failed）。

        Args:
            paths: 批内按归档顺序排列的 WARC 文件路径列表；空列表表示
                空批，立即 ready 且 ``rows == []``。

        Returns:
            本批批号 ``int``。

        Raises:
            IngestError: 受理校验失败，坐标为批内零基文件号与记录零基
                字节起点（文件级错误偏移为 0）。
        """
        # 先在事务外读完所有原字节：读盘失败不触碰数据库。
        blobs = []
        for file_index, path in enumerate(paths):
            try:
                with open(path, "rb") as fh:
                    blobs.append(fh.read())
            except OSError as exc:
                raise IngestError(
                    "无法读取 WARC 文件 %s: %s" % (path, exc),
                    file_index,
                    0,
                )

        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                next_batch = int(
                    conn.execute(
                        "SELECT value FROM meta WHERE key='next_batch'"
                    ).fetchone()[0]
                )
                batch_id = next_batch

                # 全湖分帧 + 受理校验；任何违规整体回滚，不占号。
                lake, all_recs = self._load_lake(conn, batch_id, blobs)
                try:
                    _gate(all_recs)
                except IngestError:
                    conn.rollback()
                    raise

                # 受理通过：原字节与批次行落库。
                conn.execute(
                    "INSERT INTO batches(batch_id, state) VALUES(?, ?)",
                    (batch_id, _PENDING),
                )
                conn.executemany(
                    "INSERT INTO files(batch_id, file_index, data) "
                    "VALUES(?, ?, ?)",
                    [
                        (batch_id, file_index, blob)
                        for file_index, blob in enumerate(blobs)
                    ],
                )

                # 全湖裁决并覆写所有批次状态（同一事务内原子可见）。
                adjudication = _adjudicate(lake)
                for adj_batch, (state, root_triple, rows) in adjudication.items():
                    if root_triple is None:
                        rb = rf = ro = None
                    else:
                        rb, rf, ro = root_triple
                    conn.execute(
                        "UPDATE batches SET state=?, root_batch=?, "
                        "root_file=?, root_offset=?, rows_json=? "
                        "WHERE batch_id=?",
                        (
                            state,
                            rb,
                            rf,
                            ro,
                            json.dumps(
                                rows, ensure_ascii=False, separators=(",", ":")
                            ) if rows is not None else None,
                            adj_batch,
                        ),
                    )

                conn.execute(
                    "UPDATE meta SET value=? WHERE key='next_batch'",
                    (str(next_batch + 1),),
                )
                conn.commit()
                return batch_id
            except Exception:
                # 统一兜底：任何意外都不留半截事务。
                conn.rollback()
                raise

    def query(self, n):
        """查询批号 ``n`` 的裁决结果。

        Returns:
            ``{"state": "pending"|"ready"|"failed", "rows": ..., "error": ...}``：
            ready 时 ``rows`` 为原批捕获数组、``error`` 为 ``None``；
            pending 时两者均为 ``None``；failed 时 ``rows`` 为 ``None``、
            ``error`` 为根因 ``[批号, 零基文件号, 记录零基字节起点]``。

        Raises:
            KeyError: 批号不存在（含尚未分配或受理被拒的编号）。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT state, root_batch, root_file, root_offset, rows_json "
                "FROM batches WHERE batch_id=?",
                (n,),
            ).fetchone()
        if row is None:
            raise KeyError(n)
        state, rb, rf, ro, rows_json = row
        if state == _READY:
            return {
                "state": _READY,
                "rows": json.loads(rows_json),
                "error": None,
            }
        if state == _FAILED:
            return {
                "state": _FAILED,
                "rows": None,
                "error": [rb, rf, ro],
            }
        return {"state": _PENDING, "rows": None, "error": None}
