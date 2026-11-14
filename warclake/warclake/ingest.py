"""归档入湖主流程：分帧 -> 校验 -> 分段拼块 -> HTTP 解析 -> 引用解析 -> 输出。

输出项为非 continuation 的逻辑记录（response / revisit），结构::

    {"id": ..., "target": ..., "data": <带填充 Base64>, "source": <最终 R 的 ID>}

顺序按（文件下标, 代表记录的零基字节起点）升序。任何一条记录违规或
残缺都会整次拒绝、保留旧输出；只有全部成功后才原子替换输出文件。
"""

import base64
import json
import os
import tempfile

from .digests import sha1_base32, validate_warc_digest, verify_digest
from .errors import IngestError
from .httpmsg import parse_http
from .warc import parse_warc_file

_ALLOWED_TYPES = {"response", "revisit", "continuation"}
_R = "response"
_V = "revisit"
_C = "continuation"


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------

def _strip_angle_brackets(record_id: str, location):
    """WARC-Record-ID 去尖括号；要求整体为 ``<...>`` 且内部非空无尖括号。"""
    if len(record_id) < 2 or not record_id.startswith("<") \
            or not record_id.endswith(">"):
        raise IngestError(
            "WARC-Record-ID 必须以 < > 包裹: %r" % record_id, *location
        )
    inner = record_id[1:-1]
    if not inner or "<" in inner or ">" in inner:
        raise IngestError(
            "WARC-Record-ID 去尖括号后非法: %r" % record_id, *location
        )
    return inner


def _require_header(headers, name, location):
    value = headers.get(name)
    if value is None or value == "":
        raise IngestError("记录缺少必需头 %s" % name, *location)
    return value


def _int_header(headers, name, location):
    value = headers.get(name)
    if value is None or not value.isascii() or not value.isdigit():
        raise IngestError(
            "头 %s 缺失或不是单个十进制非负整数: %r" % (name, value), *location
        )
    return int(value)


def _target_of(headers, location):
    return _require_header(headers, "warc-target-uri", location)


# --------------------------------------------------------------------------
# 主入口
# --------------------------------------------------------------------------

def build_archive(paths):
    """读取有序的未压缩 WARC 文件路径列表，还原捕获 JSON 数组。

    Args:
        paths: 按归档顺序排列的 WARC 文件路径列表（空列表返回 []）。

    Returns:
        可直接 json 序列化的列表，元素为
        ``{"id", "target", "data", "source"}``。

    Raises:
        IngestError: 任何违规或残记录；错误中带文件零基下标与记录的
            零基字节起点（文件级错误为 0）。
    """
    # 第一阶段：逐文件分帧，收集全部物理记录。
    physical = []
    for file_index, path in enumerate(paths):
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except OSError as exc:
            raise IngestError(
                "无法读取 WARC 文件 %s: %s" % (path, exc), file_index, 0
            )
        physical.extend(parse_warc_file(data, file_index))

    # 第二阶段：逐条基础校验 + Block-Digest 校验 + 全局 ID 唯一。
    seen_ids = set()
    for rec in physical:
        loc = rec.loc()
        if rec.rec_type not in _ALLOWED_TYPES:
            raise IngestError(
                "仅接受 response/revisit/continuation，实际类型: %s"
                % rec.rec_type,
                *loc,
            )
        raw_id = _require_header(rec.headers, "warc-record-id", loc)
        rec.logical_id = _strip_angle_brackets(raw_id, loc)
        if rec.logical_id in seen_ids:
            raise IngestError(
                "WARC-Record-ID 全局重复: %s" % rec.logical_id, *loc
            )
        seen_ids.add(rec.logical_id)
        block_digest = _require_header(rec.headers, "warc-block-digest", loc)
        verify_digest(block_digest, rec.block, "Block-Digest", loc)

    # 第三阶段：continuation 分段归组（只有 response 允许分段）。
    # 分段允许跨文件乱序到达，因此先按源 ID 全部收集，再统一校验组装。
    # group_id -> {"leader": rec|None, "parts": [(segno, rec)], "totals": [...]}
    groups = {}
    standalone = []  # 不带任何分段头的 R/V

    for rec in physical:
        loc = rec.loc()
        h = rec.headers
        is_segmented = (
            "warc-segment-number" in h
            or "warc-segment-origin-id" in h
            or "warc-segment-total-length" in h
        )
        if not is_segmented:
            if rec.rec_type == _C:
                raise IngestError(
                    "continuation 记录缺少分段头", *loc
                )
            standalone.append(rec)
            continue

        # 带分段头就必须三件套齐全（总长仅末段携带，下面再查）。
        segno = _int_header(h, "warc-segment-number", loc)
        if segno < 1:
            raise IngestError("WARC-Segment-Number 必须 >= 1", *loc)
        origin_raw = _require_header(h, "warc-segment-origin-id", loc)
        origin_id = _strip_angle_brackets(origin_raw, loc)
        has_total = "warc-segment-total-length" in h
        total = (
            _int_header(h, "warc-segment-total-length", loc)
            if has_total
            else None
        )

        group = groups.setdefault(
            origin_id, {"leader": None, "parts": [], "totals": []}
        )

        if rec.rec_type == _R:
            if segno != 1:
                raise IngestError(
                    "分段首段 response 的段号必须为 1，实际 %d" % segno, *loc
                )
            if has_total:
                raise IngestError(
                    "仅末段允许携带 WARC-Segment-Total-Length", *loc
                )
            if origin_id != rec.logical_id:
                raise IngestError(
                    "首段的 Segment-Origin-ID 必须等于自身 Record-ID", *loc
                )
            if group["leader"] is not None:
                raise IngestError(
                    "分段源 ID 出现多个首段: %s" % origin_id, *loc
                )
            group["leader"] = rec
            group["parts"].append((segno, rec))
        elif rec.rec_type == _C:
            # 源首段可能尚在后续文件中，这里只收集，不立即解析引用。
            group["parts"].append((segno, rec))
            if has_total:
                group["totals"].append((total, rec))
        else:  # revisit 不允许分段
            raise IngestError("revisit 记录不允许参与分段", *loc)

    # 第四阶段：组装并校验每个分段组。
    # logical_id -> {"target", "payload", "source", "order", "loc"}
    resolved_r = {}
    # 暂存待解析的 revisit
    pending_v = []

    def _register_r(logical_id, target, payload, order, loc):
        if logical_id in resolved_r:  # 理论上不可达，防御性检查
            raise IngestError("逻辑记录 ID 重复: %s" % logical_id, *loc)
        resolved_r[logical_id] = {
            "id": logical_id,
            "target": target,
            "payload": payload,
            "source": logical_id,  # response 的最终来源就是自身
            "order": order,
            "loc": loc,
        }

    for origin_id, group in groups.items():
        leader = group["leader"]
        if leader is None:
            # 只有 continuation、没有首段 R：把错误归到组内最早的记录。
            earliest = min(
                group["parts"], key=lambda item: (
                    item[1].file_index, item[1].offset
                )
            )[1]
            raise IngestError(
                "continuation 引用了不存在的分段源: %s" % origin_id,
                *earliest.loc(),
            )
        loc = leader.loc()
        parts = sorted(group["parts"], key=lambda item: item[0])
        segnos = [segno for segno, _ in parts]

        if segnos[0] != 1 or segnos != list(range(1, len(segnos) + 1)):
            raise IngestError(
                "分段 %s 的段号必须从 1 连续: %r" % (origin_id, segnos), *loc
            )
        if parts[0][1] is not leader:
            raise IngestError(
                "分段 %s 的首段必须是 response" % origin_id, *loc
            )
        if len(parts) == 1:
            raise IngestError(
                "分段 %s 只有首段、缺少 continuation 末段" % origin_id, *loc
            )

        # 仅末段携带总长，且等于拼块长度。
        if len(group["totals"]) != 1:
            raise IngestError(
                "分段 %s 必须恰有一个末段携带 Total-Length" % origin_id, *loc
            )
        total_value, total_rec = group["totals"][0]
        last_segno, last_rec = parts[-1]
        if total_rec is not last_rec:
            raise IngestError(
                "只有末段（段号 %d）可以携带 Total-Length" % last_segno,
                *total_rec.loc(),
            )

        target = _target_of(leader.headers, loc)
        for _, rec in parts:
            other_target = _target_of(rec.headers, rec.loc())
            if other_target != target:
                raise IngestError(
                    "分段 %s 各段 Target-URI 不一致" % origin_id, *rec.loc()
                )

        assembled = b"".join(rec.block for _, rec in parts)
        if total_value != len(assembled):
            raise IngestError(
                "Total-Length=%d 不等于拼块长度 %d"
                % (total_value, len(assembled)),
                *loc,
            )

        # R 必须带 Payload-Digest，校验对象是拼块解 HTTP 后的完整正文。
        payload_digest = _require_header(
            leader.headers, "warc-payload-digest", loc
        )
        _, _, body = parse_http(assembled, loc)
        verify_digest(payload_digest, body, "Payload-Digest", loc)

        order = (leader.file_index, leader.offset)
        _register_r(origin_id, target, body, order, loc)

    # 第五阶段：处理独立 R/V（非分段）。
    for rec in standalone:
        loc = rec.loc()
        h = rec.headers
        target = _target_of(h, loc)

        if rec.rec_type == _R:
            payload_digest = _require_header(h, "warc-payload-digest", loc)
            _, _, body = parse_http(rec.block, loc)
            verify_digest(payload_digest, body, "Payload-Digest", loc)
            _register_r(
                rec.logical_id, target, body,
                (rec.file_index, rec.offset), loc,
            )
        else:  # revisit
            profile = _require_header(h, "warc-profile", loc)
            if profile != "http://netpreserve.org/warc/1.1/revisit/" \
                    "identical-payload-digest":
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
            refers_raw = _require_header(h, "warc-refers-to", loc)
            refers_to = _strip_angle_brackets(refers_raw, loc)
            payload_digest = _require_header(h, "warc-payload-digest", loc)
            # 格式先校验，值的比对等解析出所引正文后再做。
            validate_warc_digest(payload_digest, "Payload-Digest", loc)
            pending_v.append(
                {
                    "rec": rec,
                    "id": rec.logical_id,
                    "target": target,
                    "refers_to": refers_to,
                    "payload_digest": payload_digest,
                }
            )

    # 第六阶段：解析 revisit 引用（可跨 URI、可跨文件、可指向后存记录）。
    # 用 DFS + 着色检测环：0=未访问, 1=在当前链上, 2=已完成。
    color = {}
    result_cache = {}  # v_id -> (target, full_payload, source_id)

    def resolve(v_id, chain):
        if v_id in result_cache:
            return result_cache[v_id]
        state = color.get(v_id, 0)
        item = pending_by_id[v_id]
        rec = item["rec"]
        loc = rec.loc()
        if state == 1:
            raise IngestError(
                "revisit 引用存在环: %s" % " -> ".join(chain + [v_id]),
                *loc,
            )
        color[v_id] = 1
        ref_id = item["refers_to"]

        if ref_id in resolved_r:
            ref = resolved_r[ref_id]
            full_payload = ref["payload"]
            source = ref_id
        elif ref_id in pending_by_id:
            # 引用可指向后存的 V：沿链解析，最终一定落到某个 R。
            _, full_payload, source = resolve(ref_id, chain + [v_id])
        else:
            # 缺失引用归到携带该 Refers-To 的记录起点。
            raise IngestError(
                "revisit 引用的记录不存在: %s" % ref_id, *loc
            )

        # 块要么为空，要么是“完整合法 HTTP 头 + 所引正文前缀”。
        if rec.block:
            _, _, stored_body = parse_http(
                rec.block, loc, expected_body_length=len(full_payload)
            )
            if not full_payload.startswith(stored_body):
                raise IngestError(
                    "revisit 存储块不是所引正文的前缀", *loc
                )
        else:
            stored_body = b""

        # Payload-Digest 针对的是完整正文（所引 R 的正文）。
        if sha1_base32(full_payload) != \
                item["payload_digest"][len("sha1:"):]:
            raise IngestError(
                "revisit Payload-Digest 与所引完整正文不一致", *loc
            )

        outcome = (item["target"], full_payload, source)
        result_cache[v_id] = outcome
        color[v_id] = 2
        return outcome

    pending_by_id = {item["id"]: item for item in pending_v}

    logical_records = list(resolved_r.values())
    for item in pending_v:
        target, full_payload, source = resolve(item["id"], [])
        logical_records.append(
            {
                "id": item["id"],
                "target": target,
                "payload": full_payload,
                "source": source,
                "order": (item["rec"].file_index, item["rec"].offset),
                "loc": item["rec"].loc(),
            }
        )

    # 第七阶段：按文件及起点序输出（continuation 已被吸收，不在此列）。
    logical_records.sort(key=lambda r: r["order"])
    output = []
    for rec in logical_records:
        output.append(
            {
                "id": rec["id"],
                "target": rec["target"],
                "data": base64.b64encode(rec["payload"]).decode("ascii"),
                "source": rec["source"],
            }
        )
    return output


def write_atomic(output_path, records):
    """成功后原子替换输出文件；失败不触碰旧文件。

    临时文件与目标放在同一目录，保证 :func:`os.replace` 在同一文件系统上。
    """
    directory = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=".warclake-", suffix=".tmp", dir=directory
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(
                records,
                fh,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, output_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
