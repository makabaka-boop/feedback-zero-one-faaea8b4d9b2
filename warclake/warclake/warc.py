"""WARC 1.1 分帧组装（自写，不依赖任何 WARC 第三方库）。

WARC 1.1 记录的物理布局为::

    WARC/1.1 (CRLF)
    名: 值 (CRLF)
    ...
    CRLF
    <Content-Length 字节的块>
    CRLF CRLF

本模块只负责按字节还原分帧、解码头字段、给出每条记录在文件中的
零基字节起点；语义校验（类型、分段、摘要、HTTP 等）放在 ingest 层，
以便报错时能精确归属到记录起点。题设保证头合法、无折行、无重名字段，
但这里仍对重名等异常做防御性拒绝。
"""

from dataclasses import dataclass, field

from .errors import IngestError

WARC_VERSION = b"WARC/1.1"
CRLF = b"\r\n"
HEADER_SEP = b"\r\n\r\n"
TRAILER = b"\r\n\r\n"


@dataclass
class WarcRecord:
    """一条已完成分帧的 WARC 记录（块为原始字节，未做任何解码）。"""

    file_index: int
    offset: int  # 记录首字节（WARC/1.1 的 W）在文件内的零基偏移
    rec_type: str
    headers: dict = field(default_factory=dict)
    block: bytes = b""

    def loc(self):
        return self.file_index, self.offset


def _parse_headers(header_blob: bytes, file_index: int, offset: int):
    """将头字节解析为 (类型, 字段字典)。名按规范小写，值保留原文。"""
    lines = header_blob.split(CRLF)
    first = lines[0]
    # 版本行不是文件级帧错位（记录起点明确），按记录级错误报起点。
    if first != WARC_VERSION:
        raise IngestError(
            "非法 WARC 版本行: %r" % first, file_index, offset
        )

    headers = {}
    for line in lines[1:]:
        colon = line.find(b":")
        if colon <= 0:
            raise IngestError(
                "非法 WARC 头行: %r" % line, file_index, offset
            )
        raw_name = line[:colon]
        raw_value = line[colon + 1:]
        try:
            name = raw_name.decode("utf-8").strip().lower()
            value = raw_value.decode("utf-8").lstrip()
        except UnicodeDecodeError:
            raise IngestError(
                "WARC 头不是合法 UTF-8: %r" % line, file_index, offset
            )
        if not name:
            raise IngestError("WARC 头名为空", file_index, offset)
        if name in headers:
            # 题设保证无重名；出现重名即违规，拒绝整批。
            raise IngestError(
                "WARC 头字段重名: %s" % name, file_index, offset
            )
        headers[name] = value

    rec_type = headers.get("warc-type")
    if rec_type is None:
        raise IngestError("记录缺少 WARC-Type", file_index, offset)
    return rec_type, headers


def parse_warc_file(data: bytes, file_index: int):
    """把一个 WARC 文件的完整字节切分成 :class:`WarcRecord` 列表。

    文件级帧错误（空文件、尾部残帧、块截断、尾部分隔不符等）按文件级
    错误处理，偏移报 0；记录头内部的语义错误携带记录自身起点。
    """
    if not data:
        raise IngestError("空 WARC 文件", file_index, 0)

    records = []
    pos = 0
    total = len(data)

    while pos < total:
        record_start = pos

        sep = data.find(HEADER_SEP, pos)
        if sep == -1:
            raise IngestError(
                "记录头缺少 CRLF CRLF 终止符（残记录）", file_index, 0
            )
        header_blob = data[pos:sep]
        block_start = sep + len(HEADER_SEP)

        rec_type, headers = _parse_headers(
            header_blob, file_index, record_start
        )

        length_text = headers.get("content-length")
        if length_text is None or not length_text.isascii() or \
                not length_text.isdigit():
            raise IngestError(
                "Content-Length 缺失或不是单个十进制非负整数",
                file_index, record_start,
            )
        block_len = int(length_text)

        block_end = block_start + block_len
        # 块后必须紧跟 CRLF CRLF；任何截断/错位都是文件级帧错误。
        if block_end + len(TRAILER) > total:
            raise IngestError(
                "记录块或尾部分隔被截断（残记录）", file_index, 0
            )
        if data[block_end:block_end + len(TRAILER)] != TRAILER:
            raise IngestError(
                "记录块后缺少 CRLF CRLF 分隔", file_index, 0
            )

        block = data[block_start:block_end]
        records.append(
            WarcRecord(
                file_index=file_index,
                offset=record_start,
                rec_type=rec_type,
                headers=headers,
                block=block,
            )
        )
        pos = block_end + len(TRAILER)

    return records
