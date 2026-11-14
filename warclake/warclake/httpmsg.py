"""HTTP 消息解析（自写，仅处理本题约束下的响应）。

题设保证头合法，额外强制以下规则，违反即整批拒绝：

* 状态行必须是 ``HTTP/... 200 ...``，仅接受 200；
* 禁止 Transfer-Encoding 与 Content-Encoding（任意取值都拒绝）；
* 恰好一个十进制 Content-Length，且等于完整正文长度；
* 头字段不允许重名；
* 头之后必须以 CRLF CRLF 结束，正文后不允许多余字节。
"""

from .errors import IngestError

CRLF = "\r\n"
HEADER_END = "\r\n\r\n"


def parse_http(block: bytes, location, expected_body_length=None):
    """解析响应块，返回 ``(状态行, 头字典, 正文)``。

    空块不允许走 HTTP 解析（revisit 的空块是另一套语义，在 ingest 层
    先行分流）。

    Args:
        expected_body_length: 为 ``None`` 时要求 Content-Length 等于本块
            正文实际长度（response 场景）；revisit 前缀块则传入所引完整
            正文的长度，此时本块正文允许更短，但 Content-Length 必须
            等于该完整长度。
    """
    if not block:
        raise IngestError("响应块为空，无法解析 HTTP", *location)

    try:
        text = block.decode("iso-8859-1")
    except UnicodeDecodeError:  # pragma: no cover - iso-8859-1 不会失败
        raise IngestError("HTTP 块不是合法字节", *location)

    end = text.find(HEADER_END)
    if end == -1:
        raise IngestError("HTTP 头缺少 CRLF CRLF 终止符", *location)

    header_part = text[:end]
    body = block[end + len(HEADER_END):]
    lines = header_part.split(CRLF)
    status_line = lines[0]
    tokens = status_line.split(" ")
    if len(tokens) < 2 or not tokens[0].startswith("HTTP/"):
        raise IngestError("非法 HTTP 状态行: %r" % status_line, *location)
    if tokens[1] != "200":
        raise IngestError(
            "仅接受 HTTP 200 响应，实际状态: %s" % tokens[1], *location
        )

    headers = {}
    for line in lines[1:]:
        colon = line.find(":")
        if colon <= 0:
            raise IngestError("非法 HTTP 头行: %r" % line, *location)
        name = line[:colon].strip().lower()
        value = line[colon + 1:].strip()
        if not name:
            raise IngestError("HTTP 头名为空", *location)
        if name in headers:
            raise IngestError("HTTP 头字段重名: %s" % name, *location)
        headers[name] = value

    if "transfer-encoding" in headers:
        raise IngestError("禁止使用 Transfer-Encoding", *location)
    if "content-encoding" in headers:
        raise IngestError("禁止使用 Content-Encoding", *location)

    cl = headers.get("content-length")
    if cl is None or not cl.isascii() or not cl.isdigit():
        raise IngestError(
            "HTTP Content-Length 缺失或不是单个十进制非负整数", *location
        )
    if int(cl) != (
        len(body) if expected_body_length is None else expected_body_length
    ):
        raise IngestError(
            "HTTP Content-Length=%s 与期望正文长度 %d（本块实际 %d）不一致"
            % (
                cl,
                len(body) if expected_body_length is None
                else expected_body_length,
                len(body),
            ),
            *location,
        )

    return status_line, headers, body
