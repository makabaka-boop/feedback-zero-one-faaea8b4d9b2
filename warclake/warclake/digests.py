"""摘要工具：仅支持 sha1，编码为大写无填充 Base32（RFC 4648）。

WARC 的 Block-Digest / Payload-Digest 形如 ``sha1:<BASE32>``，算法标签
小写，摘要部分为 32 个字符的大写 Base32，不含 ``=`` 填充。
"""

import base64
import hashlib

from .errors import IngestError

_SCHEME = "sha1:"
_BASE32_ALPHABET = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567")


def sha1_base32(data: bytes) -> str:
    """计算 ``data`` 的 sha1，并编码为大写无填充 Base32。"""
    digest = hashlib.sha1(data).digest()
    return base64.b32encode(digest).decode("ascii").rstrip("=")


def validate_warc_digest(value: str, kind: str, location) -> None:
    """校验 WARC 摘要头的格式：必须是 ``sha1:<大写Base32>``。

    只做格式检查，不做内容比对；内容比对由 :func:`verify_digest` 完成。
    """
    if not value.startswith(_SCHEME):
        raise IngestError("%s 必须以 sha1: 开头: %r" % (kind, value), *location)
    encoded = value[len(_SCHEME):]
    if len(encoded) != 32 or any(ch not in _BASE32_ALPHABET for ch in encoded):
        raise IngestError(
            "%s 必须是 sha1 加大写无填充 Base32: %r" % (kind, value), *location
        )


def verify_digest(value: str, data: bytes, kind: str, location) -> None:
    """先校验摘要头格式，再将其与 ``data`` 的实际 sha1 摘要比对。"""
    validate_warc_digest(value, kind, location)
    actual = sha1_base32(data)
    if value[len(_SCHEME):] != actual:
        raise IngestError(
            "%s 不匹配: 头=%s 实算=%s" % (kind, value, actual), *location
        )
