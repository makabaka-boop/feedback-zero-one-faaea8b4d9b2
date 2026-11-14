"""归档入湖后端的统一错误类型。

任何违规或残记录都会抛出 :class:`IngestError`，其中携带出错文件的
零基下标以及出错记录在该文件内的零基字节起点；文件级错误一律报 0。
整次拒绝时不会替换已有的成功输出。
"""


class IngestError(Exception):
    """归档失败异常。

    Attributes:
        file_index: 出错文件在输入文件列表中的零基下标；若错误尚未
            归属到具体输入文件则为 ``None``。
        offset: 出错记录的零基字节起点；文件级错误固定为 0。
    """

    def __init__(self, reason: str, file_index=None, offset: int = 0):
        self.reason = reason
        self.file_index = file_index
        self.offset = offset
        if file_index is None:
            message = reason
        else:
            message = "file[%d]@%d: %s" % (file_index, offset, reason)
        super().__init__(message)
