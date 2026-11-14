# warclake — WARC 1.1 归档入湖后端

从零实现的纯标准库 Python 后端：接收一组**有序、未压缩**的 WARC 1.1
文件，按 [WARC 1.1](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/)
分帧组装并做严格语义校验，还原为捕获 JSON 数组。WARC 分帧、HTTP 解析、
引用解析均为自写，仅使用标准库的基础字节操作与 `hashlib` 摘要库，
不依赖任何第三方 WARC 库。

## 输出格式

只输出非 `continuation` 的逻辑记录（response / revisit），按
**（文件零基下标, 记录零基字节起点）** 升序：

```json
[
  {"id": "r1", "target": "http://a/", "data": "QUI=", "source": "r1"}
]
```

* `id`：`WARC-Record-ID` 去掉尖括号后的值，全局唯一；
* `target`：`WARC-Target-URI`；
* `data`：完整 HTTP 正文的标准 **带填充** Base64；
* `source`：该捕获最终所依据的 response 记录 ID（revisit 沿引用链回溯
  到的最后一个 R）；
* 输入为空列表时输出 `[]`。

## 实现的校验规则（任一违规即整次拒绝）

| 主题 | 规则 |
| --- | --- |
| 收录类型 | 仅 `response(R)`、`revisit(V)`、`continuation(C)`，其它类型拒绝 |
| 记录头 | 题设保证合法、无折行、无重名；仍防御性拒绝重名字段与非 UTF-8 头 |
| 分帧 | 严格 `WARC/1.1` 版本行、`Content-Length` 十进制定长块、块后 `CRLF CRLF`；残帧/截断按文件级错误报字节 0 |
| ID | 必须 `<...>` 包裹，去尖括号后非空且全局唯一 |
| 分段 | 仅 R 可分段；按源 ID 归组，段号从 1 连续；仅末段带 `WARC-Segment-Total-Length` 且等于拼块长度；各段 target 一致；可跨文件、可乱序 |
| HTTP | 仅 200；禁止 `Transfer-Encoding` 与 `Content-Encoding`；单个十进制 `Content-Length` 且等于完整正文长度；头不允许重名 |
| revisit | 仅 `identical-payload-digest` profile；必带 `WARC-Refers-To` 与 `WARC-Truncated: length`；块为空，或为“完整合法 HTTP 头 + 所引正文前缀”（其 `Content-Length` 写所引完整正文长度） |
| 引用 | 可跨 URI、可跨文件、可指向后存的 R 或 V；引用缺失或成环（含自环）拒绝 |
| 摘要 | 每记录必带 `Block-Digest`；每个 R/V 必带 `Payload-Digest`；只接受 `sha1:` + 大写无填充 Base32；Block 校验原始块，Payload 校验完整正文（V 为所引 R 的正文） |

## 错误坐标与失败语义

错误统一为 `IngestError`，携带：

* `file_index`：出错文件在输入列表中的**零基下标**；
* `offset`：任一出错记录在该文件内的**零基字节起点**；
* 文件级错误（空文件、残帧、块/尾部分隔截断、无法读取）固定报字节 `0`。

CLI 失败时以退出码 1 打印形如
`拒绝: 文件下标=1 字节起点=277 原因=...`。**任何失败都保留旧输出文件
原样不动**；全部校验通过后才在同目录写临时文件、`fsync` 并
`os.replace` 原子替换。

## 目录结构

```
warclake/
├── pyproject.toml              # 包元数据与 console 入口
├── run_warclake.py             # 显式入口（python -I 隔离模式可用）
├── warclake/
│   ├── __init__.py             # 公开 API
│   ├── __main__.py             # CLI: python -m warclake
│   ├── errors.py               # IngestError（文件下标 + 字节起点）
│   ├── warc.py                 # 自写 WARC 1.1 分帧组装
│   ├── httpmsg.py              # 自写 HTTP 响应解析
│   ├── digests.py              # sha1 / 大写 Base32 摘要校验
│   └── ingest.py               # 主流程与原子输出
├── tests/
│   └── test_warclake.py        # 独立字节验收（46 项）
└── demo/
    └── end_to_end_demo.py      # 端到端 CLI 演练
```

## 入口与用法

命令行（安装后）：

```bash
python3 -m warclake -o capture.json part-a.warc part-b.warc
```

不安装、且在 `python -I` 隔离模式下（忽略环境变量与 cwd，避免被
同目录下的伪造模块劫持）：

```bash
python3 -I run_warclake.py -o capture.json part-a.warc part-b.warc
```

作为库调用：

```python
from warclake import build_archive, write_atomic

records = build_archive(["part-a.warc", "part-b.warc"])  # 失败抛 IngestError
write_atomic("capture.json", records)                    # 成功才原子替换
```

## 独立字节验收

测试中的所有 WARC 字节均在测试文件内手工拼装，摘要用 `hashlib/base64`
现场独立计算，不经过生产代码的任何构造逻辑；既精确断言成功路径的
Base64 字节，也逐条断言拒绝路径的（文件下标, 字节起点），并含 CLI
子进程、跨文件乱序分段、引用环、失败保留旧输出等验收：

```bash
python3 -I tests/test_warclake.py -v          # 46 项
python3 -I demo/end_to_end_demo.py            # 端到端演练
```

## 安全说明

读取的 WARC 属不可信数据：建议始终以 `python -I` 运行，并通过参数
传入文件绝对路径，避免解释器从数据目录加载到伪造的同名模块。
