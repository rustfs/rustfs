# SSE 黑盒测试套件

[English](README.md)

针对真实 `rustfs` 进程的服务端加密业务测试。用例只看客户端能看到的东西：S3 响应、admin 接口响应、环境变量，以及产品承诺的两条启动事件。套件用 Python 编写，只依赖标准库。

## 它回答什么问题

- 每种 SSE 方案（SSE-S3、SSE-KMS、SSE-C）在进程可能启动的每种密钥来源下，能否原样读回字节，响应头是否恰好是应有的那几个。
- 格式错误、方案不匹配、无权限的请求是否被明确拒绝，且没有任何东西落盘。
- 两个服务端二进制的行为是否一致。用例里的每次 HTTP 交互都记成一条观测向量，`diff` 逐字段比较两次运行。

## 运行

```bash
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py bin add --name head-default --path target/debug/rustfs --commit "$(git rev-parse --short HEAD)" --build default
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py diff <运行甲> <运行乙>
```

`bin add` 把二进制复制到 `target/sse-bb/bin/<名称>/` 并记录 SHA-256，之后重新编译 `target/debug/rustfs` 不会改变一次运行测的是什么。早于 `sse_local_master_key` 启动事件的二进制用 `--no-master-key-event` 登记，这条事件对它只记录、不断言。

一次运行为每种进程配置各启动一个服务端，依次使用同一个端口，每个都用 `target/sse-bb/runs/<运行编号>/` 下全新的数据目录。端口默认 20105，可用 `SSE_BB_PORT` 或 `--port` 修改。端口被占用就报错：套件不会换端口，也不会去停占用它的进程。

## 进程配置

| 名称 | 密钥来源 |
|---|---|
| `e0` | 无 KMS，未设置本地主密钥 |
| `e1` | 无 KMS，本地主密钥已加载 |
| `e1x-text`、`e1x-short` | 无 KMS，本地主密钥取值非法 |
| `e2` | Local KMS 后端，按密钥授权关闭 |
| `e2e` | Local KMS 后端，强制按密钥授权 |
| `e2e-audit` | 同 `e2e`，另把审计条目投递到驱动进程内的接收端 |
| `e2m` | Local KMS 后端，同时加载本地主密钥 |
| `e3` | Vault Transit 后端 |
| `e4` | Vault KV2 后端 |
| `e8` | Static 后端：唯一的一把密钥来自环境变量 |

`e3` 与 `e4` 需要 `PATH` 里有 `vault` 可执行文件（或用 `SSE_BB_VAULT_BIN` 指定路径）。其中先启动的那个配置会在 `SSE_BB_VAULT_PORT`（默认 20108；Vault 还会占用下一个端口作集群监听）上拉起一个 Vault 开发模式服务，作为驱动进程的子进程，运行结束时停掉；事先不需要有任何服务在运行，也不使用 Docker。根令牌在每次运行时生成，经环境变量交给被测服务端，运行目录下的 `vault/vault.log` 里已把它抹去。没有该可执行文件或端口被占用时，这两个配置的所有用例都报 `env-error`。这些配置和各后端的能力表都在 `backends.py` 里；用例只依据这张表跳过。

`e2e-audit` 会在驱动进程的一个线程里、在 `SSE_BB_AUX_PORT`（默认 20107）上启动一个 HTTP 接收端，并把服务端的审计 webhook 指向它。端口被占用时，这个配置的用例报 `env-error`。

## 泄露扫描

一次运行里用到的每个秘密都登记为“金丝雀”：请求送出的每把 SSE-C 密钥、本地主密钥（非法取值也算，并且包括它开头和结尾的若干字符）、服务端环境变量里的其它秘密、Local 后端每个密钥文件里的密钥材料、admin 接口返回的每把明文数据密钥。每个金丝雀按标准 base64、URL 安全 base64、十六进制和原始字节四种形式查找，每种写法另按开头 12 个字符查找。登记表只存在于内存；报告里只用种类和摘要指代金丝雀，从不写出它的值。

`archive.py` 把本次运行的每一次 HTTP 交互存进 `archive/exchanges.jsonl`：响应头与文档类正文存全文，较大的对象正文只存哈希和首尾若干字节。完整的响应在收到时就已扫描过。请求正文只存哈希与长度，带秘密的请求头只存摘要，响应里发现的金丝雀替换成标记。

配置名为 `run` 的四条用例在最后一个站点停止后对整次运行做判定：响应里没有金丝雀（`SEC-01`），服务端日志里没有（`SEC-02`），响应不带存储内部头（`SEC-03`），每个 500 的消息都属于一组固定文本（`SEC-06`）。唯一被接受的金丝雀出现位置，是数据密钥生成接口自己那次响应的正文。扫描之前，运行会先往一个文件里写入一个一次性的金丝雀并要求扫描器把它找出来；没有东西可查的扫描报 `skip`，不报 `pass`。

## 输出

每个运行目录包含 `run.json`（二进制身份、配置、计数）、`results.jsonl`（每条用例与每个输入行的结论）、`observations.jsonl`（观测向量）、`report.md`、`sites/` 下的服务端日志、`archive/` 下的交互存档，以及写有总体状态的 `DONE`。没有 `DONE` 的运行没有跑完，不能参与比较。

基本结论有三种：`pass`、`fail`（对产品的某条期望不成立）、`env-error`（站点没起来或依赖缺失）。环境错误既不算通过，也不算产品失败；只要出现一条，整次运行就不算成功。

另有两种结论用于产品目前已知达不到的行为。用 `known_deviation` 写下的期望仍然留在用例里，表示应有的行为：它不成立时用例报 `known-deviation`，运行仍算成功；它成立之后用例报 `deviation-fixed`，运行失败，直到把这个标记改成普通的 `check`。

## 比较两个二进制

`diff` 报告三类内容：结论不同、观测不同、只在一边出现的用例或交互。它接受的差异是 `diffing.py` 里的一张封闭清单，清单之外的任何差异都使比较失败。没有阻断性差异时退出码为 0，有则为 1；两次运行没有任何共同交互时为 2，这样"结果为空"不会被当成"一致"。

观测向量保留确定性的内容（状态码、错误码与消息、SSE 响应头、响应头名称集合、正文哈希、对象落盘的加密元数据键名），把随机值（加密对象的 ETag、版本 ID、上传 ID）归约为形态。

两个二进制行为一致，并不能说明一个能读另一个写下的对象。`cross` 负责这一点：写方二进制把该配置能产生的每种落盘形态都写一遍（每种方案、从空对象到数 MiB、单段与多段），停掉服务端，再用读方二进制在同一个数据目录上启动，必须把每个对象原样读回。

```bash
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py cross --writer base-default --reader head-default
```

## 落盘形态

`SF` 用例是第二层：它们读站点的磁盘，而不是读响应。它们陈述加密对象有哪些元数据键、哪些键的值是固定的、随机值是什么形态、每块盘上的副本是否一致，以及任何一块盘上的任何文件里都没有明文、客户密钥、数据密钥或对象密钥。这一层失败，意思是落盘形态变了，而不是某个请求的行为不对。

这些用例用仓库自带的读取工具解析 `xl.meta`，需要先构建一次：`cargo build -p rustfs-filemeta --example dump_fileinfo`；也可以用 `SSE_BB_DUMP_FILEINFO` 指向另一份。没有读取工具时，依赖它的用例报 `env-error`。读取工具要固定用一个格式可信的提交构建出来的那份：与被测服务端一起重编的读取工具会跟着格式一起变，形态变化就被它自己消化掉了。`SF-01` 直接搜索原始字节，不需要读取工具，属于默认级别。

一种构建必须落盘成什么样，写在 `gold/<构建>.json` 里：每种方案、每种布局一条，列出全部元数据键，键的值要么给出精确值，要么（值按设计是随机的）给出形态。`SF-02` 会点名每一个缺失、多出或不同的键。哪些键算随机，只由 `stored.py` 里的一张表决定；表外的键一律逐字比较，所以新出现的随机值会表现为差异，而不会悄悄被归约成形态。

不允许为了让运行变绿而改清单。落盘形态变化时，先由人确认这是有意的，并且其它读方（旧版本、另一种构建）仍然能处理；然后用新二进制实际落盘的内容整体替换清单：

```bash
uv run --python 3.12 --no-project scripts/test/sse_blackbox/stored.py propose <运行目录> > scripts/test/sse_blackbox/gold/<构建>.json
```

这些用例还为每个对象记录一条 `stored-form` 观测，`cross` 也为写方写下的每个对象记录一条。因此对两次运行（或两次 `cross`）做 `diff`，比较的就是两个二进制各自写到盘上的内容：键名与固定值逐字比较，随机值比较形态。

## 证明套件会失败

`suite.py selftest` 不需要服务端。它检查套件自己的判断力：列举结果里分段的大小、数量或顺序变了，观测必须跟着变，而上传 ID 与时间戳变了不能跟着变；已知偏差只容忍它登记的那一处差异；`diff` 该报的都会报。改动 `observe.py`、`diffing.py` 或 `framework.py` 之后要跑一遍。

有两个开关只用来证明"全绿"是有意义的：

```bash
# 在没有主密钥的进程上跑 e1 的用例：RT-01、BD-01、KS-02 必须失败。
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default --configs e1 --run-as e1=e0
# 在跑用例之前停掉服务端：所有结论必须是 env-error。
uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default --configs e1 --sabotage kill-site
```

## 新增用例

在某个 `cases_*.py` 里用 `@case(编号, 标题, 配置)` 注册一个函数。期望通过上下文表达（`check`、`refused`、`projection`、`roundtrip`、`absent`）：每个都会记下决定判定的证据，被拒绝的写之后一定跟一次"没有落盘"的检查。表驱动用例的每个输入用 `with c.row(名称)` 包起来，让它有自己的结论。请求序列要保持确定，轮询放进 `observe.quiet()`，因为比较是按交互在用例里的位置对齐的。
