# AES-256-GCM 数据面解密完整走读：密文输入 → 调用解密算法 → 明文输出

> 状态：走读记录（2026-09-16），对照当前 checkout（1.0.0-rc.6_caohui）代码。
> 范围：默认构建（legacy `crates/rio`，非 `rio-v2` feature）下的对象解密读路径。
> 目的：精确回答"数据在哪里、用什么参数调用 Aes256Gcm 算法完成解密"。
> 姊妹篇：见 `reading-record/10-encryption-walkthrough-aes256-gcm.md`（写端加密链路）。
> 关键文件：`crates/rio/src/encrypt_reader.rs`（核心，`DecryptReader`）、`crates/ecstore/src/object_api/readers.rs` + `crates/ecstore/src/io_support/rio.rs`（读端入口）、`crates/ecstore/src/object_api/encryption.rs` + `rustfs/src/storage/sse.rs`（读端 key/nonce 还原）。

## 0. 一句话答案（解密调用点清单）

调用 Aes256Gcm 算法解密（`cipher.decrypt`）的代码共 **2 处**，全部在 `crates/rio/src/encrypt_reader.rs` 的 `DecryptReader::poll_read`：

| # | 位置 | 调用 | 用途 |
|---|---|---|---|
| 1 | encrypt_reader.rs:734-747 | `cipher.decrypt(nonce, Payload { msg, aad })` | **v2 帧解密**（AAD 认证，auth 失败 → `InvalidData`） |
| 2 | encrypt_reader.rs:774 | `this.cipher.decrypt(candidate_nonce, ciphertext)` | **v1 帧解密**（无 AAD；依次试 3 种历史 nonce 布局） |

另有 2 处算法对象构造：`DecryptReader::new`（:422）与 `new_multipart`（:474）都是 `Aes256Gcm::new_from_slice(&key)`。

密文帧 → 算法 → 明文：

```
密文帧（8B 头 + uvarint + 密文）──┐
nonce 12B（base nonce + 块序派生，与写端同一函数）─┤→ Aes256Gcm::decrypt（aead::Aead trait 方法）
AAD 16B（仅 v2：帧头 + 块序号，与写端同一构造）───┘        │
                                                          ▼
                                            明文 Vec<u8>（认证失败则返回错误，绝不给假明文）
```

**核心对称性（读写的契约）**：解密用的 nonce 派生函数与 AAD 构造函数**和写端是同一份代码**（`derive_block_nonce` :861 / `v2_frame_aad` :51），帧类型字节是唯一的 on-disk 事实来源——读端不看任何配置/env 决定用什么算法。

## 1. 全链路数据流

```
S3 GET（含 Range）
    │
    ▼ rustfs/src/storage/sse.rs
SseObjectEncryptionResolver::resolve_read_material（:1386-1452）
→ 从对象 metadata 还原 32B key 与 12B nonce
→ ReadEncryptionMaterial { key_bytes [u8;32], mode }
    │
    ▼ crates/ecstore/src/object_api/readers.rs
ReadTransform::Encrypted 分支（:943-986）── 按 is_multipart × material.mode 选入口函数
→ decrypt_reader / decrypt_multipart_reader / *_with_object_key
    │  （key、base_nonce、part_numbers、sequence_number 作为参数传入）
    ▼ crates/ecstore/src/io_support/rio.rs
DecryptReader::new / new_at_block / new_multipart（选择解密构造器）
    │
    ▼ crates/rio/src/encrypt_reader.rs
DecryptReader（AsyncRead）被上层逐块拉取
→ poll_read 读 8B 帧头 → 解析类型/长度 → 读密文 payload
→ 按帧类型 dispatch（0x00=v1 / 0x01、0x02=v2）
→ 派生 nonce →（v2 造 AAD）→ 调 Aes256Gcm::decrypt（:734-747 / :774）
→ 明文长度/CRC 校验 → 明文出 buffer
    │
    ▼
输出明文流 → （可压缩则解压）→ 返回给 S3 调用方
```

## 2. 读端参数来源：key 与 nonce 如何还原（sse.rs）

### 2.1 解析入口：`resolve_read_material`（sse.rs:1386-1452）

```rust
// 核心产物（crates/ecstore/src/object_api/encryption.rs:22-30）
pub enum ReadEncryptionMode {
    Direct { base_nonce: [u8; 12] },   // 直接 key：nonce 从 metadata 读回
    Object,                            // object-key 模式：nonce 恒为 [0u8; 12]
}
pub struct ReadEncryptionMaterial {
    pub key_bytes: [u8; 32],
    pub mode: ReadEncryptionMode,
}
```

映射规则（sse.rs:1443-1451）：写端 `key_kind=Direct` → 读端 `Direct{ base_nonce }`；写端 `key_kind=Object` → 读端 `Object`（对象级派生 key，nonce 用固定零）。

### 2.2 key 还原

| SSE 类型 | key_bytes 还原 | 代码位置 |
|---|---|---|
| SSE-C | 客户在 GET 请求里再带一次 key → `validate_ssec_params` 校验 32B | sse.rs:2620（`apply_ssec_decryption_material` :2601） |
| SSE-S3 / SSE-KMS | 从 metadata 的 envelope（`INTERNAL_ENCRYPTION_KEY_HEADER` / MinIO sealed key）经 KMS 解密还原 | `sse_decryption`（:1406 调用） |

读端必验：对象若带加密标记，客户端必须提供一致的 SSE 参数，否则拒绝（sse.rs:1393-1405，fail-closed）。

### 2.3 base_nonce（12B）还原

```rust
// sse.rs:2592-2599
fn read_stored_ssec_nonce(metadata, bucket, key) -> [u8; 12] {
    metadata.get(INTERNAL_ENCRYPTION_IV_HEADER)                       // "x-rustfs-encryption-iv"
        .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER)) // MinIO 兼容别名
        .and_then(|encoded| BASE64_STANDARD.decode_to_vec(encoded).ok())
        .and_then(|bytes| <[u8; 12]>::try_from(bytes.as_slice()).ok())
        .unwrap_or_else(|| generate_ssec_nonce(bucket, key))          // 旧对象无 IV → 回退派生
}
```

即：优先读写端持久化的随机 12B IV；老对象没有 IV 头时回退到旧的确定性派生。**这个 nonce 必须和写端完全一致，否则 GCM 认证立刻失败**。

## 3. 读端入口（readers.rs + io_support/rio.rs）

### 3.1 分派：`ReadTransform::Encrypted`（crates/ecstore/src/object_api/readers.rs:943-986）

| 条件 | 入口函数（io_support/rio.rs） |
|---|---|
| multipart + `Object` | `decrypt_multipart_reader_with_object_key`（:267-291） |
| multipart + `Direct` | `decrypt_multipart_reader`（:233-265） |
| 单 part + `Object` | `decrypt_reader_with_object_key`（:209-231） |
| 单 part + `Direct` | `decrypt_reader`（:170-207） |

### 3.2 参数传递（`decrypt_reader` :170-207，默认构建分支）

```rust
pub fn decrypt_reader<R>(
    reader: R,                 // 上游密文 AsyncRead（存储读到的密文帧流）
    key: [u8; 32],             // 32B 密钥（sse 还原）
    base_nonce: [u8; 12],      // 12B 基础 nonce（sse 还原）
    backend: ReadEncryptionBackend,   // 默认构建忽略（总是 legacy）
    sequence_number: u32,      // 起始帧号：>0 = v2 帧 seek（range read）
) -> Box<dyn AsyncRead + ...> {
    if sequence_number > 0 {
        // 单 part v2 帧 seek：nonce/AAD 绑绝对帧号，从指定块开始解
        DecryptReader::new_at_block(reader, key, base_nonce, sequence_number as usize)
    } else {
        DecryptReader::new(reader, key, base_nonce)
    }
}
```

`decrypt_reader_with_object_key`（:209-231）：`Object` 模式下 nonce 恒为 `[0u8; 12]`，`sequence_number` 在默认构建被忽略（:228）。multipart 版（:233-265）额外传 `multipart_parts: Vec<usize>`（存在的 part 号列表，来自对象的 part 元数据）。

## 4. 核心层：DecryptReader（crates/rio/src/encrypt_reader.rs:379-851）

### 4.1 解密参数与状态（结构体 :382-412）

```rust
pub struct DecryptReader<R> {
    #[pin] pub inner: R,               // 上游密文 AsyncRead
    cipher: Aes256Gcm,                 // :385 算法对象（:422/:474 构造）
    base_nonce: [u8; 12],              // :386 对象级基础 nonce（记录在 metadata）
    current_nonce_base: [u8; 12],      // :387 当前段有效 nonce 基（multipart 切 part 会变）
    multipart_mode: bool,              // :388 是否 multipart
    multipart_parts: Vec<usize>,       // :389 part 号列表
    current_part_index: usize,         // :390 在列表中的下标
    current_part: usize,               // :391 当前 part 号
    block_index: usize,                // :392 块序号（nonce 派生 + AAD 用）
    buffer / buffer_pos,               // 明文输出缓冲
    finished: bool,                    // 解密流是否结束
    header_buf: [u8; 8],               // 帧头缓冲
    header_read / header_done,         // 帧头读取状态
    ciphertext_buf / ciphertext_read / ciphertext_len,  // 密文 payload 读取状态
    current_frame_type: u8,            // 当前帧类型
    segment_frame_version: Option<u8>, // 段内已锁定帧版本（1 或 2）
    saw_final_frame: bool,             // 段内是否已见认证末帧
    segment_frames: usize,             // 段内帧数
    stream_saw_v2: bool,               // 整条流是否见过 v2 帧
    segments_completed: usize,         // 已完成的段数
    v1_nonce_layout: Option<V1NonceLayout>,  // v1 段锁定的 nonce 布局
    legacy_nonce_fallback: bool,       // :411 旧 v1 nonce 布局回退开关
}
```

### 4.2 构造器（:419-500）

| 构造器 | key | nonce | 其他参数 | 用途 |
|---|---|---|---|---|
| `new`（:419） | `[u8; 32]` | `[u8; 12]` | — | 单 part 或段首 |
| `new_at_block`（:460） | `[u8; 32]` | `[u8; 12]` | `starting_block_index: usize` | **v2 Range read**：从指定绝对帧号开始（nonce/AAD 绑绝对序号） |
| `new_multipart`（:466） | `[u8; 32]` | `[u8; 12]` | `multipart_parts: Vec<usize>` | multipart：首 part nonce 用 `derive_part_nonce(base, first_part)`（:468） |

v1 段起始 nonce 布局：`V1NonceLayout` 枚举（:372-377）——`Current`（现布局）/ `LegacyBlock`（旧版块窗口）/ `ReusedPart`（整个段复用 part nonce，仅当 `ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK` 开启，:344-365 默认 true）。

### 4.3 poll_read 主循环（:507-848）

**① 输出明文缓冲**（:512-521）：有缓存的明文先拷给调用方。

**② 读 8B 帧头**（:527-583）逐字节读满 8B；EOF 时做**截断检测**（:536-567）：
- v2 段没见到认证末帧就 EOF → `"encrypted stream truncated before its final frame"`（:541-549）
- v2 multipart 段数不足 → `"encrypted stream ended before all part segments were read"`（:553-561）

**③ 解析帧头**（:585-589）：
```rust
let typ = this.header_buf[0];                                              // 帧类型
let len = (header_buf[1]) | (header_buf[2] << 8) | (header_buf[3] << 16);  // 24-bit 小端长度
```

**④ `0xFF` 段结束帧**（:591-631）：v2 段在认证末帧前出现结束帧 → 报错（:595-599）；合法结束则 multipart 切到下一 part（:607-623，重建 `current_nonce_base = derive_part_nonce(base_nonce, next_part)`，`block_index` 归零）或整流结束（:626-630）。

**⑤ 帧版本 dispatch + 一致性检查**（:633-662）：

```rust
let frame_version = match typ {
    FRAME_TYPE_V1 => 1,                                    // 0x00
    FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => 2,              // 0x01 / 0x02
    other => Err("unknown encrypted frame type")           // ★ 目前 0x03/0x04 在此报错
};
// saw_final_frame 之后再出现帧 → 错（:643-648）
// 段内版本混用（v1/v2）→ "mixes frame format versions"（:649-658）
```

**⑥ 读密文 payload**（:679-715）：`payload_len = len - 4`（:679，与写端 `clen` 公式对称）；密文不足 16B 的截断安全处理（:718-724 注释）。

**⑦ 解析明文长度 uvarint**（:725-731），切割出真正的密文切片。

**⑧ ★ v2 解密（:734-747）**——**真算法调用处 1**：

```rust
// v2：nonce 派生与写端完全一致，AAD 与写端同一函数；认证失败一律判为篡改
let aad = v2_frame_aad(this.header_buf, *this.block_index);   // 帧头 8B + 块序号 lilEndian
this.cipher
    .decrypt(&nonce, Payload { msg: ciphertext, aad: &aad })  // aead::Aead::decrypt
    .map_err(|_| Error::new(InvalidData, "v2 encrypted frame failed authentication"))?
```

**⑨ ★ v1 解密（:748-795）**——**真算法调用处 2**：无 AAD；依次试 `Current` / `LegacyBlock` / `ReusedPart` 三种历史 nonce 布局（:759-763），非零块不再试首块已排除的布局（:764-770）；首块成功时锁定布局（:791-793）。

**⑩ 输出前校验**（:796-845）：
- 解出明文长度必须等于帧头里的 uvarint 明文长（:807-811）
- 明文 CRC-32 必须等于帧头 [4..8) 存的值（:813-826）
- 明文入 `buffer` 输出（:828-845）；空明文帧（v2 块边界对齐的空末帧）继续解析不下发（:834-840）

## 5. 读写对称性对照（解密契约全靠与写端同源）

| 项 | 写端（10 号文档） | 读端（本文档） | 一致性保证 |
|---|---|---|---|
| nonce 派生 | `derive_block_nonce` :861 | **同一函数** :861 | 同一函数，天然一致 |
| part nonce | `derive_part_nonce` :869 | **同一函数** :869 | 同上 |
| AAD 构造 | `v2_frame_aad` :51 | **同一函数** :51 | 同上 |
| 帧类型字节 | 0x00/0x01/0x02 写出 | :633-642 读入 dispatch | on-disk 唯一事实来源 |
| 帧 len 公式 | clen = int_len + 密文 + 4 | payload = len - 4 | 对称 |
| 明文长度 | uvarint 写入 | uvarint 解析 + :807 校验 | 双保险 |
| CRC-32 | 明文 CRC 写帧头 [4..8) | 解出后重算比对 :813-826 | 篡改检测 |
| 认证 | 加密时 tag 附在密文尾 | tag 校验失败 → InvalidData | **防篡改核心** |

## 6. 汇总：密文输入 → Aes256Gcm → 明文输出

| 环节 | 位置 | 参数（全部标注） |
|---|---|---|
| material 还原 | sse.rs:1386-1452 | 读 metadata → `key_bytes [u8;32]` + `mode`（Direct{base_nonce: [u8;12]} / Object） |
| nonce 还原 | sse.rs:2592-2599 | `INTERNAL_ENCRYPTION_IV_HEADER` / MinIO 别名 → 12B；缺失回退 `generate_ssec_nonce(bucket, key)` |
| 入口分派 | readers.rs:943-986 | `is_multipart` × `material.mode` → 4 个 `decrypt_reader*` |
| 构造器 | io_support/rio.rs:170-207 | `sequence_number > 0` → `new_at_block`；`Object` 模式 nonce=`[0u8;12]` |
| 算法构造 | encrypt_reader.rs:422 / :474 | 入参 `key: [u8; 32]`；产物 `Aes256Gcm` |
| 帧头解析 | encrypt_reader.rs:585-589 | `typ=header[0]`、`len=24-bit 小端` |
| 版本 dispatch | encrypt_reader.rs:633-642 | 0x00→v1，0x01/0x02→v2，其余 → "unknown encrypted frame type" |
| **Aes256Gcm 调用（v2）** | **:734-747** | `decrypt(nonce, Payload { msg: ciphertext, aad: &v2_frame_aad(header, block_index) })` |
| **Aes256Gcm 调用（v1）** | **:774** | `decrypt(candidate_nonce, ciphertext)`（试 3 种 nonce 布局） |
| 认证失败处理 | :747 / :782-790 | `InvalidData` / 布局全失败报错——**绝不输出假明文** |
| 明文校验 | :807-826 | 长度一致 + CRC-32 一致后才输出 |
| 明文输出 | :828-845 | `plaintext` 入 buffer，按调用方容量逐段拷贝 |

## 7. 兼容性 / 安全要点

- **fail-closed**：v2 认证失败、长度不符、CRC 不符、段截断、版本混用——全部直接报错，不给部分明文。
- **截断检测**：v2 段的认证末帧（0x02）使"删除尾部帧"可被检出（:541-549，:643-648）。
- **旧对象兼容**：v1 无认证头，故用"试 nonce 布局 + CRC 校验"兜底（:748-795）；`RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK` 只影响 v1。
- **Range read**：`new_at_block`（:460）只对 v2 帧有意义——v1 帧无闭式位置；nonce/AAD 绑绝对序号保证中间帧只有在正确位置才能认证。

## 8. 与 multi-cipher 方案（09 号文档）的对应

本走读的 5 个读端锚点正是 09 号方案改动点 E–H 的落点：

| 改动点 | 落点（本文档锚点） | 内容 |
|---|---|---|
| 改动点 E：cipher 类型化 | `DecryptReader.cipher`（:385）→ `Option<EncryptCipher>`，新增 `key` 字段（:386 附近） | 算法由首帧类型决定，新增 `cipher_for_type(typ, key)` 按帧类型字节构造 |
| 改动点 F：帧版本 dispatch | :633-642 | match 增加 `0x03/0x04 => 2`（复用 v2 逻辑）；`segment_frame_version` 混用检查（:649-658）零改动——0x01/0x03 混用自然命中 "mixes" 分支 |
| 改动点 G：首帧初始化 + 混用检查 | :662 附近 | 首帧 `cipher.is_none()` → `cipher_for_type` 初始化；后续帧对比 cipher token，不一致 → "mixes cipher algorithms" |
| 改动点 H：解密调用路由 | :739-747（v2）、:774（v1） | `this.cipher` 显式 match 两个变体分别调 `decrypt`（按 0.5-E 契约，Aes256GcmDemo 的 `decrypt` 与 AES 同构） |

读端白名单纪律：0x03/0x04 进入读端（改动点 F）后，旧节点读新算法对象不再报 "unknown frame type"，而是按 v2 路径认证失败——**读白名单只增不减**（09 号第 5 节 R1）。

## 9. 详细代码流程（真实代码逐段全标注）

> 以下全部为 `crates/rio/src/encrypt_reader.rs` 的真实代码，注释为逐个参数/逐行含义标注。行号 = 当前 checkout（1.0.0-rc.6_caohui）实际行号。

### 9.1 DecryptReader 结构体与构造器（:379-500）

```rust
pin_project! {
    /// A reader wrapper that decrypts data on the fly using AES-256-GCM.
    pub struct DecryptReader<R> {
        #[pin]
        pub inner: R,                       // 上游密文 AsyncRead（读盘密文帧流）
        cipher: Aes256Gcm,                  // :385 ★ 算法对象（:422/:474 new_from_slice 构造）
        base_nonce: [u8; 12],               // :386 对象级基础 nonce（metadata 记录值）
        current_nonce_base: [u8; 12],       // :387 当前段有效 nonce 基（multipart 切 part 会重建）
        multipart_mode: bool,               // :388 是否 multipart
        multipart_parts: Vec<usize>,        // :389 对象实际存在的 part 号列表
        current_part_index: usize,          // :390 列表下标（推进用）
        current_part: usize,                // :391 当前 part 号
        block_index: usize,                 // :392 块序号：nonce 派生 + v2 AAD（与写端对称）
        buffer: Vec<u8>,                    // 明文输出缓冲
        buffer_pos: usize,                  // 明文缓冲消费位置
        finished: bool,                     // 解密流结束
        header_buf: [u8; 8],                // 8B 帧头缓冲
        header_read: usize,                 // 已读帧头字节数
        header_done: bool,                  // 帧头是否读完
        ciphertext_buf: Vec<u8>,            // 密文 payload 缓冲
        ciphertext_read: usize,             // 已读密文字节数
        ciphertext_len: usize,              // 本帧密文目标长度
        current_frame_type: u8,             // 当前帧类型字节
        segment_frame_version: Option<u8>,  // 段内锁定的帧版本（1/2）
        saw_final_frame: bool,              // 段内是否已见认证末帧（0x02）
        segment_frames: usize,              // 段内帧计数
        stream_saw_v2: bool,                // 整流是否见过 v2 帧
        segments_completed: usize,          // 已完成段数
        v1_nonce_layout: Option<V1NonceLayout>,   // v1 段锁定的 nonce 布局
        legacy_nonce_fallback: bool,        // :411 旧 v1 布局回退开关（env，默认 true）
    }
}

/// 参数：
///   inner: R          —— 密文上游（storage reader）
///   key:   [u8; 32]   —— 32B 密钥（sse 还原，见 §2）
///   nonce: [u8; 12]   —— 对象级基础 nonce（sse 还原）
pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
    Self {
        inner,
        cipher: Aes256Gcm::new_from_slice(&key).expect("key"),  // ★ 算法对象在此构造
        base_nonce: nonce,
        current_nonce_base: nonce,        // 单 part 段：直接以对象 nonce 为基
        multipart_mode: false,
        multipart_parts: Vec::new(),
        current_part_index: 0,
        current_part: 0,
        block_index: 0,                   // 从块 0 开始（new_at_block 才从中间开始）
        buffer: Vec::new(),
        buffer_pos: 0,
        finished: false,
        header_buf: [0u8; 8],
        header_read: 0,
        header_done: false,
        ciphertext_buf: Vec::new(),
        ciphertext_read: 0,
        ciphertext_len: 0,
        current_frame_type: FRAME_TYPE_V1,   // 默认 v1，读到帧头后按类型更新
        segment_frame_version: None,
        saw_final_frame: false,
        segment_frames: 0,
        stream_saw_v2: false,
        segments_completed: 0,
        v1_nonce_layout: None,
        legacy_nonce_fallback: legacy_nonce_fallback_enabled(),
    }
}

/// v2 Range read：从指定绝对帧号开始解
/// 参数 starting_block_index: usize —— 首帧绝对序号（nonce/AAD 绑绝对序号，
///    只有真正定位到帧边界并给出正确的块号才能认证通过）
pub fn new_at_block(inner: R, key: [u8; 32], nonce: [u8; 12], starting_block_index: usize) -> Self {
    let mut reader = Self::new(inner, key, nonce);
    reader.block_index = starting_block_index;
    reader
}

/// multipart 解密
/// 参数 multipart_parts: Vec<usize> —— 对象存在的 part 号列表（来自 part 元数据）
pub fn new_multipart(inner: R, key: [u8; 32], base_nonce: [u8; 12], multipart_parts: Vec<usize>) -> Self {
    let first_part = multipart_parts.first().copied().unwrap_or(1);   // 首个 part 号
    let initial_nonce = derive_part_nonce(&base_nonce, first_part);   // part 级 nonce 基
    Self {
        inner,
        cipher: Aes256Gcm::new_from_slice(&key).expect("key"),  // ★ 算法对象构造
        base_nonce,                      // 对象级基础 nonce 保留，切 part 时重建
        current_nonce_base: initial_nonce,   // 当前段 = 首 part 的 nonce 基
        multipart_mode: true,
        multipart_parts,
        current_part_index: 0,
        current_part: first_part,
        block_index: 0,                  // 每个 part 段都从块 0 开始
        // ……其余状态字段同上，省略……
        legacy_nonce_fallback: legacy_nonce_fallback_enabled(),
    }
}
```

### 9.2 poll_read：输出缓冲 + 帧头读取 + EOF/截断检查（:507-600）

```rust
fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
    let mut this = self.project();

    loop {
        // ① 有缓存的明文先拷给调用方（可能一帧分多次 poll 才消费完）
        if *this.buffer_pos < this.buffer.len() {
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len() - *this.buffer_pos);
            buf.put_slice(&this.buffer[*this.buffer_pos..*this.buffer_pos + to_copy]);
            *this.buffer_pos += to_copy;
            if *this.buffer_pos == this.buffer.len() {
                this.buffer.clear();
                *this.buffer_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        if *this.finished {
            return Poll::Ready(Ok(()));              // 整流解完
        }

        if *this.ciphertext_len == 0 {
            // ② 读 8B 帧头（仅当上一帧密文已消费完）
            while !*this.header_done && *this.header_read < 8 {
                let mut temp = [0u8; 8];
                let mut temp_buf = ReadBuf::new(&mut temp[0..8 - *this.header_read]);
                match this.inner.as_mut().poll_read(cx, &mut temp_buf) {   // inner = 密文上游
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = temp_buf.filled().len();
                        if n == 0 {
                            // —— EOF 截断检测（:536-567）——
                            if *this.header_read == 0 {
                                // v2 段必须有认证末帧（0x02）；干净 EOF 先于末帧 = 尾帧被删
                                if *this.segment_frame_version == Some(2)
                                    && !*this.saw_final_frame
                                    && *this.segment_frames > 0
                                {
                                    return Poll::Ready(Err(Error::new(UnexpectedEof,
                                        "encrypted stream truncated before its final frame")));
                                }
                                // v2 multipart 必须读完所有列出的 part 段
                                if *this.stream_saw_v2
                                    && *this.multipart_mode
                                    && *this.segments_completed < this.multipart_parts.len()
                                {
                                    return Poll::Ready(Err(Error::new(UnexpectedEof,
                                        "encrypted stream ended before all part segments were read")));
                                }
                                *this.finished = true;      // 正常读完
                                return Poll::Ready(Ok(()));
                            }
                            // 帧头读到一半 EOF：帧被截断
                            return Poll::Ready(Err(Error::new(UnexpectedEof,
                                "unexpected EOF while reading encrypted block header")));
                        }
                        this.header_buf[*this.header_read..*this.header_read + n]
                            .copy_from_slice(&temp_buf.filled()[..n]);   // 累计帧头字节
                        *this.header_read += n;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            }

            if !*this.header_done && *this.header_read == 8 {
                *this.header_done = true;      // 帧头凑满 8B
            }
            if !*this.header_done {
                return Poll::Pending;          // 帧头不完整：等下次 poll
            }

            // ③ 解析帧头：type + 24-bit 小端长度
            let typ = this.header_buf[0];
            let len = (this.header_buf[1] as usize)
                | ((this.header_buf[2] as usize) << 8)
                | ((this.header_buf[3] as usize) << 16);
            *this.header_read = 0;             // 复位，供下一帧
            *this.header_done = false;

            // ④ 0xFF 段结束帧处理（:591-631，见 9.3）
            if typ == FRAME_TYPE_END {
                // v2 段结束帧必须紧跟认证末帧；提前出现 = 尾帧被删
                if *this.segment_frame_version == Some(2) && !*this.saw_final_frame {
                    return Poll::Ready(Err(Error::new(InvalidData,
                        "encrypted segment terminator before the final frame")));
                }
                // …（段结束收尾 / multipart 切 part / 整流结束，见 9.3）…
            }

            // ⑤ 帧版本 dispatch + 一致性检查（:633-662，见 9.3）
            let frame_version = match typ {
                FRAME_TYPE_V1 => 1,                              // 0x00
                FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => 2,        // 0x01 / 0x02
                other => return Poll::Ready(Err(Error::new(InvalidData,
                    format!("unknown encrypted frame type {other:#04x}")))),  // ★ 0x03/0x04 现状在此报错
            };
        }
        // ……密文读取与解密，见 9.4 / 9.5……
    }
}
```

### 9.3 段结束帧 + 版本一致性 + 密文读取（:591-731）

```rust
// （接上）typ == FRAME_TYPE_END 时的处理（:591-631）
if typ == FRAME_TYPE_END {
    if *this.segment_frame_version == Some(2) && !*this.saw_final_frame {
        return Poll::Ready(Err(Error::new(InvalidData, "encrypted segment terminator before the final frame")));
    }
    *this.segments_completed += 1;         // 完成一个段
    *this.segment_frame_version = None;    // 段状态复位
    *this.saw_final_frame = false;
    *this.segment_frames = 0;
    *this.v1_nonce_layout = None;

    if *this.multipart_mode {
        // multipart：推进到下一 part，重建 nonce 基，块号归零
        let next_part = if *this.current_part_index + 1 < this.multipart_parts.len() {
            *this.current_part_index += 1;
            this.multipart_parts[*this.current_part_index]
        } else {
            *this.current_part + 1
        };
        *this.current_part = next_part;
        *this.current_nonce_base = derive_part_nonce(this.base_nonce, *this.current_part);  // ★ 新段 nonce 基
        *this.block_index = 0;
        *this.ciphertext_read = 0;
        *this.ciphertext_len = 0;
        continue;                          // 回到 loop 顶部，读下一段帧头
    }

    *this.finished = true;                 // 单 part：整流结束
    *this.block_index = 0;
    *this.ciphertext_read = 0;
    *this.ciphertext_len = 0;
    continue;
}

// 帧版本 dispatch（:633-642）—— 读端只认磁盘帧类型字节
let frame_version = match typ {
    FRAME_TYPE_V1 => 1,
    FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => 2,
    other => {
        return Poll::Ready(Err(Error::new(InvalidData,
            format!("unknown encrypted frame type {other:#04x}"))));
    }
};
// 认证末帧之后不允许再出现帧（:643-648）
if *this.saw_final_frame {
    return Poll::Ready(Err(Error::new(InvalidData, "encrypted frame after the segment's final frame")));
}
// 段内版本混用检查（:649-658）：首帧锁定版本，后续不一致报错
match *this.segment_frame_version {
    None => *this.segment_frame_version = Some(frame_version),
    Some(version) if version != frame_version => {
        return Poll::Ready(Err(Error::new(InvalidData, "encrypted segment mixes frame format versions")));
    }
    Some(_) => {}
}
if frame_version == 2 {
    *this.stream_saw_v2 = true;            // 记录整流见过 v2（用于 EOF 缺段检查）
}
*this.current_frame_type = typ;            // 记录当前帧类型（解密用）

if len == 0 {
    // v2 空长帧必是伪造（v2 每帧都带 tag）；v1 空长按历史行为当段结束
    if frame_version == 2 {
        return Poll::Ready(Err(Error::new(InvalidData, "zero-length v2 encrypted frame")));
    }
    tracing::warn!("encountered zero-length encrypted block, treating as end of stream");
    *this.finished = true;
    *this.ciphertext_read = 0;
    *this.ciphertext_len = 0;
    continue;
}

// 帧头 len 字段包含 uvarint + 密文（+写端另加的 4，此处减回）：读 payload 目标长度
let Some(payload_len) = len.checked_sub(4) else {          // len < 4 = 损坏
    return Poll::Ready(Err(Error::other("Invalid encrypted block length")));
};
if this.ciphertext_buf.len() < payload_len {
    this.ciphertext_buf.resize(payload_len, 0);            // 按需扩容密文缓冲
}
*this.ciphertext_len = payload_len;
*this.ciphertext_read = 0;

// ⑥ 读满 payload_len 字节密文（:691-715）
while *this.ciphertext_read < *this.ciphertext_len {
    let mut temp_buf = ReadBuf::new(&mut this.ciphertext_buf[*this.ciphertext_read..*this.ciphertext_len]);
    match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Ok(())) => {
            let n = temp_buf.filled().len();
            if n == 0 {
                return Poll::Ready(Err(Error::new(UnexpectedEof,
                    "unexpected EOF while reading encrypted block payload")));
            }
            *this.ciphertext_read += n;
        }
        Poll::Ready(Err(e)) => { *this.ciphertext_read = 0; *this.ciphertext_len = 0; return Poll::Ready(Err(e)); }
    }
}
if *this.ciphertext_read < *this.ciphertext_len {
    return Poll::Pending;
}

// ⑦ 密文开头是明文长度 uvarint，解析后切出真正的密文（:725-731）
// 注意：uvarint 解析对任意长度切片都安全（截断/损坏帧不会 panic）
let ciphertext_buf = &this.ciphertext_buf[..*this.ciphertext_len];
let (plaintext_len, uvarint_len) = rustfs_utils::uvarint(ciphertext_buf);
if uvarint_len <= 0 || uvarint_len as usize > ciphertext_buf.len() {
    return Poll::Ready(Err(Error::new(InvalidData, "Invalid encrypted block length prefix")));
}
let ciphertext = &ciphertext_buf[uvarint_len as usize..];   // 纯密文（明文 + 16B tag）
```

### 9.4 v2 解密：★ 真算法调用处 1（:732-747）

```rust
// ⑧ v2 解密：nonce 派生与写端同一函数；AAD 与写端同一构造
let block_nonce = derive_block_nonce(this.current_nonce_base, *this.block_index);  // 块 nonce
let nonce = Nonce::try_from(block_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
let plaintext = if *this.current_frame_type != FRAME_TYPE_V1 {
    // 参数：nonce（12B 派生值，与写端 :221 完全一致）
    //       Payload { msg: 密文切片, aad: 16B }（aad 与写端 :154 完全一致）
    // 行为：tag 校验失败 → Err → 视为篡改，报 InvalidData（绝不放行假明文）
    let aad = v2_frame_aad(this.header_buf, *this.block_index);   // 帧头 8B ‖ 块序号 lilEndian
    this.cipher
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| Error::new(InvalidData, "v2 encrypted frame failed authentication"))?
} else {
    // ⑨ v1 解密（:748-795，见 9.5）
};
```

### 9.5 v1 解密 + 输出校验：★ 真算法调用处 2（:748-848）

```rust
} else {
    // v1 历史 nonce 布局：同一密文试 3 种候选 nonce（无 AAD，靠 tag + CRC 判定）
    let legacy_part_nonce = if *this.multipart_mode {
        derive_legacy_part_nonce(this.base_nonce, *this.current_part)  // 旧版 part 窗口也在 [8..12)
    } else {
        *this.base_nonce
    };
    let legacy_block_nonce = derive_block_nonce(&legacy_part_nonce, *this.block_index);
    let legacy_part_nonce = Nonce::try_from(legacy_part_nonce.as_slice())
        .map_err(|_| Error::other("invalid nonce length"))?;
    let legacy_block_nonce = Nonce::try_from(legacy_block_nonce.as_slice())
        .map_err(|_| Error::other("invalid nonce length"))?;
    let layouts = [
        (V1NonceLayout::Current, &nonce),                // 现行布局
        (V1NonceLayout::LegacyBlock, &legacy_block_nonce), // 旧版块窗口
        (V1NonceLayout::ReusedPart, &legacy_part_nonce),   // 整段复用 part nonce（受 env 开关控制）
    ];
    // 非零块只试首块已锁定的布局（防重放：把 0 号块密文挪到别处解不开）
    let selected = if *this.block_index == 0 { None } else { *this.v1_nonce_layout };
    let mut plaintext = None;
    let mut last_error = None;
    for (layout, candidate_nonce) in layouts {
        if selected.is_some_and(|expected| expected != layout) { continue; }   // 排除已锁定布局以外的
        if layout == V1NonceLayout::ReusedPart && !*this.legacy_nonce_fallback { continue; }
        match this.cipher.decrypt(candidate_nonce, ciphertext) {   // ★ v1 真算法调用处（无 AAD）
            Ok(value) => { plaintext = Some((value, layout)); break; }   // tag 校验通过 = 命中
            Err(error) => last_error = Some(error),
        }
    }
    let (plaintext, layout) = plaintext.ok_or_else(|| {
        Error::new(InvalidData, format!("decrypt error: {}", last_error.map_or_else(|| "nonce layout rejected".to_string(), |e| e.to_string())))
    })?;
    if *this.block_index > 0 && this.v1_nonce_layout.is_none() {
        *this.v1_nonce_layout = Some(layout);   // 首块之后锁布局
    }
    plaintext
};
if *this.current_frame_type == FRAME_TYPE_V2_FINAL {
    *this.saw_final_frame = true;              // 认证末帧已见（供截断检查）
}
*this.segment_frames += 1;

// ⑩ 输出前校验：次数看（明文长度 + CRC），过了才交给上层
if plaintext.len() != plaintext_len as usize {            // 明文长必须 == 帧头 uvarint
    return Poll::Ready(Err(Error::other("Plaintext length mismatch")));
}
let expected_crc = (this.header_buf[4] as u32) | ((this.header_buf[5] as u32) << 8)
    | ((this.header_buf[6] as u32) << 16) | ((this.header_buf[7] as u32) << 24);
let actual_crc = {                                          // 重算明文 CRC-32
    let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    hasher.update(&plaintext);
    hasher.finalize() as u32
};
if actual_crc != expected_crc {
    return Poll::Ready(Err(Error::other("CRC32 mismatch")));
}

*this.buffer = plaintext;          // 明文入输出缓冲
*this.buffer_pos = 0;
*this.block_index += 1;            // 块号 +1（下帧 nonce/AAD 变化）
*this.ciphertext_read = 0;
*this.ciphertext_len = 0;

if this.buffer.is_empty() {
    // 认证过的空帧（块对齐段 v2 空末帧）无明文可下发：继续解析，不当作 EOF
    continue;
}
let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
buf.put_slice(&this.buffer[..to_copy]);   // 明文拷进调用方
*this.buffer_pos += to_copy;
return Poll::Ready(Ok(()));
```

### 9.6 读端 nonce 派生与 v1 历史布局（:344-377，:861-885）

```rust
// 读端 v1 布局回退开关（:344-365，OnceLock 缓存，默认 true）
pub const ENV_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK: &str = "RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK";
const DEFAULT_RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK: bool = true;
// 开 = 兼容 1.0.0-alpha.91 之前写的老对象；关 = 移除 "整段复用 part nonce" 攻击面（backlog#2369 P2）

/// v1 段 nonce 布局枚举（:372-377）
enum V1NonceLayout {
    Current,      // 现行布局（块窗口 base[8..12)）
    LegacyBlock,  // 旧版块布局
    ReusedPart,   // 旧版整段复用 part nonce（仅 fallback 开启时参与试解）
}

// —— nonce 派生（与写端完全同源的 4 个函数，:861-885）——
fn derive_block_nonce(base: &[u8; 12], block_index: usize) -> [u8; 12] {
    derive_nonce_offset(base, 8, block_index)   // 块窗口 [8..12)：与写端同一函数
}
pub fn multipart_part_nonce(base_nonce: [u8; 12], part_number: usize) -> [u8; 12] {
    derive_part_nonce(&base_nonce, part_number) // part 窗口 [4..8)
}
fn derive_part_nonce(base: &[u8; 12], part_number: usize) -> [u8; 12] {
    derive_nonce_offset(base, 4, part_number)
}
fn derive_legacy_part_nonce(base: &[u8; 12], part_number: usize) -> [u8; 12] {
    derive_nonce_offset(base, 8, part_number)   // 旧版 part 窗口也在 [8..12)，与块窗口重叠 → v1 试解用
}
/// 参数：base 12B 基础 nonce；start 窗口起点（8=块 / 4=part）；offset 序号
fn derive_nonce_offset(base: &[u8; 12], start: usize, offset: usize) -> [u8; 12] {
    let mut nonce = *base;
    let mut suffix = [0u8; 4];
    suffix.copy_from_slice(&nonce[start..start + 4]);   // 取窗口 4B
    let current = u32::from_be_bytes(suffix);           // 大端读
    let next = current.wrapping_add(offset as u32);     // 加序号
    nonce[start..start + 4].copy_from_slice(&next.to_be_bytes());  // 大端写回
    nonce
}
```