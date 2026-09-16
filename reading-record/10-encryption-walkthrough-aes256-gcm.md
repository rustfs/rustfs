# AES-256-GCM 数据面加密完整走读：输入 → 调用算法 → 输出密文

> 状态：走读记录（2026-09-16），对照当前 checkout（1.0.0-rc.6_caohui）代码。
> 范围：默认构建（legacy `crates/rio`，非 `rio-v2` feature）下的对象加密写路径与读端对照。
> 目的：精确回答"数据在哪里、用什么参数调用 Aes256Gcm 算法完成加密"。
> 关键文件：`crates/rio/src/encrypt_reader.rs`（核心）、`crates/ecstore/src/io_support/rio.rs`（写端接线）、`rustfs/src/storage/sse.rs`（key/nonce 来源）。

## 0. 一句话答案（加密调用点清单）

真正调用 Aes256Gcm 算法加密的代码共 **3 处**，全部在 `crates/rio/src/encrypt_reader.rs`：

| # | 位置 | 调用 | 用途 |
|---|---|---|---|
| 1 | encrypt_reader.rs:85 | `Aes256Gcm::new_from_slice(&key)` | 用 32B key 构造算法对象（`EncryptReader::new` 内） |
| 2 | encrypt_reader.rs:151-164 | `cipher.encrypt(nonce, Payload { msg, aad })` | **v2 帧加密**（`build_frame` 内，推荐格式） |
| 3 | encrypt_reader.rs:273-276 | `this.cipher.encrypt(&nonce, plaintext)` | **v1 帧加密**（`poll_read` 内联，默认格式） |

输入 → 算法 → 输出：

```
明文块 &[u8]（≤ 8KB） ─┐
nonce 12B（base nonce + 块序派生）─┤→ Aes256Gcm::encrypt（aead::Aead trait 方法）
AAD 16B（仅 v2：帧头 + 块序号）───┘        │
                                           ▼
                             密文 Vec<u8> = 明文 + 16B GCM tag
```

`cipher.encrypt(...)` 是 `aead::Aead::encrypt` trait 方法（`:16` 导入 `Aead`），`Aes256Gcm` 是 `aes-gcm` crate 对它的实现。

## 1. 全链路数据流

```
S3 PUT 请求体（明文）
    │
    ▼ rustfs/src/storage/sse.rs
sse_encryption / sse_prepare_encryption（:2013 / :2084）
→ 生成 EncryptionMaterial { key_bytes [u8;32], base_nonce [u8;12], key_kind, ... }
    │
    ▼ crates/ecstore/src/io_support/rio.rs
WritePlan::apply（:425-505）── 按 WriteEncryption.mode + RUSTFS_ENCRYPTION_FRAME_V2
→ EncryptReader::new / new_v2 / new_multipart*（选择加密构造器）
    │  （key、base_nonce 作为构造器参数传入）
    ▼ crates/rio/src/encrypt_reader.rs
EncryptReader（AsyncRead）被下游 HashReader/写盘逐块拉取
→ poll_read 从 inner 读明文块
→ 派生块 nonce → 调 Aes256Gcm::encrypt（build_frame :151-164 / 内联 :273-276）
    │
    ▼
输出帧流：8B 帧头 + 明文长度 uvarint + 密文（明文 + 16B tag）→ 写盘
```

## 2. 参数来源层：key 与 nonce 从哪来（sse.rs）

### 2.1 `EncryptionMaterial`（rustfs/src/storage/sse.rs:796）

写端所需的加密参数就两个，封在 `EncryptionMaterial` 里：

| 字段 | 类型 | 含义 |
|---|---|---|
| `key_bytes` | `[u8; 32]` | AES-256 密钥（直接传给 `Aes256Gcm::new_from_slice`） |
| `base_nonce` | `[u8; 12]` | 96-bit GCM 基础 nonce（每个对象独立，存进 metadata） |
| `key_kind` | `EncryptionKeyKind` | `Direct` = 门面 key；`Object` = object-key 派生模式 |

`EncryptionMaterial` 在写端转换成 `WriteEncryption`（sse.rs:1528-1538 的到 `WritePlan` 的映射）：

| key_kind | 路径 | 构造的 WriteEncryption |
|---|---|---|
| `Object`（无 part） | sse.rs:1534 | `WriteEncryption::singlepart_object_key(key_bytes)` |
| `Direct`（无 part） | sse.rs:1538 | `WriteEncryption::singlepart(key_bytes, base_nonce)` |
| `Object`（有 part） | sse.rs:1532 | `WriteEncryption::multipart_object_key(key_bytes, part_number)` |
| `Direct`（有 part） | sse.rs:1536 | `WriteEncryption::multipart(key_bytes, base_nonce, part_number)` |

### 2.2 三种 key 来源

| SSE 类型 | key_bytes 来源 | 代码位置 |
|---|---|---|
| SSE-C（客户自带 key） | 客户 key Base64 解码，校验必须恰好 32B | `validate_ssec_params` sse.rs:4156；直接 `validated.key_bytes` sse.rs:2564 |
| SSE-S3 / SSE-KMS（托管） | `derive_object_key`：`HMAC-SHA256(external_key, OBJECT_KEY_DERIVATION_CONTEXT ‖ random32)` | sse.rs:1655-1667 |
| rio-v2 object-key 模式 | 同上派生（封在 sealed key 里） | sse.rs:1655 |

### 2.3 base_nonce（12B）的生成与持久化

- **SSE-C Direct + 默认构建（非 rio-v2）**：每个对象生成**随机** 12B nonce（sse.rs:2562-2563 `rand::rng().fill_bytes`）。注释明确：若用确定性 nonce，同一对象在相同 SSE-C key 下覆写会重用 (key, nonce) 对，灾难性破坏 AES-GCM。
- **SSE-S3/KMS**：sse_prepare_encryption 侧对 multipart 会话也生成随机 nonce。
- nonce 持久化：`encryption_material_to_metadata`（sse.rs:1810）写到对象 metadata（`INTERNAL_ENCRYPTION_IV_HEADER` / MinIO 别名 `MINIO_INTERNAL_ENCRYPTION_IV_HEADER`），解密时读回。

## 3. 写端入口：WritePlan::apply（crates/ecstore/src/io_support/rio.rs:425-505）

### 3.1 参数载体

```rust
pub struct WriteEncryption {          // io_support/rio.rs:317-321
    key_bytes: [u8; 32],              // 32B 密钥
    mode: WriteEncryptionMode,        // 4 个写模式
}
```

`WriteEncryptionMode`（:349-362）4 个变体携带写端参数：

| 变体 | 参数 |
|---|---|
| `SinglepartObjectKey` | （无，key 即 object key） |
| `Singlepart { base_nonce }` | base_nonce `[u8; 12]` |
| `MultipartLegacy { base_nonce, multipart_part_number }` | base_nonce `[u8; 12]` + part 号 `usize` |
| `MultipartObjectKey { multipart_part_number }` | part 号 `u32` |

### 3.2 构造器选择矩阵（:445-494）

`RUSTFS_ENCRYPTION_FRAME_V2`（默认 `false`，:331-347 OnceLock 缓存）决定 v1/ v2 布局：

| mode | frame_v2=false（默认，v1 帧） | frame_v2=true（v2 帧） |
|---|---|---|
| `SinglepartObjectKey` | `EncryptReader::new(reader, key, [0u8; 12])`（:450） | 同左（v2 开关不适用） |
| `Singlepart` | `EncryptReader::new(reader, key, base_nonce)`（:462） | `EncryptReader::new_v2(reader, key, base_nonce)`（:460） |
| `MultipartLegacy` | `new_multipart(reader, key, base_nonce, part)`（:476） | `new_multipart_v2(reader, key, base_nonce, part)`（:474） |
| `MultipartObjectKey` | `new_multipart(reader, key, [0u8; 12], part)`（:487） | 同左 |

构造器返回值再包一层 `HashReader::from_reader(..., SIZE_PRESERVE_LAYER, actual_size, ...)`（明文层 etag/checksum 不变）。

## 4. 核心层：EncryptReader（crates/rio/src/encrypt_reader.rs）

### 4.1 加密参数（结构体字段）

```rust
// :58-76
pub struct EncryptReader<R> {
    #[pin] pub inner: R,              // 上游明文 AsyncRead
    cipher: Aes256Gcm,                // :64 算法对象（:85 构造）
    base_nonce: [u8; 12],             // :65 96-bit 基础 nonce
    buffer / buffer_pos,              // 输出帧缓冲
    read_buffer: Vec<u8>,             // :69 明文块缓冲
    block_index: usize,               // :70 块序号（nonce 派生 + AAD 用）
    finished, frame_v2, pending, input_done,   // v2 帧状态
}
```

常量：`ENCRYPTION_BLOCK_SIZE = 8 * 1024`（:26，8KB 块）。

4 个构造器的入参：

| 构造器 | key | nonce | 其他 |
|---|---|---|---|
| `new`（:82） | `[u8; 32]` | `[u8; 12]` | —（v1，`frame_v2=false`） |
| `new_multipart`（:98） | `[u8; 32]` | `[u8; 12]` | `part_number: usize` → 先用 `multipart_part_nonce`（:865）派生 part nonce |
| `new_v2`（:107） | `[u8; 32]` | `[u8; 12]` | `frame_v2=true` |
| `new_multipart_v2`（:114） | `[u8; 32]` | `[u8; 12]` | `part_number: usize`，`frame_v2=true` |

### 4.2 nonce 派生（写端，:861-885）

```rust
derive_block_nonce(base, block_index)   // :861
  → derive_nonce_offset(base, 8, block_index)   // 改 base[8..12]（4B BE u32）
      = 读 base[8..12] 为 u32，wrapping_add(block_index)，写回

multipart_part_nonce(base, part_number) // :865
  → derive_part_nonce(base, part)       // :869，改 base[4..8]
```

即：**每块的 GCM nonce = base_nonce 的第 [8..12) 字节与大端块序号相加**；multipart 先在第 [4..8) 字节处叠 part 号，再按块加序号。`base_nonce` 其余字节原样保留。

### 4.3 输入：poll_read 读明文（:175-317）

- **v2 路径**（:196-243）：循环读 inner，累积满 `ENCRYPTION_BLOCK_SIZE`（8KB）才加密，保证每个非末帧定长（闭式偏移映射，供 range read/seek）。EOF 且不足一块 = **末帧**（`is_final`，:219）。
- **v1 路径**（:245-316）：每次 poll_read 读到多少就加密多少（块长度可变）。

### 4.4 AAD（仅 v2，:51-56）

```rust
fn v2_frame_aad(header: &[u8; 8], block_index: usize) -> [u8; 16] {
    // [0..8]  = 8B 帧头副本
    // [8..16) = block_index 的 u64 小端
}
```

作用：把帧头和帧序号绑进 AEAD，帧头篡改、帧重排、跨帧拼接都过不了认证。

### 4.5 核心算法调用：build_frame（:123-173）—— **v2 加密点**

入参（全部在此，参数即契约）：

| 参数 | 类型 | 来源/含义 |
|---|---|---|
| `cipher` | `&Aes256Gcm` | :85 构造的算法对象 |
| `nonce_bytes` | `&[u8; 12]` | `derive_block_nonce(this.base_nonce, block_index)`（:221 调用处） |
| `type_byte` | `u8` | `0x01` 非末帧 / `0x02` 末帧（:220） |
| `block_index` | `usize` | 当前块序号（AAD + nonce 共用） |
| `plaintext` | `&[u8]` | 明文块（v2 恒为 8KB，末帧 ≤8KB，可为 0） |

加密调用（**真正调 Aes256Gcm 处**，:151-164）：

```rust
let ciphertext = match type_byte {
    FRAME_TYPE_V1 => cipher.encrypt(nonce, plaintext),                 // :152
    _ => {                                                            // v2
        let aad = v2_frame_aad(&header, block_index);                 // :154
        cipher.encrypt(nonce, Payload { msg: plaintext, aad: &aad })  // :155-161
    }
}.map_err(|e| Error::other(format!("encrypt error: {e}")))?;          // :164
```

输出参数：`Payload { msg: 明文, aad: 16B }` → 返回 `Vec<u8>` = **密文（明文 + 16B tag）**。
帧长预计算（:138-140，tag 尺寸决定帧布局闭式性）：`clen = int_len + plaintext.len() + 16 + 4`。

### 4.6 v1 内联加密（:273-276）—— **v1 加密点**

```rust
let block_nonce = derive_block_nonce(this.base_nonce, *this.block_index);  // :264
let plaintext = &this.read_buffer[..n];                                    // :266
let ciphertext = this.cipher.encrypt(&nonce, plaintext)                    // :273-276
    .map_err(|e| Error::other(format!("encrypt error: {e}")))?;
```

无 AAD（密文不认证帧头），块长度可变。

## 5. 输出：落盘帧布局

每块加密后拼成帧（8B 帧头 + uvarint 明文长度 + 密文）：

```
┌─ 8B 帧头 ─────────────────────────────┐
│ [0]    type：0x00 v1 / 0x01 v2非末 / 0x02 v2末 / 0xFF 结束 │
│ [1..4) len：24-bit 小端（= int_len + 密文长 + 4）         │
│ [4..8) crc32：明文 CRC-32（v2 帧头进 AAD 认证）          │
└──────────────────────────────────────┘
uvarint：明文长度（≤10B）
密文：明文 + 16B GCM tag
```

- 帧类型常量（:44-47）：`FRAME_TYPE_V1=0x00`、`FRAME_TYPE_V2=0x01`、`FRAME_TYPE_V2_FINAL=0x02`、`FRAME_TYPE_END=0xFF`。
- v2 末帧后追加 8B `0xFF` 结束帧（:229-233）；v1 EOF 时写单 8B `0xFF`（:253-255）。
- v1 拼帧：:277-304；v2 拼帧：build_frame :166-172。

## 6. 读端对照（DecryptReader，:379-413 / :503-851）

| 步骤 | 代码 | 说明 |
|---|---|---|
| 按帧类型 dispatch | :633-642 | `0x00→v1`，`0x01/0x02→v2`，其余报错（**读端只认磁盘帧类型**） |
| 版本混用检查 | :649-658 | 同一段内 v1/v2 混用报 "mixes frame format versions" |
| v2 解密 | :734-747 | **用同一派生 nonce + 同一 AAD** 调 `cipher.decrypt(nonce, Payload{msg, aad})`，认证失败 → `InvalidData` |
| v1 解密 | :748-795 | 依次试 3 种历史 nonce 布局（Current / LegacyBlock / ReusedPart），非零块只认首块锁定的布局 |
| multipart | :619 | 段切换时 `derive_part_nonce(base_nonce, next_part)` 重建 nonce 基 |

读端 `key` 来自对象 metadata 的 sealed key / IV 还原（`apply_ssec_decryption_material` sse.rs 与 `decrypt_reader*` io_support/rio.rs:170/209）。

## 7. 汇总：输入 → Aes256Gcm → 输出

| 环节 | 位置 | 参数（全部标注） |
|---|---|---|
| 算法构造 | encrypt_reader.rs:85 | 入参 `key: [u8; 32]`；产物 `Aes256Gcm` |
| v2 加密输入 | build_frame :123-129 | `cipher: &Aes256Gcm`、`nonce_bytes: &[u8; 12]`、`type_byte: u8`、`block_index: usize`、`plaintext: &[u8]` |
| **Aes256Gcm 调用（v2）** | **:151-164** | `encrypt(nonce, Payload { msg: plaintext, aad: &v2_frame_aad(header, block_index) })` |
| v1 加密输入 | poll_read :264-266 | `base_nonce`、`block_index`、`&read_buffer[..n]` |
| **Aes256Gcm 调用（v1）** | **:273-276** | `encrypt(&derive_block_nonce(base_nonce, block_index), plaintext)` |
| 加密输出 | :166-172 / :299-304 | `Vec<u8>` = 8B 帧头 + uvarint 明文长 + **密文（明文 + 16B tag）** |
| AAD 生成 | :51-56 | 帧头 8B ‖ block_index u64 小端 → 16B |
| nonce 派生 | :861-885 | `base[8..12]` BE u32 + block_index；multipart 先 `base[4..8]` + part |
| key/nonce 来源 | sse.rs:796 / :1655 / :2562 | `key_bytes [u8;32]`（客户 key 或 HMAC 派生）+ 随机 `base_nonce [u8;12]` |
| 写端接线 | io_support/rio.rs:445-494 | `WriteEncryption.mode` → 选 `new` / `new_v2` / `new_multipart*` |

## 8. 与 multi-cipher 方案（09 号文档）的对应

本走读标出的两个加密调用点（v2 的 :151-164、v1 的 :273-276）正是 09 号方案改动点 C 的改造位置：把 `cipher: &Aes256Gcm` 参数化为 `&EncryptCipher` 枚举后，`:151-164` 的 match 按算法路由到 `Aes256Gcm` / `Aes256GcmDemo` 两种原语的 `encrypt`，帧类型字节 0x03/0x04 与 `build_frame` 的 `type_byte` 选择（改动点 D）在 :220 接入。