# AES-256-GCM 数据面加密完整走读：输入 → 调用算法 → 输出密文

> 状态：走读记录（2026-09-16），对照当前 checkout（1.0.0-rc.6_caohui）代码。
> 复审2：2026-09-16 按用户要求将 §1 全链路数据流升级为**逐层函数调用链 + 参数数据**：每层标注 `文件:函数:行号`、函数签名、实参类型与来源、返回值去向；补齐写端生产调用方（单对象 PUT / multipart part）与 sse.rs 参数组装细节；同步修正 poll_read 行号（:179-317）。遵循本文"原 → 改后"两段式对比，改后块中 `// New Add` 标记新增内容。
> 范围：默认构建（legacy `crates/rio`，非 `rio-v2` feature）下的对象加密写路径与读端对照。
> 目的：精确回答"数据在哪里、用什么参数调用 Aes256Gcm 算法完成加密"。
> 关键文件：`crates/rio/src/encrypt_reader.rs`（核心）、`crates/ecstore/src/io_support/rio.rs`（写端接线）、`rustfs/src/storage/sse.rs`（key/nonce 来源）、`rustfs/src/app/object/extract.rs` / `internal_put.rs`（生产调用方）。

## 0. 一句话答案（加密调用点清单）

真正调用 Aes256Gcm 算法加密的代码共 **3 处**，全部在 `crates/rio/src/encrypt_reader.rs`，按调用顺序自上而下：

```
① encrypt_reader.rs:85     Aes256Gcm::new_from_slice(&key)
   → 用 32B key 构造算法对象（EncryptReader::new 内）
② encrypt_reader.rs:151-164  cipher.encrypt(nonce, Payload { msg, aad })
   → v2 帧加密（build_frame 内，推荐格式）
③ encrypt_reader.rs:273-276  this.cipher.encrypt(&nonce, plaintext)
   → v1 帧加密（poll_read 内联，默认格式）
```

输入 → 算法 → 输出：

```
明文块 &[u8]（≤ 8KB） ─┐
nonce 12B（base nonce + 块序派生）─┤→ Aes256Gcm::encrypt（aead::Aead trait 方法）
AAD 16B（仅 v2：帧头 + 块序号）───┘        │
                                           ▼
                             密文 Vec<u8> = 明文 + 16B GCM tag
```

`cipher.encrypt(...)` 是 `aead::Aead::encrypt` trait 方法（`:16` 导入 `Aead`），`Aes256Gcm` 是 `aes-gcm` crate 对它的实现。

## 1. 全链路数据流：逐层函数调用链 + 参数数据

### 原（初版走读，仅总览数据流）

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

### 改后（复审2：全链路改为**依次调用流**描述——自上而下每一步标注 文件:函数:行号、签名、实参、返回，全为 New Add）

写端加密全链路是同一串依次调用，四个层次（① 生产调用方 → ② 参数来源层 → ③ 写端接线层 → ④ 核心层）按调用顺序自上而下展开：

```
S3 PUT 请求体（明文）
  │
  ▼ ① 生产调用方（请求侧，实际调用点见 §3.3 / §2.1）
  extract.rs:2491   sse_encryption(EncryptionRequest { bucket, key, server_side_encryption,
                     ssekms_key_id, ssekms_context, sse_customer_algorithm, sse_customer_key,
                     sse_customer_key_md5, content_size: actual_size, principal })
                     → 返回 Option<EncryptionMaterial>
  （multipart part 走 internal_put.rs:487 sse_decryption(...)；multipart 首传/续传走
     multipart_usecase.rs:1358-1381 / :1826-1853 直接构造 WriteEncryption，不经 sse_encryption）
  │
  ▼ ② 参数来源层（sse.rs，key/nonce 在此产生，见 §2）
  sse.rs:2013  sse_encryption(request)                             // 单对象 PUT 入口
    ├─ SSE-C → apply_ssec_encryption_material (:2531)              // :2025-2031 优先级 1
    │            validate_ssec_params (:4156) 校验客户 key 恰 32B
    │            key_bytes = validated.key_bytes (:2564)
    │            base_nonce = rand::rng().fill_bytes 12B (:2562-2563)
    └─ managed → apply_managed_encryption_material (:2657 → :2705)  // :2036-2050 优先级 2
                 provider.generate_sse_dek(&object_context, &kms_key_to_use) (:2789)
                 非 rio-v2 组装：key_bytes=plaintext_key、base_nonce=data_key.nonce (:2799-2801)
  sse.rs:1529-1540  material.write_encryption(part: Option<usize>)
                     → WriteEncryption { key_bytes, mode }（rio.rs:318-321，构造器 :364-393）
  │
  ▼ ③ 写端接线层（ecstore，选加密构造器）
  extract.rs:2507        write_plan.with_encryption(write)             // 单对象 PUT
  internal_put.rs:505    write_plan.with_encryption(managed_write)     // multipart part
  rio.rs:425-505         WritePlan::apply(reader, actual_size)
    ├─ rio.rs:446-493    match encryption.mode（4 分支选构造器）
    ├─ rio.rs:332-345    encryption_frame_v2_enabled() → 选 new / new_v2（默认 false = v1）
    │                    实参 (reader, encryption.key_bytes, base_nonce / [0u8;12])，
    │                    multipart 分支多传 multipart_part_number
    └─ → HashReader::from_reader(encrypt_reader, SIZE_PRESERVE_LAYER, actual_size, ..., false)
  │
  ▼ ④ 核心层（crates/rio，真算法调用，见 §4/§9）
  encrypt_reader.rs:82-96     EncryptReader::new(inner: R, key: [u8;32], nonce: [u8;12])
                                 :85  Aes256Gcm::new_from_slice(&key)    ★ 算法对象构造
  encrypt_reader.rs:98-100    new_multipart(inner, key, base_nonce, part_number)
                                 // 先 multipart_part_nonce(base_nonce, part_number) (:865) 叠 part 号
  encrypt_reader.rs:107-118   new_v2 / new_multipart_v2                  // 多置 frame_v2 = true
  poll_read (:179-317)  ← 下游（HashReader/写盘）逐块拉取
    ├─ v2 路径 (:196-243)：累积满 8KB → :221 derive_block_nonce(base_nonce, block_index)
    │                        → :222-228 build_frame(cipher, &block_nonce, type_byte, block_index,
    │                          &read_buffer[..pending]) → :151-164 cipher.encrypt(nonce, Payload{msg, aad})
    │                        ★ 真算法调用（见 §4.5）
    └─ v1 路径 (:245-316)：:264 derive_block_nonce → :273-276 cipher.encrypt(&nonce, plaintext)
                           ★ 真算法调用（无 AAD，见 §4.6）
  │
  ▼ 输出帧流（落盘）
  8B 帧头（type / len 24-bit / crc32）+ uvarint 明文长 + 密文（明文 + 16B GCM tag）
  （v2 末帧后追加 8B 0xFF 结束帧 :229-233；v1 EOF 写单 8B 0xFF :253-255）
```

**参数数据流总览（每个参数从哪来、传到哪、在哪被消费）**

```
key_bytes [u8;32]
  ├─ SSE-C  : sse.rs:2564（validate_ssec_params 校验的客户 key）
  ├─ managed: sse.rs:2789/2801（KMS DEK 的 plaintext_key）
  └─ 流向 : WriteEncryption.key_bytes (rio.rs:318-321) → EncryptReader::new/new_v2/new_multipart*
           → encrypt_reader.rs:85 Aes256Gcm::new_from_slice(&key)  ★ 算法构造

base_nonce [u8;12]
  ├─ SSE-C  : sse.rs:2562-2563（每个对象随机 rand::rng().fill_bytes）
  ├─ managed: sse.rs:2801（KMS 生成的 data_key.nonce）
  ├─ object-key 分支: [0u8;12]（rio.rs:450/:487 直接传零 nonce）
  └─ 流向 : WriteEncryption.mode 内嵌（Singlepart{base_nonce} / MultipartLegacy{base_nonce,...}）
           → 构造器 → encrypt_reader.rs:65 base_nonce 字段
           → :221/:264 derive_block_nonce 派生每块 nonce  ★ 逐块消费

part_number（multipart 才有）
  └─ 会话 : multipart_usecase.rs / internal_put.rs:499-503 → WriteEncryption.mode
           → new_multipart* 构造器 → multipart_part_nonce（encrypt_reader.rs:865）把 part 号
             叠进 base_nonce[4..8) → 段内所有块 nonce 都以 part 窗口打底

block_index（EncryptReader 内部）
  └─ 字段 :65 → 每帧加密后 +1（:236 v2 / :307 v1）
           → derive_block_nonce 入参 + v2 AAD 入参（build_frame 内） ★ 双重消费
```

## 2. 参数来源层：key 与 nonce 从哪来（sse.rs）

### 2.1 `EncryptionMaterial`（rustfs/src/storage/sse.rs:796）

写端所需的加密参数就两个，封在 `EncryptionMaterial` 里（sse.rs:796-822）：

```
key_bytes   : [u8; 32]   AES-256 密钥（直接传给 Aes256Gcm::new_from_slice）
base_nonce  : [u8; 12]   96-bit GCM 基础 nonce（每个对象独立，存进 metadata）
key_kind    : EncryptionKeyKind   Direct = 门面 key；Object = object-key 派生模式
```

`EncryptionMaterial` 在写端转换成 `WriteEncryption`（`EncryptionMaterial::write_encryption` sse.rs:1529-1540，impl 块自 :1528），按 `(key_kind, part)` 四选一：

```
(key_kind=Object, part=Some(n))  → sse.rs:1532  WriteEncryption::multipart_object_key(key_bytes, part_number)
(key_kind=Object, part=None)     → sse.rs:1534  WriteEncryption::singlepart_object_key(key_bytes)
(key_kind=Direct, part=Some(n))  → sse.rs:1536  WriteEncryption::multipart(key_bytes, base_nonce, part_number)
(key_kind=Direct, part=None)     → sse.rs:1538  WriteEncryption::singlepart(key_bytes, base_nonce)
```

`key_kind` 定义在 `EncryptionKeyKind`（sse.rs:843-846）：`Direct` = 直接用 `key_bytes` 作流密钥；`Object` = MinIO-style object key（`key_bytes` 是派生对象 key，直接传给加密器，nonce 用零值，见 §3.2）。

### 2.2 三种 key 来源

按 SSE 类型自上而下（默认构建，非 rio-v2 feature）：

```
SSE-C（客户自带 key）
  key_bytes：客户 key Base64 解码，校验必须恰好 32B
    validate_ssec_params sse.rs:4156；直接 validated.key_bytes sse.rs:2564
SSE-S3 / SSE-KMS（托管，默认构建）
  key_bytes：KMS DEK → provider.generate_sse_dek(...) → data_key.plaintext_key
    （key 与 nonce 一并由 provider 生成）
    sse.rs:2789（DEK 生成）、:2799-2801（Direct 组装）
rio-v2 object-key 模式（feature 门控，默认构建不编译）
  key_bytes：derive_object_key：HMAC-SHA256(external_key, OBJECT_KEY_DERIVATION_CONTEXT ‖ random32)
    → 封进 sealed key
    sse.rs:1655-1667、:2794
```

### 2.3 base_nonce（12B）的生成与持久化

- **SSE-C Direct + 默认构建（非 rio-v2）**：每个对象生成**随机** 12B nonce（sse.rs:2562-2563 `rand::rng().fill_bytes`）。注释明确：若用确定性 nonce，同一对象在相同 SSE-C key 下覆写会重用 (key, nonce) 对，灾难性破坏 AES-GCM。
- **SSE-S3/KMS**：sse_prepare_encryption 侧对 multipart 会话也生成随机 nonce。
- nonce 持久化：`encryption_material_to_metadata`（sse.rs:1810）写到对象 metadata（`INTERNAL_ENCRYPTION_IV_HEADER` / MinIO 别名 `MINIO_INTERNAL_ENCRYPTION_IV_HEADER`），解密时读回。

## 3. 写端入口：WritePlan::apply（crates/ecstore/src/io_support/rio.rs:425-505）

### 3.1 参数载体

```rust
pub struct WriteEncryption {          // io_support/rio.rs:318-321
    key_bytes: [u8; 32],              // 32B 密钥
    mode: WriteEncryptionMode,        // 4 个写模式
}
```

`WriteEncryptionMode`（:350-362）4 个变体携带写端参数，自上而下：

```
SinglepartObjectKey                     →（无参数，key 即 object key）
Singlepart { base_nonce }               → base_nonce [u8; 12]
MultipartLegacy { base_nonce,
                  multipart_part_number } → base_nonce [u8; 12] + part 号 usize
MultipartObjectKey { multipart_part_number } → part 号 u32
```

4 个构造器（`impl WriteEncryption`，:364-393）——生产代码里由 `sse.rs:1529-1540 write_encryption` 或 app 层直接调用：

```rust
// New Add：构造器签名与实参（io_support/rio.rs:365/372/379/389）
pub const fn singlepart_object_key(object_key: [u8; 32]) -> Self           // :365，key = object key，nonce 用零
pub const fn singlepart(key_bytes: [u8; 32], base_nonce: [u8; 12]) -> Self // :372
pub const fn multipart(key_bytes: [u8; 32], base_nonce: [u8; 12],
                       multipart_part_number: usize) -> Self               // :379
pub const fn multipart_object_key(object_key: [u8; 32],
                                  multipart_part_number: u32) -> Self      // :389
```

### 3.2 构造器选择流程（:446-493）

`RUSTFS_ENCRYPTION_FRAME_V2`（默认 `false`，:332-345 OnceLock 缓存，`encryption_frame_v2_enabled` :337-347）决定 v1/v2 布局。`WritePlan::apply` 内按 mode 自上而下依次判断：

```
mode = SinglepartObjectKey
  → EncryptReader::new(reader, key, [0u8; 12])（:450）
    // object-key 模式：v2 开关不适用，恒 v1 零 nonce
mode = Singlepart { base_nonce }
  → frame_v2=false : EncryptReader::new(reader, key, base_nonce)      （:462）
  → frame_v2=true  : EncryptReader::new_v2(reader, key, base_nonce)   （:460）
mode = MultipartLegacy { base_nonce, multipart_part_number }
  → frame_v2=false : new_multipart(reader, key, base_nonce, part)     （:476）
  → frame_v2=true  : new_multipart_v2(reader, key, base_nonce, part)  （:474）
mode = MultipartObjectKey { multipart_part_number }
  → new_multipart(reader, key, [0u8; 12], part)                        （:487）
    // object-key 模式：v2 开关不适用，恒 v1 零 nonce
```

构造器返回值再包一层 `HashReader::from_reader(..., SIZE_PRESERVE_LAYER, actual_size, ...)`（明文层 etag/checksum 不变）。

### 3.3 生产调用方（New Add：谁把 WritePlan 送到 apply —— 自上而下调用流）

`WritePlan::apply`（:425-505）的上游实际调用流：

```
单对象 PUT（extract.rs:2491 → :2507）
  extract.rs:2491   sse_encryption(EncryptionRequest {...})        // 组装全部 sse 请求头
                    → Option<EncryptionMaterial>
  extract.rs:2507   write_plan.with_encryption(material.write_encryption(None))
                    // 实参 = WriteEncryption{key_bytes, mode: Singlepart...}
                    → 后续 put 流程中 write_plan.apply(reader, actual_size)

multipart part（internal_put.rs:487 → :508）
  internal_put.rs:487   sse_decryption(DecryptionRequest {...})    // 恢复会话加密材料
                        → managed_material（key_kind 决定组装方式）
  internal_put.rs:497-503  按 key_kind 分支：
       Object  → WriteEncryption::multipart_object_key(managed_material.key_bytes, part_number)
       Direct  → WriteEncryption::multipart(managed_material.key_bytes,
                                             managed_material.base_nonce, part_number)
  internal_put.rs:505   write_plan.with_encryption(managed_write)
  internal_put.rs:508   write_plan.apply(reader, actual_size)

multipart 首传（multipart_usecase.rs:1358-1381）
  1358-1360  SSE-C → WriteEncryption::multipart_object_key(ssec_material.key_bytes, part_id)
                     / multipart(ssec_material.key_bytes, ssec_material.base_nonce, part_id)
  1379-1381  managed → 同上用 managed_material 组装
  后续      write_plan.with_encryption(...) → apply(...)

multipart 续传（multipart_usecase.rs:1826-1853）
  1826-1828  SSE-C → WriteEncryption::multipart_object_key / multipart（同首传）
  1851-1853  managed → 同上
  后续      write_plan.with_encryption(...) → apply(...)
```

## 4. 核心层：EncryptReader（crates/rio/src/encrypt_reader.rs）

### 4.1 加密参数（结构体字段）

```rust
// :58 pin_project! 块 / :61-75 struct
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

4 个构造器的入参，自上而下：

```
new（:82）              key [u8;32] + nonce [u8;12]                → v1，frame_v2=false
new_multipart（:98）     key [u8;32] + base_nonce [u8;12] + part_number: usize
                          → 先用 multipart_part_nonce（:865）派生 part nonce
new_v2（:107）           key [u8;32] + nonce [u8;12]                → frame_v2=true
new_multipart_v2（:114） key [u8;32] + base_nonce [u8;12] + part_number: usize
                          → frame_v2=true
```

### 4.2 nonce 派生（写端，:861-885）

```rust
derive_block_nonce(base, block_index)   // :861
  → derive_nonce_offset(base, 8, block_index)   // 改 base[8..12]（4B BE u32）
      = 读 base[8..12] 为 u32，wrapping_add(block_index)，写回

multipart_part_nonce(base, part_number) // :865
  → derive_part_nonce(base, part)       // :869，改 base[4..8]
```

即：**每块的 GCM nonce = base_nonce 的第 [8..12) 字节与大端块序号相加**；multipart 先在第 [4..8) 字节处叠 part 号，再按块加序号。`base_nonce` 其余字节原样保留。

### 4.3 输入：poll_read 读明文（:179-317）

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

入参（全部在此，参数即契约），自上而下逐参说明：

1. `cipher : &Aes256Gcm` —— :85 构造的算法对象，调用方持有贯穿整个流式读写生命周期。
2. `nonce_bytes : &[u8; 12]` —— `derive_block_nonce(this.base_nonce, block_index)` 的结果：由 :221 派生，:222-228 传入 build_frame。
3. `type_byte : u8` —— 帧类型：`0x01` 非末帧 / `0x02` 末帧（:220 判定）。
4. `block_index : usize` —— 当前块序号，AAD 与 nonce 共用同一个值（把「序号」这一个事实同时绑进认证和加密两份材料）。
5. `plaintext : &[u8]` —— 明文块：v2 非末帧恒为 8KB，末帧 ≤8KB，可为 0（空末帧照常认证）。

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

## 6. 读端对照（DecryptReader，:382-413 / :503-851）

读端按磁盘帧类型自上而下依次处理（**读端只认磁盘帧类型，不读 env**）：

```
:633-642   按帧类型 dispatch：0x00 → v1；0x01/0x02 → v2；其余报错
:649-658   版本混用检查：同一段内 v1/v2 混用报 "mixes frame format versions"
:734-747   v2 解密：用同一派生 nonce + 同一 AAD 调 cipher.decrypt(nonce, Payload{msg, aad})
           → 认证失败 → InvalidData（fail-closed）
:748-795   v1 解密：依次试 3 种历史 nonce 布局（Current / LegacyBlock / ReusedPart）
           → 非零块只认首块锁定的布局
:619       multipart：段切换时 derive_part_nonce(base_nonce, next_part) 重建 nonce 基
```

读端 `key` 来自对象 metadata 的 sealed key / IV 还原（`apply_ssec_decryption_material` sse.rs 与 `decrypt_reader*` io_support/rio.rs:170/209）。

## 7. 汇总：输入 → Aes256Gcm → 输出

**全链路函数调用链（写端，默认构建）**（New Add）：

```
extract.rs:2491  sse_encryption(EncryptionRequest{...})          [单对象 PUT]
internal_put.rs:487  sse_decryption(...)                         [multipart part 恢复会话 material]
        │  EncryptionMaterial { key_bytes [u8;32], base_nonce [u8;12], key_kind }
        ▼
sse.rs:1529-1540  material.write_encryption(part: Option<usize>)
        │  → WriteEncryption { key_bytes, mode }（rio.rs:318-321，构造器 :364-393）
        ▼
extract.rs:2507 / internal_put.rs:505  write_plan.with_encryption(write)
        ▼
rio.rs:425-505  WritePlan::apply(reader, actual_size)
        │  match mode（:446-493）+ encryption_frame_v2_enabled()（:332-345）
        ▼
encrypt_reader.rs:82-118  EncryptReader::new / new_v2 / new_multipart*（key、base_nonce、part 实参）
        │  HashReader::from_reader(..., SIZE_PRESERVE_LAYER, actual_size, ...)
        ▼
encrypt_reader.rs:179-317  poll_read  ← 下游逐块拉取
        │  :221/:264 derive_block_nonce(base_nonce, block_index)
        ▼
build_frame :123-173（v2 :222-228）/ v1 内联 :273-276
        │  :151-164  cipher.encrypt(nonce, Payload{msg, aad})
        ▼
8B 帧头 + uvarint 明文长 + 密文（明文 + 16B tag）→ 写盘
```

**逐环节参数流（New Add：自上而下，位置 → 调用 → 实参/产物）**：

```
S1 参数生成（sse）       sse.rs:2564 / :2789-2801
   → key_bytes [u8;32]、base_nonce [u8;12]、key_kind（§2.1 映射）
S2 写端接线              io_support/rio.rs:446-493
   → WriteEncryption.mode 选 new / new_v2 / new_multipart*
S3 算法构造              encrypt_reader.rs:85
   → 入参 key: [u8;32]；产物 Aes256Gcm
S4 v2 加密输入           build_frame :123-129
   → cipher: &Aes256Gcm、nonce_bytes: &[u8;12]、type_byte: u8、block_index: usize、plaintext: &[u8]
S5 Aes256Gcm 调用（v2）  :151-164
   → encrypt(nonce, Payload{ msg: plaintext, aad: &v2_frame_aad(header, block_index) })
S6 v1 加密输入           poll_read :264-266
   → base_nonce、block_index、&read_buffer[..n]
S7 Aes256Gcm 调用（v1）  :273-276
   → encrypt(&derive_block_nonce(base_nonce, block_index), plaintext)
S8 加密输出              :166-172 / :299-304
   → Vec<u8> = 8B 帧头 + uvarint 明文长 + 密文（明文 + 16B tag）
S9 AAD 生成              :51-56
   → 帧头 8B ‖ block_index u64 小端 → 16B
S10 nonce 派生           :861-885
   → base[8..12] BE u32 + block_index；multipart 先 base[4..8] + part
S11 key/nonce 来源       sse.rs:796 / :2789 / :2562
   → key_bytes [u8;32]（客户 key、KMS DEK 或 rio-v2 HMAC 派生）+ 随机 base_nonce [u8;12]
```

## 8. 与 multi-cipher 方案（09 号文档）的对应

本走读标出的两个加密调用点（v2 的 :151-164、v1 的 :273-276）正是 09 号方案改动点 C 的改造位置：把 `cipher: &Aes256Gcm` 参数化为 `&EncryptCipher` 枚举后，`:151-164` 的 match 按算法路由到 `Aes256Gcm` / `Aes256GcmDemo` 两种原语的 `encrypt`，帧类型字节 0x03/0x04 与 `build_frame` 的 `type_byte` 选择（改动点 D）在 :220 接入。

## 9. 详细代码流程（真实代码逐段全标注）

> 以下全部为 `crates/rio/src/encrypt_reader.rs` 的真实代码，注释为逐个参数/逐行含义标注。行号 = 当前 checkout（1.0.0-rc.6_caohui）实际行号。

### 9.1 帧类型常量与 AAD 函数（:44-56）

```rust
// —— 帧类型字节（8B 帧头的 [0] 字节）——
const FRAME_TYPE_V1: u8 = 0x00;        // AES v1 帧：无 AEAD 认证头，密文不绑帧头/序号
const FRAME_TYPE_V2: u8 = 0x01;        // AES v2 非末帧：帧头+序号进 AAD，定长 8KB
const FRAME_TYPE_V2_FINAL: u8 = 0x02;  // AES v2 末帧：类型字节本身也被 AAD 认证，防截断
const FRAME_TYPE_END: u8 = 0xFF;       // 段结束标记：不进 AEAD，仅作分段符

/// AEAD associated data of a v2 frame: the 8-byte header followed by the
/// frame index within its segment, little-endian.
/// 输入参数：
///   header: &[u8; 8]  —— 即将写入的 8B 帧头（type + len + crc32）
///   block_index       —— 本块在段内的绝对序号（从 0 起）
/// 返回：16B AAD = 帧头 8B ‖ 序号 u64 小端 8B
fn v2_frame_aad(header: &[u8; 8], block_index: usize) -> [u8; 16] {
    let mut aad = [0u8; 16];
    aad[..8].copy_from_slice(header);                     // [0..8)  = 帧头副本
    aad[8..].copy_from_slice(&(block_index as u64).to_le_bytes()); // [8..16) = 块序号小端
    aad
}
```

### 9.2 构造器：key / nonce 参数注入点（:82-96，:107-111）

```rust
/// 参数：
///   inner: R          —— 上游明文 AsyncRead（请求体/解压流等）
///   key:   [u8; 32]   —— AES-256 密钥（来自 sse.rs 的 EncryptionMaterial.key_bytes）
///   nonce: [u8; 12]   —— 96-bit 基础 nonce（来自 sse.rs 的 EncryptionMaterial.base_nonce）
pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
    Self {
        inner,
        cipher: Aes256Gcm::new_from_slice(&key).expect("key"), // ★ 算法对象在此构造
        base_nonce: nonce,          // 存 12B 基础 nonce，后续逐块派生
        buffer: Vec::new(),         // 输出帧缓冲（攒好一帧后由 poll_read 逐段拷贝出去）
        buffer_pos: 0,              // 输出帧缓冲中的消费位置
        read_buffer: vec![0u8; ENCRYPTION_BLOCK_SIZE], // 明文块缓冲（8KB）
        block_index: 0,             // 块序号：nonce 派生 + v2 AAD 都用它
        finished: false,            // 是否已写完（含段结束帧）
        frame_v2: false,            // false = v1 布局（每次读到多少加密多少）
        pending: 0,                 // (v2) 已累积进 read_buffer 的明文字节数
        input_done: false,          // (v2) 上游是否已 EOF
    }
}

pub fn new_v2(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
    let mut reader = Self::new(inner, key, nonce); // 复用 new：key/nonce 语义完全一致
    reader.frame_v2 = true;                        // 只切布局开关 → v2 定长认证帧
    reader
}
```

### 9.3 build_frame：v2 帧加密核心（:123-173）—— ★ 真算法调用处

```rust
/// 输入参数（参数即契约）：
///   cipher:      &Aes256Gcm    —— :85 构造的算法对象
///   nonce_bytes: &[u8; 12]     —— 本块 GCM nonce（poll_read 里 derive_block_nonce 派生）
///   type_byte:   u8            —— 0x01 非末帧 / 0x02 末帧（:220 决定）
///   block_index: usize         —— 本块绝对序号（AAD + nonce 共用）
///   plaintext:   &[u8]         —— 明文块（非末帧恒 8KB；末帧 ≤8KB，可为 0）
/// 返回：一帧完整字节 = 8B 帧头 + uvarint 明文长 + 密文（明文+16B tag）
fn build_frame(
    cipher: &Aes256Gcm,
    nonce_bytes: &[u8; 12],
    type_byte: u8,
    block_index: usize,
    plaintext: &[u8],
) -> std::io::Result<Vec<u8>> {
    let nonce = Nonce::try_from(nonce_bytes.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
    let nonce = &nonce;                    // [u8;12] → aead::Nonce<Aes256Gcm>（12B 类型级固定）

    // 明文 CRC-32（CRC32/Iso-Hdlc）：放进帧头 [4..8)，v2 时随帧头一起被 AAD 认证
    let crc = {
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        hasher.update(plaintext);
        hasher.finalize() as u32
    };

    let int_len = put_uvarint_len(plaintext.len() as u64); // 明文长度 uvarint 的字节数（≤10）
    // 帧头 len 字段（24-bit）提前算好：int_len + 明文 + 16B tag + 4
    // → 加密前帧头就已固定，所以它能作为 AAD 参与认证
    let clen = int_len + plaintext.len() + 16 + 4;
    let mut header = [0u8; 8];
    header[0] = type_byte;                 // [0]    = 帧类型（0x01/0x02）
    header[1] = (clen & 0xFF) as u8;       // [1..4) = clen 24-bit 小端
    header[2] = ((clen >> 8) & 0xFF) as u8;
    header[3] = ((clen >> 16) & 0xFF) as u8;
    header[4] = (crc & 0xFF) as u8;        // [4..8) = 明文 CRC-32 小端
    header[5] = ((crc >> 8) & 0xFF) as u8;
    header[6] = ((crc >> 16) & 0xFF) as u8;
    header[7] = ((crc >> 24) & 0xFF) as u8;

    // ★★★★★ 真正调用 Aes256Gcm 算法的地方 ★★★★★
    // cipher.encrypt 是 aead::Aead::encrypt trait 方法，返回 Result<Vec<u8>>：
    //   成功 = 密文 Vec<u8>（明文 + 16B GCM tag）
    //   失败 = aead::Error（如 key/nonce 非法、内部错误），映射为 io::Error
    let ciphertext = match type_byte {
        FRAME_TYPE_V1 => cipher.encrypt(nonce, plaintext),   // v1：只加密明文，无 AAD
        _ => {                                                // v2（0x01/0x02）：
            let aad = v2_frame_aad(&header, block_index);    //   AAD = 帧头 + 序号
            cipher.encrypt(
                nonce,
                Payload {       // aead::Payload：msg=明文，aad=认证数据（不进密文但参与加密）
                    msg: plaintext,
                    aad: &aad,
                },
            )
        }
    }
    .map_err(|e| Error::other(format!("encrypt error: {e}")))?; // aead 错误 → io 错误

    // —— 输出：拼一帧落盘 ——
    let mut out = Vec::with_capacity(8 + int_len + ciphertext.len()); // 8B 头 + uvarint + 密文
    out.extend_from_slice(&header);                              // ① 8B 帧头
    let mut plaintext_len_buf = [0u8; 10];
    let encoded_len = put_uvarint(&mut plaintext_len_buf, plaintext.len() as u64);
    out.extend_from_slice(&plaintext_len_buf[..encoded_len]);    // ② 明文长度 uvarint
    out.extend_from_slice(&ciphertext);                          // ③ 密文（明文+16B tag）
    Ok(out)
}
```

### 9.4 poll_read v2 路径（:196-243）—— 读明文 → 派生 nonce → 交给 build_frame

```rust
if *this.frame_v2 {
    // ① 输入侧：循环拉上游明文，累积满 8KB 才加密。
    //    目的：每个非末帧定长（ENCRYPTION_BLOCK_SIZE），range read/seek 才有闭式偏移映射。
    while !*this.input_done && *this.pending < ENCRYPTION_BLOCK_SIZE {
        let mut temp_buf = ReadBuf::new(&mut this.read_buffer[*this.pending..ENCRYPTION_BLOCK_SIZE]);
        match this.inner.as_mut().poll_read(cx, &mut temp_buf) { // inner = 上游明文 reader
            Poll::Pending => return Poll::Pending,               // 上游没数据：等下次 poll
            Poll::Ready(Ok(())) => {
                let n = temp_buf.filled().len();
                if n == 0 { *this.input_done = true; }           // 读到 0 字节 = EOF
                else      { *this.pending += n; }                // 累计明文长度
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        }
    }

    // ② 是否末帧：EOF 切不足一块 → 末帧（短块只可能是流尾；恰好 8KB 边界则整块非末帧 + 空末帧）
    let is_final = *this.input_done && *this.pending < ENCRYPTION_BLOCK_SIZE;
    let type_byte = if is_final { FRAME_TYPE_V2_FINAL } else { FRAME_TYPE_V2 }; // 0x02 / 0x01

    // ③ 本块 GCM nonce：base_nonce 的 [8..12) 字节（BE u32）+ 块序号
    let block_nonce = derive_block_nonce(this.base_nonce, *this.block_index);

    // ④ ★ 调 build_frame → 内部走 :151-164 的 Aes256Gcm::encrypt（见 9.3）
    let mut out = build_frame(
        this.cipher,                                   // 算法对象 &Aes256Gcm
        &block_nonce,                                  // 本块 nonce [u8;12]
        type_byte,                                     // 0x01 / 0x02
        *this.block_index,                             // 块序号（AAD 用）
        &this.read_buffer[..*this.pending],            // 明文块（非末帧 8KB / 末帧 ≤8KB）
    )?;

    if is_final {
        // 末帧后追加 8B 结束帧（0xFF）作为段分隔符
        let mut end_header = [0u8; 8];
        end_header[0] = FRAME_TYPE_END;
        out.extend_from_slice(&end_header);
        *this.finished = true;
    }
    *this.pending = 0;          // 清累积计数
    *this.block_index += 1;     // 块序号 +1（影响下帧 nonce + AAD）
    *this.buffer = out;         // ⑤ 输出侧：整帧入缓冲，下面按调用方容量逐段拷贝出去
    *this.buffer_pos = 0;
    let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
    buf.put_slice(&this.buffer[..to_copy]);   // 把帧内容拷贝进调用方的 ReadBuf
    *this.buffer_pos += to_copy;
    return Poll::Ready(Ok(()));
}
```

### 9.5 poll_read v1 路径（:245-316）—— 默认布局的内联加密：★ 真算法调用处（无 AAD）

```rust
// ① 输入侧：每次只读一块（最多 8KB），读到多少加密多少 → v1 帧长度可变
let mut temp_buf = ReadBuf::new(&mut this.read_buffer[..]);
match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
    Poll::Pending => Poll::Pending,
    Poll::Ready(Ok(())) => {
        let n = temp_buf.filled().len();     // 本次实际读到的明文长度
        if n == 0 {
            // EOF：写 8B 结束帧（0xFF）后结束
            let mut header = [0u8; 8];
            header[0] = 0xFF;                // type: end
            *this.buffer = header.to_vec();
            *this.buffer_pos = 0;
            *this.finished = true;
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.buffer_pos += to_copy;
            Poll::Ready(Ok(()))
        } else {
            // ② 派生本块 nonce（与 v2 同一函数）
            let block_nonce = derive_block_nonce(this.base_nonce, *this.block_index);
            let nonce = Nonce::try_from(block_nonce.as_slice()).map_err(|_| Error::other("invalid nonce length"))?;
            let plaintext = &this.read_buffer[..n];   // 明文切片 = 本次读到的 n 字节
            let plaintext_len = plaintext.len();
            // ③ 明文 CRC-32（帧头 [4..8)，v1 无 AAD 认证，仅校验用途）
            let crc = {
                let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
                hasher.update(plaintext);
                hasher.finalize() as u32
            };
            // ★★★★★ v1 的 Aes256Gcm 调用点（:273-276）★★★★★
            // 参数：nonce（12B 派生值） + 明文 whole slice，无 AAD；返回 明文+16B tag
            let ciphertext = this
                .cipher                       // &Aes256Gcm 算法对象
                .encrypt(&nonce, plaintext)   // aead::Aead::encrypt
                .map_err(|e| Error::other(format!("encrypt error: {e}")))?;
            let int_len = put_uvarint_len(plaintext_len as u64); // 明文长 uvarint 字节数
            // v1 帧头 len 字段 = int_len + 密文长 + 4（密文已含 16B tag）
            let clen = int_len + ciphertext.len() + 4;
            let mut header = [0u8; 8];
            header[0] = 0x00;                        // 0 = encrypted（v1 类型恒定 0x00）
            header[1] = (clen & 0xFF) as u8;         // [1..4) len 24-bit 小端
            header[2] = ((clen >> 8) & 0xFF) as u8;
            header[3] = ((clen >> 16) & 0xFF) as u8;
            header[4] = (crc & 0xFF) as u8;          // [4..8) 明文 CRC-32 小端
            header[5] = ((crc >> 8) & 0xFF) as u8;
            header[6] = ((crc >> 16) & 0xFF) as u8;
            header[7] = ((crc >> 24) & 0xFF) as u8;
            // ④ 输出侧：拼帧 = 8B 头 + uvarint 明文长 + 密文
            let mut out = Vec::with_capacity(8 + int_len + ciphertext.len());
            out.extend_from_slice(&header);
            let mut plaintext_len_buf = [0u8; 10];
            let encoded_len = put_uvarint(&mut plaintext_len_buf, plaintext_len as u64);
            out.extend_from_slice(&plaintext_len_buf[..encoded_len]);
            out.extend_from_slice(&ciphertext);
            *this.buffer = out;                      // 整帧进缓冲，供上层逐段拷贝
            *this.buffer_pos = 0;
            *this.block_index += 1;                  // 块序号 +1
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.buffer_pos += to_copy;
            Poll::Ready(Ok(()))
        }
    }
    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
}
```

### 9.6 nonce 派生族（:861-885）—— 参数含义逐行

```rust
/// 块 nonce：在 base_nonce 的第 [8..12) 字节处叠 block_index
fn derive_block_nonce(base: &[u8; 12], block_index: usize) -> [u8; 12] {
    derive_nonce_offset(base, 8, block_index)   // start=8 → 修改 base[8..12)
}

/// multipart part 的 nonce 基：与块 nonce 用不同的字节窗口（[4..8)），互不重叠
pub fn multipart_part_nonce(base_nonce: [u8; 12], part_number: usize) -> [u8; 12] {
    derive_part_nonce(&base_nonce, part_number)
}

fn derive_part_nonce(base: &[u8; 12], part_number: usize) -> [u8; 12] {
    derive_nonce_offset(base, 4, part_number)   // start=4 → 修改 base[4..8)
}

/// 读端 v1 历史布局之一（旧版 part nonce 窗口在 [8..12)，与块 nonce 同窗）
fn derive_legacy_part_nonce(base: &[u8; 12], part_number: usize) -> [u8; 12] {
    derive_nonce_offset(base, 8, part_number)
}

/// 统一实现：
///   参数 base: &[u8; 12] —— 基础 nonce（对象级 base_nonce，或 part nonce 基）
///   参数 start: usize    —— 叠加窗口起点：8 = 块窗口（[8..12)），4 = part 窗口（[4..8)）
///   参数 offset: usize   —— 要叠加的序号：block_index 或 part_number
///   做法：把窗口内 4B 当大端 u32，wrapping_add(offset) 后写回；窗口外字节原样保留
///   成因：GCM 要求同一 key 下 nonce 不重用 → 每个 (对象,part,块) nonce 唯一
fn derive_nonce_offset(base: &[u8; 12], start: usize, offset: usize) -> [u8; 12] {
    let mut nonce = *base;
    let mut suffix = [0u8; 4];
    suffix.copy_from_slice(&nonce[start..start + 4]);   // 取窗口 4B
    let current = u32::from_be_bytes(suffix);           // 大端读成 u32
    let next = current.wrapping_add(offset as u32);     // 加序号（wrapping：溢出回绕不 panic）
    nonce[start..start + 4].copy_from_slice(&next.to_be_bytes()); // 大端写回
    nonce
}
```