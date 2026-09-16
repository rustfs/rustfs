# Multi-Cipher 加密算法扩展方案

> 状态：设计定稿（2026-09-16），尚未实现。
> 复审：2026-09-16 对照 origin/main（a7341abdb，#7924）复核——核心结论与全部关键引用点仍成立，仅下文中若干行号已按最新代码更新（差异原因：put.rs/new 结构 +22 行、sse.rs EncryptionMaterial +6 行等，见正文标注）。
> 范围：在现有 AES-256-GCM 基础上新增一个加密算法（ChaCha20-Poly1305），最小改动、可扩展。
> 前置：已完成数据面加密 I/O 与配置系统的完整代码走读（crates/rio、crates/rio-v2、crates/ecstore/src/io_support、crates/config、rustfs/src/storage/sse.rs 等）。

## 0. 设计结论（先给结论）

**选型：在默认构建（legacy `crates/rio`）上扩展帧类型字节，用 `RUSTFS_ENCRYPTION_CIPHER` env 控制写端算法；读端纯按 on-disk 帧类型 dispatch，不读 env。**

不走 `crates/rio-v2` 的原因：它是独立 crate、非默认构建（Cargo.toml:46 明确 "ships in no default build"），启用它意味着压缩格式（S2）、索引格式、加密帧全部切换成 MinIO 兼容布局，属于大改造，违背"最小改动"。而 legacy 路径的读端 `DecryptReader` 已是类型字节驱动的（v1/v2 自动识别），把 `0x03/0x04` 定义为新算法的 v2 帧，读端只需加两个 match 分支——改动面收敛在一个 crate。

**兼容性核心保证**：读端永远只认磁盘上的帧类型字节，配置/env 永不参与读路径。因此 AES→ChaCha20、ChaCha20→AES 双向切换后，旧数据都照常可读。

---

## 1. 第一层：加密算法配置（env 环境变量）

### 1.1 新增环境变量

| 变量 | 取值 | 默认值 | 作用 |
|---|---|---|---|
| `RUSTFS_ENCRYPTION_CIPHER` | `aes256-gcm` \| `chacha20-poly1305` | `aes256-gcm` | **仅写端**：新写入对象使用的加密算法 |

设计要点（对齐现有 `RUSTFS_ENCRYPTION_FRAME_V2` 的成熟模式，crates/ecstore/src/io_support/rio.rs:332-347）：

```rust
// crates/ecstore/src/io_support/rio.rs（新增，与 ENV_RUSTFS_ENCRYPTION_FRAME_V2 并列）
pub(crate) const ENV_RUSTFS_ENCRYPTION_CIPHER: &str = "RUSTFS_ENCRYPTION_CIPHER";
pub(crate) const DEFAULT_RUSTFS_ENCRYPTION_CIPHER: &str = "aes256-gcm";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EncryptionCipher {
    Aes256Gcm,
    ChaCha20Poly1305,
}

impl FromStr for EncryptionCipher { /* "aes256-gcm" | "chacha20-poly1305"，其它值 warn 并回落默认 */ }

pub(crate) fn encryption_cipher() -> EncryptionCipher {
    #[cfg(test)]
    { rustfs_utils::get_env_str(ENV_RUSTFS_ENCRYPTION_CIPHER, DEFAULT_RUSTFS_ENCRYPTION_CIPHER).parse().unwrap_or_default() }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<EncryptionCipher> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| { /* 同上 */ })
    }
}
```

- 命名扁平 `RUSTFS_*`，符合 `crates/config/AGENTS.md` 强制约定
- 用 `rustfs_utils::get_env_str` 自动获得 `MINIO_*` 别名兼容注入（envs.rs:88-120 已有机制，无需新代码）
- 进程启动时一次性求值 + OnceLock 缓存，避免热路径重复读 env

### 1.2 无配置文件的原因

走读确认：RustFS 无全局配置文件加载机制，所有全局配置（`RUSTFS_REGION`、`RUSTFS_SCANNER_ENABLED` 等）均为扁平 env 常量（crates/config 只是常量仓库）。bucket 级配置（如 SSE）走 bucket metadata XML，但那是 `s3s::dto::ServerSideEncryptionConfiguration` 严格 round-trip DTO——塞内部字段会污染协议层。因此**全局 env 是本需求下最小方案**；bucket 级算法属于后续扩展（见第 6 节），不作为本期实现。

---

## 2. 第二层：写入端接线（ecstore io_support）

**改动集中在 `crates/ecstore/src/io_support/rio.rs`，上层（sse.rs / put.rs / copy.rs / multipart_usecase.rs）零改动。**

`WritePlan::apply` 的 4 个加密分支（:444-495）按 `encryption_cipher()` 选择构造器——cipher 是 `chacha20-poly1305` 时强制 v2 帧（ChaCha 只支持认证 v2 帧，不支持旧的 v1 无认证帧）：

```
SinglepartObjectKey  : EncryptReader::new_v2_with_cipher(reader, key, [0;12], cipher)   // 原来是 new()（v1）
Singlepart{base_nonce}: (frame_v2 || cipher==ChaCha) → new_v2_with_cipher 否则 new()
MultipartLegacy{..}   : (frame_v2 || cipher==ChaCha) → new_multipart_v2_with_cipher 否则 new_multipart()
MultipartObjectKey   : EncryptReader::new_multipart_v2_with_cipher(reader, key, part, cipher)
```

读端 `decrypt_reader*` 系列（:170-291）**完全不动**。

---

## 3. 第三层：I/O 层帧格式（crates/rio，核心改动）

### 3.1 帧类型扩展（crates/rio/src/encrypt_reader.rs:44-47）

```rust
const FRAME_TYPE_V1: u8 = 0x00;              // 不变：AES，无认证头
const FRAME_TYPE_V2: u8 = 0x01;              // 不变：AES v2
const FRAME_TYPE_V2_FINAL: u8 = 0x02;        // 不变：AES v2 末帧
const FRAME_TYPE_CHACHA20_V2: u8 = 0x03;     // 新增：ChaCha20-Poly1305 v2
const FRAME_TYPE_CHACHA20_V2_FINAL: u8 = 0x04; // 新增：ChaCha20-Poly1305 v2 末帧
const FRAME_TYPE_END: u8 = 0xFF;             // 不变
```

帧头 8 字节布局不变（type + 24bit len + crc32），长度字段与 tag 尺寸（ChaCha20-Poly1305 同为 12B nonce + 16B tag）与 AES-GCM 完全一致 → **闭式偏移映射、seek、压缩索引全部不受影响**。

### 3.2 写端 cipher 抽象（仿 rio-v2 已验证的 `DareDecryptCipher` 模式）

```rust
enum EncryptCipher {
    Aes256Gcm(Aes256Gcm),
    ChaCha20Poly1305(ChaCha20Poly1305),
}
impl EncryptCipher {
    fn new(cipher: EncryptionCipher, key: &[u8; 32]) -> io::Result<Self> { /* 按枚举构造 */ }
    fn cipher_id(&self) -> u8;
    fn encrypt(&self, nonce, plaintext, aad) -> Result<Vec<u8>, _>;
}
```

- `EncryptReader.cipher: Aes256Gcm` → `cipher: EncryptCipher`
- 新增 `new_v2_with_cipher(inner, key, nonce, cipher)` / `new_multipart_v2_with_cipher(inner, key, nonce, part, cipher)`；现有 `new / new_v2 / new_multipart / new_multipart_v2` 保持默认 AES，签名不变（旧调用零影响）
- `build_frame`（:123-173）按 `cipher_id()` 选类型字节 0x01/0x02 或 0x03/0x04，AEAD 加密统一走 `Payload { msg, aad }`（ChaCha20-Poly1305 与 AES-GCM 的 API 完全同构）

### 3.3 读端 dispatch（DecryptReader，:382-412 + :633-788）

```rust
// cipher: Aes256Gcm → Option<EncryptCipher>（None = 流内首个帧确定）
fn cipher_for_type(typ: u8, key: [u8;32]) -> io::Result<EncryptCipher> {
    match typ {
        FRAME_TYPE_V1 | FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => EncryptCipher::Aes256Gcm(..),
        FRAME_TYPE_CHACHA20_V2 | FRAME_TYPE_CHACHA20_V2_FINAL => EncryptCipher::ChaCha20Poly1305(..),
        _ => Err(InvalidData "unknown encrypted frame type ..."),
    }
}
```

改动点：
1. `frame_version` match（:633-641）：`0x03/0x04` 归入 version 2（复用现有 v2 逻辑：AAD 认证、nonce 派生、final 帧检测全部继承）
2. 首个帧初始化 cipher 后缓存；后续帧执行「cipher token 一致性」检查——段内混用 0x01/0x03 → 报错（复用现有 `saw_final_frame` / `segment_frame_version` 检查的同一位置，仿 rio-v2:658-665 的 `cipher.cipher_id() != header[1]` 检查）
3. 解密调用点（:734-788）：`this.cipher` 改为 `Option` 展开后按 `current_frame_type` 路由

---

## 4. 新增算法引入配置方案（如何加第 3 个算法）

后续加一个 Rust 编写的 AEAD 算法（如 `aes-256-gcm-siv`、`xchacha20poly1305`），固定五步：

1. **依赖**：`Cargo.toml` 加算法 crate（如 `aes-gcm-siv`）
2. **帧类型**：encrypt_reader.rs 常量区加 `FRAME_TYPE_X_V2` / `FRAME_TYPE_X_V2_FINAL`（crates/rio 侧扩展值域不受限）
3. **实现**：`EncryptCipher` 加一个变体 + `encrypt` 分支；`cipher_for_type` 加两行 match
4. **env 值**：`EncryptionCipher::from_str` 加一个字符串值
5. **测试**：roundtrip + 混用报错（照抄现有 ChaCha 测试模式）

约束（必须满足 DARE v2 / legacy v2 帧硬条件）：**12 字节 nonce + 16 字节 tag**。16 字节 key 的算法可在 `EncryptCipher::new` 边界派生为 32B（结构体 `[u8; 32]` 不变）。

---

## 5. 兼容性 / 回滚 / 升级纪律

| 场景 | 行为 |
|---|---|
| AES 数据 + 切到 ChaCha 写 | 新对象 0x03/0x04，旧对象 0x00/0x01/0x02 → 读端类型驱动，**旧数据照常读** |
| ChaCha 数据 + 切回 AES 写 | 同上，反向成立 |
| 混用（复制/transition 密文透传） | 明文层 etag/checksum 不变（`WritePlan.apply` 明确 SIZE_PRESERVE_LAYER，上游节点读时按类型 dispatch） |
| 旧版本节点读 ChaCha 对象 | 帧类型 0x03 → "unknown encrypted frame type" 报错（**fail-closed**，不静默损坏） |

**滚动升级纪律**：R1 先全集群升级到含 0x03/0x04 读支持的版本（读白名单只增不减）；R2 确认全集群可读后，再设 `RUSTFS_ENCRYPTION_CIPHER=chacha20-poly1305` 开启写端。

---

## 6. 后续可扩展方向（本期不做）

**bucket 级算法**：`BucketMetadata` 手写 msgpack 序列化（metadata.rs `decode_from` :667 / `encode_to` :747，原文 :551/627 已随 #7768/#7759 偏移），可加 `cipher: Option<EncryptionCipher>` 字段（缺省 None）；但写路径取 cipher 需把值从 sse.rs 的 `EncryptionMaterial` 一路传到 `WriteEncryption`（约 6 处调用点），且 bucket SSE XML DTO 是严格 round-trip 类型不能污染 → 改动用例更多。若未来需要，建议作为独立版本演进，勿与全局 env 混用优先级。

---

## 7. 测试计划

- crates/rio 单测：`encrypt_chacha20_v2_roundtrip`、`decrypt_accepts_chacha20_v2`（仿 rio-v2:786-819 已有测试）、`segment_mixing_aes_chacha_fails`、`v1_aes_still_reads`
- crates/ecstore：`temp_env::async_with_vars` 设 `RUSTFS_ENCRYPTION_CIPHER` 后 `WritePlan.apply` → 产物帧类型断言 + 解密 roundtrip（仿 rustfs/src/app/object/put.rs:2347 既有模式；原文引用的 object.rs 模块已拆分为 object/ 目录）；读端不读 env 的回归：ChaCha env 下读 AES 对象仍成功

---

## 8. 改动清单汇总（最小 diff）

| 文件 | 改动 |
|---|---|
| `crates/rio/Cargo.toml` | +`chacha20poly1305.workspace = true` |
| `crates/rio/src/encrypt_reader.rs` | 帧类型常量 +2；`EncryptCipher` enum；`EncryptReader.cipher` 类型化 + 2 个新构造器；`DecryptReader.cipher` → `Option<EncryptCipher>` + `cipher_for_type` + 混用检查 |
| `crates/ecstore/src/io_support/rio.rs` | +`ENV_RUSTFS_ENCRYPTION_CIPHER`、`EncryptionCipher`、`encryption_cipher()`；`WritePlan.apply` 4 处构造器选择 |

上层（sse.rs、put.rs、copy.rs、multipart_usecase.rs、readers.rs）**零改动**。