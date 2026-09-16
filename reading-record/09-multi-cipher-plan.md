# Multi-Cipher 加密算法扩展方案（Aes256GcmDemo 示例适配版）

> 状态：设计定稿（2026-09-16），尚未实现。
> 复审：2026-09-16 对照 origin/main（a7341abdb，#7924）复核——核心结论与全部关键引用点仍成立，行号已按最新代码更新。
> 示例算法：**Aes256GcmDemo**，独立 Rust crate 放 `vendor/` 目录（不并入 `crates/`），通过 workspace path 依赖引入；以独立帧类型字节 + 独立函数命名演示"新增一个算法"的完整接线，函数名一律以算法名编写，后续换成真算法（如 ChaCha20-Poly1305）时按第 4 节六步替换即可。
> 范围：在现有 AES-256-GCM 基础上新增一个加密算法，最小改动、可扩展。
> 前置：已完成数据面加密 I/O 与配置系统的完整代码走读（crates/rio、crates/rio-v2、crates/ecstore/src/io_support、crates/config、rustfs/src/storage/sse.rs 等）。

## 0. 设计结论（先给结论）

**选型：在默认构建（legacy `crates/rio`）上扩展帧类型字节，用 `RUSTFS_ENCRYPTION_CIPHER` env 控制写端算法；读端纯按 on-disk 帧类型 dispatch，不读 env。**

不走 `crates/rio-v2` 的原因：它是独立 crate、非默认构建（Cargo.toml:46 明确 "ships in no default build"），启用它意味着压缩格式（S2）、索引格式、加密帧全部切换成 MinIO 兼容布局，属于大改造，违背"最小改动"（改动面见第 8 节，只落 5 个仓库内代码文件 + 2 处 Cargo.toml + .gitignore 一处（D1 定案）+ 1 个 vendor crate）。而 legacy 路径的读端 `DecryptReader` 已是类型字节驱动的（v1/v2 自动识别），把 `0x03/0x04` 定义为新算法的 v2 帧，读端只需加两个 match 分支。

**兼容性核心保证**：读端永远只认磁盘上的帧类型字节，配置/env 永不参与读路径。因此 AES→Aes256GcmDemo、Aes256GcmDemo→AES 双向切换后，旧数据都照常可读。

## 0.5 第零层：构建依赖配置（Cargo.toml 引入 vendor crate）

Aes256GcmDemo 作为独立 crate 放 `vendor/aes256-gcm-demo/`（crate 名暂定 `aes256-gcm-demo`，如仓库约定加 `rustfs-` 前缀则统一改为 `rustfs-aes256-gcm-demo`）。涉及 3 处 Cargo.toml 修改 + 1 处仓库约定：

**改动点 0.5-A：workspace members**（Cargo.toml:16 起的 members 列表，追加一行）：

```toml
[workspace]
members = [
    # ...既有成员...
    "vendor/aes256-gcm-demo", # Aes256GcmDemo demo cipher crate (native vendor)
]
```

**改动点 0.5-B：`[workspace.dependencies]`**（Cargo.toml:91 起，依赖表追加——仿 :96 等内部 crate 的 path 写法）：

```toml
[workspace.dependencies]
# RustFS Internal Crates
# ...既有依赖...
aes256-gcm-demo = { path = "vendor/aes256-gcm-demo" }
```

**改动点 0.5-C：`crates/rio/Cargo.toml [dependencies]`** 追加一行（仿既有 `aes-gcm` 行）：

```toml
aes-gcm = { workspace = true, features = ["rand_core"] }
aes256-gcm-demo = { workspace = true }
```

**改动点 0.5-D：`vendor/` 目录的 git 跟踪约定（已定案：D1）**。`.gitignore:19` 是裸 `vendor`，整个目录被 git 忽略——vendor crate 默认**不会被提交**。这是 MinIO/rustfs 系仓库的第三方依赖 vendor 惯例（CI 侧用 `cargo vendor` 重新生成），但自制 crate 若只存在本地，换机器/CI 构建时 path 依赖会失配。**定案 D1（提交自制 crate）**：`.gitignore:19` 由裸 `vendor` 改为限定通配：

```gitignore
# vendor/ 整体忽略，仅自制 crate 可跟踪（D1）
vendor/*
!vendor/aes256-gcm-demo/
```

自制 crate 随仓库走，真正的第三方 vendor 目录（`cargo vendor` 生成物）继续忽略、不受污染。被否决的备选 **D2**（保持裸 `vendor` 忽略、人人各自本地放置 crate、CI 额外步骤）不采用——违背可复现构建。

**改动点 0.5-E：vendor crate 的 API 契约（编译期锁定帧参数）**。帧布局闭式假设（第 3.1 节）依赖 nonce=12B、tag=16B，必须由 vendor crate 自己在类型层固定，而不是靠 crates/rio 侧约定。做法：vendor crate 依赖 workspace 的 `aes-gcm`（**仅取 `aead` 类型重导出**，encrypt_reader.rs:16-17 的 `Nonce`/`Payload` 正来自 `aes_gcm::aead`，同源即无版本错配），对 `aead::Aead` trait（aead 0.6.1，见 Cargo.lock）实现加解密：

```rust
// vendor/aes256-gcm-demo/src/lib.rs（契约骨架）
use aes_gcm::aead::{Aead, KeyInit, Payload};

pub struct Aes256GcmDemo { /* 内部状态 */ }

impl Aead for Aes256GcmDemo {
    type NonceSize = aes_gcm::aead::U12; // 12B nonce，类型级锁定
    type TagSize = aes_gcm::aead::U16;   // 16B tag，类型级锁定
    type CiphertextOverhead = aes_gcm::aead::U16;
    fn encrypt<'msg, 'aad>(&self, nonce: &aead::Nonce<Self::NonceSize>, plaintext: Payload<'msg, 'aad>) -> Result<Vec<u8>, aead::Error> { /* ... */ }
    fn decrypt<'msg, 'aad>(&self, nonce: &aead::Nonce<Self::NonceSize>, ciphertext: Payload<'msg, 'aad>) -> Result<Vec<u8>, aead::Error> { /* ... */ }
}

pub fn new_from_key(key: &[u8; 32]) -> Aes256GcmDemo { /* demo 实现，key 派生 } */
```

实现 `Aead` trait 后，crates/rio 侧所有调用点（改动点 C 的 encrypt、改动点 H 的 decrypt）都是既有的 trait 方法调用，**零适配层**；12B nonce 由 `NonceSize` 关联类型保证，长度与帧头换算自动成立。demo 的核心逻辑可用任意占位实现（复制密文、简单 XOR 等），但必须走真实的 `encrypt`/`decrypt` 签名并产出 16B tag，以便参数契约测试（第 7 节）锁定。若未来 vendor API 改为自有 trait，则改动点 C/H 各自加一层适配（已在文中注明）。

---

## 1. 第一层：加密算法配置（env 环境变量）

### 1.1 新增环境变量

| 变量 | 取值 | 默认值 | 作用 |
|---|---|---|---|
| `RUSTFS_ENCRYPTION_CIPHER` | `aes256-gcm` \| `aes256-gcm-demo` | `aes256-gcm` | **仅写端**：新写入对象使用的加密算法 |

- `aes256-gcm`：现有行为（帧类型 0x00/0x01/0x02）
- `aes256-gcm-demo`：Aes256GcmDemo（帧类型 0x03/0x04）

### 1.2 配置位置：crates/config/src/constants/

`crates/config` 是全部 `RUSTFS_*` 扁平常量的仓库（`crates/config/AGENTS.md` 强制约定，禁止文件配置与 `RUSTFS_CONFIG_*`）。**本次改动点**：在 `crates/config/src/constants/` 下新建 `encryption.rs`（仿 `compress.rs`/`scanner.rs` 的模块样式），登记两个常量并挂到 `mod.rs`：

```rust
// crates/config/src/constants/encryption.rs（新增，公共常量）
/// Environment variable selecting the cipher used to encrypt newly written objects.
pub const ENV_RUSTFS_ENCRYPTION_CIPHER: &str = "RUSTFS_ENCRYPTION_CIPHER";
/// Default cipher name when ENV_RUSTFS_ENCRYPTION_CIPHER is unset.
pub const DEFAULT_RUSTFS_ENCRYPTION_CIPHER: &str = "aes256-gcm";
```

```rust
// crates/config/src/constants/mod.rs
pub(crate) mod encryption; // 新增一行，与 compress/scanner 等并列
```

```rust
// crates/config/src/lib.rs —— 常量经顶层 re-export 对外可见（:16-20 的惯用块）
#[cfg(feature = "constants")]
pub use constants::encryption::*; // 新增一行
```

引用方既可用 `use rustfs_config::ENV_RUSTFS_ENCRYPTION_CIPHER` 具名引入，也可全路径 `rustfs_config::ENV_RUSTFS_ENCRYPTION_CIPHER` 直接引用（ecstore 现有代码即顶层引用，见 bucket_target_sys.rs:2703、bucket_lifecycle_ops.rs:283 的既有写法）；`constants = ["dep:const-str"]` 是 config crate 默认 feature（`crates/config/Cargo.toml:36-42`，`default = ["constants"]` 在 :37，`constants` 在 :42），ecstore 依赖已带默认特性，无需新开 feature。

### 1.3 取值解析与热路径缓存（ecstore 连接层）

改动集中在 `crates/ecstore/src/io_support/rio.rs`。在现有 `ENV_RUSTFS_ENCRYPTION_FRAME_V2` 模式（:332-347，const :332-334 + `encryption_frame_v2_enabled()` :337-347）旁新增同构的一组定义：

```rust
// crates/ecstore/src/io_support/rio.rs（新增，与 :332-347 的 frame_v2 模式并列）
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EncryptionCipher {
    Aes256Gcm,
    Aes256GcmDemo,
}

impl Default for EncryptionCipher {
    fn default() -> Self {
        Self::Aes256Gcm
    }
}

impl FromStr for EncryptionCipher {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "aes256-gcm" => Ok(Self::Aes256Gcm),
            "aes256-gcm-demo" => Ok(Self::Aes256GcmDemo),
            other => {
                tracing::warn!("unknown RUSTFS_ENCRYPTION_CIPHER value {other:?}, falling back to aes256-gcm");
                Err(())
            }
        }
    }
}

pub(crate) fn encryption_cipher() -> EncryptionCipher {
    #[cfg(test)]
    {
        rustfs_utils::get_env_str(
            rustfs_config::ENV_RUSTFS_ENCRYPTION_CIPHER,
            rustfs_config::DEFAULT_RUSTFS_ENCRYPTION_CIPHER,
        )
        .parse()
        .unwrap_or_default()
    }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<EncryptionCipher> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_str(
                rustfs_config::ENV_RUSTFS_ENCRYPTION_CIPHER,
                rustfs_config::DEFAULT_RUSTFS_ENCRYPTION_CIPHER,
            )
            .parse()
            .unwrap_or_default()
        })
    }
}
```

设计要点：
- 命名扁平 `RUSTFS_*`，常量放 `crates/config/src/constants/encryption.rs`，符合 `crates/config/AGENTS.md` 强制约定（异构的本地 `ENV_RUSTFS_ENCRYPTION_FRAME_V2` 保留原地不动，不做顺手重构）
- 用 `rustfs_utils::get_env_str` 自动获得 `MINIO_*` 别名兼容注入（envs.rs:88-120 已有机制，无需新代码）
- 进程启动时一次性求值 + `OnceLock` 缓存，避免热路径重复读 env（照抄 :344-346 的 frame_v2 缓存写法）
- 非法值 warn 并回落默认（fail-safe 到 AES，不破坏启动）

### 1.4 无配置文件的原因

走读确认：RustFS 无全局配置文件加载机制，所有全局配置均为扁平 env 常量（crates/config 只是常量仓库）。bucket 级配置（如 SSE）走 bucket metadata XML，但那是 `s3s::dto::ServerSideEncryptionConfiguration` 严格 round-trip DTO——塞内部字段会污染协议层。因此**全局 env 是本需求下最小方案**；bucket 级算法属于后续扩展（见第 6 节），不作为本期实现。

---

## 2. 第二层：写入端接线（ecstore io_support）

**改动集中在 `crates/ecstore/src/io_support/rio.rs` 的 `WritePlan::apply`（:425-505），上层（sse.rs / put.rs / copy.rs / multipart_usecase.rs）零改动。**

`WritePlan::apply` 的 4 个加密分支（`match encryption.mode`，:446-497）在**选择构造器**处按 `encryption_cipher()` 分流——cipher 是 `Aes256GcmDemo` 时选择带算法名的 v2 构造器（Aes256GcmDemo 只支持认证 v2 帧，不支持旧的 v1 无认证帧）；`frame_v2` 开关（`RUSTFS_ENCRYPTION_FRAME_V2`）逻辑保持不变，两种条件独立叠加：

```rust
// 现状（:446-497 每个分支选择 EncryptReader 构造器）→ 改动后（以 Singlepart 分支为例）
WriteEncryptionMode::Singlepart { base_nonce } => {
    #[cfg(not(feature = "rio-v2"))]
    let encrypt_reader = if encryption_cipher() == EncryptionCipher::Aes256GcmDemo {
        EncryptReader::new_v2_with_aes256_gcm_demo(reader, encryption.key_bytes, base_nonce)
    } else if encryption_frame_v2_enabled() {
        EncryptReader::new_v2(reader, encryption.key_bytes, base_nonce)
    } else {
        EncryptReader::new(reader, encryption.key_bytes, base_nonce)
    };
    #[cfg(feature = "rio-v2")]
    let encrypt_reader = EncryptReader::new(reader, encryption.key_bytes, base_nonce);
    HashReader::from_reader(encrypt_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)?
}
```

4 个分支的适配矩阵（`encryption_cipher()` 为 `Aes256GcmDemo` 时的构造器选择，其余情况行为不变）：

| WriteEncryptionMode | 现状构造器 | Aes256GcmDemo 适配 |
|---|---|---|
| `SinglepartObjectKey`（object-key 派生 key） | `new(reader, key, [0u8; 12])`（v1） | `new_v2_with_aes256_gcm_demo(reader, key, [0u8; 12])` |
| `Singlepart { base_nonce }` | `frame_v2 ? new_v2 : new` | `new_v2_with_aes256_gcm_demo(reader, key, base_nonce)` |
| `MultipartLegacy { base_nonce, part }` | `frame_v2 ? new_multipart_v2 : new_multipart` | `new_multipart_v2_with_aes256_gcm_demo(reader, key, base_nonce, part)` |
| `MultipartObjectKey { part }` | `new_multipart(reader, key, [0u8; 12], part)`（v1） | `new_multipart_v2_with_aes256_gcm_demo(reader, key, [0u8; 12], part)` |

读端 `decrypt_reader*` 系列（:170-291）**完全不动**（见第 3.3 节：解密所需的 cipher 由读端按帧类型自行构造）。

---

## 3. 第三层：I/O 层帧格式（crates/rio，核心改动）

### 3.1 帧类型扩展（crates/rio/src/encrypt_reader.rs:44-47）

```rust
const FRAME_TYPE_V1: u8 = 0x00;                    // 不变：AES，无认证头
const FRAME_TYPE_V2: u8 = 0x01;                    // 不变：AES v2
const FRAME_TYPE_V2_FINAL: u8 = 0x02;              // 不变：AES v2 末帧
const FRAME_TYPE_AES256GCMDEMO_V2: u8 = 0x03;      // 新增：Aes256GcmDemo v2
const FRAME_TYPE_AES256GCMDEMO_V2_FINAL: u8 = 0x04; // 新增：Aes256GcmDemo v2 末帧
const FRAME_TYPE_END: u8 = 0xFF;                   // 不变
```

帧头 8 字节布局不变（type + 24bit len + crc32）。vendor crate 复刻 AES-256-GCM 的 AEAD 参数（12B nonce + 16B tag），因此长度字段与 tag 尺寸与 AES-GCM 完全一致 → **闭式偏移映射、seek、压缩索引全部不受影响**（vendor crate 须在 README/测试中锁定该参数契约，见第 7 节）。

### 3.2 写端 cipher 抽象（encrypt_reader.rs:61-119 + build_frame:123-173）

**改动点 A：`EncryptReader` 成员类型化**（:64）。`cipher: Aes256Gcm` → `cipher: EncryptCipher`（枚举），`EncryptCipher` 的两个变体分别持有 `aes_gcm::Aes256Gcm`（现有原语）与 vendored `aes256_gcm_demo::Aes256GcmDemo`：

```rust
// :61-75 结构体成员改动
pub struct EncryptReader<R> {
    #[pin]
    pub inner: R,
    cipher: EncryptCipher,   // 原 Aes256Gcm
    // ...其余字段不变
}

// 新增（放在 EncryptReader struct 定义之前）
/// Write-side cipher: wraps the AEAD primitive. Aes256GcmDemo is a
/// vendored crate under vendor/aes256-gcm-demo; swapping in a real new
/// cipher later only extends this enum.
enum EncryptCipher {
    Aes256Gcm(aes_gcm::Aes256Gcm),
    Aes256GcmDemo(aes256_gcm_demo::Aes256GcmDemo),
}

impl EncryptCipher {
    /// 1-byte algorithm token used by the read-side mix check (3.3 改动点 G).
    fn cipher_token(&self) -> u8 {
        match self {
            EncryptCipher::Aes256Gcm(_) => 0,
            EncryptCipher::Aes256GcmDemo(_) => 1,
        }
    }
}
```

注意两个枚举分属不同层，**不可互相引用**：`EncryptionCipher`（配置枚举，`crates/ecstore::io_support`，1.3 节）与 `EncryptCipher`（I/O 层 AEAD 包装，crates/rio 本文件）。算法选择的翻译点在 `WritePlan.apply` 的构造器分派（第 2 节）：ecstore 按 `encryption_cipher()` 选择调用 `new_v2_with_aes256_gcm_demo` / `new_v2` / `new`，rio 层只见构造器、不见配置枚举——保持 `crates/rio` 不依赖 `crates/ecstore` 的分层。16 字节 key 的算法在 `EncryptCipher` 变体构造处派生为 32B（结构体 `[u8; 32]` 不变）。

**改动点 B：新增带算法名的构造器**（:82-118 现有 4 个构造器之后）：

```rust
impl<R> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    /// Writer for Aes256GcmDemo: authenticated fixed-frame v2 layout written
    /// with frame types 0x03/0x04. Payload, nonce and AAD handling are
    /// identical to [`EncryptReader::new_v2`]; only the type byte differs.
    pub fn new_v2_with_aes256_gcm_demo(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        let mut reader = Self::new(inner, key, nonce);
        reader.frame_v2 = true; // 复用定长帧缓冲/末帧/END 逻辑
        reader.cipher =
            EncryptCipher::Aes256GcmDemo(aes256_gcm_demo::Aes256GcmDemo::new_from_key(&key));
        reader
    }

    /// Multipart writer for Aes256GcmDemo; see [`EncryptReader::new_v2_with_aes256_gcm_demo`].
    pub fn new_multipart_v2_with_aes256_gcm_demo(
        inner: R,
        key: [u8; 32],
        base_nonce: [u8; 12],
        part_number: usize,
    ) -> Self {
        let mut reader = Self::new_multipart(inner, key, base_nonce, part_number);
        reader.frame_v2 = true;
        reader.cipher =
            EncryptCipher::Aes256GcmDemo(aes256_gcm_demo::Aes256GcmDemo::new_from_key(&key));
        reader
    }
}
```

现有 `new / new_multipart / new_v2 / new_multipart_v2` 签名与行为不变（旧调用零影响）；因 `cipher` 字段类型化，它们的初始化行由 `cipher: Aes256Gcm::new_from_slice(&key).expect("key")` 改为 `cipher: EncryptCipher::Aes256Gcm(Aes256Gcm::new_from_slice(&key).expect("key"))` 即可。

**改动点 C：`build_frame` cipher 参数化**（:123-129）。签名 `cipher: &Aes256Gcm` → `cipher: &EncryptCipher`；加密分支按枚举路由，两个变体各自调用自己的 `encrypt`：

```rust
// :151-164 现有 match 改为按 cipher + type_byte 双重路由
let ciphertext = match (cipher, type_byte) {
    (EncryptCipher::Aes256Gcm(c), FRAME_TYPE_V1) => c.encrypt(nonce, plaintext),
    (EncryptCipher::Aes256Gcm(c), _) => c.encrypt(nonce, Payload { msg: plaintext, aad: &v2_frame_aad(&header, block_index) }),
    (EncryptCipher::Aes256GcmDemo(c), _) => c.encrypt(nonce, Payload { msg: plaintext, aad: &v2_frame_aad(&header, block_index) }),
}
.map_err(|e| Error::other(format!("encrypt error: {e}")))?;
```

vendor crate 按 0.5-E 契约对 `aead::Aead` 实现加解密（关联类型在编译期锁 12B nonce / 16B tag），此处两个 encrypt 调用都是既有 trait 方法、**零适配**；若未来 vendor 改为自有 trait，则在此处加一层适配。`FRAME_TYPE_V1` 分支永不落入 demo 变体（demo 构造器一律 `frame_v2 = true`），故 match 的 v1 分支只匹配 Aes256Gcm。

**改动点 D：帧类型字节下发**（poll_read :220）。v2 路径现有 `type_byte = if is_final { FRAME_TYPE_V2_FINAL } else { FRAME_TYPE_V2 }` 改为按 cipher 选字节：

```rust
// :220 原取值 → 按 cipher_id 选 type byte
let (non_final_byte, final_byte) = match this.cipher {
    EncryptCipher::Aes256Gcm(_) => (FRAME_TYPE_V2, FRAME_TYPE_V2_FINAL),
    EncryptCipher::Aes256GcmDemo(_) => (FRAME_TYPE_AES256GCMDEMO_V2, FRAME_TYPE_AES256GCMDEMO_V2_FINAL),
};
let type_byte = if is_final { final_byte } else { non_final_byte };
```

### 3.3 读端 dispatch（DecryptReader，:382-412 + :633-788）

**改动点 E：`DecryptReader.cipher` 类型化 + 帧类型→cipher 派生**。字段（:385）`cipher: Aes256Gcm` → `cipher: Option<EncryptCipher>`（`None` = 流内首个帧到位后确定，因单对象内算法由首个帧锁定）；新增私有函数：

```rust
// 新增（DecryptReader impl 附近）
/// Map an on-disk frame type byte to the cipher that must decrypt it.
/// 0x03/0x04 construct the vendored Aes256GcmDemo; the plain AES frame
/// types construct the existing aes_gcm::Aes256Gcm.
fn cipher_for_type(typ: u8, key: [u8; 32]) -> std::io::Result<EncryptCipher> {
    match typ {
        FRAME_TYPE_V1 | FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => {
            Ok(EncryptCipher::Aes256Gcm(Aes256Gcm::new_from_slice(&key).expect("key")))
        }
        FRAME_TYPE_AES256GCMDEMO_V2 | FRAME_TYPE_AES256GCMDEMO_V2_FINAL => {
            Ok(EncryptCipher::Aes256GcmDemo(aes256_gcm_demo::Aes256GcmDemo::new_from_key(&key)))
        }
        other => Err(Error::other(format!("unknown encrypted frame type {other:#04x}"))),
    }
}
```

**改动点 F：`frame_version` match 扩展**（:633-641）。`0x03/0x04` 归入 version 2（复用现有 v2 逻辑：AAD 认证、nonce 派生、final 帧检测全部继承）：

```rust
// :633-641 原 match
let frame_version = match typ {
    FRAME_TYPE_V1 => 1,
    FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL | FRAME_TYPE_AES256GCMDEMO_V2 | FRAME_TYPE_AES256GCMDEMO_V2_FINAL => 2,
    other => {
        return Poll::Ready(Err(Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown encrypted frame type {other:#04x}"),
        )));
    }
};
```

`segment_frame_version` 混用检查（:649-658）与 `saw_final_frame`（:643-648）**零改动**——版本号已归并，段内混用 0x01/0x03 会因 `version != frame_version` 命中既有报错分支。

**改动点 G：首个帧初始化 cipher + 后续帧一致性检查**。在 `current_frame_type = typ`（:662）附近插入：

```rust
// :662 附近新增
if this.cipher.is_none() {
    *this.cipher = Some(cipher_for_type(typ, *this.key)?);
} else if this.cipher.as_ref().unwrap().cipher_token() != cipher_token_for_type(typ) {
    return Poll::Ready(Err(Error::new(
        std::io::ErrorKind::InvalidData,
        "encrypted segment mixes cipher algorithms",
    )));
}
```

其中 `cipher_token()` 是 `EncryptCipher` 实例上的 1 字节标记方法（Aes256Gcm=0 / Aes256GcmDemo=1），`cipher_token_for_type(typ)` 是按帧类型字节映射同一标记的辅助函数（与 `cipher_for_type` 同构，只取 token 不构造 cipher），做法仿 rio-v2:658-665 的 `cipher.cipher_id() != header[1]` 检查（`DecryptReader` 需新增 `key: [u8; 32]` 字段，:386 附近）。

**改动点 H：解密调用点路由**（:734-747）。`this.cipher` 改为 `Option` 展开后按枚举变体调用 decrypt；v1 非认证回退路径（:748-788）同样改走 `.as_ref().expect(...)`——不变量是首个帧（含 v1 首帧）已在改动点 G 处初始化 cipher，故解密时恒为 `Some`，v1 回退路径的变体只可能是 Aes256Gcm（`FRAME_TYPE_V1` 不会映射到 demo 类型）：

```rust
// :739-747 原 this.cipher.decrypt(...) → 显式匹配
let aad = v2_frame_aad(this.header_buf, *this.block_index);
// None 不可能到达这里：首个 v2 帧已在上方初始化 cipher
let cipher = this.cipher.as_ref().expect("cipher initialized on first frame");
let decrypted = match cipher {
    EncryptCipher::Aes256Gcm(c) => c.decrypt(&nonce, Payload { msg: ciphertext, aad: &aad }),
    EncryptCipher::Aes256GcmDemo(c) => c.decrypt(&nonce, Payload { msg: ciphertext, aad: &aad }),
};
let plaintext = decrypted.map_err(|_| Error::new(std::io::ErrorKind::InvalidData, "v2 encrypted frame failed authentication"))?;
```

Aes256GcmDemo 按 0.5-E 契约实现 `aead::Aead`，decrypt 调用与 Aes256Gcm 完全同构；此处保留显式 match 而非合并——未来换成真算法时只需改两个分支体、不动 dispatch 骨架，若 vendor API 偏离 `aead::Aead`，适配层也加在这里。

---

## 4. 新增算法引入配置方案（如何加第 3 个真实算法）

Aes256GcmDemo 是插桩示例，验证了整套扩展骨架。后续加一个真实 Rust AEAD 算法（如 `chacha20-poly1305`、`aes-256-gcm-siv`），固定六步：

1. **依赖**：两种引入方式，按算法来源二选一——(a) 自制/内购或需 vendor 的算法：仿第 0.5 节 Aes256GcmDemo 的 vendor 模式，三处配置（workspace members + `[workspace.dependencies]` path 依赖 + `crates/rio/Cargo.toml` 一行）；(b) crates.io 公开算法：workspace 根 `[workspace.dependencies]` 加算法 crate（`chacha20poly1305 = 0.11.0` 已在 workspace，见 Cargo.toml:206），`crates/rio/Cargo.toml` 加一行 `chacha20poly1305 = { workspace = true }`
2. **帧类型**：encrypt_reader.rs:44-47 常量区加 `FRAME_TYPE_X_V2` / `FRAME_TYPE_X_V2_FINAL`（legacy 帧类型值域 0x00-0xFF 不受限）
3. **写端**：`EncryptCipher` enum 加变体（如 `ChaCha20Poly1305(ChaCha20Poly1305)`）+ `build_frame` 加密分支 + poll_read :220 的 type-byte 路由；新增 `new_v2_with_chacha20_poly1305` / `new_multipart_v2_with_chacha20_poly1305` 构造器赋值 `frame_v2 = true`
4. **读端**：`cipher_for_type` 加两行 match；混合算法检查的 cipher token 加一个值
5. **env 值**：`EncryptionCipher::from_str`（io_support/rio.rs 1.3 节）加一个字符串值，`crates/config` 常量不动（只存默认值）
6. **测试**：roundtrip + 混用报错（照抄第 7 节模式）

约束（必须满足 legacy v2 帧硬条件，与 DARE 一致）：**12 字节 nonce + 16 字节 tag**。16 字节 key 的算法可在 `EncryptCipher` 变体构造处派生为 32B（结构体 `[u8; 32]` 不变，即 cipher 边界派生）。

---

## 5. 兼容性 / 回滚 / 升级纪律

| 场景 | 行为 |
|---|---|
| AES 数据 + 切到 Aes256GcmDemo 写 | 新对象 0x03/0x04，旧对象 0x00/0x01/0x02 → 读端类型驱动，**旧数据照常读** |
| Aes256GcmDemo 数据 + 切回 AES 写 | 同上，反向成立 |
| 混用（复制/transition 密文透传） | 明文层 etag/checksum 不变（`WritePlan.apply` 明确 `HashReader::SIZE_PRESERVE_LAYER`，上游节点读时按类型 dispatch） |
| 旧版本节点读 Aes256GcmDemo 对象 | 帧类型 0x03 → "unknown encrypted frame type" 报错（**fail-closed**，不静默损坏） |

**滚动升级纪律**：R1 先全集群升级到含 0x03/0x04 读支持的版本（读白名单只增不减）；R2 确认全集群可读后，再设 `RUSTFS_ENCRYPTION_CIPHER=aes256-gcm-demo` 开启写端。`RUSTFS_ENCRYPTION_FRAME_V2` 与 `RUSTFS_ENCRYPTION_CIPHER` 相互独立，任一生效都会写出 v2 帧（0x01/0x02 或 0x03/0x04），读端都能解。

---

## 6. 后续可扩展方向（本期不做）

**bucket 级算法**：`BucketMetadata` 手写 msgpack 序列化（metadata.rs `decode_from` :667 / `encode_to` :747），可加 `cipher: Option<EncryptionCipher>` 字段（缺省 None）；但写路径取 cipher 需把值从 sse.rs 的 `EncryptionMaterial` 一路传到 `WriteEncryption`（约 6 处调用点），且 bucket SSE XML DTO 是严格 round-trip 类型不能污染 → 改动用例更多。若未来需要，建议作为独立版本演进，勿与全局 env 混用优先级。

---

## 7. 测试计划

- vendor crate 侧（`vendor/aes256-gcm-demo/` 自带测试，**参数契约锁定**）：对 `aead::Aead` 的实现满足 0.5-E 契约——`NonceSize=U12`/`TagSize=U16`（编译期），回环 roundtrip + 篡改 tag 报认证错误——确保第 3.1 节"闭式偏移映射、seek、压缩索引不受影响"的帧布局假设成立
- crates/rio 单测（encrypt_reader.rs `#[cfg(test)]` 模块，:980 附近 helpers 可复用）：
  - `encrypt_aes256_gcm_demo_v2_roundtrip`：`new_v2_with_aes256_gcm_demo` 写 → `DecryptReader` 读，断言明文一致 + 产物首字节为 `0x03`、末帧前为 `0x04`
  - `decrypt_accepts_aes256_gcm_demo_v2`：手工构造 0x03/0x04 帧流 → `DecryptReader` 成功解密（仿 rio-v2:786-819 已有测试）
  - `segment_mixing_aes_and_demo_fails`：同一段混 0x01 与 0x03 → 报 "mixes cipher algorithms"
  - `v1_aes_still_reads`：既有 v1 帧流回归（确保 `cipher` 类型化未破坏旧路径）
- crates/ecstore：`temp_env::async_with_vars` 设 `RUSTFS_ENCRYPTION_CIPHER=aes256-gcm-demo` 后 `WritePlan.apply`（io_support/rio.rs tests 模块，仿 rustfs/src/app/object/put.rs:2347 既有模式）→ 产物帧类型断言 + 解密 roundtrip；读端不读 env 的回归：demo env 下读 AES 对象仍成功

---

## 8. 改动清单汇总（最小 diff）

| 文件 | 改动 | 是否新增依赖 |
|---|---|---|
| `crates/config/src/constants/encryption.rs` | 新增：`ENV_RUSTFS_ENCRYPTION_CIPHER` / `DEFAULT_RUSTFS_ENCRYPTION_CIPHER` 两个常量 | 否 |
| `crates/config/src/constants/mod.rs` | +`pub(crate) mod encryption;` 一行 | 否 |
| `crates/config/src/lib.rs` | +`pub use constants::encryption::*;` 一行（constants feature 块内） | 否 |
| `crates/rio/src/encrypt_reader.rs` | 帧类型常量 +2（0x03/0x04）；`EncryptCipher` enum；`EncryptReader.cipher` 类型化 + `new_v2_with_aes256_gcm_demo` / `new_multipart_v2_with_aes256_gcm_demo`；`build_frame` cipher 参数化；poll_read type-byte 路由；`DecryptReader.cipher` → `Option<EncryptCipher>` + `key` 字段 + `cipher_for_type` + 混用检查 + 解密调用路由 | 是：+`aes256-gcm-demo`（见下行） |
| `crates/ecstore/src/io_support/rio.rs` | +`EncryptionCipher` enum + `encryption_cipher()`（OnceLock 缓存）；`WritePlan.apply` 4 处构造器选择 | 否 |
| `Cargo.toml`（workspace 根） | members + `[workspace.dependencies]` 各 +1 行（0.5-A/B，path 指向 vendor） | 是：引入 `vendor/aes256-gcm-demo` |
| `crates/rio/Cargo.toml` | +`aes256-gcm-demo = { workspace = true }`（0.5-C） | 是 |
| `vendor/aes256-gcm-demo/`（新目录） | 新增 crate：`Cargo.toml`（依赖 workspace `aes-gcm` 取 `aead` 类型）+ `src/lib.rs`（0.5-E 契约）+ 参数契约测试（第 7 节） | —（新 crate） |
| `.gitignore` | **定案 D1**：:19 裸 `vendor` 改为 `vendor/*` + `!vendor/aes256-gcm-demo/`（0.5-D） | 否 |

上层（sse.rs、put.rs、copy.rs、extract.rs、multipart_usecase.rs、readers.rs）**零改动**。

## 9. 本次适配相对原方案的差异（Aes256GcmDemo 示例）

| 项 | 原方案（ChaCha20-Poly1305） | 本版（Aes256GcmDemo） |
|---|---|---|
| 依赖变更 | +`chacha20poly1305` 到 crates/rio | vendor path 依赖引入 `aes256-gcm-demo`（workspace members + `[workspace.dependencies]` + crates/rio 三处，0.5-A/B/C）+ 新增 `vendor/aes256-gcm-demo/` crate |
| 帧类型 | 0x03/0x04 | 0x03/0x04（复用，值域不同语义） |
| env 值 | `chacha20-poly1305` | `aes256-gcm-demo` |
| 写端构造器 | `new_v2_with_chacha20_poly1305` | `new_v2_with_aes256_gcm_demo` / `new_multipart_v2_with_aes256_gcm_demo` |
| 读端原语 | ChaCha20Poly1305 | vendored `Aes256GcmDemo`（独立类型/原语，对 `aead::Aead` 的实现与 AES 同构，独立 cipher token） |
| 新增配置常量 | 直接写 io_support 本地 | **前置到 crates/config/src/constants/encryption.rs**（合规：常量仓库统一管理） |

变更一处的代表性收益：Aes256GcmDemo 作为独立 vendor crate 演示完整接线——写端按 env 选算法 → 帧类型落盘 → 读端按帧类型还原算法，从依赖引入到 I/O 帧格式全链路独立命名；demo 实现仅复刻 AES-256-GCM 参数（12B nonce + 16B tag），帧布局闭式假设不变、无安全审查负担；生产换真算法时按第 4 节六步逐点替换（vendor 模式或 registry 模式二选一）。