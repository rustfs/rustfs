# Multi-Cipher 加密算法扩展方案（Aes256GcmDemo 示例适配版）

> 状态：设计定稿（2026-09-16）；§0.5 构建依赖层（vendor crate + Cargo.toml 三处接线 + .gitignore D1）已实现，其余章节尚未实现。
> 复审：2026-09-16 对照 origin/main（a7341abdb，#7924）复核——核心结论与全部关键引用点仍成立，行号已按最新代码更新。
> 复审2：2026-09-16 按用户三约束重审并重构改动表述——**①不新增代码文件**，全部在现有文件上扩展（§1.2 废除"新建 encryption.rs + mod.rs/lib.rs 各加一行"计划，改为在现有 `crates/config/src/constants/env.rs` 内追加，mod.rs / lib.rs 零改动）；**②新增/修改内容先列原码，再列改动后代码**，改动后块内新增行以 `// New Add` 标注；**③每笔代码改动标注所属文件与函数位置**。
> 对比约定：本文所有代码块一律"原 → 改后"两段式；改后块中 `// New Add` 标记的为新增/修改内容，其余为现有代码原文（仅作定位上下文）。`文件:函数:行号` 标注在每笔改动标题行。
> 示例算法：**Aes256GcmDemo**，独立 Rust crate `rustfs-aes256-gcm-demo` 放 `vendor/rustfs-aes256-gcm-demo/`（不并入 `crates/`），通过 workspace path 依赖引入；以独立帧类型字节 + 独立函数命名演示"新增一个算法"的完整接线，函数名一律以算法名编写，后续换成真算法（如 ChaCha20-Poly1305）时按第 4 节六步替换即可。
> 范围：在现有 AES-256-GCM 基础上新增一个加密算法，最小改动、可扩展。
> 前置：已完成数据面加密 I/O 与配置系统的完整代码走读（crates/rio、crates/rio-v2、crates/ecstore/src/io_support、crates/config、rustfs/src/storage/sse.rs 等）。

## 0. 设计结论（先给结论）

**选型：在默认构建（legacy `crates/rio`）上扩展帧类型字节，用 `RUSTFS_ENCRYPTION_CIPHER` env 控制写端算法；读端纯按 on-disk 帧类型 dispatch，不读 env。**

不走 `crates/rio-v2` 的原因：它是独立 crate、非默认构建（Cargo.toml:46 明确 "ships in no default build"），启用它意味着压缩格式（S2）、索引格式、加密帧全部切换成 MinIO 兼容布局，属于大改造，违背"最小改动"（改动面见第 8 节，只落 3 个仓库内代码文件（全部现有文件、零新增）+ 2 处 Cargo.toml + .gitignore 一处（D1 定案）+ 1 个 vendor crate）。而 legacy 路径的读端 `DecryptReader` 已是类型字节驱动的（v1/v2 自动识别），把 `0x03/0x04` 定义为新算法的 v2 帧，读端只需加两个 match 分支。

**兼容性核心保证**：读端永远只认磁盘上的帧类型字节，配置/env 永不参与读路径。因此 AES→Aes256GcmDemo、Aes256GcmDemo→AES 双向切换后，旧数据都照常可读。

## 0.5 第零层：构建依赖配置（Cargo.toml 引入 vendor crate）

Aes256GcmDemo 作为独立 crate 放 `vendor/rustfs-aes256-gcm-demo/`（crate 名定案 `rustfs-aes256-gcm-demo`，遵循仓库内部 crate 的 `rustfs-` 前缀约定，Rust 引用路径 `rustfs_aes256_gcm_demo::`）。涉及 3 处 Cargo.toml 修改 + 1 处仓库约定（**已实现**）：

**改动点 0.5-A：workspace members**（Cargo.toml:16 起的 members 列表，追加一行）：

```toml
[workspace]
members = [
    # ...既有成员...
    "vendor/rustfs-aes256-gcm-demo", # rustfs-aes256-gcm-demo demo cipher crate (native vendor)
]
```

**改动点 0.5-B：`[workspace.dependencies]`**（Cargo.toml:92 起，依赖表追加——仿 :96 等内部 crate 的 path 写法）：

```toml
[workspace.dependencies]
# RustFS Internal Crates
# ...既有依赖...
rustfs-aes256-gcm-demo = { path = "vendor/rustfs-aes256-gcm-demo", version = "0.1.0" }
```

**改动点 0.5-C：`crates/rio/Cargo.toml [dependencies]`** 追加一行（仿既有 `aes-gcm` 行）：

```toml
aes-gcm = { workspace = true, features = ["rand_core"] }
rustfs-aes256-gcm-demo = { workspace = true }
```

**改动点 0.5-D：`vendor/` 目录的 git 跟踪约定（已定案：D1）**。改动前的 `.gitignore:19` 是裸 `vendor`，整个目录被 git 忽略（改动后当前文件的 :21-22 已是 D1 通配）——vendor crate 默认**不会被提交**。这是 MinIO/rustfs 系仓库的第三方依赖 vendor 惯例（CI 侧用 `cargo vendor` 重新生成），但自制 crate 若只存在本地，换机器/CI 构建时 path 依赖会失配。**定案 D1（提交自制 crate）**：将原 `:19` 的裸 `vendor` 改为限定通配：

```gitignore
# vendor/ 整体忽略，仅自制 crate 可跟踪（D1）
vendor/*
!vendor/rustfs-aes256-gcm-demo/
```

自制 crate 随仓库走，真正的第三方 vendor 目录（`cargo vendor` 生成物）继续忽略、不受污染。被否决的备选 **D2**（保持裸 `vendor` 忽略、人人各自本地放置 crate、CI 额外步骤）不采用——违背可复现构建。

**改动点 0.5-E：vendor crate 的 API 契约（编译期锁定帧参数）**。帧布局闭式假设（第 3.1 节）依赖 nonce=12B、tag=16B，必须由 vendor crate 自己在类型层固定，而不是靠 crates/rio 侧约定。做法：vendor crate 依赖 workspace 的 `aes-gcm`（**仅取 `aead` 类型重导出**，encrypt_reader.rs:16-17 的 `Nonce`/`Payload` 正来自 `aes_gcm::aead`，同源即无版本错配），作为 `aead::AeadCore` / `aead::Aead`（aead 0.6.1，见 Cargo.lock）的实现者：

```rust
// vendor/rustfs-aes256-gcm-demo/src/lib.rs（定案实现，契约骨架）
use aes_gcm::aead::{Aead, AeadCore, Nonce, Payload, TagPosition};
use aes_gcm::{Aes256Gcm, KeyInit};

pub struct Aes256GcmDemo { inner: Aes256Gcm }

impl Aes256GcmDemo {
    pub fn new_from_key(key: &[u8; 32]) -> Self {
        Self { inner: Aes256Gcm::new_from_slice(key).expect("32-byte key") }
    }
}

impl AeadCore for Aes256GcmDemo {
    type NonceSize = <Aes256Gcm as AeadCore>::NonceSize; // 12B nonce，类型级锁定
    type TagSize = <Aes256Gcm as AeadCore>::TagSize;     // 16B tag，类型级锁定
    const TAG_POSITION: TagPosition = TagPosition::Postfix;
}

impl Aead for Aes256GcmDemo {
    fn encrypt<'msg, 'aad>(&self, nonce: &Nonce<Self>, plaintext: impl Into<Payload<'msg, 'aad>>) -> aes_gcm::aead::Result<Vec<u8>> {
        self.inner.encrypt(nonce, plaintext)
    }
    fn decrypt<'msg, 'aad>(&self, nonce: &Nonce<Self>, ciphertext: impl Into<Payload<'msg, 'aad>>) -> aes_gcm::aead::Result<Vec<u8>> {
        self.inner.decrypt(nonce, ciphertext)
    }
}
```

实现 `AeadCore` + `Aead` 后，crates/rio 侧所有调用点（改动点 C 的 encrypt、改动点 H 的 decrypt）都是既有的 trait 方法调用，**零适配层**；12B nonce / 16B tag 由 `NonceSize` / `TagSize` 关联类型（跟随内层 `Aes256Gcm` 同源派生）在编译期锁定，`TAG_POSITION = Postfix` 与 AES-GCM 一致，长度与帧头换算自动成立。aead 0.6.1 的 `Aead` **无 `CiphertextOverhead` 关联类型**，参数契约由 `Nonce<Self>` / `Tag<Self>` 的 `size_of` 断言锁定（第 7 节）。demo 核心是 newtype 包装 `Aes256Gcm` 的占位实现，逐字段转发真实加解密并产出 16B tag；核心逻辑可任意替换（复制密文、简单 XOR 等），但必须走真实的 `encrypt`/`decrypt` 签名并产出 16B tag。若未来 vendor API 改为自有 trait，则改动点 C/H 各自加一层适配（已在文中注明）。

---

## 1. 第一层：加密算法配置（env 环境变量）

### 1.1 新增环境变量

| 变量 | 取值 | 默认值 | 作用 |
|---|---|---|---|
| `RUSTFS_ENCRYPTION_CIPHER` | `aes256-gcm` \| `aes256-gcm-demo` | `aes256-gcm` | **仅写端**：新写入对象使用的加密算法 |

- `aes256-gcm`：现有行为（帧类型 0x00/0x01/0x02）
- `aes256-gcm-demo`：Aes256GcmDemo（帧类型 0x03/0x04）

### 1.2 配置位置：现有 crates/config/src/constants/env.rs 内追加（零新增文件）

`crates/config` 是全部 `RUSTFS_*` 扁平常量的仓库（`crates/config/AGENTS.md` 强制约定，禁止文件配置与 `RUSTFS_CONFIG_*`）。**本次改动点**：两个新常量**追加到现有文件 `crates/config/src/constants/env.rs`**（不新建模块文件）——env.rs 的主题正是"全局行为开关类 env 常量"（`ENV_AUDIT_ENABLE` :30、`ENV_NOTIFY_ENABLE` :32、`ENV_ILM_PROCESS_TIME` :38、`ENV_ILM_DEBUG_DAY_SECS` :56 等），`RUSTFS_ENCRYPTION_CIPHER` 同属此类。

**原代码**（`crates/config/src/constants/env.rs`，模块顶层常量区 :58-64，插入点上下文）：

```rust
// crates/config/src/constants/env.rs:58-64（原，文件顶层，无函数——常量区）
/// Number of seconds in one lifecycle "day" (86400) used as the default when
/// [`ENV_ILM_DEBUG_DAY_SECS`] is unset.
pub const DEFAULT_ILM_DAY_SECS: u32 = 86400;

/// Medium-drawn lines separator
/// This is used to separate words in environment variable names.
pub const ENV_WORD_DELIMITER_DASH: &str = "-";
```

**改后**（在两个常量之间插入，`// New Add` 为新行；函数位置：文件顶层常量区，`DEFAULT_ILM_DAY_SECS` :60 之后、`ENV_WORD_DELIMITER_DASH` :64 之前）：

```rust
pub const DEFAULT_ILM_DAY_SECS: u32 = 86400;

// New Add ↓（新增 2 个常量）
/// Environment variable selecting the cipher used to encrypt newly written objects.
/// Only the write path reads this; the decrypt reader dispatches on the on-disk
/// frame type byte and never consults this variable.
pub const ENV_RUSTFS_ENCRYPTION_CIPHER: &str = "RUSTFS_ENCRYPTION_CIPHER";
/// Default cipher name when [`ENV_RUSTFS_ENCRYPTION_CIPHER`] is unset.
pub const DEFAULT_RUSTFS_ENCRYPTION_CIPHER: &str = "aes256-gcm";
// New Add ↑

/// Medium-drawn lines separator
/// This is used to separate words in environment variable names.
pub const ENV_WORD_DELIMITER_DASH: &str = "-";
```

挂接说明（**mod.rs / lib.rs 零改动**）：env 模块早已挂接——`crates/config/src/constants/mod.rs:22` 有 `pub(crate) mod env;`，`crates/config/src/lib.rs:32` 有 `pub use constants::env::*;`（constants feature 块内）。因此追加进 env.rs 的常量自动对外可见，**不需要**原方案里"mod.rs 加一行 + lib.rs 加一行"两处改动（这正是本次复审按约束 1 删除的部分）。

命名说明：常量名 `ENV_RUSTFS_*` 取全仓主导命名（`app.rs:129` `ENV_RUSTFS_ADDRESS`、`internode.rs:304`、`tls.rs:42`、`workload.rs:24` 等 30+ 处），环境变量值 `RUSTFS_*` 与 env.rs 自身既有 `ENV_<X> = "RUSTFS_<X>"` 模式一致（:30/:32/:38/:56），两者不存在歧义。另注：`crates/config/AGENTS.md` 将 `app.rs` 列为 Source of Truth，但那只约束常量权威定义所在，不禁止按主题分文件（constants/ 下现有 27 个模块文件并存）。

引用方既可用 `use rustfs_config::ENV_RUSTFS_ENCRYPTION_CIPHER` 具名引入，也可全路径 `rustfs_config::ENV_RUSTFS_ENCRYPTION_CIPHER` 直接引用（ecstore 现有代码即顶层引用，见 bucket_target_sys.rs:2703、bucket_lifecycle_ops.rs:283 的既有写法）；`constants = ["dep:const-str"]` 是 config crate 默认 feature（`crates/config/Cargo.toml:36-42`，`default = ["constants"]` 在 :37，`constants` 在 :42），ecstore 依赖已带默认特性，无需新开 feature。

### 1.3 取值解析与热路径缓存（ecstore 连接层）

改动集中在 `crates/ecstore/src/io_support/rio.rs`。在现有 `ENV_RUSTFS_ENCRYPTION_FRAME_V2` 模式（`io_support/rio.rs::encryption_frame_v2_enabled`，const :332-334 + fn :337-347）旁新增同构的一组定义。

**原代码**（`crates/ecstore/src/io_support/rio.rs:331-347`，新增内容的定位锚点）：

```rust
// crates/ecstore/src/io_support/rio.rs:331-347（原，文件顶层）
#[cfg(not(feature = "rio-v2"))]
pub(crate) const ENV_RUSTFS_ENCRYPTION_FRAME_V2: &str = "RUSTFS_ENCRYPTION_FRAME_V2";
#[cfg(not(feature = "rio-v2"))]
pub(crate) const DEFAULT_RUSTFS_ENCRYPTION_FRAME_V2: bool = false;

#[cfg(not(feature = "rio-v2"))]
pub(crate) fn encryption_frame_v2_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_ENCRYPTION_FRAME_V2, DEFAULT_RUSTFS_ENCRYPTION_FRAME_V2)
    }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_ENCRYPTION_FRAME_V2, DEFAULT_RUSTFS_ENCRYPTION_FRAME_V2))
    }
}
```

**改后**（上述原代码块**原样保留**，其后追加，`// New Add` 为新内容；函数位置：文件顶层，紧跟 `encryption_frame_v2_enabled` :347 之后）：

```rust
// New Add ↓（全部为新增）
/// Write-side cipher selected by RUSTFS_ENCRYPTION_CIPHER.
/// The decrypt reader never consults this: it dispatches on the on-disk
/// frame type byte (see crates/rio/src/encrypt_reader.rs `cipher_for_type`).
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
// New Add ↑
```

设计要点：
- `FromStr` **已在 `io_support/rio.rs:23` 导入**（`use std::str::FromStr;`），此处无需也不应新增 use 行——原稿曾把它写进新增块，本次复审按约束 2/3 修正（既有导入，非新增内容）；
- 命名扁平 `RUSTFS_*`，常量追加到现有 `crates/config/src/constants/env.rs`（1.2 节，零新增文件），符合 `crates/config/AGENTS.md` 强制约定（异构的本地 `ENV_RUSTFS_ENCRYPTION_FRAME_V2` 保留原地不动，不做顺手重构）；
- 用 `rustfs_utils::get_env_str`（`crates/utils/src/envs.rs:651`）自动获得 `MINIO_*` 别名兼容注入（envs.rs:88-120 已有机制，无需新代码）；
- 进程启动时一次性求值 + `OnceLock` 缓存，避免热路径重复读 env（照抄 :344-346 的 frame_v2 缓存写法）；
- 非法值 warn 并回落默认（fail-safe 到 AES，不破坏启动）。

### 1.4 无配置文件的原因

走读确认：RustFS 无全局配置文件加载机制，所有全局配置均为扁平 env 常量（crates/config 只是常量仓库）。bucket 级配置（如 SSE）走 bucket metadata XML，但那是 `s3s::dto::ServerSideEncryptionConfiguration` 严格 round-trip DTO——塞内部字段会污染协议层。因此**全局 env 是本需求下最小方案**；bucket 级算法属于后续扩展（见第 6 节），不作为本期实现。

---

## 2. 第二层：写入端接线（ecstore io_support）

**改动集中在 `crates/ecstore/src/io_support/rio.rs` 的 `WritePlan::apply`（:425-505），上层（sse.rs / put.rs / copy.rs / multipart_usecase.rs）零改动。**

`WritePlan::apply` 的 4 个加密分支（`match encryption.mode`，:445-494）在**选择构造器**处按 `encryption_cipher()` 分流——cipher 是 `Aes256GcmDemo` 时选择带算法名的 v2 构造器（Aes256GcmDemo 只支持认证 v2 帧，不支持旧的 v1 无认证帧）；`frame_v2` 开关（`RUSTFS_ENCRYPTION_FRAME_V2`）逻辑保持不变，两种条件独立叠加。下面按分支逐笔给出"原 → 改后"，`// New Add` 标注新增/修改行。

### 2.1 SinglepartObjectKey 分支（object-key 派生 key）

**原代码**（`crates/ecstore/src/io_support/rio.rs::WritePlan::apply`，:446-456）：

```rust
WriteEncryptionMode::SinglepartObjectKey => HashReader::from_reader(
    #[cfg(feature = "rio-v2")]
    EncryptReader::new_with_object_key(reader, encryption.key_bytes),
    #[cfg(not(feature = "rio-v2"))]
    EncryptReader::new(reader, encryption.key_bytes, [0u8; 12]),
    HashReader::SIZE_PRESERVE_LAYER,
    actual_size,
    None,
    None,
    false,
)?,
```

**改后**（分支体提升为 `let` 形式——与下方 Singlepart 分支现有写法同构；`// New Add` 为新增/修改行。注意：**非 demo 分支保持原行为**——object-key 模式恒走 v1 `new`，不碰 `frame_v2` 开关）：

```rust
WriteEncryptionMode::SinglepartObjectKey => {
    #[cfg(feature = "rio-v2")]
    let encrypt_reader = EncryptReader::new_with_object_key(reader, encryption.key_bytes);
    #[cfg(not(feature = "rio-v2"))]
    // New Add: demo cipher 只写 v2 帧（0x03/0x04）；非 demo 维持原 v1 行为（不读 frame_v2 开关）
    let encrypt_reader = if encryption_cipher() == EncryptionCipher::Aes256GcmDemo {
        EncryptReader::new_v2_with_aes256_gcm_demo(reader, encryption.key_bytes, [0u8; 12])
    } else {
        EncryptReader::new(reader, encryption.key_bytes, [0u8; 12])
    };
    HashReader::from_reader(encrypt_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)?
}
```

### 2.2 Singlepart 分支（base_nonce）

**原代码**（`crates/ecstore/src/io_support/rio.rs::WritePlan::apply`，:457-467）：

```rust
WriteEncryptionMode::Singlepart { base_nonce } => {
    #[cfg(not(feature = "rio-v2"))]
    let encrypt_reader = if encryption_frame_v2_enabled() {
        EncryptReader::new_v2(reader, encryption.key_bytes, base_nonce)
    } else {
        EncryptReader::new(reader, encryption.key_bytes, base_nonce)
    };
    #[cfg(feature = "rio-v2")]
    let encrypt_reader = EncryptReader::new(reader, encryption.key_bytes, base_nonce);
    HashReader::from_reader(encrypt_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)?
}
```

**改后**（最外层 `if` 优先判 cipher，`frame_v2` 分支原样保留为 `else if`；`// New Add` 为新增/修改行）：

```rust
WriteEncryptionMode::Singlepart { base_nonce } => {
    #[cfg(not(feature = "rio-v2"))]
    // New Add: 外层 if 优先判 cipher（demo 恒 v2，:458 原 if 改为 else if，原逻辑不变）
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

### 2.3 MultipartLegacy 分支（base_nonce + part）

**原代码**（`crates/ecstore/src/io_support/rio.rs::WritePlan::apply`，:468-482）：

```rust
WriteEncryptionMode::MultipartLegacy {
    base_nonce,
    multipart_part_number,
} => {
    #[cfg(not(feature = "rio-v2"))]
    let encrypt_reader = if encryption_frame_v2_enabled() {
        EncryptReader::new_multipart_v2(reader, encryption.key_bytes, base_nonce, multipart_part_number)
    } else {
        EncryptReader::new_multipart(reader, encryption.key_bytes, base_nonce, multipart_part_number)
    };
    #[cfg(feature = "rio-v2")]
    let encrypt_reader =
        EncryptReader::new_multipart(reader, encryption.key_bytes, base_nonce, multipart_part_number);
    HashReader::from_reader(encrypt_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)?
}
```

**改后**（同上模式；`// New Add` 为新增/修改行）：

```rust
WriteEncryptionMode::MultipartLegacy {
    base_nonce,
    multipart_part_number,
} => {
    #[cfg(not(feature = "rio-v2"))]
    // New Add: 外层 if 优先判 cipher（demo 恒 v2，:473 原 if 改为 else if，原逻辑不变）
    let encrypt_reader = if encryption_cipher() == EncryptionCipher::Aes256GcmDemo {
        EncryptReader::new_multipart_v2_with_aes256_gcm_demo(reader, encryption.key_bytes, base_nonce, multipart_part_number)
    } else if encryption_frame_v2_enabled() {
        EncryptReader::new_multipart_v2(reader, encryption.key_bytes, base_nonce, multipart_part_number)
    } else {
        EncryptReader::new_multipart(reader, encryption.key_bytes, base_nonce, multipart_part_number)
    };
    #[cfg(feature = "rio-v2")]
    let encrypt_reader =
        EncryptReader::new_multipart(reader, encryption.key_bytes, base_nonce, multipart_part_number);
    HashReader::from_reader(encrypt_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)?
}
```

### 2.4 MultipartObjectKey 分支（object-key + part）

**原代码**（`crates/ecstore/src/io_support/rio.rs::WritePlan::apply`，:483-493）：

```rust
WriteEncryptionMode::MultipartObjectKey { multipart_part_number } => HashReader::from_reader(
    #[cfg(feature = "rio-v2")]
    EncryptReader::new_multipart_with_object_key(reader, encryption.key_bytes, multipart_part_number),
    #[cfg(not(feature = "rio-v2"))]
    EncryptReader::new_multipart(reader, encryption.key_bytes, [0u8; 12], multipart_part_number as usize),
    HashReader::SIZE_PRESERVE_LAYER,
    actual_size,
    None,
    None,
    false,
)?,
```

**改后**（同 2.1 的提升模式；`// New Add` 为新增/修改行。非 demo 分支保持原行为——object-key 恒 v1，不碰 `frame_v2` 开关）：

```rust
WriteEncryptionMode::MultipartObjectKey { multipart_part_number } => {
    #[cfg(feature = "rio-v2")]
    let encrypt_reader = EncryptReader::new_multipart_with_object_key(reader, encryption.key_bytes, multipart_part_number);
    #[cfg(not(feature = "rio-v2"))]
    // New Add: demo cipher 只写 v2 帧（0x03/0x04）；非 demo 维持原 v1 行为（不读 frame_v2 开关）
    let encrypt_reader = if encryption_cipher() == EncryptionCipher::Aes256GcmDemo {
        EncryptReader::new_multipart_v2_with_aes256_gcm_demo(reader, encryption.key_bytes, [0u8; 12], multipart_part_number as usize)
    } else {
        EncryptReader::new_multipart(reader, encryption.key_bytes, [0u8; 12], multipart_part_number as usize)
    };
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

读端 `decrypt_reader*` 系列（`io_support/rio.rs:170-291`）**完全不动**（见第 3.3 节：解密所需的 cipher 由读端按帧类型自行构造）。

---

## 3. 第三层：I/O 层帧格式（crates/rio，核心改动）

### 3.1 帧类型扩展（crates/rio/src/encrypt_reader.rs，文件顶层常量区 :44-47）

**原代码**（`crates/rio/src/encrypt_reader.rs:44-47`，文件顶层常量区）：

```rust
const FRAME_TYPE_V1: u8 = 0x00;
const FRAME_TYPE_V2: u8 = 0x01;
const FRAME_TYPE_V2_FINAL: u8 = 0x02;
const FRAME_TYPE_END: u8 = 0xFF;
```

**改后**（在 :46 与 :47 之间插入两行；`// New Add` 为新行）：

```rust
const FRAME_TYPE_V1: u8 = 0x00;
const FRAME_TYPE_V2: u8 = 0x01;
const FRAME_TYPE_V2_FINAL: u8 = 0x02;
// New Add ↓
const FRAME_TYPE_AES256GCMDEMO_V2: u8 = 0x03;      // Aes256GcmDemo v2
const FRAME_TYPE_AES256GCMDEMO_V2_FINAL: u8 = 0x04; // Aes256GcmDemo v2 末帧
// New Add ↑
const FRAME_TYPE_END: u8 = 0xFF;
```

帧头 8 字节布局不变（type + 24bit len + crc32）。vendor crate 复刻 AES-256-GCM 的 AEAD 参数（12B nonce + 16B tag），因此长度字段与 tag 尺寸与 AES-GCM 完全一致 → **闭式偏移映射、seek、压缩索引全部不受影响**（vendor crate 须在 README/测试中锁定该参数契约，见第 7 节）。

### 3.2 写端 cipher 抽象（encrypt_reader.rs:61-119 + build_frame:123-173）

**改动点 A：`EncryptReader` 成员类型化**（`crates/rio/src/encrypt_reader.rs`，`EncryptReader` struct :61-75，字段 :64）。`cipher: Aes256Gcm` → `cipher: EncryptCipher`（枚举），`EncryptCipher` 的两个变体分别持有 `aes_gcm::Aes256Gcm`（现有原语）与 vendored `rustfs_aes256_gcm_demo::Aes256GcmDemo`：

**原代码**（`encrypt_reader.rs:58-76` 的 `pin_project!` 块，结构体字段 :61-75）：

```rust
pin_project! {
    /// A reader wrapper that encrypts data on the fly using AES-256-GCM.
    /// This is a demonstration. For production, use a secure and audited crypto library.
    pub struct EncryptReader<R> {
        #[pin]
        pub inner: R,
        cipher: Aes256Gcm,          // :64 原字段
        base_nonce: [u8; 12], // 96-bit base nonce for GCM
        // ...其余字段不变（:66-74）
    }
}
```

**改后**（`// New Add` 为新增/修改；`EncryptCipher` enum 放在 :58 `pin_project!` 之前；:64 原字段行改为 `cipher: EncryptCipher`）：

```rust
// New Add ↓（新增 enum，放在 EncryptReader struct 定义之前）
/// Write-side cipher: wraps the AEAD primitive. Aes256GcmDemo is a
/// vendored crate under vendor/rustfs-aes256-gcm-demo; swapping in a real new
/// cipher later only extends this enum.
enum EncryptCipher {
    Aes256Gcm(aes_gcm::Aes256Gcm),
    Aes256GcmDemo(rustfs_aes256_gcm_demo::Aes256GcmDemo),
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
// New Add ↑

pin_project! {
    pub struct EncryptReader<R> {
        #[pin]
        pub inner: R,
        cipher: EncryptCipher, // New Add: 原 :64 的 `cipher: Aes256Gcm,` 改为枚举
        base_nonce: [u8; 12], // 96-bit base nonce for GCM
        // ...其余字段不变（:66-74）
    }
}
```

注意两个枚举分属不同层，**不可互相引用**：`EncryptionCipher`（配置枚举，`crates/ecstore::io_support`，1.3 节）与 `EncryptCipher`（I/O 层 AEAD 包装，crates/rio 本文件）。算法选择的翻译点在 `WritePlan.apply` 的构造器分派（第 2 节）：ecstore 按 `encryption_cipher()` 选择调用 `new_v2_with_aes256_gcm_demo` / `new_v2` / `new`，rio 层只见构造器、不见配置枚举——保持 `crates/rio` 不依赖 `crates/ecstore` 的分层。

**改动点 B：构造器对比（现有 Aes256Gcm vs 新增 Aes256GcmDemo）**（`crates/rio/src/encrypt_reader.rs`，`impl<R> EncryptReader<R>` :78-119）。构造器是写端算法选择的入口——ecstore 按 `encryption_cipher()` 在这里分流（第 2 节）。

**原代码**（现有 4 个 Aes256Gcm 构造器原样，:82-118）：

```rust
impl<R> EncryptReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self {
            inner,
            cipher: Aes256Gcm::new_from_slice(&key).expect("key"),  // :85 原行
            base_nonce: nonce,
            buffer: Vec::new(),
            buffer_pos: 0,
            read_buffer: vec![0u8; ENCRYPTION_BLOCK_SIZE],
            block_index: 0,
            finished: false,
            frame_v2: false,
            pending: 0,
            input_done: false,
        }
    }

    pub fn new_multipart(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        Self::new(inner, key, multipart_part_nonce(base_nonce, part_number))
    }

    /// Writer for the authenticated, fixed-frame v2 layout.
    ///
    /// Key and nonce derivation are identical to [`EncryptReader::new`]; only
    /// the frame format changes (header + frame index bound as AEAD associated
    /// data, an authenticated final frame, fixed-size non-final frames).
    pub fn new_v2(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        let mut reader = Self::new(inner, key, nonce);
        reader.frame_v2 = true;
        reader
    }

    /// Multipart writer for the v2 layout; see [`EncryptReader::new_v2`].
    pub fn new_multipart_v2(inner: R, key: [u8; 32], base_nonce: [u8; 12], part_number: usize) -> Self {
        let mut reader = Self::new_multipart(inner, key, base_nonce, part_number);
        reader.frame_v2 = true;
        reader
    }
}
```

`cipher: Aes256Gcm` 类型化为 `cipher: EncryptCipher`（改动点 A）后，上述 4 个构造器**只有 :85 初始化行一处改动**，签名与 `frame_v2` 逻辑全不变：

**改后**（`// New Add` 为新增/修改行；:85 原行 → 枚举包装；新增 2 个 Aes256GcmDemo 构造器附加在 impl 块末）：

```rust
    pub fn new(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        Self {
            inner,
            // New Add: :85 原行 `cipher: Aes256Gcm::new_from_slice(&key).expect("key"),` → 枚举包装
            cipher: EncryptCipher::Aes256Gcm(Aes256Gcm::new_from_slice(&key).expect("key")),
            base_nonce: nonce,
            // ...其余字段不变（new_v2 / new_multipart_v2 沿用现有写法：
            //     Self::new / Self::new_multipart 后置 frame_v2 = true，零改动）
        }
    }

    // New Add ↓（新增 2 个 Aes256GcmDemo 构造器）
    // crate 名按 0.5 节定案为 rustfs-aes256-gcm-demo，引用路径 rustfs_aes256_gcm_demo::Aes256GcmDemo
    /// Writer for Aes256GcmDemo: authenticated fixed-frame v2 layout written
    /// with frame types 0x03/0x04. Payload, nonce and AAD handling are
    /// identical to [`EncryptReader::new_v2`]; only the type byte differs.
    pub fn new_v2_with_aes256_gcm_demo(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self {
        let mut reader = Self::new(inner, key, nonce);
        reader.frame_v2 = true; // 复用定长帧缓冲/末帧/END 逻辑
        reader.cipher =
            EncryptCipher::Aes256GcmDemo(rustfs_aes256_gcm_demo::Aes256GcmDemo::new_from_key(&key));
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
            EncryptCipher::Aes256GcmDemo(rustfs_aes256_gcm_demo::Aes256GcmDemo::new_from_key(&key));
        reader
    }
    // New Add ↑
```

| 现有（Aes256Gcm） | 对应新增（Aes256GcmDemo） | 差异 |
|---|---|---|
| `new`（:82，v1 无认证帧，`frame_v2 = false`） | —（demo 只支持 v2，无 v1 对应物） | — |
| `new_v2`（:107） | `new_v2_with_aes256_gcm_demo` | cipher 变体不同 + 帧类型字节 0x01/0x02 → 0x03/0x04 |
| `new_multipart`（:98，v1） | —（同上，demo 无 v1 对应物） | — |
| `new_multipart_v2`（:114） | `new_multipart_v2_with_aes256_gcm_demo` | cipher 变体不同 + 帧类型字节 |

对照要点：
- demo 构造器内部结构与 `new_v2` / `new_multipart_v2` 完全同构——都是 `Self::new` / `Self::new_multipart` + `frame_v2 = true`，唯一差别是覆写 `cipher` 为 demo 变体；
- 对应关系可归结为一句话：**demo = v2 布局 + Aes256GcmDemo 原语 + 0x03/0x04 帧类型**，其余（nonce 派生、AAD、末帧、multipart part nonce）全部继承 v2 路径；
- 新增接口逐一对齐既有命名风格（`new_*_v2` → `new_*_v2_with_<algorithm>`），未来加真算法时照此再加一对构造器（第 4 节）。

**改动点 C：`build_frame` cipher 参数化**（`crates/rio/src/encrypt_reader.rs`，文件顶层函数 `build_frame` :123-173，签名 :123-129，加密 match :151-164）。签名 `cipher: &Aes256Gcm` → `cipher: &EncryptCipher`；加密分支从按 `type_byte` 路由改为按 `(cipher, type_byte)` 双重路由，两个变体各自调用自己的 `encrypt`：

**原代码**（签名 :123-129 + match :151-164）：

```rust
fn build_frame(
    cipher: &Aes256Gcm,          // :124 原形参
    nonce_bytes: &[u8; 12],
    type_byte: u8,
    block_index: usize,
    plaintext: &[u8],
) -> std::io::Result<Vec<u8>> {
    // ...:130-150 头构造、crc、clen 计算不变...
    let ciphertext = match type_byte {
        FRAME_TYPE_V1 => cipher.encrypt(nonce, plaintext),          // :152
        _ => {
            let aad = v2_frame_aad(&header, block_index);           // :154
            cipher.encrypt(nonce, Payload { msg: plaintext, aad: &aad })
        }
    }
    .map_err(|e| Error::other(format!("encrypt error: {e}")))?;
    // ...:166-172 组装输出不变...
}
```

**改后**（`// New Add` 为新增/修改行；形参类型、match 首行、两个变体分支为改动，其余原样）：

```rust
fn build_frame(
    cipher: &EncryptCipher,      // New Add: :124 原 `&Aes256Gcm` → `&EncryptCipher`
    nonce_bytes: &[u8; 12],
    type_byte: u8,
    block_index: usize,
    plaintext: &[u8],
) -> std::io::Result<Vec<u8>> {
    // ...:130-150 不变...
    let ciphertext = match (cipher, type_byte) {
        // New Add: :151-163 原 `match type_byte` → `match (cipher, type_byte)` 双重路由
        (EncryptCipher::Aes256Gcm(c), FRAME_TYPE_V1) => c.encrypt(nonce, plaintext),
        (EncryptCipher::Aes256Gcm(c), _) => {
            let aad = v2_frame_aad(&header, block_index);           // :154
            c.encrypt(nonce, Payload { msg: plaintext, aad: &aad })
        }
        (EncryptCipher::Aes256GcmDemo(c), _) => {
            let aad = v2_frame_aad(&header, block_index);
            c.encrypt(nonce, Payload { msg: plaintext, aad: &aad })
        }
    }
    .map_err(|e| Error::other(format!("encrypt error: {e}")))?;
    // ...:166-172 不变...
}
```

vendor crate 按 0.5-E 契约对 `aead::Aead` 实现加解密（关联类型在编译期锁 12B nonce / 16B tag），此处两个 encrypt 调用都是既有 trait 方法、**零适配**；若未来 vendor 改为自有 trait，则在此处加一层适配。`FRAME_TYPE_V1` 分支永不落入 demo 变体（demo 构造器一律 `frame_v2 = true`），故 match 的 v1 分支只匹配 Aes256Gcm。

**改动点 D：帧类型字节下发**（`crates/rio/src/encrypt_reader.rs`，`impl<R> AsyncRead for EncryptReader<R>::poll_read`，:220）。v2 写路径的 type-byte 取值改为按 cipher 选字节：

**原代码**（`poll_read` v2 分支，:219-220）：

```rust
let is_final = *this.input_done && *this.pending < ENCRYPTION_BLOCK_SIZE;
let type_byte = if is_final { FRAME_TYPE_V2_FINAL } else { FRAME_TYPE_V2 };
```

**改后**（`// New Add` 为新增/修改行；demo 恒走 v2 分支——demo 构造器 `frame_v2 = true`，v1 分支（:245-316）不会到达 demo，故只需改这一处）：

```rust
let is_final = *this.input_done && *this.pending < ENCRYPTION_BLOCK_SIZE;
// New Add: :220 原单行取值改为按 cipher 选 type byte（Aes256Gcm 0x01/0x02，Aes256GcmDemo 0x03/0x04）
let (non_final_byte, final_byte) = match this.cipher {
    EncryptCipher::Aes256Gcm(_) => (FRAME_TYPE_V2, FRAME_TYPE_V2_FINAL),
    EncryptCipher::Aes256GcmDemo(_) => (FRAME_TYPE_AES256GCMDEMO_V2, FRAME_TYPE_AES256GCMDEMO_V2_FINAL),
};
let type_byte = if is_final { final_byte } else { non_final_byte };
```

### 3.3 读端 dispatch（DecryptReader，:382-413 + :633-788）

**改动点 E：`DecryptReader.cipher` 类型化 + 帧类型→cipher 派生**（`crates/rio/src/encrypt_reader.rs`，`DecryptReader` struct :382-413，字段 :385）。字段 `cipher: Aes256Gcm` → `cipher: Option<EncryptCipher>`（`None` = 流内首个帧到位后确定，因单对象内算法由首个帧锁定）；新增 `key: [u8; 32]` 字段（帧类型→cipher 派生需要 key）；5 处构造器（:419/:422/:460/:466/:474）初始化同步补 `cipher: None, key,` 两字段；新增私有函数 `cipher_for_type`：

**原代码**（struct :382-412，关键行 :385）：

```rust
    pub struct DecryptReader<R> {
        #[pin]
        pub inner: R,
        cipher: Aes256Gcm,                    // :385 原字段
        base_nonce: [u8; 12], // Base nonce recorded in object metadata
        // ...其余字段不变（:387-412）
    }
```

**改后**（`// New Add` 为新增/修改行；新增 `cipher_for_type` 放在 `DecryptReader` impl 附近）：

```rust
    pub struct DecryptReader<R> {
        #[pin]
        pub inner: R,
        // New Add: :385 原 `cipher: Aes256Gcm,` → Option<EncryptCipher>（None = 首帧到位后确定）
        cipher: Option<EncryptCipher>,
        // New Add: 新增 key 字段（cipher_for_type 派生时传入）
        key: [u8; 32],
        base_nonce: [u8; 12], // Base nonce recorded in object metadata
        // ...其余字段不变（:387-412）
    }
    // New Add: 5 处构造器（:419/:422/:460/:466/:474）初始化列表补 `cipher: None, key,`

// New Add ↓（新增私有函数，放在 DecryptReader impl 附近）
/// Map an on-disk frame type byte to the cipher that must decrypt it.
/// 0x03/0x04 construct the vendored Aes256GcmDemo; the plain AES frame
/// types construct the existing aes_gcm::Aes256Gcm.
fn cipher_for_type(typ: u8, key: [u8; 32]) -> std::io::Result<EncryptCipher> {
    match typ {
        FRAME_TYPE_V1 | FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => {
            Ok(EncryptCipher::Aes256Gcm(Aes256Gcm::new_from_slice(&key).expect("key")))
        }
        FRAME_TYPE_AES256GCMDEMO_V2 | FRAME_TYPE_AES256GCMDEMO_V2_FINAL => {
            Ok(EncryptCipher::Aes256GcmDemo(rustfs_aes256_gcm_demo::Aes256GcmDemo::new_from_key(&key)))
        }
        other => Err(Error::other(format!("unknown encrypted frame type {other:#04x}"))),
    }
}
// New Add ↑
```

**改动点 F：`frame_version` match 扩展**（`crates/rio/src/encrypt_reader.rs`，`poll_read` :633-642）。`0x03/0x04` 归入 version 2（复用现有 v2 逻辑：AAD 认证、nonce 派生、final 帧检测全部继承）：

**原代码**（:633-642）：

```rust
let frame_version = match typ {
    FRAME_TYPE_V1 => 1,
    FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => 2,
    other => {
        return Poll::Ready(Err(Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown encrypted frame type {other:#04x}"),
        )));
    }
};
```

**改后**（`// New Add` 为修改行；仅 :635 一行扩展）：

```rust
let frame_version = match typ {
    FRAME_TYPE_V1 => 1,
    // New Add: :635 原 `FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL => 2,` 加入 0x03/0x04
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

**改动点 G：首个帧初始化 cipher + 后续帧一致性检查**（`crates/rio/src/encrypt_reader.rs`，`poll_read`，在 :662 `*this.current_frame_type = typ;` 之前插入）：

**原代码**（:659-662）：

```rust
if frame_version == 2 {
    *this.stream_saw_v2 = true;
}
*this.current_frame_type = typ;
```

**改后**（`// New Add` 为新增行；插入在 :660 与 :662 之间）：

```rust
if frame_version == 2 {
    *this.stream_saw_v2 = true;
}
// New Add ↓（首个帧确定 cipher；后续帧做算法一致性检查，0x01/0x03 混用报错）
if this.cipher.is_none() {
    *this.cipher = Some(cipher_for_type(typ, *this.key)?);
} else if this.cipher.as_ref().unwrap().cipher_token() != cipher_token_for_type(typ) {
    return Poll::Ready(Err(Error::new(
        std::io::ErrorKind::InvalidData,
        "encrypted segment mixes cipher algorithms",
    )));
}
// New Add ↑
*this.current_frame_type = typ;
```

其中 `cipher_token()` 是 `EncryptCipher` 实例上的 1 字节标记方法（Aes256Gcm=0 / Aes256GcmDemo=1，改动点 A），`cipher_token_for_type(typ)` 是按帧类型字节映射同一标记的辅助函数（与 `cipher_for_type` 同构，只取 token 不构造 cipher），做法仿 rio-v2:658-665 的 `cipher.cipher_id() != header[1]` 检查。

**改动点 H：解密调用点路由**（`crates/rio/src/encrypt_reader.rs`，`poll_read`，v2 路径 :734-747 + v1 回退路径 :748-789）。`this.cipher` 改为 `Option` 展开后按枚举变体调用 decrypt；v1 非认证回退路径同样改走 `.as_ref().expect(...)`——不变量是首个帧（含 v1 首帧）已在改动点 G 处初始化 cipher，故解密时恒为 `Some`，v1 回退路径的变体只可能是 Aes256Gcm（`FRAME_TYPE_V1` 不会映射到 demo 类型）：

**原代码**（v2 路径 :734-747）：

```rust
let plaintext = if *this.current_frame_type != FRAME_TYPE_V1 {
    // v2: the header and frame index are associated data, the nonce
    // derivation is exactly the modern scheme, and there are no
    // legacy fallbacks — any mismatch is tampering, not history.
    let aad = v2_frame_aad(this.header_buf, *this.block_index);
    this.cipher
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| Error::new(std::io::ErrorKind::InvalidData, "v2 encrypted frame failed authentication"))?
} else {
    // ...v1 回退路径 :748-789（关键调用 :774 `match this.cipher.decrypt(candidate_nonce, ciphertext)`）...
};
```

**改后**（`// New Add` 为新增/修改行；v2 路径 :739-747 显式匹配；v1 路径 :774 同样展开）：

```rust
let plaintext = if *this.current_frame_type != FRAME_TYPE_V1 {
    let aad = v2_frame_aad(this.header_buf, *this.block_index);
    // New Add: :739-747 原 `this.cipher.decrypt(...)` → 展开 Option + 按变体路由
    // None 不可能到达这里：首个 v2 帧已在上方（改动点 G）初始化 cipher
    let cipher = this.cipher.as_ref().expect("cipher initialized on first frame");
    let decrypted = match cipher {
        EncryptCipher::Aes256Gcm(c) => c.decrypt(&nonce, Payload { msg: ciphertext, aad: &aad }),
        EncryptCipher::Aes256GcmDemo(c) => c.decrypt(&nonce, Payload { msg: ciphertext, aad: &aad }),
    };
    decrypted.map_err(|_| Error::new(std::io::ErrorKind::InvalidData, "v2 encrypted frame failed authentication"))?
} else {
    // ...v1 回退路径 :748-789 其余不变；
    // New Add: :774 原 `match this.cipher.decrypt(candidate_nonce, ciphertext)` →
    // let cipher = this.cipher.as_ref().expect("cipher initialized on first frame");
    // match cipher {
    //     EncryptCipher::Aes256Gcm(c) => c.decrypt(candidate_nonce, ciphertext),
    //     EncryptCipher::Aes256GcmDemo(_) => unreachable!("FRAME_TYPE_V1 never maps to Aes256GcmDemo"),
    // }
    // （v1 首帧已在改动点 G 初始化，恒 Some；demo 变体不可达——0x00 不映射 demo）
    // ...
};
```

Aes256GcmDemo 按 0.5-E 契约实现 `aead::Aead`，decrypt 调用与 Aes256Gcm 完全同构；此处保留显式 match 而非合并——未来换成真算法时只需改两个分支体、不动 dispatch 骨架，若 vendor API 偏离 `aead::Aead`，适配层也加在这里。

### 3.4 Aes256GcmDemo 算法接入接口要求与限制（规格 + 伪代码 demo）

本节把前文散落在 0.5-E / §2 / §3.1-3.3 的接口与行为约束汇总成一份**新增算法接入规格**：以 Aes256GcmDemo 为例，任何新算法（含第 4 节未来的 ChaCha20-Poly1305）接入时都须逐条满足 A-D——即第 4 节六步的验收标准，全部满足才可提交。

**A. 类型/接口要求（vendor crate 侧，编译期锁定，见 改动点 0.5-E）**

| # | 要求 | 取值/形态 | 锁定方式 | 违反后果 |
|---|---|---|---|---|
| 1 | 实现 AEAD trait | `aead::AeadCore` + `aead::Aead`（aead 0.6.1，Cargo.lock） | 编译期（impl 块） | 编译失败——调用点（改动点 C/H）是既有 trait 方法，零适配层不成立 |
| 2 | nonce 长度 | 12 字节（= AES-256-GCM） | `NonceSize` 关联类型 + `size_of` 断言（第 7 节） | 帧内 nonce 窗口错位 → 闭式偏移映射/seek/压缩索引破坏 |
| 3 | tag 长度 | 16 字节（= AES-256-GCM） | `TagSize` 关联类型 + `size_of` 断言 | 帧长字段/tag 位置错位 → 同上 |
| 4 | tag 位置 | Postfix（密文尾部） | `TAG_POSITION = TagPosition::Postfix` | 与 `build_frame` 的 clen 预计算（:137-149）不符 |
| 5 | 加解密签名 | `encrypt(&self, &Nonce<Self>, impl Into<Payload>) -> Result<Vec<u8>>` / `decrypt(...)`，认证失败返回 `Err`（不 panic、不吞错误） | 签名实现 + 测试断言 | 调用点需加适配层（0.5-E 末句）或读端无法 fail-closed |
| 6 | 短 key 算法（16B） | 到 32B 的派生放在 `EncryptCipher` 变体构造处（cipher 边界，第 4 节） | 约定 | 结构体 `[u8; 32]` 字段假设被破坏 |

**B. 写端行为限制（crates/rio 侧，改动点 B/C/D）**

- 只写认证 v2 帧：类型字节恒为 0x03（非末帧）/ 0x04（末帧），`frame_v2 = true`；**无 v1 对应物**——demo 不写 0x00 无认证帧（否则旧版本节点会按 v1 路径误读）；
- 帧参数与 AES-GCM v2 全等继承：nonce 派生（`derive_block_nonce`，窗口 [8..12)）、AAD（8B 帧头 `‖` u64le block_index，`v2_frame_aad` :51-56）、定长帧缓冲/末帧/END 逻辑——即"demo = v2 布局 + Aes256GcmDemo 原语 + 0x03/0x04 帧类型"（3.2 节对照要点）；
- 构造器命名对齐 `new_*_v2_with_<algorithm>`，供 `WritePlan::apply` 按 `encryption_cipher()` 分派（第 2 节）；配置层（ecstore）与 I/O 层（crates/rio）的枚举**不可互相引用**。

**C. 读端行为约束（crates/rio 侧，改动点 E/F/G/H）**

- 只认 on-disk 帧类型字节：`cipher_for_type` 把 0x00/0x01/0x02 → Aes256Gcm、0x03/0x04 → Aes256GcmDemo；**永不读 env**——配置变更不影响已落盘数据（第 0 节兼容性核心保证）；
- 未知帧类型 → `InvalidData` **fail-closed**（0x05 等不静默，旧版本节点读新算法对象时报错而非损坏）；
- 首个帧锁定段内 cipher；段内混用（0x01+0x03）→ `"mixes cipher algorithms"` 报错（改动点 G，仿 rio-v2:658-665 的 cipher_id 检查）；
- 认证失败 → `InvalidData`，**不降级到 v1 回退路径**——v2 是认证帧，失败即篡改/损坏；:748-789 的 3 种 nonce 布局回退只属于 0x00 帧、且只可能命中 Aes256Gcm 变体（改动点 H）。

**D. 接口函数声明与参数描述（Aes256GcmDemo 特有，基于 A 的要求）**

A 表要求的具体落地形态——Aes256GcmDemo 的全部对外接口声明如下（函数签名即约束；参数描述即用途说明）。

```rust
// ── D-1. vendor crate 公开接口（vendor/rustfs-aes256-gcm-demo/src/lib.rs，落实 A.1-A.6）──

/// 用 32 字节密钥构造 Aes256GcmDemo 实例。
/// # 参数
/// - `key`：32 字节对称密钥（写端来自 `EncryptionMaterial.key_bytes`），加解密共用同一把；
///   16B key 的算法须在 `EncryptCipher` 变体构造处派生为 32B 再传入（A.6）。
/// # 说明
/// 定长引用 `&[u8; 32]` 在类型层面排除长度错误，`expect("32-byte key")` 的 panic 路径不可达。
pub fn new_from_key(key: &[u8; 32]) -> Self;

impl AeadCore for Aes256GcmDemo {
    // 关联类型在编译期锁定帧参数契约（A.2/A.3/A.4）——闭式帧长、nonce 窗口、seek 映射的前提
    type NonceSize = <Aes256Gcm as AeadCore>::NonceSize;  // 12B，帧内 nonce 窗口 [8..12) 派生
    type TagSize = <Aes256Gcm as AeadCore>::TagSize;      // 16B，build_frame 闭式帧长 clen 依赖
    const TAG_POSITION: TagPosition = TagPosition::Postfix; // tag 追加于密文尾部（A.4）
}

impl Aead for Aes256GcmDemo {
    /// 加密单帧：输出 = 密文 ‖ 16B tag。
    /// # 参数
    /// - `nonce`：12B 一次性 nonce，每帧唯一（写端 `derive_block_nonce(base_nonce, block_index)` 派生）
    /// - `plaintext`：明文字节（实现了 `Into<Payload>`，含可选 AAD）
    ///   - `Payload::msg`：单帧明文，非末帧恒为定长 `ENCRYPTION_BLOCK_SIZE`（定长是闭式偏移的前提），末帧可短
    ///   - `Payload::aad`：v2 帧为 8B 帧头 ‖ u64le block_index（`v2_frame_aad`），仅参与认证不参与加密
    /// # 返回
    /// - `Ok(密文 ‖ tag)`：成功
    /// - `Err`：AEAD 内部失败（写路径一般不发生，调用方按 `Error::other` 包装，A.5）
    fn encrypt<'msg, 'aad>(&self, nonce: &Nonce<Self>, plaintext: impl Into<Payload<'msg, 'aad>>) -> aes_gcm::aead::Result<Vec<u8>>;

    /// 解密单帧：校验 tag（含 AAD）通过后返回明文。
    /// # 参数
    /// - `nonce`：与加密时一致的 12B nonce（读端 `derive_block_nonce(current_nonce_base, block_index)`，逐帧必须一致）
    /// - `ciphertext`：`Payload::msg` 为 密文 ‖ 16B tag；`Payload::aad` 为 8B 帧头 ‖ u64le block_index
    /// # 返回
    /// - `Ok(明文)`：认证通过
    /// - `Err`：认证失败（篡改/错误 key/nonce 漂移/AAD 不匹配）——调用方必须 fail-closed（改动点 H，不得降级 v1，A.5/C）
    fn decrypt<'msg, 'aad>(&self, nonce: &Nonce<Self>, ciphertext: impl Into<Payload<'msg, 'aad>>) -> aes_gcm::aead::Result<Vec<u8>>;
}
```

```rust
// ── D-2. crates/rio 集成入口（encrypt_reader.rs，Aes256GcmDemo 特有部分）──

impl<R> EncryptReader<R> {
    /// Aes256GcmDemo 单段 v2 写构造器（帧类型 0x03/0x04，`frame_v2 = true`）。
    /// §2 中 `WritePlan::apply` 在 `encryption_cipher() == Aes256GcmDemo` 时选择本构造器。
    /// # 参数
    /// - `inner`：上游明文 reader（约束 `AsyncRead + Unpin + Send + Sync`）
    /// - `key`：32B 对称密钥（`EncryptionMaterial.key_bytes`）
    /// - `nonce`：12B base nonce（`EncryptionMaterial.base_nonce`；后续帧 nonce 由它逐块派生）
    /// # 行为
    /// 与 `new_v2` 同构：`Self::new` + `frame_v2 = true` + 覆写 cipher 为 demo 变体（3.2 对照要点）。
    pub fn new_v2_with_aes256_gcm_demo(inner: R, key: [u8; 32], nonce: [u8; 12]) -> Self;

    /// Aes256GcmDemo multipart v2 写构造器（0x03/0x04）。
    /// # 参数
    /// - `base_nonce`：12B part 级 base nonce
    /// - `part_number`：part 序号，经既有 `multipart_part_nonce(base_nonce, part_number)` 派生
    ///   每 part 的独立 base（语义与现有 `new_multipart_v2` 参数一致，只换 cipher 与帧类型）
    pub fn new_multipart_v2_with_aes256_gcm_demo(
        inner: R,
        key: [u8; 32],
        base_nonce: [u8; 12],
        part_number: usize,
    ) -> Self;
}

/// 读端帧类型 → cipher 派生态（改动点 E，模块内私有）。
/// # 参数
/// - `typ`：on-disk 帧类型字节；0x00/0x01/0x02 → `Aes256Gcm`，0x03/0x04 → `Aes256GcmDemo`
/// - `key`：32B 对称密钥（来自 `DecryptReader.key` 新字段），用于构造对应算法实例
/// # 返回
/// - `Ok(EncryptCipher)`：对应算法的包装实例
/// - `Err`：未知帧类型——调用方转 `InvalidData` 报错（fail-closed，C 要求）
fn cipher_for_type(typ: u8, key: [u8; 32]) -> std::io::Result<EncryptCipher>;

impl EncryptCipher {
    /// 实例侧 1 字节算法标记（Aes256Gcm=0 / Aes256GcmDemo=1），段内混用检查用（改动点 G）
    fn cipher_token(&self) -> u8;
}
/// 帧类型字节 → 同一标记（与 `cipher_for_type` 同构，只取标记、不构造 cipher；改动点 G）
fn cipher_token_for_type(typ: u8) -> u8;
```

**E. 伪代码 demo（写端 + 读端数据流）**

```rust
// 伪代码 demo（非真实实现，表达式为示意）——写端：
// §2 WritePlan::apply 构造器分派 → EncryptReader::poll_read v2 帧构建（改动点 B/D）
fn write_object(reader, key[32], base_nonce[12], env_cipher) {
    // 1) 配置→I/O 层翻译点（§2）：按配置枚举选构造器；demo 恒选中 v2 demo 构造器
    enc = match env_cipher {
        Aes256Gcm     => if frame_v2 { EncryptReader::new_v2(key, base_nonce) }        // 0x01/0x02
                         else        { EncryptReader::new(key, base_nonce) },          // 0x00
        Aes256GcmDemo => EncryptReader::new_v2_with_aes256_gcm_demo(key, base_nonce),  // 恒 0x03/0x04
    }
    // 2) poll_read v2 路径：逐块构建帧
    while !eof {
        block    = read(ENCRYPTION_BLOCK_SIZE)
        is_final = input_done && len(block) < ENCRYPTION_BLOCK_SIZE
        // 改动点 D：demo 写 0x03/0x04，AES 写 0x01/0x02
        type_byte = is_final ? FRAME_TYPE_AES256GCMDEMO_V2_FINAL       // 0x04
                             : FRAME_TYPE_AES256GCMDEMO_V2            // 0x03
        header = build_header(type_byte, clen(block), crc32(block))   // 8B 帧头；长度字段按 build_frame :137-149 原式
        aad    = v2_frame_aad(&header, block_index)                   // 8B 头 ‖ u64le 块序号
        nonce  = derive_block_nonce(base_nonce, block_index)          // 窗口 [8..12) BE wrapping
        ciphertext = demo.encrypt(&nonce, Payload { msg: block, aad: &aad })  // 12B nonce + 16B tag（Postfix）
        emit(header ++ ciphertext)
        block_index += 1
    }
    emit(FRAME_TYPE_END)   // 0xFF 段结束标记（无认证、仅定界，与 AES 共用）
}
```

```rust
// 伪代码 demo（非真实实现，表达式为示意）——读端：
// DecryptReader::poll_read（改动点 E/F/G/H），只按 on-disk 帧类型字节 dispatch，不读 env
fn read_object(stream, key[32], nonce_base[12]) {
    while true {
        typ = read_u8()
        if typ == FRAME_TYPE_END { break }   // 0xFF 段结束
        // F：帧版本归并（0x03/0x04 并入 v2，AAD 认证/nonce 派生/final 检测全继承）
        version = match typ {
            FRAME_TYPE_V1 => 1,              // 0x00 无认证帧
            FRAME_TYPE_V2 | FRAME_TYPE_V2_FINAL
            | FRAME_TYPE_AES256GCMDEMO_V2 | FRAME_TYPE_AES256GCMDEMO_V2_FINAL => 2,
            other => return InvalidData("unknown encrypted frame type {other:#04x}")  // fail-closed
        }
        // G：首帧确定 cipher，后续帧做算法一致性检查（0x01+0x03 混用报错）
        if segment.first_frame {
            cipher = cipher_for_type(typ, key)  // 0x00/01/02 → Aes256Gcm；0x03/04 → Aes256GcmDemo
        } else if cipher.cipher_token() != cipher_token_for_type(typ) {
            return InvalidData("encrypted segment mixes cipher algorithms")
        }
        (header, ciphertext) = read_frame_body()  // 8B 头 + uvarint 明文长 + 密文（含 16B tag）
        if version == 2 {
            nonce     = derive_block_nonce(nonce_base, block_index)
            aad       = v2_frame_aad(&header, block_index)
            plaintext = cipher.decrypt(&nonce, Payload { msg: ciphertext, aad: &aad })
                            .or(InvalidData("v2 encrypted frame failed authentication"))  // 认证失败不降级 v1（H）
        } else {
            plaintext = legacy_v1_decrypt(stream_state, ciphertext)   // 3 种 nonce 布局回退（:748-789），仅 Aes256Gcm
        }
        block_index += 1
    }
}
```

**F. 验收映射**：A-E 每条都能对到第 7 节测试用例——A1-A5→vendor crate 契约测试（`NonceSize`/`TagSize` size_of 断言、roundtrip、篡改 tag/错 AAD）；B→`encrypt_aes256_gcm_demo_v2_roundtrip`（产物首字节 0x03、末帧前 0x04）；C→`segment_mixing_aes_and_demo_fails`、`v1_aes_still_reads`、读端不读 env 回归；D 为接口声明（编译期锁定，由 A 的测试覆盖）；E 是数据流示意，无独立测试。

---

## 4. 新增算法引入配置方案（如何加第 3 个真实算法）

Aes256GcmDemo 是插桩示例，验证了整套扩展骨架。后续加一个真实 Rust AEAD 算法（如 `chacha20-poly1305`、`aes-256-gcm-siv`），固定六步：

1. **依赖**：两种引入方式，按算法来源二选一——(a) 自制/内购或需 vendor 的算法：仿第 0.5 节 Aes256GcmDemo 的 vendor 模式，三处配置（workspace members + `[workspace.dependencies]` path 依赖 + `crates/rio/Cargo.toml` 一行）；(b) crates.io 公开算法：workspace 根 `[workspace.dependencies]` 加算法 crate（`chacha20poly1305 = 0.11.0` 已在 workspace，见 Cargo.toml:208），`crates/rio/Cargo.toml` 加一行 `chacha20poly1305 = { workspace = true }`
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

**bucket 级算法**：`BucketMetadata` 手写 msgpack 序列化（metadata.rs `decode_from` :525 / `encode_to` :605），可加 `cipher: Option<EncryptionCipher>` 字段（缺省 None）；但写路径取 cipher 需把值从 sse.rs 的 `EncryptionMaterial` 一路传到 `WriteEncryption`（约 6 处调用点），且 bucket SSE XML DTO 是严格 round-trip 类型不能污染 → 改动用例更多。若未来需要，建议作为独立版本演进，勿与全局 env 混用优先级。

---

## 7. 测试计划

- vendor crate 侧（`vendor/rustfs-aes256-gcm-demo/` 自带测试，**参数契约锁定**）：对 `aead::AeadCore` / `aead::Aead` 的实现满足 0.5-E 契约——`NonceSize`/`TagSize` 关联类型类型级锁 12B nonce / 16B tag（编译期 `size_of` 断言），回环 roundtrip + 篡改 tag / 错 AAD 报认证错误——确保第 3.1 节"闭式偏移映射、seek、压缩索引不受影响"的帧布局假设成立
- crates/rio 单测（encrypt_reader.rs `#[cfg(test)]` 模块，:980 附近 helpers 可复用）：
  - `encrypt_aes256_gcm_demo_v2_roundtrip`：`new_v2_with_aes256_gcm_demo` 写 → `DecryptReader` 读，断言明文一致 + 产物首字节为 `0x03`、末帧前为 `0x04`
  - `decrypt_accepts_aes256_gcm_demo_v2`：手工构造 0x03/0x04 帧流 → `DecryptReader` 成功解密（仿 rio-v2:786-819 已有测试）
  - `segment_mixing_aes_and_demo_fails`：同一段混 0x01 与 0x03 → 报 "mixes cipher algorithms"
  - `v1_aes_still_reads`：既有 v1 帧流回归（确保 `cipher` 类型化未破坏旧路径）
- crates/ecstore：`temp_env::async_with_vars` 设 `RUSTFS_ENCRYPTION_CIPHER=aes256-gcm-demo` 后 `WritePlan.apply`（io_support/rio.rs tests 模块，仿 rustfs/src/app/object/put.rs:2325 既有模式）→ 产物帧类型断言 + 解密 roundtrip；读端不读 env 的回归：demo env 下读 AES 对象仍成功

---

## 8. 改动清单汇总（最小 diff，零新增代码文件）

| 文件 | 改动 | 是否新增依赖 |
|---|---|---|
| `crates/config/src/constants/env.rs` | **现有文件内追加** 2 个常量：`ENV_RUSTFS_ENCRYPTION_CIPHER` / `DEFAULT_RUSTFS_ENCRYPTION_CIPHER`（:60 后，1.2 节）；`mod.rs:22` / `lib.rs:32` 已有 env 挂接，**零改动** | 否 |
| `crates/rio/src/encrypt_reader.rs` | 帧类型常量 +2（0x03/0x04，:47 前）；`EncryptCipher` enum；`EncryptReader.cipher` 类型化（:64）+ `new_v2_with_aes256_gcm_demo` / `new_multipart_v2_with_aes256_gcm_demo`（3.2 节）；`build_frame` cipher 参数化（:124 + :151-164）；poll_read type-byte 路由（:220）；`DecryptReader.cipher` → `Option<EncryptCipher>`（:385）+ `key` 字段 + `cipher_for_type` + 混用检查（:662）+ 解密调用路由（:739-747/:774） | 是：+`rustfs-aes256-gcm-demo`（见下行） |
| `crates/ecstore/src/io_support/rio.rs` | `EncryptionCipher` enum + `encryption_cipher()`（OnceLock 缓存，:347 后）；`WritePlan.apply` 4 处构造器选择（:446-493） | 否 |
| `Cargo.toml`（workspace 根） | members + `[workspace.dependencies]` 各 +1 行（0.5-A/B，path 指向 vendor；**已实现**） | 是：引入 `vendor/rustfs-aes256-gcm-demo` |
| `crates/rio/Cargo.toml` | +`rustfs-aes256-gcm-demo = { workspace = true }`（0.5-C；**已实现**） | 是 |
| `vendor/rustfs-aes256-gcm-demo/`（新目录，**已实现**） | 新增 crate：`Cargo.toml`（依赖 workspace `aes-gcm` 取 `aead` 类型）+ `src/lib.rs`（0.5-E 契约）+ 参数契约测试（第 7 节） | —（新 crate） |
| `.gitignore` | **定案 D1**（**已实现**）：:19 裸 `vendor` 改为 `vendor/*` + `!vendor/rustfs-aes256-gcm-demo/`（0.5-D） | 否 |

仓库内代码文件共 **3 个**（`env.rs`、`encrypt_reader.rs`、`io_support/rio.rs`），**全部现有文件、零新增**；`mod.rs` / `lib.rs` 零改动（env 已挂接，1.2 节）；上层（sse.rs、put.rs、copy.rs、extract.rs、multipart_usecase.rs、readers.rs）**零改动**。

## 9. 本次适配相对原方案的差异（Aes256GcmDemo 示例）

| 项 | 原方案（ChaCha20-Poly1305） | 本版（Aes256GcmDemo） |
|---|---|---|
| 依赖变更 | +`chacha20poly1305` 到 crates/rio | vendor path 依赖引入 `rustfs-aes256-gcm-demo`（workspace members + `[workspace.dependencies]` + crates/rio 三处，0.5-A/B/C）+ 新增 `vendor/rustfs-aes256-gcm-demo/` crate |
| 帧类型 | 0x03/0x04 | 0x03/0x04（复用，值域不同语义） |
| env 值 | `chacha20-poly1305` | `aes256-gcm-demo` |
| 写端构造器 | `new_v2_with_chacha20_poly1305` | `new_v2_with_aes256_gcm_demo` / `new_multipart_v2_with_aes256_gcm_demo` |
| 读端原语 | ChaCha20Poly1305 | vendored `Aes256GcmDemo`（独立类型/原语，对 `aead::AeadCore` / `aead::Aead` 的实现与 AES 同构，独立 cipher token） |
| 新增配置常量 | 直接写 io_support 本地 | **追加到现有 crates/config/src/constants/env.rs**（合规：常量仓库统一管理；零新增文件，`mod.rs`/`lib.rs` 零改动） |

变更一处的代表性收益：Aes256GcmDemo 作为独立 vendor crate 演示完整接线——写端按 env 选算法 → 帧类型落盘 → 读端按帧类型还原算法，从依赖引入到 I/O 帧格式全链路独立命名；demo 实现仅复刻 AES-256-GCM 参数（12B nonce + 16B tag），帧布局闭式假设不变、无安全审查负担；生产换真算法时按第 4 节六步逐点替换（vendor 模式或 registry 模式二选一）。