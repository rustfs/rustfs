# RustFS Key Management Service (KMS)

RustFS KMS 是一个为 RustFS 项目设计的企业级密钥管理服务，提供安全的密钥存储、轮换、审计和访问控制功能。它支持多种后端实现，包括通过 `rusty_vault` 集成 HashiCorp Vault。

## ✨ 功能特性

- 🔐 **统一的密钥管理接口** - 支持多种 KMS 后端的抽象接口
- 🏦 **HashiCorp Vault 集成** - 通过 `rusty_vault` 支持企业级 Vault 功能
- 📁 **本地文件系统 KMS** - 用于开发和测试的简单实现
- 🔄 **密钥轮换** - 支持定期密钥轮换和版本管理
- 📊 **审计日志** - 完整的操作审计和追踪
- 🛡️ **访问控制** - 细粒度的权限控制和认证
- ⚡ **异步支持** - 完全异步的 API 设计
- 🔧 **灵活配置** - 支持配置文件和环境变量配置

## 📖 参考实现

为了深入理解行业标准的桶加密模式，我们提供了 [MinIO 桶加密实现分析](docs/minio-bucket-encryption-analysis.md)，其中包含了 MinIO 实现的详细分析和 RustFS 集成建议。这份文档展示了：

- MinIO 的多层次桶加密架构
- 三层密钥管理机制（Master Key → Object Encryption Key → Sealed Key）
- 配置管理和应用流程
- 对 RustFS 实现的具体建议和代码示例

## 🚀 快速开始

### 添加依赖

在您的 `Cargo.toml` 中添加：

```toml
[dependencies]
rustfs-kms = { path = "../kms" }

# 如果需要 Vault 支持
rustfs-kms = { path = "../kms", features = ["vault"] }
```

### MinIO 兼容的 Admin API

RustFS 提供了与 MinIO 完全兼容的 KMS 管理 API，支持所有标准的密钥管理操作：

#### 支持的端点

| 操作 | 方法 | 端点 | 描述 |
|-----|------|------|------|
| 创建密钥 | POST | `/rustfs/admin/v3/kms/key/create?keyName=<name>` | 创建新的主密钥 |
| 查询密钥状态 | GET | `/rustfs/admin/v3/kms/key/status?keyName=<name>` | 获取密钥详细信息 |
| 列出所有密钥 | GET | `/rustfs/admin/v3/kms/key/list` | 列出所有可用密钥 |
| 启用密钥 | PUT | `/rustfs/admin/v3/kms/key/enable?keyName=<name>` | 启用指定密钥 |
| 禁用密钥 | PUT | `/rustfs/admin/v3/kms/key/disable?keyName=<name>` | 禁用指定密钥 |
| KMS 状态 | GET | `/rustfs/admin/v3/kms/status` | 检查 KMS 健康状态 |

#### 使用示例

```bash
# 检查 KMS 状态
curl -X GET http://localhost:9000/rustfs/admin/v3/kms/status

# 创建新的主密钥
curl -X POST http://localhost:9000/rustfs/admin/v3/kms/key/create?keyName=my-master-key

# 列出所有密钥
curl -X GET http://localhost:9000/rustfs/admin/v3/kms/key/list

# 查询密钥状态
curl -X GET "http://localhost:9000/rustfs/admin/v3/kms/key/status?keyName=my-master-key"

# 禁用密钥
curl -X PUT "http://localhost:9000/rustfs/admin/v3/kms/key/disable?keyName=my-master-key"

# 启用密钥
curl -X PUT "http://localhost:9000/rustfs/admin/v3/kms/key/enable?keyName=my-master-key"
```

#### 与 MinIO MC 客户端兼容

RustFS 的 KMS API 与 MinIO 的 `mc admin kms` 命令完全兼容：

```bash
# 配置 mc 客户端指向 RustFS
mc alias set rustfs http://localhost:9000 <access-key> <secret-key>

# 创建密钥（即将支持）
mc admin kms key create rustfs my-master-key

# 查询密钥状态（即将支持）
mc admin kms key status rustfs my-master-key

# 列出密钥（即将支持）
mc admin kms key list rustfs
```

### 基本使用

```rust
use rustfs_kms::{KmsConfig, KmsManager, GenerateKeyRequest};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建本地 KMS（用于开发）
    let config = KmsConfig::local(PathBuf::from("./keys"));
    let kms = KmsManager::new(config).await?;

    // 创建主密钥
    let master_key = kms.create_key("my-master-key", "AES-256", None).await?;
    println!("Created master key: {}", master_key.key_id);

    // 生成数据加密密钥
    let dek_request = GenerateKeyRequest::new(
        "my-master-key".to_string(),
        "AES_256".to_string()
    );
    let data_key = kms.generate_data_key(&dek_request, None).await?;
    println!("Generated data key with {} bytes", data_key.ciphertext.len());

    Ok(())
}
```

### 全局 KMS 管理

对于应用程序级别的集成，KMS crate 提供了全局实例管理功能：

```rust
use rustfs_kms::{KmsConfig, KmsManager, init_global_kms, get_global_kms, is_kms_healthy};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化全局 KMS
    let config = KmsConfig::from_env()?;
    let kms_manager = KmsManager::new(config).await?;
    init_global_kms(Arc::new(kms_manager))?;

    // 在应用程序的任何地方使用全局 KMS
    if let Some(kms) = get_global_kms() {
        let health = kms.health_check(None).await?;
        println!("KMS is healthy: {}", health.is_healthy);
    }

    // 检查 KMS 健康状态
    if is_kms_healthy().await {
        println!("Global KMS is ready to use");
    }

    Ok(())
}
```

## 🏗️ 配置说明

### 环境变量配置

```bash
# KMS 类型选择
export RUSTFS_KMS_TYPE=vault  # 可选: vault, local, aws, azure, gcp

# Vault 配置
export RUSTFS_KMS_VAULT_ADDRESS=http://localhost:8200
export RUSTFS_KMS_VAULT_TOKEN=your-vault-token
export RUSTFS_KMS_VAULT_NAMESPACE=your-namespace  # 可选，用于 Vault Enterprise

# 本地 KMS 配置
export RUSTFS_KMS_LOCAL_KEY_DIR=/path/to/keys
export RUSTFS_KMS_LOCAL_MASTER_KEY=your-master-key

# 通用配置
export RUSTFS_KMS_DEFAULT_KEY_ID=default-key
export RUSTFS_KMS_TIMEOUT_SECS=30
export RUSTFS_KMS_RETRY_ATTEMPTS=3
```

### 程序配置

```rust
use rustfs_kms::{KmsConfig, KmsType};
use url::Url;

// Vault 配置
let vault_config = KmsConfig::vault(
    Url::parse("https://vault.example.com")?,
    "your-vault-token".to_string(),
);

// 本地配置
let local_config = KmsConfig::local(PathBuf::from("./keys"));

// 从环境变量加载
let env_config = KmsConfig::from_env()?;
```

## 🔧 HashiCorp Vault 集成

### Vault 服务器设置

1. **安装 Vault**
   ```bash
   # 使用包管理器安装
   brew install vault  # macOS
   # 或下载二进制文件
   ```

2. **开发模式启动**
   ```bash
   vault server -dev
   ```

3. **生产模式配置**
   ```bash
   # 创建配置文件
   cat > vault.hcl <<EOF
   storage "file" {
     path = "/opt/vault/data"
   }
   
   listener "tcp" {
     address = "0.0.0.0:8200"
     tls_disable = 1
   }
   
   api_addr = "http://127.0.0.1:8200"
   cluster_addr = "https://127.0.0.1:8201"
   ui = true
   EOF
   
   # 启动 Vault
   vault server -config=vault.hcl
   ```

### 认证配置

RustFS KMS 支持多种 Vault 认证方式：

```rust
use rustfs_kms::config::{VaultConfig, VaultAuthMethod};

// Token 认证
let auth = VaultAuthMethod::Token {
    token: "your-token".to_string(),
};

// AppRole 认证
let auth = VaultAuthMethod::AppRole {
    role_id: "your-role-id".to_string(),
    secret_id: "your-secret-id".to_string(),
};

// Kubernetes 认证
let auth = VaultAuthMethod::Kubernetes {
    role: "your-k8s-role".to_string(),
    jwt_path: PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount/token"),
};
```

## 🔐 密钥管理操作

### 创建和管理密钥

```rust
// 创建主密钥
let master_key = kms.create_key("production-key", "AES-256", None).await?;

// 获取密钥信息
let key_info = kms.describe_key("production-key", None).await?;

// 列出所有密钥
let list_request = ListKeysRequest::default();
let keys = kms.list_keys(&list_request, None).await?;

// 禁用密钥
kms.disable_key("old-key", None).await?;

// 轮换密钥
let rotated_key = kms.rotate_key("production-key", None).await?;
```

### 数据加密和解密

```rust
// 直接加密数据
let encrypt_request = EncryptRequest::new(
    "production-key".to_string(),
    b"sensitive data".to_vec()
);
let encrypted = kms.encrypt(&encrypt_request, None).await?;

// 解密数据
let decrypt_request = DecryptRequest::new(encrypted.ciphertext);
let decrypted = kms.decrypt(&decrypt_request, None).await?;

// 生成数据密钥（推荐用于大文件）
let dek_request = GenerateKeyRequest::new(
    "production-key".to_string(),
    "AES_256".to_string()
);
let data_key = kms.generate_data_key(&dek_request, None).await?;

// 使用生成的数据密钥加密大文件
// 然后存储加密的数据密钥和加密的文件
```

## 🛡️ 安全最佳实践

### 密钥轮换策略

```rust
// 定期轮换密钥
async fn rotate_keys_periodically(kms: &KmsManager) -> Result<(), KmsError> {
    let keys = kms.list_keys(&ListKeysRequest::default(), None).await?;
    
    for key in keys.keys {
        if should_rotate(&key) {
            kms.rotate_key(&key.key_id, None).await?;
            println!("Rotated key: {}", key.key_id);
        }
    }
    
    Ok(())
}

fn should_rotate(key: &KeyInfo) -> bool {
    // 实现您的轮换策略
    // 例如：90天轮换一次
    if let Some(rotated_at) = key.rotated_at {
        rotated_at.elapsed().unwrap_or_default().as_secs() > 90 * 24 * 3600
    } else {
        key.created_at.elapsed().unwrap_or_default().as_secs() > 90 * 24 * 3600
    }
}
```

### 操作上下文和审计

```rust
use rustfs_kms::OperationContext;

// 创建操作上下文用于审计
let context = OperationContext::new("user@example.com".to_string())
    .with_source_ip("192.168.1.100".to_string())
    .with_user_agent("RustFS/1.0".to_string())
    .with_context("service".to_string(), "file-encryption".to_string());

// 在所有操作中使用上下文
let result = kms.create_key("audit-key", "AES-256", Some(&context)).await?;
```

## 🧪 测试

运行测试：

```bash
# 运行所有测试
cargo test

# 运行特定测试
cargo test test_local_kms_basic_operations

# 运行 Vault 集成测试（需要运行的 Vault 服务器）
cargo test --features vault vault_tests

# 运行示例
cargo run --example basic_usage
```

## 📊 监控和健康检查

```rust
// 健康检查
match kms.health_check().await {
    Ok(_) => println!("KMS is healthy"),
    Err(e) => println!("KMS health check failed: {}", e),
}

// 获取后端信息
let info = kms.backend_info();
println!("Backend: {} v{}", info.backend_type, info.version);
println!("Endpoint: {}", info.endpoint);
```

## 🔄 在 RustFS 中集成 KMS

在 RustFS 主服务中集成 KMS：

```rust
// 在 RustFS 配置中添加
use rustfs_kms::{KmsConfig, KmsManager};

pub struct RustFSConfig {
    // 其他配置...
    pub kms: Option<KmsConfig>,
}

// 在服务启动时初始化 KMS
pub async fn start_rustfs(config: RustFSConfig) -> Result<(), Error> {
    let kms = if let Some(kms_config) = config.kms {
        Some(KmsManager::new(kms_config).await?)
    } else {
        None
    };

    // 将 KMS 传递给存储层和加密服务
    let storage = StorageService::new(kms.clone());
    let crypto_service = CryptoService::new(kms);

    // 启动服务...
    Ok(())
}
```

## 🚀 生产部署建议

### Vault 生产配置

1. **启用 TLS**
   ```bash
   listener "tcp" {
     address = "0.0.0.0:8200"
     tls_cert_file = "/etc/vault/tls/vault.crt"
     tls_key_file = "/etc/vault/tls/vault.key"
   }
   ```

2. **使用外部存储**
   ```bash
   storage "consul" {
     address = "consul.service.consul:8500"
     path = "vault/"
   }
   ```

3. **配置高可用性**
   ```bash
   ha_storage "consul" {
     address = "consul.service.consul:8500"
     path = "vault/"
   }
   ```

### 密钥管理策略

- 🔄 **定期轮换**: 设置自动化密钥轮换流程
- 📊 **监控**: 监控 KMS 操作和性能指标
- 🔐 **备份**: 定期备份密钥和配置
- 🛡️ **访问控制**: 实施最小权限原则
- 📝 **审计**: 启用完整的操作审计日志

## 🤝 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feat/amazing-feature`)
3. 提交更改 (`git commit -m 'feat: add amazing feature'`)
4. 推送到分支 (`git push origin feat/amazing-feature`)
5. 创建 Pull Request

## 📄 许可证

本项目采用 Apache 2.0 许可证 - 查看 [LICENSE](../../LICENSE) 文件了解详情。

## 🧪 运行示例

### 基本 KMS 使用示例

```bash
cd crates/kms
cargo run --example basic_usage
```

### RustFS Admin API 示例

首先启动 RustFS 服务器（确保已配置 KMS），然后运行：

```bash
cd crates/kms
cargo run --example rustfs_admin_api
```

这个示例将演示：
- 检查 KMS 状态
- 列出现有密钥
- 创建新的主密钥
- 查询密钥状态
- 密钥生命周期管理（启用/禁用）

## 🔗 相关链接

- [RustFS 主项目](../../README.md)
- [MinIO KMS 文档](https://min.io/docs/minio/linux/reference/minio-mc-admin/mc-admin-kms-key.html)
- [HashiCorp Vault 文档](https://www.vaultproject.io/docs)
- [rusty_vault 项目](https://github.com/Tongsuo-Project/RustyVault)
- [Rust 异步编程](https://rust-lang.github.io/async-book/) 