# RustFS 文档中心

欢迎来到 RustFS 分布式文件系统文档中心！

## 📚 文档导航

### 🔐 KMS (密钥管理服务)

RustFS KMS 提供企业级密钥管理和数据加密服务。

| 文档 | 描述 | 适用场景 |
|------|------|----------|
| [KMS 使用指南](./kms/README.md) | 完整的 KMS 使用文档，包含快速开始、配置和部署 | 所有用户必读 |
| [HTTP API 接口](./kms/http-api.md) | HTTP REST API 接口文档和使用示例 | 管理员和运维 |
| [编程 API 接口](./kms/api.md) | Rust 库编程接口和代码示例 | 开发者集成 |
| [配置参考](./kms/configuration.md) | 完整的配置选项和环境变量说明 | 系统管理员 |
| [故障排除](./kms/troubleshooting.md) | 常见问题诊断和解决方案 | 运维人员 |
| [安全指南](./kms/security.md) | 安全最佳实践和合规指导 | 安全架构师 |

## 🚀 快速开始

### 1. KMS 5分钟快速部署

**生产环境（使用 Vault）**

```bash
# 1. 启用 Vault 功能编译
cargo build --features vault --release

# 2. 配置环境变量
export RUSTFS_VAULT_ADDRESS=https://vault.company.com:8200
export RUSTFS_VAULT_TOKEN=hvs.CAESIJ...

# 3. 启动服务
./target/release/rustfs server
```

**开发测试（使用本地后端）**

```bash
# 1. 编译测试版本
cargo build --release

# 2. 配置本地存储
export RUSTFS_KMS_BACKEND=Local
export RUSTFS_KMS_LOCAL_KEY_DIR=/tmp/rustfs-keys

# 3. 启动服务
./target/release/rustfs server
```

### 2. S3 兼容加密

```bash
# 上传加密文件
curl -X PUT https://rustfs.company.com/bucket/sensitive.txt \
  -H "x-amz-server-side-encryption: AES256" \
  --data-binary @sensitive.txt

# 自动解密下载
curl https://rustfs.company.com/bucket/sensitive.txt
```

## 🏗️ 架构概览

### KMS 三层安全架构

```
┌─────────────────────────────────────────────────┐
│                  应用层                          │
│  ┌─────────────┐    ┌─────────────┐             │
│  │   S3 API    │    │   REST API  │             │
│  └─────────────┘    └─────────────┘             │
├─────────────────────────────────────────────────┤
│                  加密层                          │
│  ┌─────────────┐ 加密 ┌─────────────────┐        │
│  │  对象数据    │ ◄───► │  数据密钥 (DEK) │        │
│  └─────────────┘      └─────────────────┘        │
├─────────────────────────────────────────────────┤
│                 密钥管理层                        │
│  ┌─────────────────┐ 加密 ┌──────────────┐       │
│  │  数据密钥 (DEK) │ ◄────│   主密钥     │       │
│  └─────────────────┘      │ (Vault/HSM)  │       │
│                           └──────────────┘       │
└─────────────────────────────────────────────────┘
```

### 核心特性

- ✅ **多层加密**: Master Key → DEK → Object Data
- ✅ **高性能**: 1MB 流式加密，支持大文件
- ✅ **多后端**: Vault (生产) + Local (测试)
- ✅ **S3 兼容**: 支持标准 SSE-S3/SSE-KMS 头
- ✅ **企业级**: 审计、监控、合规支持

## 📖 学习路径

### 👨‍💻 开发者

1. 阅读 [编程 API 接口](./kms/api.md) 了解 Rust 库使用
2. 查看代码示例学习集成方法
3. 参考 [故障排除](./kms/troubleshooting.md) 解决问题

### 👨‍💼 系统管理员

1. 从 [KMS 使用指南](./kms/README.md) 开始
2. 学习 [HTTP API 接口](./kms/http-api.md) 进行管理
3. 详细阅读 [配置参考](./kms/configuration.md)
4. 设置监控和日志

### 👨‍🔧 运维工程师

1. 熟悉 [HTTP API 接口](./kms/http-api.md) 进行日常管理
2. 掌握 [故障排除](./kms/troubleshooting.md) 技能
3. 了解 [安全指南](./kms/security.md) 要求
4. 建立运维流程

### 🔒 安全架构师

1. 深入学习 [安全指南](./kms/security.md)
2. 评估威胁模型和风险
3. 制定安全策略

## 🤝 贡献指南

我们欢迎社区贡献！

### 文档贡献

```bash
# 1. Fork 项目
git clone https://github.com/your-username/rustfs.git

# 2. 创建文档分支
git checkout -b docs/improve-kms-guide

# 3. 编辑文档
# 编辑 docs/kms/ 下的 Markdown 文件

# 4. 提交更改
git add docs/
git commit -m "docs: improve KMS configuration examples"

# 5. 创建 Pull Request
gh pr create --title "Improve KMS documentation"
```

### 文档规范

- 使用清晰的标题和结构
- 提供可运行的代码示例
- 包含适当的警告和提示
- 支持多种使用场景
- 保持内容最新

## 📞 支持与反馈

### 获取帮助

- **GitHub Issues**: https://github.com/rustfs/rustfs/issues
- **讨论区**: https://github.com/rustfs/rustfs/discussions  
- **文档问题**: 在相关文档页面创建 Issue
- **安全问题**: security@rustfs.com

### 问题报告模板

报告问题时请提供：

```markdown
**环境信息**
- RustFS 版本: v1.0.0
- 操作系统: Ubuntu 20.04
- Rust 版本: 1.75.0

**问题描述**
简要描述遇到的问题...

**重现步骤**
1. 步骤一
2. 步骤二
3. 步骤三

**期望行为**
描述期望的正确行为...

**实际行为**
描述实际发生的情况...

**相关日志**
```bash
# 粘贴相关日志
```

**附加信息**
其他可能有用的信息...
```

## 📈 版本历史

| 版本 | 发布日期 | 主要特性 |
|------|----------|----------|
| v1.0.0 | 2024-01-15 | 🎉 首个正式版本，完整 KMS 功能 |
| v0.9.0 | 2024-01-01 | 🔐 KMS 系统重构，性能优化 |
| v0.8.0 | 2023-12-15 | ⚡ 流式加密，1MB 块大小优化 |

## 🗺️ 开发路线图

### 即将发布 (v1.1.0)

- [ ] 密钥自动轮转
- [ ] HSM 集成支持
- [ ] Web UI 管理界面
- [ ] 更多合规性支持 (SOC2, HIPAA)

### 长期规划

- [ ] 多租户密钥隔离
- [ ] 密钥导入/导出工具
- [ ] 性能基准测试套件
- [ ] Kubernetes Operator

## 📋 文档反馈

帮助我们改进文档！

**这些文档对您有帮助吗？**
- 👍 很有帮助
- 👌 基本满意  
- 👎 需要改进

**改进建议**：
请在 GitHub Issues 中提出具体的改进建议。

---

**最后更新**: 2024-01-15  
**文档版本**: v1.0.0

*感谢使用 RustFS！我们致力于为您提供最好的分布式文件系统解决方案。*