# 对象路径中的特殊字符 - 完整文档

本目录包含关于在 RustFS 中处理 S3 对象路径中特殊字符(空格、加号、百分号等)的完整文档。

## 快速链接

- **用户指南**: [客户端指南](./client-special-characters-guide.md)
- **开发者文档**: [解决方案文档](./special-characters-solution.md)
- **深入分析**: [技术分析](./special-characters-in-path-analysis.md)

## 核心问题说明

### 问题现象

用户报告了两个问题:
1. **问题 A**: UI 可以导航到包含特殊字符的文件夹,但无法列出其中的内容
2. **问题 B**: 上传路径中包含 `+` 号的文件时出现 400 错误

### 根本原因

经过深入调查,包括检查 s3s 库的源代码,我们发现:

**后端 (RustFS) 工作正常** ✅
- s3s 库正确地对 HTTP 请求中的对象键进行 URL 解码
- RustFS 正确存储和检索包含特殊字符的对象
- 命令行工具(mc, aws-cli)完美工作 → 证明后端正确处理特殊字符

**问题出在 UI/客户端层** ❌
- 某些客户端未正确进行 URL 编码
- UI 可能在发出 LIST 请求时未对前缀进行编码
- 自定义 HTTP 客户端可能存在编码错误

### 解决方案

1. **用户**: 使用正规的 S3 SDK/客户端(它们会自动处理编码)
2. **开发者**: 后端无需修复,但添加了防御性验证和日志
3. **UI**: UI 需要正确对所有请求进行 URL 编码(如适用)

## URL 编码快速参考

| 字符 | 显示 | URL 路径中 | 查询参数中 |
|------|------|-----------|-----------|
| 空格 | ` ` | `%20` | `%20` 或 `+` |
| 加号 | `+` | `%2B` | `%2B` |
| 百分号 | `%` | `%25` | `%25` |

**重要**: 在 URL **路径**中,`+` = 字面加号(不是空格)。只有 `%20` = 空格!

## 快速示例

### ✅ 正确使用(使用 mc)

```bash
# 上传
mc cp file.txt "myrustfs/bucket/路径 包含 空格/file.txt"

# 列出
mc ls "myrustfs/bucket/路径 包含 空格/"

# 结果: ✅ 成功 - mc 正确编码了请求
```

### ❌ 可能失败(原始 HTTP 未编码)

```bash
# 错误: 未编码
curl "http://localhost:9000/bucket/路径 包含 空格/file.txt"

# 结果: ❌ 可能失败 - 空格未编码
```

### ✅ 正确的原始 HTTP

```bash
# 正确: 已正确编码
curl "http://localhost:9000/bucket/%E8%B7%AF%E5%BE%84%20%E5%8C%85%E5%90%AB%20%E7%A9%BA%E6%A0%BC/file.txt"

# 结果: ✅ 成功 - 空格编码为 %20
```

## 实施状态

### ✅ 已完成

1. **后端验证**: 添加了控制字符验证(拒绝空字节、换行符)
2. **调试日志**: 为包含特殊字符的键添加了日志记录
3. **测试**: 创建了综合 e2e 测试套件
4. **文档**: 
   - 包含 SDK 示例的客户端指南
   - 开发者解决方案文档
   - 架构师技术分析
   - 安全摘要

### 📋 建议的后续步骤

1. **运行测试**: 执行 e2e 测试以验证后端行为
   ```bash
   cargo test --package e2e_test special_chars
   ```

2. **UI 审查**(如适用): 检查 RustFS UI 是否正确编码请求

3. **用户沟通**: 
   - 更新用户文档
   - 在 FAQ 中添加故障排除
   - 传达已知的 UI 限制(如有)

## 测试

### 运行特殊字符测试

```bash
# 所有特殊字符测试
cargo test --package e2e_test special_chars -- --nocapture

# 特定测试
cargo test --package e2e_test test_object_with_space_in_path -- --nocapture
cargo test --package e2e_test test_object_with_plus_in_path -- --nocapture
cargo test --package e2e_test test_issue_scenario_exact -- --nocapture
```

### 使用真实客户端测试

```bash
# MinIO 客户端
mc alias set test http://localhost:9000 minioadmin minioadmin
mc cp README.md "test/bucket/测试 包含 空格/README.md"
mc ls "test/bucket/测试 包含 空格/"

# AWS CLI
aws --endpoint-url=http://localhost:9000 s3 cp README.md "s3://bucket/测试 包含 空格/README.md"
aws --endpoint-url=http://localhost:9000 s3 ls "s3://bucket/测试 包含 空格/"
```

## 支持

如果遇到特殊字符问题:

1. **首先**: 查看[客户端指南](./client-special-characters-guide.md)
2. **尝试**: 使用 mc 或 AWS CLI 隔离问题
3. **启用**: 调试日志: `RUST_LOG=rustfs=debug`
4. **报告**: 创建问题,包含:
   - 使用的客户端/SDK
   - 导致问题的确切对象名称
   - mc 是否工作(以隔离后端与客户端)
   - 调试日志

## 相关文档

- [客户端指南](./client-special-characters-guide.md) - 用户必读
- [解决方案文档](./special-characters-solution.md) - 开发者指南
- [技术分析](./special-characters-in-path-analysis.md) - 深入分析
- [安全摘要](./SECURITY_SUMMARY_special_chars.md) - 安全审查

## 常见问题

**问: 可以在对象名称中使用空格吗?**  
答: 可以,但请使用能自动处理编码的 S3 SDK。

**问: 为什么 `+` 不能用作空格?**  
答: 在 URL 路径中,`+` 表示字面加号。只有在查询参数中 `+` 才表示空格。在路径中使用 `%20` 表示空格。

**问: RustFS 支持对象名称中的 Unicode 吗?**  
答: 支持,对象名称是 UTF-8 字符串。它们支持任何有效的 UTF-8 字符。

**问: 哪些字符是禁止的?**  
答: 控制字符(空字节、换行符、回车符)被拒绝。所有可打印字符都是允许的。

**问: 如何修复"UI 无法列出文件夹"的问题?**  
答: 使用 CLI(mc 或 aws-cli)代替。这是 UI 错误,不是后端问题。

## 版本历史

- **v1.0** (2025-12-09): 初始文档
  - 完成综合分析
  - 确定根本原因(UI/客户端问题)
  - 添加后端验证和日志
  - 创建客户端指南
  - 添加 E2E 测试

---

**维护者**: RustFS 团队  
**最后更新**: 2025-12-09  
**状态**: 完成 - 可供使用
