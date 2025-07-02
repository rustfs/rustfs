[![RustFS](https://github.com/user-attachments/assets/547d72f7-d1f4-4763-b9a8-6040bad9251a)](https://rustfs.com)

<p align="center">RustFS 是一个使用 Rust 构建的高性能分布式对象存储软件</p >

<p align="center">
  <!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
  <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/rustfs/rustfs"/>
  <img alt="Github Last Commit" src="https://img.shields.io/github/last-commit/rustfs/rustfs"/>
  <img alt="Github Contributors" src="https://img.shields.io/github/contributors/rustfs/rustfs"/>
  <img alt="GitHub closed issues" src="https://img.shields.io/github/issues-closed/rustfs/rustfs"/>
  <img alt="Discord" src="https://img.shields.io/discord/1107178041848909847?label=discord"/>
</p >

<p align="center">
  <a href=" ">快速开始</a >
  · <a href="https://docs.rustfs.com">文档</a >
  · <a href="https://github.com/rustfs/rustfs/issues">问题报告</a >
  · <a href="https://github.com/rustfs/rustfs/discussions">讨论</a >
</p >

<p align="center">
<a href="https://github.com/rustfs/rustfs/blob/main/README.md">English</a > | 简体中文
</p >

RustFS 是一个使用 Rust（全球最受欢迎的编程语言之一）构建的高性能分布式对象存储软件。与 MinIO 一样，它具有简单性、S3 兼容性、开源特性以及对数据湖、AI 和大数据的支持等一系列优势。此外，与其他存储系统相比，它采用 Apache 许可证构建，拥有更好、更用户友好的开源许可证。由于以 Rust 为基础，RustFS 为高性能对象存储提供了更快的速度和更安全的分布式功能。

## 特性

- **高性能**：使用 Rust 构建，确保速度和效率。
- **分布式架构**：可扩展且容错的设计，适用于大规模部署。
- **S3 兼容性**：与现有 S3 兼容应用程序无缝集成。
- **数据湖支持**：针对大数据和 AI 工作负载进行了优化。
- **开源**：采用 Apache 2.0 许可证，鼓励社区贡献和透明度。
- **用户友好**：设计简单，易于部署和管理。

## RustFS vs MinIO

压力测试服务器参数

|  类型  |  参数   | 备注 |
| - | - | - |
|CPU | 2 核心 | Intel Xeon(Sapphire Rapids) Platinum 8475B , 2.7/3.2 GHz|   |
|内存| 4GB |     |
|网络 | 15Gbp |      |
|驱动器  | 40GB x 4 |   IOPS 3800 / 驱动器 |

<https://github.com/user-attachments/assets/8cdc78f3-5ccb-413b-aa08-ff005022cf52>

### RustFS vs 其他对象存储

| RustFS | 其他对象存储|
| - | - |
| 强大的控制台 | 简单且无用的控制台 |
| 基于 Rust 语言开发，内存更安全 | 使用 Go 或 C 开发，存在内存 GC/泄漏等潜在问题 |
| 不向第三方国家报告日志  | 向其他第三方国家报告日志可能违反国家安全法律 |
| 采用 Apache 许可证，对商业更友好  | AGPL V3 许可证等其他许可证，污染开源和许可证陷阱，侵犯知识产权 |
| 全面的 S3 支持，适用于国内外云提供商  | 完全支持 S3，但不支持本地云厂商 |
| 基于 Rust 开发，对安全和创新设备有强大支持  | 对边缘网关和安全创新设备支持较差|
| 稳定的商业价格，免费社区支持 | 高昂的定价，1PiB 成本高达 $250,000 |
| 无风险 | 知识产权风险和禁止使用的风险 |

## 快速开始

要开始使用 RustFS，请按照以下步骤操作：

1. **安装 RustFS**：从我们的 [GitHub Releases](https://github.com/rustfs/rustfs/releases) 下载最新版本。
2. **运行 RustFS**：使用提供的二进制文件启动服务器。

   ```bash
   ./rustfs  /data
   ```

3. **访问控制台**：打开 Web 浏览器并导航到 `http://localhost:9001` 以访问 RustFS 控制台。
4. **创建存储桶**：使用控制台为您的对象创建新的存储桶。
5. **上传对象**：您可以直接通过控制台上传文件，或使用 S3 兼容的 API 与您的 RustFS 实例交互。

## 文档

有关详细文档，包括配置选项、API 参考和高级用法，请访问我们的[文档](https://docs.rustfs.com)。

## 获取帮助

如果您有任何问题或需要帮助，您可以：

- 查看[常见问题解答](https://docs.rustfs.com/faq)以获取常见问题和解决方案。
- 加入我们的 [GitHub 讨论](https://github.com/rustfs/rustfs/discussions)来提问和分享您的经验。
- 在我们的 [GitHub Issues](https://github.com/rustfs/rustfs/issues) 页面上开启问题，报告错误或功能请求。

## 链接

- [文档](https://docs.rustfs.com) - 您应该阅读的手册
- [更新日志](https://docs.rustfs.com/changelog) - 我们破坏和修复的内容
- [GitHub 讨论](https://github.com/rustfs/rustfs/discussions) - 社区所在地

## 联系

- **错误报告**：[GitHub Issues](https://github.com/rustfs/rustfs/issues)
- **商务合作**：<hello@rustfs.com>
- **招聘**：<jobs@rustfs.com>
- **一般讨论**：[GitHub 讨论](https://github.com/rustfs/rustfs/discussions)
- **贡献**：[CONTRIBUTING.md](CONTRIBUTING.md)

## 贡献者

RustFS 是一个社区驱动的项目，我们感谢所有的贡献。查看[贡献者](https://github.com/rustfs/rustfs/graphs/contributors)页面，了解帮助 RustFS 变得更好的杰出人员。

<a href="https://github.com/rustfs/rustfs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=rustfs/rustfs" />
</a >

## 许可证

[Apache 2.0](https://opensource.org/licenses/Apache-2.0)

**RustFS** 是 RustFS, Inc. 的商标。所有其他商标均为其各自所有者的财产。
