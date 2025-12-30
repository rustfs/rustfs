[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

<p align="center">RustFS 是一个基于 Rust 构建的高性能分布式对象存储系统。</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://github.com/rustfs/rustfs/actions/workflows/docker.yml"><img alt="构建并推送 Docker 镜像" src="https://github.com/rustfs/rustfs/actions/workflows/docker.yml/badge.svg" /></a>
  <img alt="GitHub 提交活跃度" src="https://img.shields.io/github/commit-activity/m/rustfs/rustfs"/>
  <img alt="Github 最新提交" src="https://img.shields.io/github/last-commit/rustfs/rustfs"/>
  <a href="https://hellogithub.com/repository/rustfs/rustfs" target="_blank"><img src="https://abroad.hellogithub.com/v1/widgets/recommend.svg?rid=b95bcb72bdc340b68f16fdf6790b7d5b&claim_uid=MsbvjYeLDKAH457&theme=small" alt="Featured｜HelloGitHub" /></a>
</p>

<p align="center">
<a href="https://trendshift.io/repositories/14181" target="_blank"><img src="https://trendshift.io/api/badge/repositories/14181" alt="rustfs%2Frustfs | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</p>

<p align="center">
  <a href="https://docs.rustfs.com/installation/">快速开始</a>
  · <a href="https://docs.rustfs.com/">文档</a>
  · <a href="https://github.com/rustfs/rustfs/issues">报告 Bug</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">社区讨论</a>
</p>



<p align="center">
  <a href="https://github.com/rustfs/rustfs/blob/main/README.md">English</a> | 简体中文 |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=de">Deutsch</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=es">Español</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=fr">français</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=ja">日本語</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=ko">한국어</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=pt">Portuguese</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=ru">Русский</a>
</p>

RustFS 是一个基于 Rust 构建的高性能分布式对象存储系统。Rust 是全球最受开发者喜爱的编程语言之一，RustFS 完美结合了 MinIO 的简洁性与 Rust 的内存安全及高性能优势。它提供完整的 S3 兼容性，完全开源，并专为数据湖、人工智能（AI）和大数据负载进行了优化。

与其他存储系统不同，RustFS 采用更宽松、商业友好的 Apache 2.0 许可证，避免了 AGPL 协议的限制。以 Rust 为基石，RustFS 为下一代对象存储提供了更快的速度和更安全的分布式特性。

## 特征和功能状态

- **高性能**：基于 Rust 构建，确保极致的速度和资源效率。
- **分布式架构**：可扩展且容错的设计，适用于大规模部署。
- **S3 兼容性**：与现有的 S3 兼容应用和工具无缝集成。
- **数据湖支持**：专为高吞吐量的大数据和 AI 工作负载优化。
- **完全开源**：采用 Apache 2.0 许可证，鼓励社区贡献和商业使用。
- **简单易用**：设计简洁，易于部署和管理。


| 功能 | 状态 |   功能 | 状态 | 
| :--- | :--- | :--- | :--- |
| **S3 核心功能** | ✅ 可用 |    **Bitrot (防数据腐烂)** | ✅ 可用 |
| **上传 / 下载** | ✅ 可用 |     **单机模式** | ✅ 可用 |
| **版本控制** | ✅ 可用 | **存储桶复制** | ✅ 可用 |
| **日志功能** | ✅ 可用 |  **生命周期管理** | 🚧 测试中 |
| **事件通知** | ✅ 可用 |  **分布式模式** | 🚧 测试中 |
| **K8s Helm Chart** | ✅ 可用 |  **OPA (策略引擎)** | 🚧 测试中 |




## RustFS vs MinIO 性能对比

**压力测试环境参数：**

| 类型    | 参数 | 备注                                                   |
|---------|-----------|----------------------------------------------------------|
| CPU     | 2 核    | Intel Xeon (Sapphire Rapids) Platinum 8475B , 2.7/3.2 GHz |
| 内存  | 4GB       |                                                          |
| 网络 | 15Gbps     |                                                          |
| 硬盘  | 40GB x 4  | IOPS 3800 / Drive                                       |

<https://github.com/user-attachments/assets/2e4979b5-260c-4f2c-ac12-c87fd558072a>

### RustFS vs 其他对象存储

| 特性 | RustFS | 其他对象存储 |
| :--- | :--- | :--- |
| **控制台体验** | **功能强大的控制台**<br>提供全面的管理界面。 | **基础/简陋的控制台**<br>通常功能过于简单或缺失关键特性。 |
| **语言与安全** | **基于 Rust 开发**<br>天生的内存安全。 | **基于 Go 或 C 开发**<br>存在内存 GC 停顿或内存泄漏的潜在风险。 |
| **数据主权** | **无遥测 / 完全合规**<br>防止未经授权的数据跨境传输。完全符合 GDPR (欧盟/英国)、CCPA (美国) 和 APPI (日本) 等法规。 | **潜在风险**<br>可能存在法律风险和隐蔽的数据遥测（Telemetry）。 |
| **开源协议** | **宽松的 Apache 2.0**<br>商业友好，无“毒丸”条款。 | **受限的 AGPL v3**<br>存在许可证陷阱和知识产权污染的风险。 |
| **兼容性** | **100% S3 兼容**<br>适用于任何云提供商和客户端，随处运行。 | **兼容性不一**<br>虽然支持 S3，但可能缺乏对本地云厂商或特定 API 的支持。 |
| **边缘与 IoT** | **强大的边缘支持**<br>非常适合安全、创新的边缘设备。 | **边缘支持较弱**<br>对于边缘网关来说通常过于沉重。 |
| **成本** | **稳定且免费**<br>免费社区支持，稳定的商业定价。 | **高昂成本**<br>1PiB 的成本可能高达 250,000 美元。 |
| **风险控制** | **企业级风险规避**<br>清晰的知识产权，商业使用安全无忧。 | **法律风险**<br>知识产权归属模糊及使用限制风险。 |

## 快速开始

请按照以下步骤快速上手 RustFS：

### 1. 一键安装脚本 (选项 1)

  ```bash
  curl -O https://rustfs.com/install_rustfs.sh && bash install_rustfs.sh
````

### 2\. Docker 快速启动 (选项 2)

RustFS 容器以非 root 用户 `rustfs` (UID `10001`) 运行。如果您使用 Docker 的 `-v` 参数挂载宿主机目录，请务必确保宿主机目录的所有者已更改为 `1000`，否则会遇到权限拒绝错误。

```bash
 # 创建数据和日志目录
 mkdir -p data logs

 # 更改这两个目录的所有者
 chown -R 10001:10001 data logs

 # 使用最新版本运行
 docker run -d -p 9000:9000 -p 9001:9001 -v $(pwd)/data:/data -v $(pwd)/logs:/logs rustfs/rustfs:latest

 # 使用指定版本运行
 docker run -d -p 9000:9000 -p 9001:9001 -v $(pwd)/data:/data -v $(pwd)/logs:/logs rustfs/rustfs:1.0.0.alpha.68
```

您也可以使用 Docker Compose。使用根目录下的 `docker-compose.yml` 文件：

```bash
docker compose --profile observability up -d
```

**注意**: 我们建议您在运行前查看 `docker-compose.yaml` 文件。该文件定义了包括 Grafana、Prometheus 和 Jaeger 在内的多个服务，有助于 RustFS 的可观测性监控。如果您还想启动 Redis 或 Nginx 容器，可以指定相应的 profile。

### 3\. 源码编译 (选项 3) - 进阶用户

适用于希望从源码构建支持多架构 RustFS Docker 镜像的开发者：

```bash
# 在本地构建多架构镜像
./docker-buildx.sh --build-arg RELEASE=latest

# 构建并推送到仓库
./docker-buildx.sh --push

# 构建指定版本
./docker-buildx.sh --release v1.0.0 --push

# 构建并推送到自定义仓库
./docker-buildx.sh --registry your-registry.com --namespace yourname --push
```

`docker-buildx.sh` 脚本支持：
\- **多架构构建**: `linux/amd64`, `linux/arm64`
\- **自动版本检测**: 使用 git tags 或 commit hash
\- **灵活的仓库支持**: 支持 Docker Hub, GitHub Container Registry 等
\- **构建优化**: 包含缓存和并行构建

为了方便起见，您也可以使用 Make 命令：

```bash
make docker-buildx                    # 本地构建
make docker-buildx-push               # 构建并推送
make docker-buildx-version VERSION=v1.0.0  # 构建指定版本
make help-docker                      # 显示所有 Docker 相关命令
```

> **注意 (macOS 交叉编译)**: macOS 默认的 `ulimit -n` 限制为 256，因此在使用 `cargo zigbuild` 或 `./build-rustfs.sh --platform ...` 交叉编译 Linux 版本时，可能会因 `ProcessFdQuotaExceeded` 失败。构建脚本会尝试自动提高限制，但如果您仍然看到警告，请在构建前在终端运行 `ulimit -n 4096` (或更高)。

### 4\. 使用 Helm Chart 安装 (选项 4) - 云原生环境

请按照 [Helm Chart README](https://charts.rustfs.com) 上的说明在 Kubernetes 集群上安装 RustFS。

-----

### 访问 RustFS

5.  **访问控制台**: 打开浏览器并访问 `http://localhost:9000` 进入 RustFS 控制台。
      * 默认账号/密码: `rustfsadmin` / `rustfsadmin`
6.  **创建存储桶**: 使用控制台为您​​的对象创建一个新的存储桶 (Bucket)。
7.  **上传对象**: 您可以直接通过控制台上传文件，或使用 S3 兼容的 API/客户端与您的 RustFS 实例进行交互。

**注意**: 如果您希望通过 `https` 访问 RustFS 实例，请参考 [TLS 配置文档](https://docs.rustfs.com/integration/tls-configured.html)。

## 文档

有关详细文档，包括配置选项、API 参考和高级用法，请访问我们的 [官方文档](https://docs.rustfs.com)。

## 获取帮助

如果您有任何问题或需要帮助：

  - 查看 [FAQ](https://github.com/rustfs/rustfs/discussions/categories/q-a) 寻找常见问题和解决方案。
  - 加入我们的 [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) 提问并分享您的经验。
  - 在我们的 [GitHub Issues](https://github.com/rustfs/rustfs/issues) 页面提交 Bug 报告或功能请求。

## 链接

  - [官方文档](https://docs.rustfs.com) - 必读手册
  - [更新日志](https://github.com/rustfs/rustfs/releases) - 版本变更记录
  - [社区讨论](https://github.com/rustfs/rustfs/discussions) - 社区交流地

## 联系方式

  - **Bug 反馈**: [GitHub Issues](https://github.com/rustfs/rustfs/issues)
  - **商务合作**: [hello@rustfs.com](mailto:hello@rustfs.com)
  - **工作机会**: [jobs@rustfs.com](mailto:jobs@rustfs.com)
  - **一般讨论**: [GitHub Discussions](https://github.com/rustfs/rustfs/discussions)
  - **贡献指南**: [CONTRIBUTING.md](https://www.google.com/search?q=CONTRIBUTING.md)

## 贡献者

RustFS 是一个社区驱动的项目，我们感谢所有的贡献。请查看 [贡献者](https://github.com/rustfs/rustfs/graphs/contributors) 页面，看看那些让 RustFS 变得更好的了不起的人们。

<a href="https://github.com/rustfs/rustfs/graphs/contributors">
<img src="https://opencollective.com/rustfs/contributors.svg?width=890&limit=500&button=false" alt="Contributors" />
</a>



## Star 历史

[![Star History Chart](https://api.star-history.com/svg?repos=rustfs/rustfs&type=date&legend=top-left)](https://www.star-history.com/#rustfs/rustfs&type=date&legend=top-left)


## 许可证

[Apache 2.0](https://opensource.org/licenses/Apache-2.0)

**RustFS** 是 RustFS, Inc. 的商标。所有其他商标均为其各自所有者的财产。

