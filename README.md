[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

<p align="center">RustFS is a high-performance distributed object storage software built using Rust</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://github.com/rustfs/rustfs/actions/workflows/docker.yml"><img alt="Build and Push Docker Images" src="https://github.com/rustfs/rustfs/actions/workflows/docker.yml/badge.svg" /></a>
  <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/rustfs/rustfs"/>
  <img alt="Github Last Commit" src="https://img.shields.io/github/last-commit/rustfs/rustfs"/>
  <a href="https://hellogithub.com/repository/rustfs/rustfs" target="_blank"><img src="https://abroad.hellogithub.com/v1/widgets/recommend.svg?rid=b95bcb72bdc340b68f16fdf6790b7d5b&claim_uid=MsbvjYeLDKAH457&theme=small" alt="Featured｜HelloGitHub" /></a>
</p>

<p align="center">
  <a href="https://docs.rustfs.com/introduction.html">Getting Started</a>
  · <a href="https://docs.rustfs.com/">Docs</a>
  · <a href="https://github.com/rustfs/rustfs/issues">Bug reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">Discussions</a>
</p>

<p align="center">
English | <a href="https://github.com/rustfs/rustfs/blob/main/README_ZH.md">简体中文</a> |
  <!-- Keep these links. Translations will automatically update with the README. -->
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=de">Deutsch</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=es">Español</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=fr">français</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=ja">日本語</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=ko">한국어</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=pt">Portuguese</a> |
  <a href="https://readme-i18n.com/rustfs/rustfs?lang=ru">Русский</a>
</p>

RustFS is a high-performance distributed object storage software built using Rust, one of the most popular languages
worldwide. Along with MinIO, it shares a range of advantages such as simplicity, S3 compatibility, open-source nature,
support for data lakes, AI, and big data. Furthermore, it has a better and more user-friendly open-source license in
comparison to other storage systems, being constructed under the Apache license. As Rust serves as its foundation,
RustFS provides faster speed and safer distributed features for high-performance object storage.

> ⚠️ **RustFS is under rapid development. Do NOT use in production environments!**

## Features

- **High Performance**: Built with Rust, ensuring speed and efficiency.
- **Distributed Architecture**: Scalable and fault-tolerant design for large-scale deployments.
- **S3 Compatibility**: Seamless integration with existing S3-compatible applications.
- **Data Lake Support**: Optimized for big data and AI workloads.
- **Open Source**: Licensed under Apache 2.0, encouraging community contributions and transparency.
- **User-Friendly**: Designed with simplicity in mind, making it easy to deploy and manage.

## RustFS vs MinIO

Stress test server parameters

| Type    | parameter | Remark                                                   |
|---------|-----------|----------------------------------------------------------|
| CPU     | 2 Core    | Intel Xeon(Sapphire Rapids) Platinum 8475B , 2.7/3.2 GHz |   |
| Memory  | 4GB       |                                                          |
| Network | 15Gbp     |                                                          |
| Driver  | 40GB x 4  | IOPS 3800 / Driver                                       |

<https://github.com/user-attachments/assets/2e4979b5-260c-4f2c-ac12-c87fd558072a>

### RustFS vs Other object storage

| RustFS                                                                          | Other object storage                                                                                                    |
|---------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| Powerful Console                                                                | Simple and useless Console                                                                                              |
| Developed based on Rust language, memory is safer                               | Developed in Go or C, with potential issues like memory GC/leaks                                                        |
| Does not report logs to third-party countries                                   | Reporting logs to other third countries may violate national security laws                                              |
| Licensed under Apache, more business-friendly                                   | AGPL V3 License and other License, polluted open source and License traps, infringement of intellectual property rights |
| Comprehensive S3 support, works with domestic and international cloud providers | Full support for S3, but no local cloud vendor support                                                                  |
| Rust-based development, strong support for secure and innovative devices        | Poor support for edge gateways and secure innovative devices                                                            |
| Stable commercial prices, free community support                                | High pricing, with costs up to $250,000 for 1PiB                                                                        |
| No risk                                                                         | Intellectual property risks and risks of prohibited uses                                                                |

## Quickstart

To get started with RustFS, follow these steps:

1. **One-click installation script (Option 1)​​**

  ```bash
  curl -O  https://rustfs.com/install_rustfs.sh && bash install_rustfs.sh
  ```

2. **Docker Quick Start (Option 2)​​**

RustFS container run as non-root user `rustfs` with id `1000`, if you run docker with `-v` to mount host directory into docker container, please make sure the owner of host directory has been changed to `1000`, otherwise you will encounter permission denied error.

  ```bash
   # create data and logs directories
   mkdir -p data logs

   # change the owner of those two ditectories
   chown -R 1000:1000 data logs

   # using latest version
   docker run -d -p 9000:9000 -p 9001:9001 -v $(pwd)/data:/data -v $(pwd)/logs:/logs rustfs/rustfs:latest

   # using specific version
   docker run -d -p 9000:9000 -p 9001:9001 -v $(pwd)/data:/data -v $(pwd)/logs:/logs rustfs/rustfs:1.0.0.alpha.68
   ```

For docker installation, you can also run the container with docker compose. With the `docker-compose.yml` file under
root directory, running the command:

  ```
  docker compose --profile observability up -d
  ```

**NOTE**: You should be better to have a look for `docker-compose.yaml` file. Because, several services contains in the
file. Grafan,prometheus,jaeger containers will be launched using docker compose file, which is helpful for rustfs
observability. If you want to start redis as well as nginx container, you can specify the corresponding profiles.

3. **Build from Source (Option 3) - Advanced Users**

   For developers who want to build RustFS Docker images from source with multi-architecture support:

   ```bash
   # Build multi-architecture images locally
   ./docker-buildx.sh --build-arg RELEASE=latest

   # Build and push to registry
   ./docker-buildx.sh --push

   # Build specific version
   ./docker-buildx.sh --release v1.0.0 --push

   # Build for custom registry
   ./docker-buildx.sh --registry your-registry.com --namespace yourname --push
   ```

   The `docker-buildx.sh` script supports:
    - **Multi-architecture builds**: `linux/amd64`, `linux/arm64`
    - **Automatic version detection**: Uses git tags or commit hashes
    - **Registry flexibility**: Supports Docker Hub, GitHub Container Registry, etc.
    - **Build optimization**: Includes caching and parallel builds

   You can also use Make targets for convenience:

   ```bash
   make docker-buildx                    # Build locally
   make docker-buildx-push               # Build and push
   make docker-buildx-version VERSION=v1.0.0  # Build specific version
   make help-docker                      # Show all Docker-related commands
   ```

   > **Heads-up (macOS cross-compilation)**: macOS keeps the default `ulimit -n` at 256, so `cargo zigbuild` or `./build-rustfs.sh --platform ...` may fail with `ProcessFdQuotaExceeded` when targeting Linux. The build script now tries to raise the limit automatically, but if you still see the warning, run `ulimit -n 4096` (or higher) in your shell before building.

4. **Build with helm chart(Option 4) - Cloud Native environment**

   Following the instructions on [helm chart README](./helm/README.md) to install RustFS on kubernetes cluster.

5. **Access the Console**: Open your web browser and navigate to `http://localhost:9000` to access the RustFS console,
   default username and password is `rustfsadmin` .
6. **Create a Bucket**: Use the console to create a new bucket for your objects.
7. **Upload Objects**: You can upload files directly through the console or use S3-compatible APIs to interact with your
   RustFS instance.

**NOTE**: If you want to access RustFS instance with `https`, you can refer
to [TLS configuration docs](https://docs.rustfs.com/integration/tls-configured.html).

## Documentation

For detailed documentation, including configuration options, API references, and advanced usage, please visit
our [Documentation](https://docs.rustfs.com).

## Getting Help

If you have any questions or need assistance, you can:

- Check the [FAQ](https://github.com/rustfs/rustfs/discussions/categories/q-a) for common issues and solutions.
- Join our [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) to ask questions and share your
  experiences.
- Open an issue on our [GitHub Issues](https://github.com/rustfs/rustfs/issues) page for bug reports or feature
  requests.

## Links

- [Documentation](https://docs.rustfs.com) - The manual you should read
- [Changelog](https://github.com/rustfs/rustfs/releases) - What we broke and fixed
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Where the community lives

## Contact

- **Bugs**: [GitHub Issues](https://github.com/rustfs/rustfs/issues)
- **Business**: <hello@rustfs.com>
- **Jobs**: <jobs@rustfs.com>
- **General Discussion**: [GitHub Discussions](https://github.com/rustfs/rustfs/discussions)
- **Contributing**: [CONTRIBUTING.md](CONTRIBUTING.md)

## Contributors

RustFS is a community-driven project, and we appreciate all contributions. Check out
the [Contributors](https://github.com/rustfs/rustfs/graphs/contributors) page to see the amazing people who have helped
make RustFS better.

<a href="https://github.com/rustfs/rustfs/graphs/contributors">
  <img src="https://opencollective.com/rustfs/contributors.svg?width=890&limit=500&button=false" alt="Contributors"/>
</a>

## Github Trending Top

🚀 RustFS is beloved by open-source enthusiasts and enterprise users worldwide, often appearing on the GitHub Trending
top charts.

<a href="https://trendshift.io/repositories/14181" target="_blank"><img src="https://raw.githubusercontent.com/rustfs/rustfs/refs/heads/main/docs/rustfs-trending.jpg" alt="rustfs%2Frustfs | Trendshift" /></a>

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=rustfs/rustfs&type=date&legend=top-left)](https://www.star-history.com/#rustfs/rustfs&type=date&legend=top-left)

## License

[Apache 2.0](https://opensource.org/licenses/Apache-2.0)

**RustFS** is a trademark of RustFS, Inc. All other trademarks are the property of their respective owners.
