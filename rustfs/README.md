[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

<p align="center">RustFS is a high-performance distributed object storage software built using Rust</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://github.com/rustfs/rustfs/actions/workflows/docker.yml"><img alt="Build and Push Docker Images" src="https://github.com/rustfs/rustfs/actions/workflows/docker.yml/badge.svg" /></a>
  <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/rustfs/rustfs"/>
  <img alt="Github Last Commit" src="https://img.shields.io/github/last-commit/rustfs/rustfs"/>
</p>

<p align="center">
  <a href="https://docs.rustfs.com/installation/">Getting Started</a>
  · <a href="https://docs.rustfs.com/">Docs</a>
  · <a href="https://github.com/rustfs/rustfs/issues">Bug reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">Discussions</a>
</p>

<p align="center">
English | <a href="https://github.com/rustfs/rustfs/blob/main/README_ZH.md">Simplified Chinese</a>
</p>

RustFS is a high-performance distributed object storage software built using Rust, one of the most popular languages
worldwide. Along with MinIO, it shares a range of advantages such as simplicity, S3 compatibility, open-source nature,
support for data lakes, AI, and big data. Furthermore, it has a better and more user-friendly open-source license in
comparison to other storage systems, being constructed under the Apache license. As Rust serves as its foundation,
RustFS provides faster speed and safer distributed features for high-performance object storage.

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

  ```bash
   # Docker Hub (recommended)
   docker run -d -p 9000:9000 -v /data:/data rustfs/rustfs:latest

   # Alternative using Podman
   podman run -d -p 9000:9000 -v /data:/data rustfs/rustfs:latest
   ```

3. **Access the Console**: Open your web browser and navigate to `http://localhost:9000` to access the RustFS console,
   default username and password is `rustfsadmin` .
4. **Create a Bucket**: Use the console to create a new bucket for your objects.
5. **Upload Objects**: You can upload files directly through the console or use S3-compatible APIs to interact with your
   RustFS instance.

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
- **Contributing**: [CONTRIBUTING.md](../CONTRIBUTING.md)

## Contributors

RustFS is a community-driven project, and we appreciate all contributions. Check out
the [Contributors](https://github.com/rustfs/rustfs/graphs/contributors) page to see the amazing people who have helped
make RustFS better.

<a href="https://github.com/rustfs/rustfs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=rustfs/rustfs" />
</a>

## License

[Apache 2.0](https://opensource.org/licenses/Apache-2.0)

**RustFS** is a trademark of RustFS, Inc. All other trademarks are the property of their respective owners.
