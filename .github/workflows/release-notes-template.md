## RustFS ${VERSION_CLEAN}

${ORIGINAL_NOTES}

---

### 🚀 Quick Download

**Linux (Static Binaries - No Dependencies):**

```bash
# x86_64 (Intel/AMD)
curl -LO https://github.com/rustfs/rustfs/releases/download/${VERSION}/rustfs-x86_64-unknown-linux-musl.zip
unzip rustfs-x86_64-unknown-linux-musl.zip
sudo mv rustfs /usr/local/bin/

# ARM64 (Graviton, Apple Silicon VMs)
curl -LO https://github.com/rustfs/rustfs/releases/download/${VERSION}/rustfs-aarch64-unknown-linux-musl.zip
unzip rustfs-aarch64-unknown-linux-musl.zip
sudo mv rustfs /usr/local/bin/
```

**macOS:**

```bash
# Apple Silicon (M1/M2/M3)
curl -LO https://github.com/rustfs/rustfs/releases/download/${VERSION}/rustfs-aarch64-apple-darwin.zip
unzip rustfs-aarch64-apple-darwin.zip
sudo mv rustfs /usr/local/bin/

# Intel
curl -LO https://github.com/rustfs/rustfs/releases/download/${VERSION}/rustfs-x86_64-apple-darwin.zip
unzip rustfs-x86_64-apple-darwin.zip
sudo mv rustfs /usr/local/bin/
```

### 📁 Available Downloads

| Platform | Architecture | File | Description |
|----------|-------------|------|-------------|
| Linux | x86_64 | `rustfs-x86_64-unknown-linux-musl.zip` | Static binary, no dependencies |
| Linux | ARM64 | `rustfs-aarch64-unknown-linux-musl.zip` | Static binary, no dependencies |
| macOS | Apple Silicon | `rustfs-aarch64-apple-darwin.zip` | Native binary, ZIP archive |
| macOS | Intel | `rustfs-x86_64-apple-darwin.zip` | Native binary, ZIP archive |

### 🔐 Verification

Download checksums and verify your download:

```bash
# Download checksums
curl -LO https://github.com/rustfs/rustfs/releases/download/${VERSION}/SHA256SUMS

# Verify (Linux)
sha256sum -c SHA256SUMS --ignore-missing

# Verify (macOS)
shasum -a 256 -c SHA256SUMS --ignore-missing
```

### 🛠️ System Requirements

- **Linux**: Any distribution with glibc 2.17+ (CentOS 7+, Ubuntu 16.04+)
- **macOS**: 10.15+ (Catalina or later)
- **Windows**: Windows 10 version 1809 or later

### 📚 Documentation

- [Installation Guide](https://github.com/rustfs/rustfs#installation)
- [Quick Start](https://github.com/rustfs/rustfs#quick-start)
- [Configuration](https://github.com/rustfs/rustfs/blob/main/docs/)
- [API Documentation](https://docs.rs/rustfs)

### 🆘 Support

- 🐛 [Report Issues](https://github.com/rustfs/rustfs/issues)
- 💬 [Community Discussions](https://github.com/rustfs/rustfs/discussions)
- 📖 [Documentation](https://github.com/rustfs/rustfs/tree/main/docs)
