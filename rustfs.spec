%global _enable_debug_packages 0
%global _empty_manifest_terminate_build 0
Name:           rustfs
Version:        1.0.0
Release:        alpha.36%{?dist}
Summary:       High-performance distributed object storage for MinIO alternative

License:        Apache-2.0
URL:            https://github.com/rustfs/rustfs
Source0:        https://github.com/rustfs/rustfs/archive/refs/tags/%{version}.tar.gz

BuildRequires: cargo
BuildRequires: rust
BuildRequires: mold
BuildRequires: pango-devel
BuildRequires: cairo-devel
BuildRequires: cairo-gobject-devel
BuildRequires: gdk-pixbuf2-devel
BuildRequires: atk-devel
BuildRequires: gtk3-devel
BuildRequires: libsoup-devel
BuildRequires: cmake
BuildRequires: clang-devel
BuildRequires: webkit2gtk4.1-devel >= 2.40

%description
RustFS is a high-performance distributed object storage software built using Rust, one of the most popular languages worldwide. Along with MinIO, it shares a range of advantages such as simplicity, S3 compatibility, open-source nature, support for data lakes, AI, and big data. Furthermore, it has a better and more user-friendly open-source license in comparison to other storage systems, being constructed under the Apache license. As Rust serves as its foundation, RustFS provides faster speed and safer distributed features for high-performance object storage.

%prep 
%autosetup -n %{name}-%{version}-%{release}

%build
# Set the target directory according to the schema
export CMAKE=$(which cmake3)
%ifarch x86_64 || aarch64 || loongarch64
    TARGET_DIR="target/%_arch"
    PLATFORM=%_arch-unknown-linux-musl
%else
    TARGET_DIR="target/unknown"
    PLATFORM=unknown-platform
%endif

# Set CARGO_TARGET_DIR and build the project
#CARGO_TARGET_DIR=$TARGET_DIR RUSTFLAGS="-C link-arg=-fuse-ld=mold" cargo build --release --package rustfs
CARGO_TARGET_DIR=$TARGET_DIR RUSTFLAGS="-C link-arg=-fuse-ld=mold" cargo build --release --target $PLATFORM -p rustfs --bins

%install
mkdir -p %buildroot/usr/bin/
install %_builddir/%{name}-%{version}-%{release}/target/%_arch/$PLATFORM/release/rustfs %buildroot/usr/bin/

%files
%license LICENSE
%doc docs
%_bindir/rustfs

%changelog
* Tue Jul 08 2025 Wenlong Zhang <zhangwenlong@loongson.cn>
- Initial RPM package for RustFS 1.0.0-alpha.36
