%global _enable_debug_packages 0
%global _empty_manifest_terminate_build 0
Name:           rustfs
Version:        1.0.0
Release:        alpha.81
Summary:       High-performance distributed object storage for MinIO alternative

License:        Apache-2.0
URL:            https://github.com/rustfs/rustfs
Source0:        https://github.com/rustfs/rustfs/archive/refs/tags/%{version}-%{release}.tar.gz

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

%description
RustFS is a high-performance distributed object storage software built using Rust, one of the most popular languages worldwide. Along with MinIO, it shares a range of advantages such as simplicity, S3 compatibility, open-source nature, support for data lakes, AI, and big data. Furthermore, it has a better and more user-friendly open-source license in comparison to other storage systems, being constructed under the Apache license. As Rust serves as its foundation, RustFS provides faster speed and safer distributed features for high-performance object storage.

%prep 
%autosetup -n %{name}-%{version}-%{release}

%build
# Set the target directory according to the schema
export CMAKE=$(which cmake3)
%ifarch x86_64 || aarch64 || loongarch64
    TARGET_DIR="target/%_arch"
    PLATFORM=%_arch-unknown-linux-gnu
%else
    TARGET_DIR="target/unknown"
    PLATFORM=unknown-platform
%endif

# Set CARGO_TARGET_DIR and build the project
#CARGO_TARGET_DIR=$TARGET_DIR RUSTFLAGS="-C link-arg=-fuse-ld=mold" cargo build --release --package rustfs
%ifarch loongarch64
CFLAGS="-mcmodel=medium" CARGO_TARGET_DIR=$TARGET_DIR RUSTFLAGS="-C link-arg=-fuse-ld=mold -C link-arg=-lm" cargo build --release --target $PLATFORM -p rustfs --bins
%else
CARGO_TARGET_DIR=$TARGET_DIR RUSTFLAGS="-C link-arg=-fuse-ld=mold -C link-arg=-lm" cargo build --release --target $PLATFORM -p rustfs --bins
%endif

%install
mkdir -p %buildroot/usr/bin/
install %_builddir/%{name}-%{version}-%{release}/target/%_arch/%_arch-unknown-linux-gnu/release/rustfs %buildroot/usr/bin/

%files
%license LICENSE
%doc docs
%_bindir/rustfs

%changelog
* Thu Jan 28 2026 houseme <housemecn@gmail.com>
- Initial RPM package for RustFS 1.0.0-alpha.81

* Thu Nov 20 2025 Wenlong Zhang <zhangwenlong@loongson.cn>
- Initial RPM package for RustFS 1.0.0-alpha.69

* Tue Jul 08 2025 Wenlong Zhang <zhangwenlong@loongson.cn>
- Initial RPM package for RustFS 1.0.0-alpha.36
