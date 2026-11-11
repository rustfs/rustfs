#!/usr/bin/env bash
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

PKG_DIR="${DEB_FILE%.deb}"

echo "Building DEB: ${DEB_FILE}"

mkdir -p "${PKG_DIR}/DEBIAN"
mkdir -p "${PKG_DIR}/usr/bin"
mkdir -p "${PKG_DIR}/etc/default"
mkdir -p "${PKG_DIR}/lib/systemd/system"
mkdir -p "${PKG_DIR}/usr/share/doc/rustfs"

cp ./bin/rustfs "${PKG_DIR}/usr/bin/"
chmod 755 "${PKG_DIR}/usr/bin/rustfs"

cp deploy/build/rustfs.service "${PKG_DIR}/lib/systemd/system/"

cat > "${PKG_DIR}/etc/default/rustfs" << 'ENVEOF'
# RustFS Environment Configuration
# See https://docs.rustfs.com for more information
# RUSTFS_ACCESS_KEY=<changeme>
# RUSTFS_SECRET_KEY=<changeme>
# RUSTFS_VOLUMES=""
# RUSTFS_ADDRESS=":9000"
# RUSTFS_CONSOLE_ADDRESS=":9001"
# RUSTFS_CONSOLE_ENABLE=true
# RUSTFS_OBS_LOGGER_LEVEL=error
# RUSTFS_OBS_LOG_DIRECTORY="/var/log/rustfs/"
ENVEOF

cat > "${PKG_DIR}/DEBIAN/control" << EOF
Package: rustfs
Version: ${DEB_VERSION}
Section: utils
Priority: optional
Architecture: ${DEB_ARCH}
EOF

# gnu binaries dynamically link glibc and must declare it; musl
# binaries are statically linked and need no libc dependency.
if [[ -n "$DEB_DEPENDS" ]]; then
  echo "Depends: ${DEB_DEPENDS}" >> "${PKG_DIR}/DEBIAN/control"
fi

cat >> "${PKG_DIR}/DEBIAN/control" << EOF
Maintainer: RustFS Team <support@rustfs.com>
Description: High-performance distributed object storage (${LIBC} build)
 RustFS is a high-performance distributed object storage software
 built using Rust. It is compatible with MinIO and S3 API.
Homepage: https://rustfs.com
EOF

# Declare /etc/default/rustfs as a conffile so dpkg preserves user
# modifications on upgrade instead of silently overwriting them.
cat > "${PKG_DIR}/DEBIAN/conffiles" << 'CONFFILES'
/etc/default/rustfs
CONFFILES

./scripts/release/package_service_scripts.sh after-install > "${PKG_DIR}/DEBIAN/postinst"
chmod 755 "${PKG_DIR}/DEBIAN/postinst"

./scripts/release/package_service_scripts.sh before-remove > "${PKG_DIR}/DEBIAN/prerm"
chmod 755 "${PKG_DIR}/DEBIAN/prerm"

./scripts/release/package_service_scripts.sh after-remove > "${PKG_DIR}/DEBIAN/postrm"
chmod 755 "${PKG_DIR}/DEBIAN/postrm"

cp LICENSE "${PKG_DIR}/usr/share/doc/rustfs/"
cp README.md "${PKG_DIR}/usr/share/doc/rustfs/"

fakeroot dpkg-deb --build "${PKG_DIR}" "$DEB_FILE"

[[ $(dpkg-deb -f "$DEB_FILE" Package) == rustfs ]]
[[ $(dpkg-deb -f "$DEB_FILE" Version) == "$DEB_VERSION" ]]
[[ $(dpkg-deb -f "$DEB_FILE" Architecture) == "$DEB_ARCH" ]]
dpkg-deb --fsys-tarfile "$DEB_FILE" | tar -tf - | grep -Fx './usr/bin/rustfs' >/dev/null
stat --printf='%n %s bytes\n' "$DEB_FILE"
echo "deb_file=$DEB_FILE" >> "$GITHUB_OUTPUT"
echo "✅ DEB built: $DEB_FILE"
