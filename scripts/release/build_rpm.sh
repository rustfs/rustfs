#!/usr/bin/env bash
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

echo "Building RPM for ${RPM_ARCH} (${LIBC})"

./scripts/test_package_versions.sh --require-package-managers

# gnu binaries dynamically link glibc and must declare it; musl
# binaries are statically linked and need no libc dependency.
depends_args=()
if [[ -n "$RPM_DEPENDS" ]]; then
  depends_args+=(--depends "$RPM_DEPENDS")
fi

# Create config file for fpm (DEB build creates it in its package dir structure,
# but fpm needs the file to exist before packaging)
mkdir -p ./tmp-pkg/etc/default
cat > ./tmp-pkg/etc/default/rustfs << 'ENVEOF'
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

fpm -s dir -t rpm \
  --name rustfs \
  --version "$RPM_VERSION" \
  --iteration "$RPM_RELEASE" \
  --architecture "$RPM_ARCH" \
  --package "$RPM_FILE" \
  "${depends_args[@]}" \
  --description "High-performance distributed object storage (${LIBC} build)" \
  --maintainer "RustFS Team <support@rustfs.com>" \
  --url "https://rustfs.com" \
  --license "Apache-2.0" \
  --before-install <(./scripts/release/package_service_scripts.sh rpm-before-install) \
  --before-upgrade <(./scripts/release/package_service_scripts.sh rpm-before-upgrade) \
  --after-install <(./scripts/release/package_service_scripts.sh after-install) \
  --after-upgrade <(./scripts/release/package_service_scripts.sh after-install) \
  --before-remove <(./scripts/release/package_service_scripts.sh before-remove) \
  --after-remove <(./scripts/release/package_service_scripts.sh after-remove) \
  --rpm-posttrans <(./scripts/release/package_service_scripts.sh rpm-posttrans) \
  --config-files /etc/default/rustfs \
  ./bin/rustfs=/usr/bin/rustfs \
  ./tmp-pkg/etc/default/rustfs=/etc/default/rustfs \
  deploy/build/rustfs.service=/lib/systemd/system/rustfs.service \
  LICENSE=/usr/share/doc/rustfs/LICENSE \
  README.md=/usr/share/doc/rustfs/README.md

if [[ ! -f "$RPM_FILE" ]]; then
  echo "❌ RPM build failed"
  exit 1
fi

RPM_METADATA=$(rpm -qp --qf '%{NAME}\n%{VERSION}\n%{RELEASE}\n%{ARCH}\n' "$RPM_FILE")
EXPECTED_METADATA=$(printf 'rustfs\n%s\n%s\n%s' "$RPM_VERSION" "$RPM_RELEASE" "$RPM_ARCH")
[[ "$RPM_METADATA" == "$EXPECTED_METADATA" ]]
rpm -qpl "$RPM_FILE" | grep -Fx '/usr/bin/rustfs' >/dev/null
stat --printf='%n %s bytes\n' "$RPM_FILE"
echo "rpm_file=$RPM_FILE" >> "$GITHUB_OUTPUT"
echo "✅ RPM built: $RPM_FILE"
