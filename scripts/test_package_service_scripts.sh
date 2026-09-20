#!/usr/bin/env bash

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
generator="${repo_root}/scripts/release/package_service_scripts.sh"
test_tmp=$(mktemp -d)
trap 'rm -rf "$test_tmp"' EXIT

mkdir -p "${test_tmp}/bin"
mkdir -p "${test_tmp}/run/systemd/system"
cat > "${test_tmp}/bin/getent" <<'SCRIPT'
#!/bin/sh
exit 0
SCRIPT
cat > "${test_tmp}/bin/mkdir" <<'SCRIPT'
#!/bin/sh
exit 0
SCRIPT
cat > "${test_tmp}/bin/chown" <<'SCRIPT'
#!/bin/sh
exit 0
SCRIPT
cat > "${test_tmp}/bin/systemctl" <<'SCRIPT'
#!/bin/sh
printf 'systemctl %s\n' "$*" >> "$PACKAGE_SCRIPT_LOG"
case "$1" in
    is-active) exit "${SYSTEMCTL_ACTIVE_EXIT:-0}" ;;
esac
SCRIPT
cat > "${test_tmp}/bin/deb-systemd-invoke" <<'SCRIPT'
#!/bin/sh
printf 'deb-systemd-invoke %s\n' "$*" >> "$PACKAGE_SCRIPT_LOG"
SCRIPT
chmod +x "${test_tmp}/bin/"*

export PATH="${test_tmp}/bin:${PATH}"
export PACKAGE_SCRIPT_LOG="${test_tmp}/calls"

run_script() {
  local name=$1
  shift
  "$generator" "$name" |
    sed \
      -e "s#/run/systemd/system#${test_tmp}/run/systemd/system#g" \
      -e "s#/run/rustfs-package-upgrade-was-active#${test_tmp}/run/rustfs-package-upgrade-was-active#g" \
      > "${test_tmp}/${name}"
  chmod +x "${test_tmp}/${name}"
  "${test_tmp}/${name}" "$@"
}

assert_log() {
  local expected=$1
  printf '%s\n' "$expected" > "${test_tmp}/expected"
  diff -u "${test_tmp}/expected" "$PACKAGE_SCRIPT_LOG"
}

: > "$PACKAGE_SCRIPT_LOG"
run_script before-remove upgrade 1.0.0
assert_log 'systemctl is-active --quiet rustfs'
marker="${test_tmp}/run/rustfs-package-upgrade-was-active"
[[ -f $marker ]]

: > "$PACKAGE_SCRIPT_LOG"
run_script after-remove upgrade 1.0.1
[[ -f $marker ]]
run_script after-install configure 1.0.1
[[ ! -e $marker ]]
assert_log $'systemctl daemon-reload\nsystemctl daemon-reload\ndeb-systemd-invoke restart rustfs.service'

: > "$PACKAGE_SCRIPT_LOG"
touch "$marker"
SYSTEMCTL_ACTIVE_EXIT=1 run_script before-remove upgrade 1.0.1
[[ ! -e $marker ]]
run_script after-remove upgrade 1.0.2
run_script after-install configure 1.0.1
assert_log $'systemctl is-active --quiet rustfs\nsystemctl daemon-reload\nsystemctl daemon-reload'

: > "$PACKAGE_SCRIPT_LOG"
run_script after-install configure 1.0.0~rc.5
assert_log $'systemctl daemon-reload\ndeb-systemd-invoke restart rustfs.service'

: > "$PACKAGE_SCRIPT_LOG"
run_script after-install configure 0~dev.9000.0123456789abcdef
assert_log 'systemctl daemon-reload'

: > "$PACKAGE_SCRIPT_LOG"
run_script before-remove remove
assert_log $'systemctl is-active --quiet rustfs\nsystemctl stop rustfs'

: > "$PACKAGE_SCRIPT_LOG"
touch "$marker"
run_script after-remove remove
[[ ! -e $marker ]]
assert_log 'systemctl daemon-reload'

: > "$PACKAGE_SCRIPT_LOG"
touch "$marker"
run_script after-install configure
[[ ! -e $marker ]]
assert_log 'systemctl daemon-reload'

: > "$PACKAGE_SCRIPT_LOG"
run_script rpm-before-upgrade
[[ -f $marker ]]
run_script rpm-posttrans
[[ ! -e $marker ]]
assert_log $'systemctl is-active --quiet rustfs\nsystemctl restart rustfs'

: > "$PACKAGE_SCRIPT_LOG"
SYSTEMCTL_ACTIVE_EXIT=1 run_script rpm-before-upgrade
[[ ! -e $marker ]]
run_script rpm-posttrans
assert_log 'systemctl is-active --quiet rustfs'

echo "PASS package service script contract"
