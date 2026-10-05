#!/usr/bin/env bash

set -euo pipefail

case ${1:-} in
  after-install)
    cat <<'SCRIPT'
#!/bin/sh
set -e
if ! getent passwd rustfs > /dev/null 2>&1; then
    useradd -r -s /bin/false -d /opt/rustfs rustfs
fi
mkdir -p /opt/rustfs /data/rustfs /var/log/rustfs
chown rustfs:rustfs /opt/rustfs /data/rustfs /var/log/rustfs
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload
    if [ "${1:-}" = configure ]; then
        marker=/run/rustfs-package-upgrade-was-active
        if [ -z "${2:-}" ]; then
            rm -f "$marker"
        else
            # Versions published before this fix had no upgrade-aware prerm:
            # the old prerm stopped the service and left no marker, so the
            # marker check below cannot see it. Fall back to restarting for
            # upgrades from those versions. NOTE: these are dpkg control-file
            # versions - package_versions.sh maps the SemVer prerelease "-"
            # to "~", so 1.0.0-rc.5 was published as 1.0.0~rc.5. Drop this list once
            # upgrades from <= 1.0.0 no longer need support.
            case $2 in
                1.0.0|1.0.0~rc.[1-6]|1.0.0~rc.[1-6]~preview.*) legacy_upgrade=true ;;
                *) legacy_upgrade=false ;;
            esac
            if [ -f "$marker" ] || [ "$legacy_upgrade" = true ]; then
                rm -f "$marker"
                if command -v deb-systemd-invoke > /dev/null 2>&1; then
                    deb-systemd-invoke restart rustfs.service
                else
                    systemctl restart rustfs.service
                fi
            fi
        fi
    fi
fi
SCRIPT
    ;;
  before-remove)
    cat <<'SCRIPT'
#!/bin/sh
set -e
case ${1:-} in
    upgrade)
        marker=/run/rustfs-package-upgrade-was-active
        rm -f "$marker"
        if [ -d /run/systemd/system ] && systemctl is-active --quiet rustfs; then
            touch "$marker"
        fi
        exit 0
        ;;
    0|remove|deconfigure) ;;
    *) exit 0 ;;
esac
if [ -d /run/systemd/system ] && systemctl is-active --quiet rustfs; then
    systemctl stop rustfs
fi
SCRIPT
    ;;
  after-remove)
    cat <<'SCRIPT'
#!/bin/sh
set -e
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload
fi
if [ "${1:-}" != upgrade ]; then
    rm -f /run/rustfs-package-upgrade-was-active
fi
SCRIPT
    ;;
  rpm-before-install)
    cat <<'SCRIPT'
#!/bin/sh
set -e
rm -f /run/rustfs-package-upgrade-was-active
SCRIPT
    ;;
  rpm-before-upgrade)
    cat <<'SCRIPT'
#!/bin/sh
set -e
marker=/run/rustfs-package-upgrade-was-active
rm -f "$marker"
if [ -d /run/systemd/system ] && systemctl is-active --quiet rustfs; then
    touch "$marker"
fi
SCRIPT
    ;;
  rpm-posttrans)
    cat <<'SCRIPT'
#!/bin/sh
set -e
marker=/run/rustfs-package-upgrade-was-active
if [ -f "$marker" ]; then
    rm -f "$marker"
    systemctl restart rustfs
fi
SCRIPT
    ;;
  *)
    echo "usage: $0 {after-install|before-remove|after-remove|rpm-before-install|rpm-before-upgrade|rpm-posttrans}" >&2
    exit 2
    ;;
esac
