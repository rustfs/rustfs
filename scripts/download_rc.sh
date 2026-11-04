#!/bin/sh
set -eu

if [ "$#" -ne 4 ]; then
  echo "usage: download_rc.sh <amd64|arm64> <musl|gnu> <release|latest> <output>" >&2
  exit 2
fi

ARCH="$1"
LIBC="$2"
RELEASE="$3"
OUTPUT="$4"

case "$ARCH" in
  amd64|arm64) ;;
  *) echo "Unsupported rc architecture: $ARCH" >&2; exit 1 ;;
esac
case "$LIBC" in
  musl) LIBC_SUFFIX="" ;;
  gnu) LIBC_SUFFIX="-gnu" ;;
  *) echo "Unsupported rc libc: $LIBC" >&2; exit 1 ;;
esac

if [ "$RELEASE" = "latest" ]; then
  RELEASE_JSON=$(curl -fsSL https://api.github.com/repos/rustfs/cli/releases/latest)
else
  TAG="$RELEASE"
  case "$TAG" in v*) ;; *) TAG="v$TAG" ;; esac
  RELEASE_JSON=$(curl -fsSL "https://api.github.com/repos/rustfs/cli/releases/tags/$TAG")
fi

TAG=$(printf '%s' "$RELEASE_JSON" | jq -er '.tag_name')
ASSET_NAME="rustfs-cli-linux-${ARCH}${LIBC_SUFFIX}-${TAG}.tar.gz"
ASSET_JSON=$(printf '%s' "$RELEASE_JSON" | jq -cer --arg name "$ASSET_NAME" '.assets[] | select(.name == $name)')
ASSET_URL=$(printf '%s' "$ASSET_JSON" | jq -er '.browser_download_url')
ASSET_DIGEST=$(printf '%s' "$ASSET_JSON" | jq -er '.digest | select(startswith("sha256:")) | sub("^sha256:"; "")')
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT HUP INT TERM

curl -fsSL "$ASSET_URL" -o "$TEMP_DIR/rc.tar.gz"
printf '%s  %s\n' "$ASSET_DIGEST" "$TEMP_DIR/rc.tar.gz" | sha256sum -c -
tar -tzf "$TEMP_DIR/rc.tar.gz" | grep -Fxq rc || {
  echo "The RustFS CLI archive does not contain rc" >&2
  exit 1
}
tar -xOzf "$TEMP_DIR/rc.tar.gz" rc > "$OUTPUT"
chmod 0755 "$OUTPUT"
