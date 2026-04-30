#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <tag-or-ref>" >&2
  exit 2
fi

RAW="$1"
case "$RAW" in
  refs/tags/*)
    RAW_TAG="${RAW#refs/tags/}"
    ;;
  *)
    RAW_TAG="$RAW"
    ;;
esac

APP_VERSION="${RAW_TAG#v}"
BETA_NUM=$(printf '%s\n' "$APP_VERSION" | sed -n -E 's/^.*-beta\.([0-9]+)$/\1/p')
if [ -n "$BETA_NUM" ]; then
  CHART_VERSION="0.${BETA_NUM}.0"
else
  CHART_VERSION="$APP_VERSION"
fi

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "raw_tag=$RAW_TAG"
    echo "app_version=$APP_VERSION"
    echo "chart_version=$CHART_VERSION"
  } >>"$GITHUB_OUTPUT"
else
  printf 'raw_tag=%s\n' "$RAW_TAG"
  printf 'app_version=%s\n' "$APP_VERSION"
  printf 'chart_version=%s\n' "$CHART_VERSION"
fi
