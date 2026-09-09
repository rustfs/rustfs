#!/usr/bin/env bash
# Publish the nightly DEB/RPM as assets of the rolling `nightly` release on
# rustfs/auto-testing, replacing the previous build's files in place.
#
# Required environment:
#   ASSETS_TOKEN  token with contents:write on rustfs/auto-testing
#   DEB_FILE      path to the built .deb
#   RPM_FILE      path to the built .rpm
#   DEB_DATE      build date (YYYY-MM-DD)
#   BUILD_REF     branch/ref the nightly was built from
# Optional environment:
#   GITHUB_SHA / GITHUB_RUN_ID / GITHUB_REPOSITORY / GITHUB_SERVER_URL
#
# Plain curl + python3 by design: the nightly build fleet has no gh CLI.

set -euo pipefail

: "${ASSETS_TOKEN:?ASSETS_TOKEN is required}"
: "${DEB_FILE:?DEB_FILE is required}"
: "${RPM_FILE:?RPM_FILE is required}"
: "${DEB_DATE:?DEB_DATE is required}"
: "${BUILD_REF:?BUILD_REF is required}"

for f in "${DEB_FILE}" "${RPM_FILE}"; do
  [ -f "$f" ] || { echo "missing package: $f" >&2; exit 1; }
done

API="https://api.github.com/repos/rustfs/auto-testing"
UPLOADS="https://uploads.github.com/repos/rustfs/auto-testing/releases"
AUTH="Authorization: token ${ASSETS_TOKEN}"
SOURCE_SHA="$(git rev-parse HEAD 2>/dev/null || echo "${GITHUB_SHA:-unknown}")"

release_id="$(curl -fsS --retry 3 -H "${AUTH}" "${API}/releases/tags/nightly" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id", ""))' 2>/dev/null || true)"
if [ -z "${release_id}" ]; then
  echo "creating the rolling nightly release"
  release_id="$(curl -fsS --retry 3 -X POST -H "${AUTH}" -H "Content-Type: application/json" \
    -d '{"tag_name":"nightly","name":"Nightly builds","body":"Rolling nightly builds. Assets are replaced on every build; the release body documents the provenance of the current files."}' \
    "${API}/releases" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')"
fi
[ -n "${release_id}" ] || { echo "could not resolve the nightly release id" >&2; exit 1; }

upload_asset() {
  local name="$1" file="$2" asset_id
  asset_id="$(curl -fsS --retry 3 -H "${AUTH}" "${API}/releases/tags/nightly" \
    | ASSET_NAME="${name}" python3 -c '
import json, sys, os
d = json.load(sys.stdin)
name = os.environ["ASSET_NAME"]
print(next((a["id"] for a in d.get("assets", []) if a["name"] == name), ""))')"
  if [ -n "${asset_id}" ]; then
    curl -fsS --retry 3 -X DELETE -H "${AUTH}" "${API}/releases/assets/${asset_id}" >/dev/null
  fi
  curl -fsS --retry 3 --max-time 900 -X POST \
    -H "${AUTH}" -H "Content-Type: application/octet-stream" \
    --data-binary "@${file}" \
    "${UPLOADS}/${release_id}/assets?name=${name}" >/dev/null
  echo "uploaded ${name}"
}

upload_asset "rustfs-nightly-latest.deb" "${DEB_FILE}"
upload_asset "rustfs-nightly-latest.rpm" "${RPM_FILE}"

DEB_SHA="$(sha256sum "${DEB_FILE}" | cut -d ' ' -f 1)"
RPM_SHA="$(sha256sum "${RPM_FILE}" | cut -d ' ' -f 1)"
RUN_URL="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-/rustfs/rustfs}/actions/runs/${GITHUB_RUN_ID:-0}"
export BUILD_REF SOURCE_SHA DEB_DATE DEB_FILE RPM_FILE DEB_SHA RPM_SHA RUN_URL

python3 - << 'PY' > /tmp/release-body.json
import json, os
e = os.environ
deb_mb = os.path.getsize(e["DEB_FILE"]) // 1048576
rpm_mb = os.path.getsize(e["RPM_FILE"]) // 1048576
body = (
    f"Nightly build from `{e['BUILD_REF']}@{e['SOURCE_SHA'][:12]}`, built {e['DEB_DATE']}.\n\n"
    f"[Build run]({e['RUN_URL']}). The `latest` assets are replaced in place on every nightly.\n\n"
    f"| Asset | Size | SHA256 |\n|---|---|---|\n"
    f"| rustfs-nightly-latest.deb (={e['DEB_FILE']}) | {deb_mb} MB | `{e['DEB_SHA']}` |\n"
    f"| rustfs-nightly-latest.rpm (={e['RPM_FILE']}) | {rpm_mb} MB | `{e['RPM_SHA']}` |\n"
)
print(json.dumps({"body": body}))
PY

curl -fsS --retry 3 -X PATCH -H "${AUTH}" -H "Content-Type: application/json" \
  --data-binary @/tmp/release-body.json "${API}/releases/${release_id}" >/dev/null

echo "published ${DEB_FILE} and ${RPM_FILE} to the rustfs/auto-testing 'nightly' release"
