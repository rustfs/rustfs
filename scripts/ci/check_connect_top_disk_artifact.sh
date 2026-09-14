#!/usr/bin/env bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 <rustfs-binary> <source-sha> <binary-sha256> <evidence-json> <archive-output>" >&2
  exit 2
fi

binary=$(realpath "$1")
expected_source_sha=$2
expected_binary_sha256=$3
evidence_json=$4
archive_output=$5
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

actual_binary_sha256=$(sha256sum "$binary" | awk '{print $1}')
[[ "$actual_binary_sha256" == "$expected_binary_sha256" ]]
runner_architecture=$(uname -m)
[[ "$runner_architecture" == x86_64 ]]

version=$($binary --version)
grep -Fq "git commit   : $expected_source_sha" <<<"$version"
grep -Fq "build profile: release" <<<"$version"
grep -Fq "build os     : linux-x86_64" <<<"$version"

mkdir -p "$work_dir/state/identity" "$work_dir/archive"
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$work_dir/device.pem" >/dev/null 2>&1
openssl pkcs8 -topk8 -nocrypt -in "$work_dir/device.pem" -outform DER -out "$work_dir/state/identity/device.key"
chmod 600 "$work_dir/state/identity/device.key"

now=$(date +%s)
expires_at=$((now + 600))
organization=organizations/019e3ae0-0000-7000-8000-000000000010
cluster=$organization/clusters/019e3ae0-0000-7000-8000-000000000011
device=$cluster/clusterDevices/019e3ae0-0000-7000-8000-000000000012
run_uid=019e3ae0-0000-7000-8000-000000000001
artifact_uid=019e3ae0-0000-7000-8000-000000000013
consent_uid=019e3ae0-0000-7000-8000-000000000014

common_args=(
  connect top disk
  --state-dir "$work_dir/state"
  --organization "$organization"
  --cluster "$cluster"
  --device "$device"
  --run-uid "$run_uid"
  --artifact-uid "$artifact_uid"
  --consent-uid "$consent_uid"
  --policy-revision 1
  --export-validity-seconds 300
)

output=$work_dir/top-disk.zip
success_log=$work_dir/success.log
"$binary" "${common_args[@]}" --output "$output" --consent-expires-at "$expires_at" --run-expires-at "$expires_at" --window-millis 500 --acknowledge-l3 >"$success_log"
result_json=$(sed -n 's/^result=//p' "$success_log")
[[ $(jq -r '.outcome' <<<"$result_json") == SUCCEEDED ]]
[[ $(jq -r '.reasonCode' <<<"$result_json") == COMPLETE ]]
[[ $(jq -r '.provenance.sourceCommit' <<<"$result_json") == "$expected_source_sha" ]]
[[ $(jq -r '.provenance.executableSha256' <<<"$result_json") == "$expected_binary_sha256" ]]
[[ $(jq -r '.provenance.osFamily' <<<"$result_json") == LINUX ]]
[[ $(jq -r '.provenance.architecture' <<<"$result_json") == x86_64 ]]
[[ $(jq -r '.data.resourceAlias' <<<"$result_json") == resource-1 ]]
counter_total=$(jq '[.data.readBytes, .data.writeBytes, .data.ioCount] | add' <<<"$result_json")
((counter_total > 0))

reported_archive_sha256=$(sed -n 's/^artifact=[^ ]* bytes=[^ ]* sha256=//p' "$success_log")
actual_archive_sha256=$(sha256sum "$output" | awk '{print $1}')
[[ "$actual_archive_sha256" == "$reported_archive_sha256" ]]
unzip -qq "$output" -d "$work_dir/archive"
mapfile -t archive_members < <(find "$work_dir/archive" -maxdepth 1 -type f -printf '%f\n' | sort)
[[ "${archive_members[*]}" == "envelope.json envelope.sig result.json" ]]
cmp -s <(jq -cS . <<<"$result_json") <(jq -cS . "$work_dir/archive/result.json")
result_sha256=$(sha256sum "$work_dir/archive/result.json" | awk '{print $1}')
[[ $(jq -r '.payload.sha256' "$work_dir/archive/envelope.json") == "$result_sha256" ]]
[[ $(jq -r '.algorithm' "$work_dir/archive/envelope.sig") == ES256 ]]

python3 - "$work_dir/archive/envelope.sig" "$work_dir/signature.der" <<'PY'
import base64
import json
import sys

signature = json.load(open(sys.argv[1], encoding="utf-8"))["value"]
raw = base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4))
if len(raw) != 64:
    raise SystemExit("ES256 signature is not 64 bytes")

def integer(value: bytes) -> bytes:
    value = value.lstrip(b"\0") or b"\0"
    if value[0] & 0x80:
        value = b"\0" + value
    return b"\x02" + bytes([len(value)]) + value

body = integer(raw[:32]) + integer(raw[32:])
open(sys.argv[2], "wb").write(b"\x30" + bytes([len(body)]) + body)
PY
printf 'rustfs-diagnostic-envelope-v1\0' >"$work_dir/signed-input"
cat "$work_dir/archive/envelope.json" >>"$work_dir/signed-input"
openssl pkey -in "$work_dir/device.pem" -pubout -out "$work_dir/device.pub" >/dev/null 2>&1
openssl dgst -sha256 -verify "$work_dir/device.pub" -signature "$work_dir/signature.der" "$work_dir/signed-input"

if grep -R -F "$work_dir" "$work_dir/archive" || grep -R -F 'device.key' "$work_dir/archive"; then
  echo "top.disk export leaked a local path" >&2
  exit 1
fi

set +e
"$binary" "${common_args[@]}" --output "$work_dir/no-consent.zip" --consent-expires-at "$expires_at" --run-expires-at "$expires_at" --window-millis 500 >"$work_dir/no-consent.log" 2>&1
no_consent_status=$?
set -e
((no_consent_status != 0))
[[ ! -e "$work_dir/no-consent.zip" ]]

set +e
"$binary" "${common_args[@]}" --output "$work_dir/expired.zip" --consent-expires-at "$((now - 1))" --run-expires-at "$((now - 1))" --window-millis 500 --acknowledge-l3 >"$work_dir/expired.log" 2>&1
expired_status=$?
set -e
((expired_status != 0))
[[ ! -e "$work_dir/expired.zip" ]]

set +e
"$binary" "${common_args[@]}" --output "$work_dir/over-limit.zip" --consent-expires-at "$expires_at" --run-expires-at "$expires_at" --window-millis 30001 --acknowledge-l3 >"$work_dir/over-limit.log" 2>&1
limit_status=$?
set -e
((limit_status != 0))
[[ ! -e "$work_dir/over-limit.zip" ]]

printf 'keep-existing\n' >"$work_dir/existing.zip"
set +e
"$binary" "${common_args[@]}" --output "$work_dir/existing.zip" --consent-expires-at "$expires_at" --run-expires-at "$expires_at" --window-millis 500 --acknowledge-l3 >"$work_dir/existing.log" 2>&1
existing_status=$?
set -e
((existing_status != 0))
[[ $(cat "$work_dir/existing.zip") == keep-existing ]]

interrupt_output=$work_dir/interrupted.zip
"$binary" "${common_args[@]}" --output "$interrupt_output" --consent-expires-at "$expires_at" --run-expires-at "$expires_at" --window-millis 30000 --acknowledge-l3 >"$work_dir/interrupted.log" 2>&1 &
interrupt_pid=$!
sleep 1
kill -INT "$interrupt_pid"
set +e
wait "$interrupt_pid"
interrupt_status=$?
set -e
((interrupt_status != 0))
[[ ! -e "$interrupt_output" ]]
if find "$work_dir" -name '*.partial' -print -quit | grep -q .; then
  echo "interrupted capture left a partial file" >&2
  exit 1
fi

if [[ -e "$archive_output" ]]; then
  echo "refusing to replace existing archive output: $archive_output" >&2
  exit 1
fi
cp -- "$output" "$archive_output"

jq -n \
  --arg sourceSha "$expected_source_sha" \
  --arg binarySha256 "$actual_binary_sha256" \
  --arg runnerArchitecture "$runner_architecture" \
  --arg archiveSha256 "$actual_archive_sha256" \
  --argjson result "$result_json" \
  '{sourceSha: $sourceSha, binarySha256: $binarySha256, runnerArchitecture: $runnerArchitecture, archiveSha256: $archiveSha256, invocation: "scripts/ci/check_connect_top_disk_artifact.sh <rustfs-binary> <source-sha> <binary-sha256> <evidence-json>", result: $result, controls: {signature: "VERIFIED", consent: "REJECTED_WITHOUT_ACKNOWLEDGEMENT", expiry: "REJECTED", limits: "REJECTED", sigint: "CANCELLED_WITHOUT_OUTPUT", noClobber: "PRESERVED", redaction: "VERIFIED"}}' >"$evidence_json"

jq '{sourceSha, binarySha256, runnerArchitecture, archiveSha256, result: {outcome: .result.outcome, reasonCode: .result.reasonCode, durationMillis: .result.durationMillis, data: .result.data}, controls}' "$evidence_json"
