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

if [[ $# -ne 6 ]]; then
  echo "usage: $0 <rustfs-binary> <source-sha> <binary-sha256> <evidence-json> <archive-output> <bundle-output>" >&2
  exit 2
fi

binary=$(realpath "$1")
expected_source_sha=$2
expected_binary_sha256=$3
evidence_json=$4
archive_output=$5
bundle_output=$6
work_dir=$(mktemp -d)
load_pids=()
cleanup() {
  if ((${#load_pids[@]} > 0)); then
    kill "${load_pids[@]}" 2>/dev/null || true
    wait "${load_pids[@]}" 2>/dev/null || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

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
organization=organizations/019e3ae0-1111-7000-8000-000000000001
cluster=$organization/clusters/019e3ae0-1111-7000-8000-000000000002
device=$cluster/clusterDevices/019e3ae0-1111-7000-8000-000000000003
consent_uid=019e3ae0-1111-7000-8000-000000000006

profile() {
  local run_uid=$1
  local artifact_uid=$2
  local output=$3
  local duration=$4
  shift 4
  "$binary" connect profile \
    --state-dir "$work_dir/state" \
    --output "$output" \
    --tool cpu \
    --capability profile.cpu@1 \
    --organization "$organization" \
    --cluster "$cluster" \
    --device "$device" \
    --run-uid "$run_uid" \
    --artifact-uid "$artifact_uid" \
    --consent-uid "$consent_uid" \
    --policy-revision 2 \
    --consent-expires-at "$expires_at" \
    --expires-at "$expires_at" \
    --duration-millis "$duration" \
    --sample-period-micros 10000 \
    "$@"
}

for _ in 1 2; do
  yes >/dev/null &
  load_pids+=("$!")
done
output=$work_dir/profile-cpu.zip
success_log=$work_dir/success.log
profile 019e3ae0-1111-7000-8000-000000000004 019e3ae0-1111-7000-8000-000000000005 "$output" 2000 --acknowledge-l3 >"$success_log"
kill "${load_pids[@]}"
wait "${load_pids[@]}" 2>/dev/null || true
load_pids=()

grep -Fq 'tool=profile.cpu outcome=' "$success_log"
reported_archive_sha256=$(sed -n 's/^artifact=[^ ]* bytes=[^ ]* sha256=//p' "$success_log")
actual_archive_sha256=$(sha256sum "$output" | awk '{print $1}')
[[ "$actual_archive_sha256" == "$reported_archive_sha256" ]]
unzip -qq "$output" -d "$work_dir/archive"
mapfile -t archive_members < <(find "$work_dir/archive" -maxdepth 1 -type f -printf '%f\n' | sort)
[[ "${archive_members[*]}" == "envelope.json envelope.sig result.json" ]]
result=$work_dir/archive/result.json
envelope=$work_dir/archive/envelope.json
[[ $(jq -r '.outcome' "$result") =~ ^(SUCCEEDED|PARTIAL)$ ]]
[[ $(jq -r '.reasonCode' "$result") =~ ^(COMPLETE|LIMIT_EXCEEDED)$ ]]
[[ $(jq -r '.provenance.sourceCommit' "$result") == "$expected_source_sha" ]]
[[ $(jq -r '.provenance.executableSha256' "$result") == "$expected_binary_sha256" ]]
[[ $(jq -r '.provenance.osFamily' "$result") == LINUX ]]
[[ $(jq -r '.provenance.architecture' "$result") == x86_64 ]]
jq -e '.provenance.buildFeatures | index("pyroscope") != null' "$result" >/dev/null
jq -e '.data.samples | length > 0 and length <= 256' "$result" >/dev/null
jq -e 'all(.data.samples[]; (.symbolId | test("^sha256:[0-9a-f]{64}$")) and (.sampleCount > 0))' "$result" >/dev/null
jq -e '(.data.droppedSampleCount >= 0) and (.data.samplePeriodMicros == 10000)' "$result" >/dev/null
result_sha256=$(sha256sum "$result" | awk '{print $1}')
[[ $(jq -r '.payload.sha256' "$envelope") == "$result_sha256" ]]
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
cat "$envelope" >>"$work_dir/signed-input"
openssl pkey -in "$work_dir/device.pem" -pubout -out "$work_dir/device.pub" >/dev/null 2>&1
openssl pkey -in "$work_dir/device.pem" -pubout -outform DER -out "$work_dir/device.pub.der" >/dev/null 2>&1
device_key_id=$(sha256sum "$work_dir/device.pub.der" | awk '{print $1}')
device_public_key=$(tail -c 65 "$work_dir/device.pub.der" | base64 -w0 | tr '+/' '-_' | tr -d '=')
[[ $(jq -r '.deviceKeyId' "$envelope") == "$device_key_id" ]]
openssl dgst -sha256 -verify "$work_dir/device.pub" -signature "$work_dir/signature.der" "$work_dir/signed-input"

bundle_uid=019e3ae0-1111-7000-8000-000000000015
bundle_dir=$work_dir/bundle
mkdir -p "$bundle_dir"
cp "$envelope" "$work_dir/archive/envelope.sig" "$result" "$bundle_dir"
for member in envelope.json envelope.sig result.json; do
  size=$(stat -c %s "$bundle_dir/$member")
  digest=$(sha256sum "$bundle_dir/$member" | awk '{print $1}')
  jq -n \
    --arg path "$member" \
    --argjson sizeBytes "$size" \
    --arg sha256 "$digest" \
    '{path: $path, type: "offline-diagnostic", sizeBytes: $sizeBytes, sha256: $sha256, classification: "L3"}' \
    >"$work_dir/$member.entry.json"
done
jq -n \
  --arg bundleUid "$bundle_uid" \
  --arg organizationName "$organization" \
  --arg clusterName "$cluster" \
  --arg deviceName "$device" \
  --arg deviceKeyId "$device_key_id" \
  --arg nonce "$(jq -r '.nonce' "$envelope")" \
  --arg producedAt "$(jq -r '.producedAt' "$envelope")" \
  --slurpfile envelopeEntry "$work_dir/envelope.json.entry.json" \
  --slurpfile signatureEntry "$work_dir/envelope.sig.entry.json" \
  --slurpfile resultEntry "$work_dir/result.json.entry.json" \
  '{formatVersion: "rustfs.connect.support.bundleManifest/1", protocolVersion: "v1", bundleUid: $bundleUid, organizationName: $organizationName, clusterName: $clusterName, deviceName: $deviceName, deviceKeyId: $deviceKeyId, nonce: $nonce, producedAt: $producedAt, redactionVersion: "rustfs.connect.redaction.v1", rulesetHash: "b37436d8e72515394a122d633865b1dc028d4ece349352a0a3a23f52ca4285f3", classificationRegistryVersion: 1, entries: [$envelopeEntry[0], $signatureEntry[0], $resultEntry[0]]}' \
  >"$bundle_dir/manifest.json"
printf 'rustfs-support-bundle-v1\0' >"$work_dir/manifest-signed-input"
cat "$bundle_dir/manifest.json" >>"$work_dir/manifest-signed-input"
openssl dgst -sha256 -sign "$work_dir/device.pem" -out "$work_dir/manifest-signature.der" "$work_dir/manifest-signed-input"
manifest_signature=$(python3 - "$work_dir/manifest-signature.der" <<'PY'
import base64
import sys

der = open(sys.argv[1], "rb").read()
if len(der) < 8 or der[0] != 0x30 or der[2] != 0x02:
    raise SystemExit("manifest signature is not a DER ECDSA sequence")
offset = 3
r_length = der[offset]
offset += 1
r = int.from_bytes(der[offset:offset + r_length], "big")
offset += r_length
if der[offset] != 0x02:
    raise SystemExit("manifest signature has no S integer")
offset += 1
s_length = der[offset]
offset += 1
s = int.from_bytes(der[offset:offset + s_length], "big")
order = int("ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551", 16)
if s > order // 2:
    s = order - s
raw = r.to_bytes(32, "big") + s.to_bytes(32, "big")
print(base64.urlsafe_b64encode(raw).decode().rstrip("="))
PY
)
jq -n --arg keyId "$device_key_id" --arg value "$manifest_signature" \
  '{algorithm: "ES256", keyId: $keyId, value: $value}' >"$bundle_dir/manifest.sig"
(cd "$bundle_dir" && zip -q -0 "$work_dir/profile-cpu-bundle.zip" manifest.json manifest.sig envelope.json envelope.sig result.json)
bundle_archive_sha256=$(sha256sum "$work_dir/profile-cpu-bundle.zip" | awk '{print $1}')

if grep -R -E '(/home/|device\.key|thread(Name|Id)?|0x[0-9a-fA-F]+|rustfs::)' "$work_dir/archive"; then
  echo "profile.cpu export leaked raw process material" >&2
  exit 1
fi

set +e
profile 019e3ae0-1111-7000-8000-000000000007 019e3ae0-1111-7000-8000-000000000008 "$work_dir/no-consent.zip" 500 >"$work_dir/no-consent.log" 2>&1
no_consent_status=$?
set -e
((no_consent_status != 0))
[[ ! -e "$work_dir/no-consent.zip" ]]

set +e
"$binary" connect profile --state-dir "$work_dir/state" --output "$work_dir/expired.zip" --tool cpu --capability profile.cpu@1 --organization "$organization" --cluster "$cluster" --device "$device" --run-uid 019e3ae0-1111-7000-8000-000000000009 --artifact-uid 019e3ae0-1111-7000-8000-00000000000a --consent-uid "$consent_uid" --policy-revision 2 --consent-expires-at "$((now - 1))" --expires-at "$((now - 1))" --duration-millis 500 --sample-period-micros 10000 --acknowledge-l3 >"$work_dir/expired.log" 2>&1
expired_status=$?
set -e
((expired_status != 0))
[[ ! -e "$work_dir/expired.zip" ]]

set +e
profile 019e3ae0-1111-7000-8000-00000000000b 019e3ae0-1111-7000-8000-00000000000c "$work_dir/over-limit.zip" 30001 --acknowledge-l3 >"$work_dir/over-limit.log" 2>&1
limit_status=$?
set -e
((limit_status != 0))
[[ ! -e "$work_dir/over-limit.zip" ]]

printf 'keep-existing\n' >"$work_dir/existing.zip"
set +e
profile 019e3ae0-1111-7000-8000-00000000000d 019e3ae0-1111-7000-8000-00000000000e "$work_dir/existing.zip" 500 --acknowledge-l3 >"$work_dir/existing.log" 2>&1
existing_status=$?
set -e
((existing_status != 0))
[[ $(cat "$work_dir/existing.zip") == keep-existing ]]

interrupt_output=$work_dir/interrupted.zip
profile 019e3ae0-1111-7000-8000-00000000000f 019e3ae0-1111-7000-8000-000000000010 "$interrupt_output" 30000 --acknowledge-l3 >"$work_dir/interrupted.log" 2>&1 &
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

busy_first=$work_dir/busy-first.zip
busy_second=$work_dir/busy-second.zip
profile 019e3ae0-1111-7000-8000-000000000011 019e3ae0-1111-7000-8000-000000000012 "$busy_first" 3000 --acknowledge-l3 >"$work_dir/busy-first.log" 2>&1 &
busy_pid=$!
sleep 1
set +e
profile 019e3ae0-1111-7000-8000-000000000013 019e3ae0-1111-7000-8000-000000000014 "$busy_second" 500 --acknowledge-l3 >"$work_dir/busy-second.log" 2>&1
busy_second_status=$?
set -e
wait "$busy_pid"
((busy_second_status != 0))
[[ -e "$busy_first" ]]
[[ ! -e "$busy_second" ]]
grep -Fq 'profile_collection_already_running' "$work_dir/busy-second.log"

if [[ -e "$archive_output" ]]; then
  echo "refusing to replace existing archive output: $archive_output" >&2
  exit 1
fi
if [[ -e "$bundle_output" ]]; then
  echo "refusing to replace existing bundle output: $bundle_output" >&2
  exit 1
fi
cp -- "$output" "$archive_output"
cp -- "$work_dir/profile-cpu-bundle.zip" "$bundle_output"
result_json=$(jq -c . "$result")
jq -n \
  --arg sourceSha "$expected_source_sha" \
  --arg binarySha256 "$actual_binary_sha256" \
  --arg runnerArchitecture "$runner_architecture" \
  --arg archiveSha256 "$actual_archive_sha256" \
  --arg bundleArchiveSha256 "$bundle_archive_sha256" \
  --arg deviceKeyId "$device_key_id" \
  --arg devicePublicKey "$device_public_key" \
  --argjson result "$result_json" \
  '{sourceSha: $sourceSha, binarySha256: $binarySha256, runnerArchitecture: $runnerArchitecture, archiveSha256: $archiveSha256, bundleArchiveSha256: $bundleArchiveSha256, deviceKeyId: $deviceKeyId, devicePublicKey: $devicePublicKey, invocation: "scripts/ci/check_connect_profile_cpu_artifact.sh <rustfs-binary> <source-sha> <binary-sha256> <evidence-json>", result: $result, controls: {signature: "VERIFIED", consent: "REJECTED_WITHOUT_ACKNOWLEDGEMENT", expiry: "REJECTED", limits: "REJECTED", sigint: "CANCELLED_WITHOUT_OUTPUT", noClobber: "PRESERVED", singleCollector: "SECOND_CAPTURE_REJECTED", redaction: "VERIFIED"}}' >"$evidence_json"
jq '{sourceSha, binarySha256, runnerArchitecture, archiveSha256, bundleArchiveSha256, result: {outcome: .result.outcome, reasonCode: .result.reasonCode, durationMillis: .result.durationMillis, sampleCount: (.result.data.samples | length), totalSamples: ([.result.data.samples[].sampleCount] | add), droppedSampleCount: .result.data.droppedSampleCount}, controls}' "$evidence_json"
