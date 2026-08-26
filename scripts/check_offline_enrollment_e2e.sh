#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_root="${repo_root}/rustfs/tests/fixtures/offline-enrollment-e2e"
root_fixture="${fixture_root}/root.json"
challenge_fixture="${fixture_root}/challenge.json"

for command in cargo python3; do
    command -v "${command}" >/dev/null 2>&1 || {
        echo "offline enrollment E2E gate: ${command} is required" >&2
        exit 1
    }
done

read_fixture_field()
{
    python3 -c 'import json, sys; value=json.load(open(sys.argv[1], encoding="utf-8"))[sys.argv[2]]; assert isinstance(value, str) and value; print(value)' "$1" "$2"
}

RUSTFS_E2E_OFFLINE_ENROLLMENT_ROOT_KEY_ID="$(read_fixture_field "${root_fixture}" keyId)"
RUSTFS_E2E_OFFLINE_ENROLLMENT_ROOT_PUBLIC_KEY="$(read_fixture_field "${root_fixture}" publicKey)"
RUSTFS_E2E_OFFLINE_ENROLLMENT_FIXTURE_TIME="$(
    python3 -c 'import base64, datetime, json, sys; envelope=json.load(open(sys.argv[1], encoding="utf-8")); document=json.loads(base64.b64decode(envelope["bytes"], validate=True)); print(int(datetime.datetime.fromisoformat(document["issuedAt"].replace("Z", "+00:00")).timestamp()))' "${challenge_fixture}"
)"
export RUSTFS_E2E_OFFLINE_ENROLLMENT_ROOT_KEY_ID
export RUSTFS_E2E_OFFLINE_ENROLLMENT_ROOT_PUBLIC_KEY
export RUSTFS_E2E_OFFLINE_ENROLLMENT_FIXTURE_TIME

[[ "${RUSTFS_E2E_OFFLINE_ENROLLMENT_ROOT_KEY_ID}" =~ ^[0-9a-f]{64}$ ]] \
    || { echo 'offline enrollment E2E gate: fixture root key id is invalid' >&2; exit 1; }
[[ "${RUSTFS_E2E_OFFLINE_ENROLLMENT_ROOT_PUBLIC_KEY}" =~ ^[A-Za-z0-9_-]{87}$ ]] \
    || { echo 'offline enrollment E2E gate: fixture root public key is invalid' >&2; exit 1; }
[[ "${RUSTFS_E2E_OFFLINE_ENROLLMENT_FIXTURE_TIME}" =~ ^[0-9]+$ ]] \
    || { echo 'offline enrollment E2E gate: fixture evaluation time is invalid' >&2; exit 1; }

task_root="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-offline-enrollment-e2e.XXXXXX")"
case "${task_root}" in
    "${TMPDIR:-/tmp}"/rustfs-offline-enrollment-e2e.*) ;;
    *) echo 'offline enrollment E2E gate: unsafe temporary path' >&2; exit 1 ;;
esac
cleanup()
{
    rm -rf -- "${task_root}"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

export CARGO_TARGET_DIR="${task_root}/target"
feature=offline-enrollment-e2e-root
cargo build --locked -p rustfs --bin rustfs-cli-e2e --features "${feature}"
cargo build --locked -p rustfs --bin rustfs-cli --features "${feature}"
cargo test --locked -p rustfs --test connect_offline_enrollment --features "${feature}" e2e_

"${CARGO_TARGET_DIR}/debug/rustfs-cli-e2e" connect offline enroll \
    --challenge "${challenge_fixture}" \
    --output "${task_root}/e2e-response.json" \
    --key-dir "${task_root}/e2e-key" \
    >"${task_root}/e2e.stdout" 2>"${task_root}/e2e.stderr"
test -s "${task_root}/e2e-response.json" \
    || { echo 'offline enrollment E2E gate: dedicated CLI produced no response' >&2; exit 1; }

if "${CARGO_TARGET_DIR}/debug/rustfs-cli" connect offline enroll \
    --challenge "${challenge_fixture}" \
    --output "${task_root}/production-response.json" \
    --key-dir "${task_root}/production-key" \
    >"${task_root}/production.stdout" 2>"${task_root}/production.stderr"
then
    echo 'offline enrollment E2E gate: production CLI accepted the E2E root' >&2
    exit 1
fi
grep -Fq 'rustfs-cli: the trust chain is not issued by a root pinned in this build' \
    "${task_root}/production.stderr" \
    || { echo 'offline enrollment E2E gate: production CLI failed for the wrong reason' >&2; exit 1; }
test ! -e "${task_root}/production-response.json" \
    || { echo 'offline enrollment E2E gate: rejected production CLI wrote a response' >&2; exit 1; }

echo 'Offline enrollment E2E root gate passed.'
