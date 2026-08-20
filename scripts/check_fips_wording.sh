#!/usr/bin/env bash
set -euo pipefail

# Guard: cryptographic capability wording must not over-claim what RustFS
# actually does. Two independent blocks, both anchored to the policy in
# docs/operations/kms-cryptographic-compliance.md:
#
#   1. Outward README and CHANGELOG material must not make an unsupported
#      FIPS validation or certification claim. This block intentionally scans
#      only the two public project-facing documents; the permitted qualifiers
#      live in the policy document.
#
#   2. Nothing in crates/kms may describe the Vault KV2 backend as wrapping
#      key material through Vault's Transit engine. `KmsBackend::VaultKv2`
#      stores RustFS-wrapped key material in Vault's KV v2 engine and never
#      calls Transit (see crates/kms/src/config.rs and
#      docs/operations/kms-backend-security.md), so such prose tells operators
#      their key material is cryptographically isolated inside Vault when it is
#      not.
#
# Block 2 replaces the unit test `test_vault_kv2_sources_do_not_claim_transit_wrapping`
# that used to live in crates/kms/src/config.rs (rustfs/backlog#1884). The
# invariant is a documentation-claim invariant, so it has no behavioral twin by
# construction and belongs in a wording guard rather than in a test. The test
# could only see four `include_str!`-pinned files and stopped compiling —
# rather than reporting a violation — the moment one of them was renamed; this
# block scans every file in the crate and reports a rename explicitly.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${CHECK_FIPS_WORDING_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"

cd "$ROOT_DIR"

TARGETS=(README.md CHANGELOG.md)
FORBIDDEN_PATTERNS=(
  'FIPS[[:space:]]+(140-[23](/[[:space:]]*140-[23])?[[:space:]]+)?(validated|certified|compliant)'
  'FIPS[[:space:]]+mode'
  'FIPS-enabled'
  'NIST[[:space:]]+(certified|approved)'
  'CMVP[[:space:]]+certificate'
  '(meets|satisfies)[[:space:]]+FIPS'
)

KMS_CRATE_DIR="crates/kms"

# The four files the retired unit test pinned with include_str!. They stay
# listed so that moving one out of crates/kms is reported here instead of
# silently shrinking the scan; the scan itself is not limited to them.
KMS_PINNED_SOURCES=(
  "crates/kms/src/config.rs"
  "crates/kms/src/api_types.rs"
  "crates/kms/src/backends/vault.rs"
  "crates/kms/src/lib.rs"
)

# Literal, case-sensitive, and byte-for-byte the needles the retired test built
# at runtime via format!("wrapping via {}", "Transit") and friends.
KMS_VAULT_KV2_FORBIDDEN=(
  'wrapping via Transit'
  'KV v2 + Transit'
  'KV2+Transit'
  "you would use Vault's transit engine"
)

fips_status=0
kms_status=0

for target in "${TARGETS[@]}"; do
  if [[ ! -f "$target" ]]; then
    printf 'FIPS wording guard failed: %s is missing\n' "$target" >&2
    fips_status=1
    continue
  fi

  for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
    matches="$(grep -E -i -n -- "$pattern" "$target" || true)"
    if [[ -n "$matches" ]]; then
      printf 'FIPS wording guard failed: forbidden pattern /%s/ in %s:\n%s\n' \
        "$pattern" "$target" "$matches" >&2
      fips_status=1
    fi
  done
done

for source in "${KMS_PINNED_SOURCES[@]}"; do
  if [[ ! -f "$source" ]]; then
    printf 'KMS wording guard failed: %s is missing; update KMS_PINNED_SOURCES in scripts/check_fips_wording.sh after moving it\n' \
      "$source" >&2
    kms_status=1
  fi
done

if [[ -d "$KMS_CRATE_DIR" ]]; then
  for pattern in "${KMS_VAULT_KV2_FORBIDDEN[@]}"; do
    matches="$(grep -r -F -n -- "$pattern" "$KMS_CRATE_DIR" || true)"
    if [[ -n "$matches" ]]; then
      printf 'KMS wording guard failed: forbidden Vault KV2 claim "%s" in %s:\n%s\n' \
        "$pattern" "$KMS_CRATE_DIR" "$matches" >&2
      kms_status=1
    fi
  done
fi

if [[ "$fips_status" -ne 0 ]]; then
  printf 'Remove unsupported FIPS validation wording from README.md or CHANGELOG.md.\n' >&2
fi

if [[ "$kms_status" -ne 0 ]]; then
  printf 'The Vault KV2 backend does not wrap key material through Vault Transit; fix the wording in crates/kms.\n' >&2
fi

if [[ "$fips_status" -ne 0 || "$kms_status" -ne 0 ]]; then
  exit 1
fi

printf 'FIPS wording guard passed (README.md and CHANGELOG.md contain no forbidden claims).\n'
printf 'KMS wording guard passed (crates/kms claims no Vault KV2 Transit wrapping).\n'
