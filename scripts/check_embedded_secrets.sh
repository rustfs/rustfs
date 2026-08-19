#!/usr/bin/env bash
set -euo pipefail

# Guard: no private key material and no long-lived provider credential may
# exist as a literal anywhere in the repository — see AGENTS.md "Security
# Baseline" ("Never commit secrets, credentials, or key material") and
# .agents/skills/security-advisory-lessons ("Do not ship hard-coded shared
# tokens, HMAC secrets, private keys, or production test keys").
#
# This replaces the unit test `test_source_does_not_embed_private_key` that
# used to live in crates/crypto/src/license_token.rs (rustfs/backlog#1884).
# That test read its own file with include_str! and asserted the file did not
# contain a PEM private-key header, protecting exactly one invariant: the RSA
# key that signs license tokens must never be checked in, because verification
# only ever needs the public key (crates/crypto/src/license_token.rs exposes
# `parse_signed_license_token`, and rustfs/src/license.rs reads the public key
# from RUSTFS_LICENSE_PUBLIC_KEY at runtime — no key material belongs in the
# tree at all). Its coverage was one file: moving the key one file sideways,
# even inside the same crate, passed silently, and renaming license_token.rs
# stopped the guard from compiling rather than reporting anything.
#
# This scan covers every tracked — and every not-yet-added, non-ignored — text
# file in the repository, so it is a strict superset of the retired assertion:
# the same needle, everywhere, plus the algorithm variants and the credential
# formats below.
#
# It deliberately does not exclude the paths .github/secret_scanning.yml tells
# GitHub push protection to ignore (crates/e2e_test, **/tests, **/benches,
# .docker, .vscode). Those exclusions exist because pasted test credentials are
# expected there, which is exactly where a real key is most likely to arrive
# unnoticed; this guard is the CI-side gate that still looks.
#
# Only literals are in scope. Key material injected at build time is out of
# scope on purpose: no build.rs in the workspace embeds key material and the
# license public key is read from the environment at startup, so an artifact
# scan would add a release build to a compile-free check job for no reachable
# failure mode today. Revisit if a build script ever bakes in key material.
# Binary files are skipped (`git grep -I`), and a key stored as bare base64
# with its header stripped is not detected — the same two blind spots the
# retired test had.
#
# Every needle below is assembled around a variable so that the script's own
# text does not match the pattern it defines (the retired test used the same
# trick with ["BEGIN", "PRIVATE KEY"].join(" ")). That is what lets this script
# scan itself along with everything else instead of carving out a blind spot.
#
# `--self-test` builds throwaway fixture repositories and asserts every pattern
# family fires and every exemption holds; it is wired into `make script-tests`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${CHECK_EMBEDDED_SECRETS_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"

BEGIN_MARK="BEGIN"
KEY_MARK="Key"

# The file the retired test pinned with include_str!. It stays listed so that
# renaming or moving it is reported here explicitly instead of quietly ending
# the license-key invariant, which is how the test failed.
PINNED_SOURCES=(
    "crates/crypto/src/license_token.rs"
)

# "<name>|<extended regex>". The name is free of "|", so the first "|" splits.
#
# The private-key family matches the header phrase without requiring the PEM
# dashes, so a key pasted into a JSON/YAML string, a doc block, or a Rust
# string built without the delimiters is still caught. The credential family is
# format-anchored — fixed prefix plus fixed-width charset — so a match is a
# credential shape and not prose.
PATTERNS=(
    "PEM private key header|${BEGIN_MARK}[[:space:]]+([A-Z0-9]+[[:space:]]+)*PRIVATE KEY"
    "PuTTY private key file|PuTTY-User-${KEY_MARK}-File"
    "AWS access key id|(A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}"
    "GitHub token|gh[pousr]_[A-Za-z0-9]{36}"
    "GitHub fine-grained token|github_pat_[A-Za-z0-9_]{22,}"
    "Slack token|xox[abprs]-[A-Za-z0-9-]{10,}"
    "Stripe live key|sk_live_[0-9a-zA-Z]{20,}"
    "Google API key|AIza[0-9A-Za-z_-]{35}"
    "SendGrid API key|SG\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}"
    "npm access token|npm_[A-Za-z0-9]{36}"
    "PyPI upload token|pypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{50,}"
)

# Exact strings that carry no secret wherever they appear. A hit is excused
# only if the line stops matching once these exact strings are removed, so a
# line holding both an example value and a real credential still fails, and
# editing an entry — swapping a placeholder body for real key material — makes
# the guard fire again. Entries that stop matching anything are reported as
# stale, so the list cannot decay into a blanket exclusion.
#
# 1-2: rustfs/src/admin/handlers/site_replication.rs negative fixtures for
#      `validate_peer_connection_inner`, which must reject a private key
#      submitted where a peer CA certificate is expected. Asserting on the
#      rejection requires the header in the input; the key bodies are the
#      literal word "secret".
# 3-4: AWS's own documented example access key id from the SigV4 test vectors,
#      which this repository pairs with the equally documented example secret
#      key across signer, IAM, madmin, and auth tests, plus the deliberate
#      one-character variant rustfs/src/auth.rs uses to prove key comparison
#      distinguishes near-identical ids.
# 5-7: the Connect agent protocol fixtures under protocol/agent/v1/fixtures,
#      which this repository carries as a byte-identical mirror of the Connect
#      tree (rustfs/tests/agent_protocol_fixtures.rs pins every set against its
#      MANIFEST.sha256, so the vectors cannot be reworded on this side). Their
#      subject *is* key material that the inventory schema must be unable to
#      carry and the redaction ruleset must replace, so the header has to appear
#      in the input. Entry 5 carries the closing quote, so it excuses only a
#      JSON string that ends at the header and can therefore hold no key body;
#      a header followed by one still fires. Entries 6-7 carry their bodies,
#      both unusable: 6 is a PKCS#8 wrapper whose OCTET STRING declares 32
#      bytes and holds the 7 ASCII bytes "example", and 7 spells out in the
#      body that it is not a real key.
AWS_EXAMPLE_STEM="AKIAIOSFODNN7EXAMPL"
AGENT_FIXTURE_RSA_BODY="MIIEowIBAAKCAQEAxEXAMPLEKEYBODYnotarealkey0000000000000000000000"
NON_SECRET_LITERALS=(
    "-----${BEGIN_MARK} PRIVATE KEY-----\\nsecret\\n-----END PRIVATE KEY-----"
    "-----${BEGIN_MARK} RSA PRIVATE KEY-----\\nsecret\\n-----END RSA PRIVATE KEY-----"
    "${AWS_EXAMPLE_STEM}E"
    "${AWS_EXAMPLE_STEM}F"
    "\"-----${BEGIN_MARK} PRIVATE KEY-----\""
    "-----${BEGIN_MARK} PRIVATE KEY-----\\nMEECAQAwEwYHKoZIzj0CAQYIKoZIzj0DAQcEJzAlAgEBBCBleGFtcGxl\\n-----END PRIVATE KEY-----"
    "-----${BEGIN_MARK} RSA PRIVATE KEY-----\\n${AGENT_FIXTURE_RSA_BODY}\\nEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLE=\\n-----END RSA PRIVATE KEY-----"
)

run_scan() {
    cd "$ROOT_DIR"

    # Without this, a scan run outside a work tree would make every `git grep`
    # fail and the guard would report success having read nothing.
    if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        printf 'Embedded secret guard failed: %s is not a git work tree, so the scan cannot enumerate files\n' \
            "$ROOT_DIR" >&2
        return 1
    fi

    local literal_used=()
    local i
    for ((i = 0; i < ${#NON_SECRET_LITERALS[@]}; i++)); do
        literal_used[i]="0"
    done

    local status=0
    local source
    for source in "${PINNED_SOURCES[@]}"; do
        if [[ ! -f "$source" ]]; then
            printf 'Embedded secret guard failed: %s is missing; update PINNED_SOURCES in scripts/check_embedded_secrets.sh after moving it\n' \
                "$source" >&2
            status=1
        fi
    done

    local entry name pattern hits grep_status hit file rest line_no text trimmed sanitized
    for entry in "${PATTERNS[@]}"; do
        name="${entry%%|*}"
        pattern="${entry#*|}"

        hits=""
        grep_status=0
        hits="$(git grep --untracked -I -n -E -e "$pattern" -- .)" || grep_status=$?
        if [[ "$grep_status" -gt 1 ]]; then
            printf 'Embedded secret guard failed: git grep exited %s while scanning for %s\n' "$grep_status" "$name" >&2
            status=1
            continue
        fi

        while IFS= read -r hit; do
            [[ -z "$hit" ]] && continue
            file="${hit%%:*}"
            rest="${hit#*:}"
            line_no="${rest%%:*}"
            text="${rest#*:}"
            trimmed="$(printf '%s' "$text" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"

            sanitized="$trimmed"
            for ((i = 0; i < ${#NON_SECRET_LITERALS[@]}; i++)); do
                if [[ "$sanitized" == *"${NON_SECRET_LITERALS[i]}"* ]]; then
                    sanitized="${sanitized//"${NON_SECRET_LITERALS[i]}"/}"
                    literal_used[i]="1"
                fi
            done

            if ! printf '%s' "$sanitized" | grep -q -E -e "$pattern"; then
                continue
            fi

            printf 'Embedded secret guard failed: %s at %s:%s\n    %s\n' "$name" "$file" "$line_no" "$trimmed" >&2
            status=1
        done <<<"$hits"
    done

    for ((i = 0; i < ${#NON_SECRET_LITERALS[@]}; i++)); do
        if [[ "${literal_used[i]}" != "1" ]]; then
            printf 'Embedded secret guard failed: stale exemption, nothing matches it any more: %s\n' \
                "${NON_SECRET_LITERALS[i]}" >&2
            status=1
        fi
    done

    if [[ "$status" -ne 0 ]]; then
        printf '\nRemove the key material or credential above, and rotate anything that was committed even briefly.\n' >&2
        printf 'A genuine non-secret match is excused by adding its exact text to NON_SECRET_LITERALS in scripts/check_embedded_secrets.sh with a reason.\n' >&2
        return 1
    fi

    printf 'Embedded secret guard passed (no private key material or provider credential literal in the tree).\n'
    return 0
}

# One synthetic value per pattern family, assembled from a filler so this
# function does not match the patterns it exercises.
self_test_violation_lines() {
    local fill="QWERTYUIOPASDFGHJKLZXCVBNM0123456789"
    printf '%s\n' \
        "-----${BEGIN_MARK} PRIVATE KEY-----" \
        "PuTTY-User-${KEY_MARK}-File: ssh-rsa" \
        "AKIA${fill:0:16}" \
        "ghp_${fill:0:36}" \
        "github_pat_${fill:0:22}" \
        "xoxb-${fill:0:12}" \
        "sk_live_${fill:0:20}" \
        "AIza${fill:0:35}" \
        "SG.${fill:0:20}.${fill:0:20}" \
        "npm_${fill:0:36}" \
        "pypi-AgEIcHlwaS5vcmc${fill}${fill:0:14}"
}

self_test_fixture() {
    local dir="$1" i
    mkdir -p "${dir}/crates/crypto/src"
    : >"${dir}/crates/crypto/src/license_token.rs"
    # Every exemption must appear, or the stale-exemption check fires and the
    # fixture would fail for a reason the case under test is not about.
    for ((i = 0; i < ${#NON_SECRET_LITERALS[@]}; i++)); do
        printf 'excused %s\n' "${NON_SECRET_LITERALS[i]}" >>"${dir}/excused.txt"
    done
    git -C "$dir" init -q
}

SELF_TEST_TMP=""

self_test() {
    SELF_TEST_TMP="$(mktemp -d)"
    trap 'rm -rf "$SELF_TEST_TMP"' EXIT

    local failures=0 out scan_status
    local clean="${SELF_TEST_TMP}/clean" dirty="${SELF_TEST_TMP}/dirty" renamed="${SELF_TEST_TMP}/renamed"

    self_test_fixture "$clean"
    if out="$(CHECK_EMBEDDED_SECRETS_ROOT="$clean" "$0" 2>&1)"; then
        printf 'self-test ok: clean fixture with every exemption present passes\n'
    else
        printf 'self-test FAILED: clean fixture should pass but reported:\n%s\n' "$out" >&2
        failures=$((failures + 1))
    fi

    self_test_fixture "$dirty"
    self_test_violation_lines >"${dirty}/leaked.txt"
    scan_status=0
    out="$(CHECK_EMBEDDED_SECRETS_ROOT="$dirty" "$0" 2>&1)" || scan_status=$?
    if [[ "$scan_status" -eq 0 ]]; then
        printf 'self-test FAILED: fixture holding one value per pattern family should fail\n' >&2
        failures=$((failures + 1))
    fi
    local entry name
    for entry in "${PATTERNS[@]}"; do
        name="${entry%%|*}"
        if ! printf '%s' "$out" | grep -q -F -- "$name"; then
            printf 'self-test FAILED: pattern family "%s" did not fire on its own probe value\n' "$name" >&2
            failures=$((failures + 1))
        fi
    done
    [[ "$failures" -eq 0 ]] && printf 'self-test ok: all %s pattern families fire\n' "${#PATTERNS[@]}"

    self_test_fixture "$renamed"
    rm -f "${renamed}/crates/crypto/src/license_token.rs"
    if CHECK_EMBEDDED_SECRETS_ROOT="$renamed" "$0" >/dev/null 2>&1; then
        printf 'self-test FAILED: a moved pinned source should be reported\n' >&2
        failures=$((failures + 1))
    else
        printf 'self-test ok: moving a pinned source is reported\n'
    fi

    if [[ "$failures" -ne 0 ]]; then
        printf '%s self-test assertion(s) failed\n' "$failures" >&2
        return 1
    fi
    printf 'Embedded secret guard self-test passed.\n'
    return 0
}

if [[ "${1:-}" == "--self-test" ]]; then
    self_test
else
    run_scan
fi
