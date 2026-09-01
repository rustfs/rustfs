#!/usr/bin/env bash
set -euo pipefail

WORKFLOW="${1:-.github/workflows/rustfs-tier-test.yml}"

fail() {
  printf 'tier artifact workflow check failed: %s\n' "$*" >&2
  exit 1
}

[ -f "${WORKFLOW}" ] || fail "workflow not found: ${WORKFLOW}"

for fixed_path in \
  /tmp/rustfs-tier.log \
  /tmp/rustfs-tier-report.md \
  /tmp/rustfs-tier-cases.md \
  /tmp/rustfs-tier-gate.rc; do
  if grep -Fq -- "${fixed_path}" "${WORKFLOW}"; then
    fail "fixed cross-run path remains: ${fixed_path}"
  fi
done

for required in \
  'TIER_ARTIFACTS_DIR: /tmp/rustfs-tier-artifacts-${{ github.run_id }}-${{ github.run_attempt }}' \
  'id: evidence' \
  'umask 077' \
  'mkdir -- "${TIER_ARTIFACTS_DIR}"' \
  'rustfs-tier.log' \
  'rustfs-tier-report.md' \
  'rustfs-tier-cases.md' \
  'rustfs-tier-gate.rc' \
  'provenance.json' \
  'Verify required tier evidence' \
  'id: evidence_verify' \
  'id: gate' \
  'path: ${{ env.TIER_ARTIFACTS_DIR }}/' \
  'if-no-files-found: error'; do
  grep -Fq -- "${required}" "${WORKFLOW}" || fail "required contract is missing: ${required}"
done

step_block() {
  local name="$1"
  awk -v name="${name}" '
    $0 == "      - name: " name { capture=1; found=1 }
    capture && $0 != "      - name: " name && $0 ~ /^      - name: / { exit }
    capture { print }
    END { if (!found) exit 1 }
  ' "${WORKFLOW}"
}

require_in_block() {
  local block="$1" expected="$2" label="$3"
  grep -Fqx -- "${expected}" <<< "${block}" \
    || fail "${label} is missing: ${expected}"
}

verify_block="$(step_block 'Verify required tier evidence')" \
  || fail "required-evidence step was not found"
require_in_block "${verify_block}" \
  "        if: \${{ always() && steps.evidence.outcome == 'success' }}" \
  "required-evidence condition"
require_in_block "${verify_block}" \
  '          failed=0' \
  "required-evidence failure accumulator"
require_in_block "${verify_block}" \
  '          [ "${failed}" -eq 0 ]' \
  "required-evidence fail-closed result"
require_in_block "${verify_block}" \
  '            if [ ! -s "${TIER_ARTIFACTS_DIR}/${name}" ]; then' \
  "required-file missing/empty predicate"
require_in_block "${verify_block}" \
  '            if [ ! -d "${TIER_ARTIFACTS_DIR}/${name}" ]; then' \
  "required-directory missing predicate"
for required_name in \
  rustfs-tier.log \
  rustfs-tier-report.md \
  rustfs-tier-cases.md \
  rustfs-tier-gate.rc \
  provenance.json; do
  grep -Fq -- "${required_name}" <<< "${verify_block}" \
    || fail "required-evidence step does not verify ${required_name}"
done
require_in_block "${verify_block}" \
  '          if ! find "${TIER_ARTIFACTS_DIR}/cases" -maxdepth 1 -type f -name '\''*.json'\'' -print -quit 2>/dev/null | grep -q .; then' \
  "required atomic-case predicate"
[ "$(grep -Fc '            failed=1' <<< "${verify_block}")" -eq 3 ] \
  || fail "required-evidence step must fail for files, directories, and atomic cases"

upload_block="$(step_block 'Upload report and logs')" \
  || fail "upload step was not found"
[ -n "${upload_block}" ] || fail "upload step was not found"
require_in_block "${upload_block}" \
  "        if: \${{ always() && steps.evidence.outcome == 'success' }}" \
  "artifact upload condition"
[ "$(grep -c '^[[:space:]]*path:' <<< "${upload_block}")" -eq 1 ] \
  || fail "upload step must contain exactly one path"
grep -Fq 'path: ${{ env.TIER_ARTIFACTS_DIR }}/' <<< "${upload_block}" \
  || fail "upload step must archive only the run-scoped evidence directory"
require_in_block "${upload_block}" \
  '          if-no-files-found: error' \
  "artifact upload empty-evidence behavior"

gate_block="$(step_block 'Enforce tier suite result')" \
  || fail "final gate step was not found"
require_in_block "${gate_block}" '        if: always()' "final gate condition"
require_in_block "${gate_block}" \
  '          EVIDENCE_OUTCOME: ${{ steps.evidence.outcome }}' \
  "final gate evidence status"
require_in_block "${gate_block}" \
  '          TEST_OUTCOME: ${{ steps.test.outcome }}' \
  "final gate suite status"
require_in_block "${gate_block}" \
  '          GATE_RC_FILE: ${{ env.TIER_ARTIFACTS_DIR }}/rustfs-tier-gate.rc' \
  "final gate structured status path"
require_in_block "${gate_block}" \
  '          if [ "${EVIDENCE_OUTCOME}" != "success" ]; then' \
  "final gate evidence enforcement"
require_in_block "${gate_block}" \
  '          if [ "${TEST_OUTCOME}" != "success" ]; then' \
  "final gate suite enforcement"
require_in_block "${gate_block}" \
  '          elif [ ! -s "${GATE_RC_FILE}" ]; then' \
  "final gate missing-result enforcement"
require_in_block "${gate_block}" \
  '            if ! [[ "${GATE_RC}" =~ ^[0-9]+$ ]] || [ "${GATE_RC}" -ne 0 ]; then' \
  "final gate nonzero-result enforcement"
require_in_block "${gate_block}" \
  '          [ "${failed}" -eq 0 ]' \
  "final gate fail-closed result"

issue_block="$(step_block 'File failure issue in rustfs/backlog')" \
  || fail "failure-issue step was not found"
require_in_block "${issue_block}" \
  '          EVIDENCE_DIR: ${{ env.TIER_ARTIFACTS_DIR }}' \
  "failure-issue evidence directory"
require_in_block "${issue_block}" \
  '          EVIDENCE_OUTCOME: ${{ steps.evidence.outcome }}' \
  "failure-issue initialization status"
require_in_block "${issue_block}" \
  '          VERIFY_OUTCOME: ${{ steps.evidence_verify.outcome }}' \
  "failure-issue verification status"
require_in_block "${issue_block}" \
  '          GATE_OUTCOME: ${{ steps.gate.outcome }}' \
  "failure-issue final-gate status"
require_in_block "${issue_block}" \
  '            if [ "${EVIDENCE_OUTCOME}" != "success" ]; then' \
  "failure-issue rejected-evidence guard"
require_in_block "${issue_block}" \
  '            elif [ ! -d "${EVIDENCE_DIR}" ] || [ -L "${EVIDENCE_DIR}" ]; then' \
  "failure-issue unsafe-evidence guard"
grep -Fq "steps.evidence_verify.outcome == 'failure'" <<< "${issue_block}" \
  || fail "failure-issue condition does not cover evidence verification"
grep -Fq "steps.gate.outcome == 'failure'" <<< "${issue_block}" \
  || fail "failure-issue condition does not cover the final gate"

step_line() {
  local name="$1"
  awk -v name="${name}" '$0 == "      - name: " name { print NR; exit }' "${WORKFLOW}"
}
verify_line="$(step_line 'Verify required tier evidence')"
gate_line="$(step_line 'Enforce tier suite result')"
issue_line="$(step_line 'File failure issue in rustfs/backlog')"
[ -n "${verify_line}" ] && [ -n "${gate_line}" ] && [ -n "${issue_line}" ] \
  || fail "cannot determine evidence/final-gate/failure-issue order"
[ "${verify_line}" -lt "${gate_line}" ] && [ "${gate_line}" -lt "${issue_line}" ] \
  || fail "failure issue must run after evidence verification and the final gate"

printf 'tier artifact workflow contract is isolated\n'
