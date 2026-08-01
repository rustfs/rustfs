#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_hotpath_warp_abba.sh"
TMP_DIR="$(mktemp -d)"
OUT_DIR="${TMP_DIR}/abba"
TRACE_FILE="${TMP_DIR}/abba-trace.log"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

"$RUNNER" \
  --baseline-bin /usr/bin/true \
  --candidate-bin /usr/bin/true \
  --baseline-revision baseline-test \
  --candidate-revision candidate-test \
  --warp-bin /usr/bin/true \
  --rounds 3 \
  --out-dir "$OUT_DIR" \
  --dry-run >"$TRACE_FILE" 2>&1

awk -F',' '
  NR == 1 {
    if ($1 != "sync_label" || $6 != "leg" || $7 != "phase" || $10 != "bucket" || $11 != "dataset_setup") exit 1
    next
  }
  {
    key = $1 SUBSEP $3
    seen[key]++
    leg[key, seen[key]] = $6
    phase[key, seen[key]] = $7
    bucket[key, seen[key]] = $10
  }
  END {
    for (key in seen) {
      if (seen[key] != 4) exit 1
      if (leg[key, 1] != "A1" || leg[key, 2] != "B1" || leg[key, 3] != "B2" || leg[key, 4] != "A2") exit 1
      if (phase[key, 1] != "baseline" || phase[key, 2] != "candidate" || phase[key, 3] != "candidate" || phase[key, 4] != "baseline") exit 1
      for (slot = 1; slot <= 4; slot++) {
        if (length(bucket[key, slot]) > 63 || bucket[key, slot] !~ /^rustfs-abba-[a-z0-9-]+$/ || bucket[key, slot] in all_buckets) exit 1
        all_buckets[bucket[key, slot]] = 1
      }
      cells++
    }
    exit cells == 12 ? 0 : 1
  }
' "$OUT_DIR/abba_schedule.csv"

rg -qx 'schedule=ABBA' "$OUT_DIR/manifest.env"
rg -qx 'rounds=3' "$OUT_DIR/manifest.env"
rg -qx 'baseline_revision=baseline-test' "$OUT_DIR/manifest.env"
rg -qx 'candidate_revision=candidate-test' "$OUT_DIR/manifest.env"
rg -qx 'external_isolation=local-per-cell-disks' "$OUT_DIR/manifest.env"
rg -qx 'evidence_mode=dry-run' "$OUT_DIR/manifest.env"
rg -qx 'formal_evidence=false' "$OUT_DIR/manifest.env"
rg -qx 'performance_conclusion=not_measured_dry_run' "$OUT_DIR/manifest.env"
rg -qx 'bucket_isolation=per-leg' "$OUT_DIR/manifest.env"
rg -qx 'dataset_setup=get-and-mixed-via-warp-put' "$OUT_DIR/manifest.env"
[[ "$(rg -c -- '--extra-args --noclear' "$TRACE_FILE")" == "64" ]]
! rg -q -- 'rustfs-bench' "$TRACE_FILE"

if "$RUNNER" \
  --baseline-bin /usr/bin/true \
  --candidate-bin /usr/bin/true \
  --warp-bin /usr/bin/true \
  --rounds 3 \
  --out-dir "${TMP_DIR}/missing-revisions" >/dev/null 2>&1; then
  echo "expected formal mode to reject unknown binary revisions" >&2
  exit 1
fi

mkdir -p "${TMP_DIR}/data/reused/put-4kib-A1-sync-true"
printf '%s\n' sentinel >"${TMP_DIR}/data/reused/put-4kib-A1-sync-true/sentinel"
if "$RUNNER" \
  --baseline-bin /usr/bin/true \
  --candidate-bin /usr/bin/true \
  --baseline-revision baseline-test \
  --candidate-revision candidate-test \
  --warp-bin /usr/bin/true \
  --rounds 3 \
  --data-root "${TMP_DIR}/data" \
  --out-dir "${TMP_DIR}/reused" >/dev/null 2>&1; then
  echo "expected local mode to reject a reused run namespace" >&2
  exit 1
fi

"$RUNNER" \
  --baseline-bin /usr/bin/true \
  --candidate-bin /usr/bin/true \
  --baseline-revision baseline-test \
  --candidate-revision candidate-test \
  --endpoint 127.0.0.1:9000 \
  --allow-unmanaged-external \
  --warp-bin /usr/bin/true \
  --rounds 3 \
  --out-dir "${TMP_DIR}/unmanaged-external-dry-run" \
  --dry-run >"${TMP_DIR}/unmanaged-external-trace.log" 2>&1
rg -qx 'external=true' "${TMP_DIR}/unmanaged-external-dry-run/manifest.env"
rg -qx 'external_isolation=unmanaged' "${TMP_DIR}/unmanaged-external-dry-run/manifest.env"
rg -qx 'evidence_mode=dry-run' "${TMP_DIR}/unmanaged-external-dry-run/manifest.env"
rg -qx 'formal_evidence=false' "${TMP_DIR}/unmanaged-external-dry-run/manifest.env"
rg -qx 'performance_conclusion=not_measured_dry_run' "${TMP_DIR}/unmanaged-external-dry-run/manifest.env"

if "$RUNNER" \
  --baseline-bin /usr/bin/true \
  --candidate-bin /usr/bin/true \
  --baseline-revision baseline-test \
  --candidate-revision candidate-test \
  --endpoint 127.0.0.1:9 \
  --allow-unmanaged-external \
  --warp-bin /usr/bin/true \
  --rounds 3 \
  --health-timeout 1 \
  --out-dir "${TMP_DIR}/unmanaged-external-nonformal" >/dev/null 2>&1; then
  echo "expected unmanaged external mode to fail readiness in this test" >&2
  exit 1
fi
rg -qx 'external=true' "${TMP_DIR}/unmanaged-external-nonformal/manifest.env"
rg -qx 'external_isolation=unmanaged' "${TMP_DIR}/unmanaged-external-nonformal/manifest.env"
rg -qx 'evidence_mode=external-unmanaged-nonformal' "${TMP_DIR}/unmanaged-external-nonformal/manifest.env"
rg -qx 'formal_evidence=false' "${TMP_DIR}/unmanaged-external-nonformal/manifest.env"
rg -qx 'performance_conclusion=not_formal_unmanaged_external' "${TMP_DIR}/unmanaged-external-nonformal/manifest.env"

if "$RUNNER" \
  --baseline-bin /usr/bin/true \
  --candidate-bin /usr/bin/true \
  --endpoint 127.0.0.1:9000 \
  --warp-bin /usr/bin/true \
  --rounds 3 \
  --out-dir "${TMP_DIR}/external" \
  --dry-run >/dev/null 2>&1; then
  echo "expected formal external mode to require a deploy hook" >&2
  exit 1
fi

if "$RUNNER" \
  --baseline-bin /usr/bin/true \
  --candidate-bin /usr/bin/true \
  --baseline-revision baseline-test \
  --candidate-revision candidate-test \
  --endpoint 127.0.0.1:9000 \
  --deploy-hook ':' \
  --warp-bin /usr/bin/true \
  --rounds 3 \
  --out-dir "${TMP_DIR}/missing-deploy-evidence" >/dev/null 2>&1; then
  echo "expected a formal external deploy hook to write evidence" >&2
  exit 1
fi
rg -qx 'external=true' "${TMP_DIR}/missing-deploy-evidence/manifest.env"
rg -qx 'external_isolation=deploy-hook-attested' "${TMP_DIR}/missing-deploy-evidence/manifest.env"
rg -qx 'evidence_mode=external-deploy-hook-attested' "${TMP_DIR}/missing-deploy-evidence/manifest.env"
rg -qx 'formal_evidence=true' "${TMP_DIR}/missing-deploy-evidence/manifest.env"
rg -qx 'performance_conclusion=not_claimed_gate_and_artifacts_required' "${TMP_DIR}/missing-deploy-evidence/manifest.env"

BAD_EVIDENCE_HOOK="${TMP_DIR}/bad-evidence-hook.sh"
cat >"$BAD_EVIDENCE_HOOK" <<'HOOK'
#!/usr/bin/env bash
set -euo pipefail
leg="$HOTPATH_ABBA_LEG"
phase="$HOTPATH_ABBA_PHASE"
bucket="$HOTPATH_ABBA_BUCKET"
binary_sha256="$HOTPATH_ABBA_BINARY_SHA256"
dataset_reset=true

case "${BAD_EVIDENCE_FIELD:?missing BAD_EVIDENCE_FIELD}" in
  leg) leg=wrong-leg ;;
  phase) phase=wrong-phase ;;
  bucket) bucket=wrong-bucket ;;
  binary_sha256) binary_sha256=wrong-sha256 ;;
  dataset_reset) dataset_reset=false ;;
  *) echo "unknown BAD_EVIDENCE_FIELD=$BAD_EVIDENCE_FIELD" >&2; exit 2 ;;
esac

cat >"$HOTPATH_ABBA_DEPLOY_EVIDENCE_FILE" <<EOF
leg=$leg
phase=$phase
bucket=$bucket
binary_sha256=$binary_sha256
dataset_reset=$dataset_reset
EOF
HOOK
chmod +x "$BAD_EVIDENCE_HOOK"

for bad_field in leg phase bucket binary_sha256 dataset_reset; do
  bad_out_dir="${TMP_DIR}/bad-deploy-evidence-${bad_field}"
  if BAD_EVIDENCE_FIELD="$bad_field" "$RUNNER" \
    --baseline-bin /usr/bin/true \
    --candidate-bin /usr/bin/true \
    --baseline-revision baseline-test \
    --candidate-revision candidate-test \
    --endpoint 127.0.0.1:9000 \
    --deploy-hook "$BAD_EVIDENCE_HOOK" \
    --warp-bin /usr/bin/true \
    --rounds 3 \
    --out-dir "$bad_out_dir" >/dev/null 2>&1; then
    echo "expected a formal external deploy hook with wrong $bad_field evidence to fail" >&2
    exit 1
  fi
  rg -qx 'external=true' "$bad_out_dir/manifest.env"
  rg -qx 'external_isolation=deploy-hook-attested' "$bad_out_dir/manifest.env"
  rg -qx 'evidence_mode=external-deploy-hook-attested' "$bad_out_dir/manifest.env"
  rg -qx 'formal_evidence=true' "$bad_out_dir/manifest.env"
  rg -qx 'performance_conclusion=not_claimed_gate_and_artifacts_required' "$bad_out_dir/manifest.env"
done

echo "hotpath warp ABBA tests passed"
