#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

mock_bin="$tmp_dir/bin"
mkdir -p "$mock_bin"

cat >"$mock_bin/ssh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'ssh:%s\n' "$*" >>"$CALL_LOG"
if [[ "${MOCK_JOURNAL_MODE:-}" == "clean" ]]; then
  echo "rustfs request completed"
  exit 0
fi
echo "rustfs request completed"
echo "No valid auth token"
echo "WARN replay cache overflow"
EOF

chmod +x "$mock_bin/ssh"

export CALL_LOG="$tmp_dir/calls.log"
output=$("$repo_root/scripts/capture_remote_journal_errors.sh" \
  --nodes "vm004,vm005" \
  --since "2026-08-08T09:05:45Z" \
  --label "get-1m" \
  --out-dir "$tmp_dir/out" \
  --ssh-bin "$mock_bin/ssh")

grep -q 'journal_since=2026-08-08 09:05:45 UTC' <<<"$output"
grep -q 'captured_nodes=2' <<<"$output"
grep -q '2026-08-08 09:05:45 UTC' "$CALL_LOG"
grep -q 'sudo su - root -c' "$CALL_LOG"
if grep -q '2026-08-08T09:05:45Z' "$CALL_LOG"; then
  echo "raw ISO timestamp was sent to journalctl" >&2
  exit 1
fi
grep -q 'No valid auth token' "$tmp_dir/out/get-1m-vm004-journal-errors.txt"
grep -q 'WARN replay cache overflow' "$tmp_dir/out/get-1m-vm005-journal-errors.txt"
if grep -q 'rustfs request completed' "$tmp_dir/out/get-1m-vm004-journal-errors.txt"; then
  echo "non-error journal line was not filtered out" >&2
  exit 1
fi

export MOCK_JOURNAL_MODE=clean
"$repo_root/scripts/capture_remote_journal_errors.sh" \
  --nodes "vm004" \
  --since "2026-08-08T09:05:45Z" \
  --label "clean" \
  --out-dir "$tmp_dir/clean-out" \
  --ssh-bin "$mock_bin/ssh" >"$tmp_dir/clean.stdout"
[[ ! -s "$tmp_dir/clean-out/clean-vm004-journal-errors.txt" ]]

if "$repo_root/scripts/capture_remote_journal_errors.sh" \
  --nodes "vm004" \
  --since "2026-08-08T09:05:45Z" \
  --label "../escape" \
  --out-dir "$tmp_dir/unsafe-out" \
  --ssh-bin "$mock_bin/ssh" >"$tmp_dir/unsafe.stdout" 2>"$tmp_dir/unsafe.stderr"; then
  echo "unsafe label was accepted" >&2
  exit 1
fi
grep -q -- '--label contains unsafe characters' "$tmp_dir/unsafe.stderr"

echo "test_capture_remote_journal_errors: ok"
