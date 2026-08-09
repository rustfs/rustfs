#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

mock_ssh="$tmp_dir/ssh"
cat >"$mock_ssh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if read -r consumed; then
  printf 'ssh-consumed-stdin:%s\n' "$consumed" >>"$CALL_LOG"
  exit 24
fi
printf 'ssh:%s\n' "$*" >>"$CALL_LOG"
echo "123 12.5 456789 00:01:02"
EOF
chmod +x "$mock_ssh"

export CALL_LOG="$tmp_dir/calls.log"
output=$("$repo_root/scripts/sample_remote_rustfs_rss.sh" \
  --nodes vm004,vm005 \
  --duration-secs 1 \
  --interval-secs 1 \
  --out "$tmp_dir/rss.tsv" \
  --ssh-bin "$mock_ssh")

[[ "$output" == "rss_samples=2" ]]
[[ "$(wc -l <"$tmp_dir/rss.tsv")" -eq 3 ]]
grep -q $'^ts_utc\tnode\tpid\tpcpu\trss_kib\tetime$' "$tmp_dir/rss.tsv"
grep -q $'\tvm004\t123\t12.5\t456789\t00:01:02$' "$tmp_dir/rss.tsv"
grep -q $'\tvm005\t123\t12.5\t456789\t00:01:02$' "$tmp_dir/rss.tsv"
[[ "$(grep -c '^ssh:' "$CALL_LOG")" -eq 2 ]]
if grep -q '^ssh-consumed-stdin:' "$CALL_LOG"; then
  echo "ssh consumed the sampling loop stdin" >&2
  exit 1
fi
grep -q 'sudo su - root -c' "$CALL_LOG"

if "$repo_root/scripts/sample_remote_rustfs_rss.sh" \
  --nodes 'vm004;rm' \
  --duration-secs 1 \
  --out "$tmp_dir/bad.tsv" \
  --ssh-bin "$mock_ssh" >"$tmp_dir/bad.stdout" 2>"$tmp_dir/bad.stderr"; then
  echo "unsafe node name was accepted" >&2
  exit 1
fi
grep -q 'node contains unsafe characters' "$tmp_dir/bad.stderr"

echo "test_sample_remote_rustfs_rss: ok"
