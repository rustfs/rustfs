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
if read -r consumed; then
  printf 'ssh-consumed-stdin:%s\n' "$consumed" >>"$CALL_LOG"
  exit 24
fi
printf 'ssh:%s\n' "$*" >>"$CALL_LOG"
echo "profile.json.gz 128 bytes"
echo "profile.syms.json 64 bytes"
EOF

cat >"$mock_bin/scp" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if read -r consumed; then
  printf 'scp-consumed-stdin:%s\n' "$consumed" >>"$CALL_LOG"
  exit 23
fi
printf 'scp:%s\n' "$*" >>"$CALL_LOG"
dest="${@: -1}"
mkdir -p "$dest"
touch "$dest/copied-profile.json.gz"
EOF

chmod +x "$mock_bin/ssh" "$mock_bin/scp"

mapping="$tmp_dir/nodes.txt"
cat >"$mapping" <<'EOF'
vm004 /data/rustfs/hotpath/put-1m
vm005 /data/rustfs/hotpath/get-1m
EOF

export CALL_LOG="$tmp_dir/calls.log"
output=$("$repo_root/scripts/collect_remote_samply_artifacts.sh" \
  --mapping "$mapping" \
  --out-dir "$tmp_dir/out" \
  --ssh-bin "$mock_bin/ssh" \
  --scp-bin "$mock_bin/scp")

[[ "$output" == "collected_nodes=2" ]]
[[ -f "$tmp_dir/out/vm004/copied-profile.json.gz" ]]
[[ -f "$tmp_dir/out/vm005/copied-profile.json.gz" ]]
[[ "$(grep -c '^ssh:' "$CALL_LOG")" -eq 2 ]]
[[ "$(grep -c '^scp:' "$CALL_LOG")" -eq 2 ]]
if grep -q '^ssh-consumed-stdin:' "$CALL_LOG"; then
  echo "ssh consumed the mapping loop stdin" >&2
  exit 1
fi
if grep -q '^scp-consumed-stdin:' "$CALL_LOG"; then
  echo "scp consumed the mapping loop stdin" >&2
  exit 1
fi

bad_mapping="$tmp_dir/bad-nodes.txt"
printf 'vm004 /\n' >"$bad_mapping"
if "$repo_root/scripts/collect_remote_samply_artifacts.sh" \
  --mapping "$bad_mapping" \
  --out-dir "$tmp_dir/bad-out" \
  --ssh-bin "$mock_bin/ssh" \
  --scp-bin "$mock_bin/scp" >"$tmp_dir/bad.stdout" 2>"$tmp_dir/bad.stderr"; then
  echo "unsafe remote directory was accepted" >&2
  exit 1
fi
grep -q 'path contains unsafe characters' "$tmp_dir/bad.stderr"

unsafe_mapping="$tmp_dir/unsafe-nodes.txt"
printf 'vm004 /data/rustfs/hotpath/put;rm\n' >"$unsafe_mapping"
if "$repo_root/scripts/collect_remote_samply_artifacts.sh" \
  --mapping "$unsafe_mapping" \
  --out-dir "$tmp_dir/unsafe-out" \
  --ssh-bin "$mock_bin/ssh" \
  --scp-bin "$mock_bin/scp" >"$tmp_dir/unsafe.stdout" 2>"$tmp_dir/unsafe.stderr"; then
  echo "unsafe remote path was accepted" >&2
  exit 1
fi
grep -q 'path contains unsafe characters' "$tmp_dir/unsafe.stderr"

echo "test_collect_remote_samply_artifacts: ok"
