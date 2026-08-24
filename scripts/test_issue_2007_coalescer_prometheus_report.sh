#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/issue_2007_coalescer_prometheus_report.py" --self-test >/dev/null

output="$("${SCRIPT_DIR}/issue_2007_coalescer_prometheus_report.py" \
  --query-url http://prometheus.example:9090 \
  --profile delay-50us \
  --window 180s \
  --rustfs-selector 'server=~"node[5-8]"' \
  --node-selector 'instance=~"node[5-8].*"' \
  --show-queries \
  --timeout 0.001 || true)"

printf '%s\n' "$output" | rg -Fq '## backlog#2007 coalescer cost report: delay-50us'
printf '%s\n' "$output" | rg -Fq 'Window: `180s`'
printf '%s\n' "$output" | rg -Fq 'RustFS selector: `server=~"node[5-8]"`'
printf '%s\n' "$output" | rg -Fq 'Node selector: `instance=~"node[5-8].*"`'
printf '%s\n' "$output" | rg -Fq 'outgoing grpc_batch_read_version requests'
printf '%s\n' "$output" | rg -Fq 'single-item batch ratio'
printf '%s\n' "$output" | rg -Fq 'batch_read_version_response_map'
printf '%s\n' "$output" | rg -Fq 'node_disk_read_time_seconds_total'
printf '%s\n' "$output" | rg -Fq '### Unavailable queries'
