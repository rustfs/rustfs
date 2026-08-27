#!/usr/bin/env bash
#
# rustfs-performance-testing.sh
# RustFS 对象存储压测脚本（固定版）：测试方法 + 执行 + 结果解析
#
# 测试方法
#   1) 方法：GET / PUT / MIXED（warp 默认混合负载 45% GET + 55% PUT）
#   2) 对象尺寸：1KiB 4KiB 16KiB 128KiB 1MiB 4MiB 8MiB 16MiB 32MiB 64MiB
#   3) 并发：64；单轮时长：5m；轮间 sleep：60s；GET 对象数：2500
#   4) 顺序：GET 全部尺寸 -> PUT 全部尺寸 -> MIXED 全部尺寸
#   5) 结果解析：每轮结束后自动解析 warp 输出，写入
#      summary.tsv（机器可读）与 summary.md（Markdown 汇总表）
#
# 依赖：warp >= v1.6（MinIO warp），bash，awk/sed/grep
# 说明：warp v1.6.1 的 put 不支持 --objects，脚本已自动处理（仅 get/mixed 传该参数）
#
# 环境变量覆盖（不传时使用固定默认值）：
#   WARP_HOST WARP_ACCESS_KEY WARP_SECRET_KEY WARP_BUCKET
#   WARP_CONCURRENCY WARP_DURATION WARP_GET_OBJECTS WARP_SLEEP_BETWEEN_ROUNDS
#   WARP_RESULT_DIR
#   WARP_METHODS WARP_SIZES   # 手动指定方法/尺寸（逗号或空格分隔），不传则全量

set -u -o pipefail

HOST="${WARP_HOST:-rustfs-node1:9000,rustfs-node2:9000,rustfs-node3:9000,rustfs-node4:9000}"
ACCESS_KEY="${WARP_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${WARP_SECRET_KEY:-rustfs@test}"
BUCKET="${WARP_BUCKET:-warp-benchmark-bucket}"
CONCURRENCY="${WARP_CONCURRENCY:-64}"
DURATION="${WARP_DURATION:-5m}"
GET_OBJECTS="${WARP_GET_OBJECTS:-2500}"
SLEEP_BETWEEN_ROUNDS="${WARP_SLEEP_BETWEEN_ROUNDS:-60}"
RESULT_DIR="${WARP_RESULT_DIR:-$(pwd)/warp-bench-results-$(date +%Y%m%d-%H%M%S)}"

if [ -n "${WARP_SIZES:-}" ] && [ "${WARP_SIZES}" != "all" ] && [ "${WARP_SIZES}" != "ALL" ]; then
  read -r -a SIZES <<<"${WARP_SIZES//,/ }"
else
  SIZES=(1KiB 4KiB 16KiB 128KiB 1MiB 4MiB 8MiB 16MiB 32MiB 64MiB)
fi
if [ -n "${WARP_METHODS:-}" ] && [ "${WARP_METHODS}" != "all" ] && [ "${WARP_METHODS}" != "ALL" ]; then
  read -r -a METHODS <<<"${WARP_METHODS//,/ }"
else
  METHODS=(get put mixed)
fi
TOTAL_ROUNDS=$(( ${#METHODS[@]} * ${#SIZES[@]} ))
ROUND=0

# --parse-only <result-dir>：只解析已有结果目录（${method}_${size}.txt），不执行压测
if [[ "${1:-}" == "--parse-only" && -n "${2:-}" ]]; then
  RESULT_DIR="$2"
fi

LOG_FILE="${RESULT_DIR}/master.log"
SUMMARY_TSV="${RESULT_DIR}/summary.tsv"
SUMMARY_MD="${RESULT_DIR}/summary.md"

if [[ "${1:-}" != "--parse-only" ]] && ! command -v warp >/dev/null 2>&1; then
  echo "错误：未找到 warp 命令，请先安装 MinIO warp。" >&2
  exit 1
fi

log() {
  echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') $*" | tee -a "${LOG_FILE}"
}

# ---- 结果解析 ----

# 提取指定 section（GET/PUT/Total 等）的 Average / Reqs / TTFB 原始行
section_lines() {
  awk -v sec="$2" '
    /^Report: / { cur = $2; sub(/\.$/, "", cur) }
    cur == sec && /^ *\* Average:/ { avg = $0 }
    cur == sec && /^ *\* Reqs:/   { reqs = $0 }
    cur == sec && /^ *\* TTFB:/   { ttfb = $0 }
    END {
      if (avg != "") print avg
      if (reqs != "") print reqs
      if (ttfb != "") print ttfb
    }
  ' "$1"
}

# 从统计行中取字段：tp objs avg p50 p90 p99 ttfb_avg ttfb_p99 ttfb_worst
field() {
  case "$2" in
    tp)         echo "$1" | sed -n 's/^ *\* Average: \(.*\), \([0-9.]*\) obj\/s.*/\1/p' ;;
    objs)       echo "$1" | sed -n 's/^ *\* Average: .*, \([0-9.]*\) obj\/s.*/\1/p' ;;
    avg)        echo "$1" | sed -n 's/^ *\* Reqs: Avg: \([^,]*\),.*/\1/p' ;;
    p50)        echo "$1" | sed -n 's/^ *\* Reqs: Avg: [^,]*, 50%: \([^,]*\),.*/\1/p' ;;
    p90)        echo "$1" | sed -n 's/^ *\* Reqs: Avg: [^,]*, 50%: [^,]*, 90%: \([^,]*\),.*/\1/p' ;;
    p99)        echo "$1" | sed -n 's/^ *\* Reqs: Avg: [^,]*, 50%: [^,]*, 90%: [^,]*, 99%: \([^,]*\),.*/\1/p' ;;
    ttfb_avg)   echo "$1" | sed -n 's/^ *\* TTFB: Avg: \([^,]*\),.*/\1/p' ;;
    ttfb_p99)   echo "$1" | sed -n 's/^ *\* TTFB: .*99th: \([^,]*\),.*/\1/p' ;;
    ttfb_worst) echo "$1" | sed -n 's/^ *\* TTFB: .*Worst: \([^ ]*\).*/\1/p' ;;
    *) echo "" ;;
  esac
}

# 解析一轮输出，追加一行到 summary.tsv
parse_round() {
  local method="$1" size="$2" file="$3"
  local line
  if [[ "${method}" == "mixed" ]]; then
    local total get put
    total=$(section_lines "$file" Total)
    get=$(section_lines "$file" GET)
    put=$(section_lines "$file" PUT)
    line=$(printf 'mixed\t%s\t%s\t%s\t%s\t%s' \
      "$size" \
      "$(field "${total}" tp)" \
      "$(field "${total}" objs)" \
      "$(field "${get}" avg)" \
      "$(field "${put}" avg)")
  else
    local sec
    sec=$(printf '%s' "${method}" | tr '[:lower:]' '[:upper:]')
    local stats
    stats=$(section_lines "$file" "${sec}")
    line=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
      "${method}" "${size}" \
      "$(field "${stats}" tp)" \
      "$(field "${stats}" objs)" \
      "$(field "${stats}" avg)" \
      "$(field "${stats}" p50)" \
      "$(field "${stats}" p90)" \
      "$(field "${stats}" p99)" \
      "$(field "${stats}" ttfb_avg)" \
      "$(field "${stats}" ttfb_p99)" \
      "$(field "${stats}" ttfb_worst)")
  fi
  printf '%s\n' "${line}" >> "${SUMMARY_TSV}"
}

# 汇总 summary.tsv -> summary.md（Markdown 表格）
gen_summary_md() {
  {
    echo "# RustFS 性能压测结果"
    echo ""
    echo "- 日期：$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "- 目标：${HOST}"
    echo "- 并发：${CONCURRENCY}；单轮：${DURATION}；sleep：${SLEEP_BETWEEN_ROUNDS}s；GET objects：${GET_OBJECTS}"
    echo "- 方法：GET / PUT / MIXED（warp 默认混合负载）；尺寸：${SIZES[*]}"
    echo ""
  } > "${SUMMARY_MD}"

  for m in get put; do
    {
      echo "## $(printf '%s' "$m" | tr '[:lower:]' '[:upper:]') 结果"
      echo ""
      echo "| 对象尺寸 | 平均吞吐 | 平均 obj/s | Avg Latency | P50 | P90 | P99 | TTFB Avg | TTFB P99 | TTFB 最差 |"
      echo "|----------|----------|-----------|-------------|-----|-----|-----|----------|----------|-----------|"
    } >> "${SUMMARY_MD}"
    while IFS=$'\t' read -r method size tp objs avg p50 p90 p99 ttfb_avg ttfb_p99 ttfb_worst; do
      [[ "${method}" == "${m}" ]] && \
        echo "| ${size} | ${tp} | ${objs} | ${avg} | ${p50} | ${p90} | ${p99} | ${ttfb_avg} | ${ttfb_p99} | ${ttfb_worst} |" >> "${SUMMARY_MD}"
    done < "${SUMMARY_TSV}"
    echo "" >> "${SUMMARY_MD}"
  done

  {
    echo "## MIXED 结果（Total 口径）"
    echo ""
    echo "| 对象尺寸 | Total 平均吞吐 | Total 平均 obj/s | Mixed-GET Avg | Mixed-PUT Avg |"
    echo "|----------|---------------|------------------|----------------|----------------|"
  } >> "${SUMMARY_MD}"
  while IFS=$'\t' read -r method size tp objs gavg pavg rest; do
    [[ "${method}" == "mixed" ]] && \
      echo "| ${size} | ${tp} | ${objs} | ${gavg} | ${pavg} |" >> "${SUMMARY_MD}"
  done < "${SUMMARY_TSV}"
  echo "" >> "${SUMMARY_MD}"
}

# ---- 主流程 ----

mkdir -p "${RESULT_DIR}"
log "CONFIG host=${HOST} bucket=${BUCKET} concurrency=${CONCURRENCY} duration=${DURATION} get_objects=${GET_OBJECTS} sleep_between_rounds=${SLEEP_BETWEEN_ROUNDS}s"
printf 'method\tsize\tthroughput\tobj_per_s\treq_avg\treq_p50\treq_p90\treq_p99\tttfb_avg\tttfb_p99\tttfb_worst\n' > "${SUMMARY_TSV}"

if [[ "${1:-}" == "--parse-only" ]]; then
  for method in "${METHODS[@]}"; do
    for size in "${SIZES[@]}"; do
      outfile="${RESULT_DIR}/${method}_${size}.txt"
      if [[ -s "${outfile}" ]]; then
        parse_round "${method}" "${size}" "${outfile}"
      fi
    done
  done
  gen_summary_md
  echo "parsed from ${RESULT_DIR}"
  echo ""
  cat "${SUMMARY_MD}"
  exit 0
fi

for method in "${METHODS[@]}"; do
  for size in "${SIZES[@]}"; do
    ROUND=$((ROUND + 1))
    outfile="${RESULT_DIR}/${method}_${size}.txt"

    log "START round=${ROUND}/${TOTAL_ROUNDS} method=${method} size=${size} concurrency=${CONCURRENCY} duration=${DURATION}"

    extra_args=()
    if [[ "${method}" != "put" ]]; then
      extra_args=(--objects "${GET_OBJECTS}")
    fi

    start_epoch=$(date +%s)
    warp "${method}" \
      --host "${HOST}" \
      --access-key "${ACCESS_KEY}" \
      --secret-key "${SECRET_KEY}" \
      --bucket "${BUCKET}" \
      --concurrent "${CONCURRENCY}" \
      --duration "${DURATION}" \
      --obj.size "${size}" \
      "${extra_args[@]}" \
      --no-color 2>&1 | tee "${outfile}"
    rc=${PIPESTATUS[0]}
    end_epoch=$(date +%s)

    if [[ ${rc} -eq 0 ]]; then
      parse_round "${method}" "${size}" "${outfile}"
      log "END round=${ROUND}/${TOTAL_ROUNDS} method=${method} size=${size} rc=${rc} elapsed=$((end_epoch - start_epoch))s parsed=ok"
    else
      log "END round=${ROUND}/${TOTAL_ROUNDS} method=${method} size=${size} rc=${rc} elapsed=$((end_epoch - start_epoch))s parsed=skipped"
    fi

    if [[ ${ROUND} -lt ${TOTAL_ROUNDS} ]]; then
      log "SLEEP ${SLEEP_BETWEEN_ROUNDS}s before next round"
      sleep "${SLEEP_BETWEEN_ROUNDS}"
    fi
  done
done

gen_summary_md
log "ALL_ROUNDS_COMPLETE summary_tsv=${SUMMARY_TSV} summary_md=${SUMMARY_MD}"
echo ""
echo "==== 结果汇总 ===="
cat "${SUMMARY_MD}"
