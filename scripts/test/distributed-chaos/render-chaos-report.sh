#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/overtrue/www/rustfs"
RENDER_FORMAT="${1:-}"
TARGET="${2:-latest}"
OUT_FILE="${OUT_FILE:-}"
REPORT_TITLE="${REPORT_TITLE:-RustFS Distributed Chaos Report}"
EXIT_WITH_EVALUATION_STATUS="${EXIT_WITH_EVALUATION_STATUS:-0}"

usage() {
    cat <<'EOF'
Usage:
  render-chaos-report.sh <markdown|html> [latest|<run-dir>|<attempt-dir>]

Environment:
  OUT_FILE                    Optional output file path
  REPORT_TITLE                Report title used in Markdown and HTML output
  EXIT_WITH_EVALUATION_STATUS Exit with the same code as evaluate-chaos-run.sh
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

main() {
    if [[ $# -lt 1 || $# -gt 2 ]]; then
        usage
        exit 1
    fi

    case "${RENDER_FORMAT}" in
        markdown|html)
            ;;
        *)
            usage
            exit 1
            ;;
    esac

    require_bin python3

    local json_output=""
    local eval_status=0
    local json_tmp=""

    set +e
    json_output="$(FORMAT=json "${ROOT_DIR}/scripts/test/distributed-chaos/evaluate-chaos-run.sh" "${TARGET}")"
    eval_status=$?
    set -e

    json_tmp="$(mktemp)"
    trap "rm -f '${json_tmp}'" EXIT
    printf '%s\n' "${json_output}" > "${json_tmp}"

    local rendered
    rendered="$(
        python3 - "${RENDER_FORMAT}" "${OUT_FILE}" "${REPORT_TITLE}" "${json_tmp}" <<'PY'
import html
import json
import os
import sys
from pathlib import Path

fmt = sys.argv[1]
out_file = sys.argv[2]
report_title = sys.argv[3]
json_path = sys.argv[4]

data = json.loads(Path(json_path).read_text())


def relative_link(path_str):
    if not path_str:
        return None
    path = Path(path_str)
    if not out_file:
        return path.as_posix()
    try:
        base = Path(out_file).resolve().parent
        return os.path.relpath(path.resolve(), base)
    except Exception:
        return Path(path_str).as_posix()


def format_num(value, suffix=""):
    if value is None:
        return "N/A"
    if isinstance(value, float):
        return f"{value:.2f}{suffix}"
    return f"{value}{suffix}"


def evidence_rows(attempt):
    rows = []
    for label, key in (
        ("Attempt directory", "attempt_dir"),
        ("Findings log", "findings_path"),
        ("Object layout", "object_layout_path"),
        ("Warp result", "warp_path"),
    ):
        path = attempt.get(key)
        if not path:
            continue
        rows.append((label, path, relative_link(path)))
    return rows


def render_markdown(report):
    lines = [
        f"# {report_title}",
        "",
        f"- Overall verdict: `{report['overall_verdict']}`",
        f"- Attempt count: `{report['attempt_count']}`",
        f"- Run directory: `{report['run_dir']}`",
        "",
    ]

    for attempt in report["attempts"]:
        lines.extend(
            [
                f"## {attempt['attempt']}",
                "",
                f"- Verdict: `{attempt['verdict']}`",
                f"- Suspect object: `{attempt.get('suspect_object') or 'N/A'}`",
                f"- Findings count: `{attempt['findings_count']}`",
            ]
        )

        warp = attempt.get("warp")
        if warp:
            lines.extend(
                [
                    "",
                    "| Metric | Value |",
                    "| --- | --- |",
                    f"| Warp total errors | `{format_num(warp.get('total_errors'))}` |",
                    f"| Warp average throughput | `{format_num(warp.get('avg_mibps'), ' MiB/s')}` |",
                    f"| Warp median segment throughput | `{format_num(warp.get('segmented_median_mibps'), ' MiB/s')}` |",
                    f"| Warp slowest segment throughput | `{format_num(warp.get('segmented_slowest_mibps'), ' MiB/s')}` |",
                    f"| DELETE p99 | `{format_num(warp.get('delete_p99_ms'), ' ms')}` |",
                    f"| STAT p99 | `{format_num(warp.get('stat_p99_ms'), ' ms')}` |",
                    f"| GET first-byte p99 | `{format_num(warp.get('get_ttfb_p99_ms'), ' ms')}` |",
                ]
            )

        layout = attempt.get("layout")
        if layout and all(key in layout for key in ("present", "missing", "expected")):
            lines.extend(
                [
                    "",
                    "| Layout | Value |",
                    "| --- | --- |",
                    f"| Present placements | `{layout['present']}` |",
                    f"| Missing placements | `{layout['missing']}` |",
                    f"| Expected placements | `{layout['expected']}` |",
                ]
            )

        if attempt.get("reasons"):
            lines.extend(["", "### Reasons", ""])
            for reason in attempt["reasons"]:
                lines.append(f"- {reason}")

        rows = evidence_rows(attempt)
        if rows:
            lines.extend(["", "### Evidence", ""])
            for label, path, rel in rows:
                if rel and out_file:
                    lines.append(f"- {label}: [`{Path(path).name}`]({rel})")
                else:
                    lines.append(f"- {label}: `{path}`")

        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def render_html(report):
    def file_link(path):
        if not path:
            return "<span class='muted'>N/A</span>"
        rel = relative_link(path)
        label = html.escape(Path(path).name)
        href = html.escape(rel or path)
        return f"<a href=\"{href}\">{label}</a>"

    cards = []
    for attempt in report["attempts"]:
        warp = attempt.get("warp") or {}
        layout = attempt.get("layout") or {}
        reason_items = "".join(f"<li>{html.escape(reason)}</li>" for reason in attempt.get("reasons") or ["No blocking reasons recorded."])
        cards.append(
            f"""
            <section class="card">
              <div class="card-header">
                <div>
                  <h2>{html.escape(attempt['attempt'])}</h2>
                  <p class="muted">{html.escape(attempt['attempt_dir'])}</p>
                </div>
                <span class="badge {'fail' if attempt['verdict'] == 'FAIL' else 'pass'}">{html.escape(attempt['verdict'])}</span>
              </div>
              <div class="grid">
                <div class="metric"><span>Suspect object</span><strong>{html.escape(attempt.get('suspect_object') or 'N/A')}</strong></div>
                <div class="metric"><span>Findings</span><strong>{attempt['findings_count']}</strong></div>
                <div class="metric"><span>Warp avg</span><strong>{html.escape(format_num(warp.get('avg_mibps'), ' MiB/s'))}</strong></div>
                <div class="metric"><span>Warp slowest</span><strong>{html.escape(format_num(warp.get('segmented_slowest_mibps'), ' MiB/s'))}</strong></div>
                <div class="metric"><span>DELETE p99</span><strong>{html.escape(format_num(warp.get('delete_p99_ms'), ' ms'))}</strong></div>
                <div class="metric"><span>GET TTFB p99</span><strong>{html.escape(format_num(warp.get('get_ttfb_p99_ms'), ' ms'))}</strong></div>
                <div class="metric"><span>Present placements</span><strong>{html.escape(format_num(layout.get('present')))}</strong></div>
                <div class="metric"><span>Missing placements</span><strong>{html.escape(format_num(layout.get('missing')))}</strong></div>
              </div>
              <div class="split">
                <div>
                  <h3>Reasons</h3>
                  <ul>{reason_items}</ul>
                </div>
                <div>
                  <h3>Evidence</h3>
                  <ul>
                    <li>Attempt directory: {file_link(attempt.get('attempt_dir'))}</li>
                    <li>Findings log: {file_link(attempt.get('findings_path'))}</li>
                    <li>Object layout: {file_link(attempt.get('object_layout_path'))}</li>
                    <li>Warp result: {file_link(attempt.get('warp_path'))}</li>
                  </ul>
                </div>
              </div>
            </section>
            """
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(report_title)}</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: rgba(255, 255, 255, 0.86);
      --ink: #1c2430;
      --muted: #586271;
      --line: rgba(28, 36, 48, 0.12);
      --pass: #1f7a53;
      --fail: #b23a2c;
      --accent: #d6891f;
      --shadow: 0 18px 40px rgba(28, 36, 48, 0.10);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(214, 137, 31, 0.18), transparent 30%),
        linear-gradient(160deg, #f8f3eb 0%, #ede3d4 100%);
    }}
    main {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 48px 24px 80px;
    }}
    .hero {{
      display: grid;
      gap: 12px;
      margin-bottom: 28px;
    }}
    .eyebrow {{
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
    }}
    h1, h2, h3, strong {{
      font-family: "IBM Plex Serif", "Georgia", serif;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(32px, 5vw, 54px);
      line-height: 1;
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 28px;
    }}
    .summary-card, .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }}
    .summary-card {{
      padding: 18px 20px;
    }}
    .summary-card span, .metric span, .muted {{
      color: var(--muted);
    }}
    .summary-card strong {{
      display: block;
      margin-top: 8px;
      font-size: 28px;
    }}
    .card {{
      padding: 22px;
      margin-bottom: 18px;
    }}
    .card-header {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      margin-bottom: 18px;
    }}
    .card-header h2 {{
      margin: 0 0 6px;
      font-size: 28px;
    }}
    .badge {{
      border-radius: 999px;
      padding: 9px 14px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      font-size: 12px;
      color: white;
      white-space: nowrap;
    }}
    .badge.pass {{ background: var(--pass); }}
    .badge.fail {{ background: var(--fail); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .metric {{
      padding: 14px 16px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.5);
    }}
    .metric strong {{
      display: block;
      margin-top: 8px;
      font-size: 22px;
    }}
    .split {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 18px;
    }}
    ul {{
      margin: 0;
      padding-left: 18px;
    }}
    a {{
      color: var(--ink);
      text-decoration-color: rgba(28, 36, 48, 0.3);
    }}
    code {{
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      font-size: 0.92em;
      background: rgba(28, 36, 48, 0.06);
      padding: 2px 6px;
      border-radius: 8px;
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <span class="eyebrow">Distributed Chaos</span>
      <h1>{html.escape(report_title)}</h1>
      <p class="muted">Run directory: {html.escape(report['run_dir'])}</p>
    </section>
    <section class="summary">
      <article class="summary-card">
        <span>Overall verdict</span>
        <strong>{html.escape(report['overall_verdict'])}</strong>
      </article>
      <article class="summary-card">
        <span>Attempt count</span>
        <strong>{report['attempt_count']}</strong>
      </article>
      <article class="summary-card">
        <span>Generated from</span>
        <strong>{html.escape(Path(report['run_dir']).name)}</strong>
      </article>
    </section>
    {''.join(cards)}
  </main>
</body>
</html>
"""


if fmt == "markdown":
    print(render_markdown(data), end="")
elif fmt == "html":
    print(render_html(data), end="")
else:
    raise SystemExit(f"unsupported render format: {fmt}")
PY
    )"

    printf '%s\n' "${rendered}"

    if [[ -n "${OUT_FILE}" ]]; then
        mkdir -p "$(dirname "${OUT_FILE}")"
        printf '%s\n' "${rendered}" > "${OUT_FILE}"
    fi

    if [[ "${EXIT_WITH_EVALUATION_STATUS}" == "1" ]]; then
        return "${eval_status}"
    fi
}

main "$@"
