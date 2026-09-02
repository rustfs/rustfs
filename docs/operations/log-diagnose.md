# Log diagnosis (`rustfs diagnose`)

**Use this when:** you have RustFS log files from a customer or a cluster (plain, rotated, archived, or `kubectl logs` output) and need an offline root-cause report, or you need to add or override a diagnosis rule without waiting for a release.
**Source of truth:** `rustfs/src/config/cli.rs` (`DiagnoseOpts`, `DiagnoseFormat`); `rustfs/src/diagnose.rs` (exit codes, time parsing); `crates/log-analyzer/src/rules/model.rs` (`Severity`, `Matcher`, `Rule`); `crates/log-analyzer/src/rules/external.rs` (`EXTERNAL_FILE` mirrors the example below).

`rustfs diagnose` parses RustFS JSON logs — including `kubectl logs`, docker compose, and journald collection prefixes, and stderr panic blocks — matches them against the built-in rule library, and prints findings sorted by severity. It starts no storage, opens no network connection, and exits once the report is written; any host with the `rustfs` binary can run it.

## Usage

```bash
# Analyze a directory (rotated .zst/.gz archives are handled automatically)
rustfs diagnose /var/log/rustfs/

# Multi-node bundle from a customer (zip/tar.gz are expanded recursively;
# the first-level directory name becomes the node label)
rustfs diagnose customer-logs.zip

# Read from stdin (container workflows)
kubectl logs rustfs-0 | rustfs diagnose -

# Last 24 hours only, Markdown output ready to paste into a ticket
rustfs diagnose logs.tar.gz --since 24h --format md > report.md
```

| Flag | Meaning | Default |
| --- | --- | --- |
| `<paths>...` | Files, directories, archives (`.zip`, `.tar`, `.tar.gz`, `.zst`, `.gz`), or `-` for stdin | required |
| `--format text\|json\|md` | Output format; JSON carries a stable `schema_version` | `text` |
| `--since`, `--until` | Time window bounds: RFC-3339, or relative (`30m`, `24h`, `7d`) counted back from now | unbounded |
| `--min-level` | Minimum level to analyze (`trace\|debug\|info\|warn\|error`) | all levels |
| `--top N` | Number of unrecognized error patterns to list | 20 |
| `--samples N` | Sample lines per finding | 3 |
| `--redact` | Hash customer identifiers in the report | off |
| `--rules <file.json>` | Extra rules file; same-id rules override built-ins | none |

Exit codes: `0` — diagnosis completed, with or without findings; `2` — a rejected argument value (`--since`, `--until`, `--min-level`, `--rules`) or no readable input; `1` — a clap usage error (missing path, unknown flag). Findings never fail the process: this is a diagnosis tool, not a CI gate.

## Reading the report

| Section | Content |
| --- | --- |
| Findings, by severity | `P0 data risk` → `P1 unavailable` → `P2 degraded` → `P3 client side` → `P4 info`. Each finding carries a diagnosis, a suggested action, evidence fields, and sample lines |
| Causal folding | When a symptom finding (for example a burst of quorum errors) follows its known root cause (for example a faulty disk) in time, the symptom is folded into the root-cause block as a cascaded symptom and that block is promoted to the higher severity of the two, so the first block answers "most likely cause". JSON output keeps every finding and marks the relation with `collapsed_into` / `caused` |
| Timeline anomalies (hints) | Three deterministic heuristics, advisory only: mixed UTC offsets (naming clock skew when signature errors coincide), node time ranges that do not overlap, and log gaps (upgraded to restart evidence when a startup finding follows the gap) |
| Low-confidence hits | Matches below a rule's `min_count` threshold (for example sporadic signature errors), for reference only |
| Unrecognized high-frequency errors | Clustered WARN/ERROR message templates the rule library does not cover. Recurring new templates are the input for new rules under `crates/log-analyzer/src/rules/seed/` |
| Skipped inputs and timezone hints | Every skipped file (binary, or over the size cap) is listed; mixed UTC offsets are called out explicitly because clock skew is a common root cause of `SignatureDoesNotMatch` |

## `--redact`

Replaces customer identifiers — bucket, object, and access-key names, IPv4/IPv6 addresses, peer and disk paths, node labels, source file paths — with stable hashes: equal values hash equally, so correlation survives. Rule ids, diagnosis text, module targets, and panic source locations (RustFS code, not customer data) are kept. Samples are redacted together with their full `fields`.

Coverage is best-effort, not a guarantee: structured fields and `key=value` / IP-shaped text are handled, but an identifier embedded in free text that is neither field-shaped nor an IP (for example a bucket name mentioned mid-sentence) can survive. Spot-check the report before forwarding it.

## Custom rules (`--rules <file.json>`)

Support teams can add rules, or hot-fix a false positive in a built-in rule, without waiting for a release:

```bash
rustfs diagnose customer.zip --rules extra-rules.json
```

The file is the JSON form of `Rule`, identical to the built-in rules. Rule text fields (`title`, `diagnosis`, `suggestion`) are operator-facing Chinese by design; this example is the one mirrored by `EXTERNAL_FILE` in `crates/log-analyzer/src/rules/external.rs`:

```json
{
  "schema_version": 1,
  "rules": [
    {
      "id": "custom-oom-killer",
      "severity": "p1_unavailable",
      "category": "process",
      "title": "内核 OOM killer 终止了进程",
      "matcher": { "message_contains": "Out of memory: Killed process" },
      "diagnosis": "内核因内存不足杀掉了 rustfs 进程。",
      "suggestion": "检查内存限制与其他驻留进程;考虑调大内存或加节点。"
    }
  ]
}
```

| Field | Values |
| --- | --- |
| `severity` | `p0_data_risk`, `p1_unavailable`, `p2_degraded`, `p3_client_side`, `p4_info` |
| `matcher` | `message_prefix`, `message_contains`, `message_regex`, `field_equals {name, value}`, `target_prefix`, `is_panic`, `min_level`, `all [..]`, `any [..]` — the same types the built-in rules use |
| Optional fields | `evidence_fields`, `min_count` (default 1), `implies_root_cause` (participates in causal folding), `anchors` |

- A rule with the same `id` as a built-in rule replaces it (this is the hot-fix path for a false positive).
- The merged rule set is validated as a whole; every error (bad regex, duplicate id, empty matcher group) is printed and the command exits with code 2 rather than analyzing with a half-broken set.
- `anchors` in external rules are not checked by the CI anchor guard (`scripts/check_log_analyzer_rules.sh`); their quality is the author's responsibility.

## Known boundaries

- Panics appear only on stderr. If a customer collected stdout only, the panic itself is absent, but a `rwlock ... poisoned` finding hints that a panic occurred.
- Audit logs (the camelCase outbound stream) are out of scope.
- Built-in rule anchors are kept in sync with the source log messages by the CI guard `scripts/check_log_analyzer_rules.sh`.
