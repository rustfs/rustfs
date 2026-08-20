# Backlog #1649 Prometheus smoke

`scripts/prometheus_metrics_1649_smoke.py` is a read-only environment check for
the metric dimensions delivered by backlog #1649 and issues #1650-#1653. It
uses Prometheus' instant-query API and does not start, stop, reconfigure, or
load RustFS nodes.

Run the parser and selector self-test without a live environment:

```bash
python3 scripts/prometheus_metrics_1649_smoke.py --self-test
```

For a live cluster, pass a Prometheus base URL (or its `/api/v1/query`
endpoint), one or more expected server label values, and the built-in profile:

```bash
python3 scripts/prometheus_metrics_1649_smoke.py \
  --query-url http://prometheus.example:9090 \
  --profile backlog-1649 \
  --server rustfs-node1 \
  --server rustfs-node2
```

The profile checks the disk, scanner, ILM, audit, and notification series and
their required labels. It also requires the legacy aggregate audit and
notification series, so an additive label change cannot silently break
existing dashboards.

Dynamic series retirement is checked with an exact label set after the
scheduler retirement window has elapsed:

```bash
python3 scripts/prometheus_metrics_1649_smoke.py \
  --query-url http://prometheus.example:9090 \
  --retired 'rustfs_scanner_bucket_drive_result_total|server=node1,bucket=removed,drive=d1,result=success' \
  --retired 'rustfs_audit_total_messages_by_server|server=node1,target_id=removed'
```

`--require METRIC|key=value,...` requires a matching series;
`--require-labels METRIC|key1,key2` requires every returned series to carry
the named labels. Use `--bearer` for a bearer token or `--basic` for a
`username:password` credential when Prometheus is protected. Do not put
credentials in committed commands, logs, or issue comments.
