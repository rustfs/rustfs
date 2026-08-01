# RustFS Observability Dashboards

This directory contains optional dashboards and observability assets for operating RustFS deployments.

## Grafana

Import `grafana/rustfs-node-observability.json` into Grafana and select a Prometheus data source that scrapes RustFS metrics.

The dashboard uses the RustFS `server` metric label introduced with the node-local observability updates. `server` represents the RustFS node identity and is preferred for RustFS node comparisons. Prometheus `instance` still identifies the scrape target and remains useful for scrape/debugging views, but dashboards that compare RustFS nodes should group and filter by `server`.

During a rolling upgrade, older nodes may still emit metrics without the `server` label. Complete the rollout before using this dashboard for node-by-node comparisons.

`grafana/rustfs-kms-observability.json` covers the KMS backend operation metrics emitted at the operation-policy choke point. Unlike the node dashboard, KMS metrics do not carry the `server` label — use `job`/`instance` or promoted OTel resource attributes to split by node. Matching Prometheus alert rules live in `.docker/observability/prometheus-rules/rustfs-kms-alerts.yml`, and the alert response procedures are documented in `docs/operations/kms-observability-runbook.md`.
