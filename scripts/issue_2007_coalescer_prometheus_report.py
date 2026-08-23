#!/usr/bin/env python3
"""Read-only Prometheus report for rustfs/backlog#2007 coalescer delay runs.

The script queries Prometheus' instant-query API and prints a Markdown summary
for one already-completed workload window. It never writes to RustFS,
Prometheus, or scrape targets.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


RUSTFS_SELECTOR_HELP = "PromQL label selector applied to RustFS metrics, for example 'server=~\"node[5-8]\"'"
NODE_SELECTOR_HELP = "PromQL label selector applied to node-exporter metrics, for example 'instance=~\"node[5-8].*\"'"


@dataclass(frozen=True)
class Sample:
    labels: dict[str, str]
    value: float


@dataclass(frozen=True)
class QueryResult:
    name: str
    query: str
    samples: list[Sample]
    error: str | None = None

    def scalar_sum(self) -> float | None:
        if self.error or not self.samples:
            return None
        return sum(sample.value for sample in self.samples)


def query_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.path.rstrip("/").endswith("/api/v1/query"):
        return value
    return value.rstrip("/") + "/api/v1/query"


def braces(selector: str = "", *pairs: tuple[str, str]) -> str:
    labels = [selector.strip().strip("{}")] if selector.strip() else []
    labels.extend(f'{key}="{value}"' for key, value in pairs)
    return "{" + ",".join(label for label in labels if label) + "}"


def braces_with_raw(selector: str = "", *raw_labels: str) -> str:
    labels = [selector.strip().strip("{}")] if selector.strip() else []
    labels.extend(raw_labels)
    return "{" + ",".join(label for label in labels if label) + "}"


def parse_vector(payload: dict[str, Any]) -> list[Sample]:
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus returned non-success: {payload}")
    data = payload.get("data", {})
    if data.get("resultType") != "vector":
        raise RuntimeError(f"Prometheus query did not return an instant vector: {payload}")
    samples: list[Sample] = []
    for item in data.get("result", []):
        value = item.get("value", [None, "nan"])[1]
        try:
            parsed_value = float(value)
        except (TypeError, ValueError):
            parsed_value = math.nan
        samples.append(Sample(dict(item.get("metric", {})), parsed_value))
    return samples


def fetch(endpoint: str, query: str, headers: dict[str, str], timeout: float) -> list[Sample]:
    request = Request(f"{endpoint}?{urlencode({'query': query})}", headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError) as error:
        raise RuntimeError(f"Prometheus query failed for {query!r}: {error}") from error
    return parse_vector(payload)


def run_query(endpoint: str, headers: dict[str, str], timeout: float, name: str, query: str) -> QueryResult:
    try:
        return QueryResult(name=name, query=query, samples=fetch(endpoint, query, headers, timeout))
    except RuntimeError as error:
        return QueryResult(name=name, query=query, samples=[], error=str(error))


def fmt_value(value: float | None, suffix: str = "", precision: int = 2) -> str:
    if value is None or math.isnan(value):
        return "UNAVAILABLE"
    if math.isinf(value):
        return "inf"
    return f"{value:.{precision}f}{suffix}"


def fmt_count(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "UNAVAILABLE"
    return f"{value:.0f}"


def batch_distribution(samples: Iterable[Sample]) -> tuple[float, float, float, list[tuple[int, float]]]:
    total_batches = 0.0
    total_items = 0.0
    single_item = 0.0
    rows: list[tuple[int, float]] = []
    for sample in samples:
        raw_count = sample.labels.get("item_count", "")
        if not raw_count.isdigit():
            continue
        item_count = int(raw_count)
        count = sample.value
        rows.append((item_count, count))
        total_batches += count
        total_items += item_count * count
        if item_count == 1:
            single_item += count
    avg_batch_size = total_items / total_batches if total_batches else math.nan
    single_item_ratio = single_item / total_batches if total_batches else math.nan
    return total_batches, avg_batch_size, single_item_ratio, sorted(rows)


def build_queries(window: str, rustfs_selector: str, coalescer_selector: str, node_selector: str) -> dict[str, str]:
    read_version = braces(rustfs_selector, ("operation", "grpc_read_version"), ("backend", "grpc"))
    batch_read_version = braces(rustfs_selector, ("operation", "grpc_batch_read_version"), ("backend", "grpc"))
    coalescer = braces(coalescer_selector, ("event", "attempted_batch"))
    cpu = braces(node_selector, ("mode", "idle"))
    node = braces(node_selector)
    net = braces_with_raw(node_selector, 'device!~"lo|docker.*|veth.*|br-.*|cni.*"')
    disk = braces_with_raw(node_selector, 'device!~"loop.*|ram.*|dm-.*"')
    return {
        "grpc_read_version_requests": (
            "sum(increase(rustfs_system_network_internode_operation_requests_outgoing_total"
            f"{read_version}[{window}]))"
        ),
        "grpc_batch_read_version_requests": (
            "sum(increase(rustfs_system_network_internode_operation_requests_outgoing_total"
            f"{batch_read_version}[{window}]))"
        ),
        "coalescer_batches_by_item_count": (
            "sum by (item_count) (increase(rustfs_get_metadata_read_version_coalescer_total"
            f"{coalescer}[{window}]))"
        ),
        "coalescer_wait_p99_ms": (
            "histogram_quantile(0.99, sum by (le) (rate("
            "rustfs_system_network_internode_operation_stage_duration_ms_bucket"
            f'{braces(rustfs_selector, ("operation", "grpc_batch_read_version"), ("backend", "grpc"), ("stage", "batch_read_version_coalescer_wait"))}'
            f"[{window}])))"
        ),
        "batch_rpc_roundtrip_p99_ms": (
            "histogram_quantile(0.99, sum by (le) (rate("
            "rustfs_system_network_internode_operation_stage_duration_ms_bucket"
            f'{braces(rustfs_selector, ("operation", "grpc_batch_read_version"), ("backend", "grpc"), ("stage", "batch_read_version_rpc_roundtrip"))}'
            f"[{window}])))"
        ),
        "batch_disk_read_p99_ms": (
            "histogram_quantile(0.99, sum by (le) (rate("
            "rustfs_system_network_internode_operation_stage_duration_ms_bucket"
            f'{braces(rustfs_selector, ("operation", "grpc_batch_read_version"), ("backend", "grpc"), ("stage", "batch_read_version_disk_read"))}'
            f"[{window}])))"
        ),
        "batch_response_map_p99_ms": (
            "histogram_quantile(0.99, sum by (le) (rate("
            "rustfs_system_network_internode_operation_stage_duration_ms_bucket"
            f'{braces(rustfs_selector, ("operation", "grpc_batch_read_version"), ("backend", "grpc"), ("stage", "batch_read_version_response_map"))}'
            f"[{window}])))"
        ),
        "node_cpu_busy_percent": f"100 * (1 - avg(rate(node_cpu_seconds_total{cpu}[{window}])))",
        "node_network_receive_bytes_per_sec": f"sum(rate(node_network_receive_bytes_total{net}[{window}]))",
        "node_network_transmit_bytes_per_sec": f"sum(rate(node_network_transmit_bytes_total{net}[{window}]))",
        "node_disk_read_await_ms": (
            "1000 * sum(rate(node_disk_read_time_seconds_total"
            f"{disk}[{window}])) / clamp_min(sum(rate(node_disk_reads_completed_total{disk}[{window}])), 1)"
        ),
        "node_disk_avg_queue_depth": (
            "sum(rate(node_disk_io_time_weighted_seconds_total"
            f"{disk}[{window}]))"
        ),
        "node_disk_util_percent": f"100 * sum(rate(node_disk_io_time_seconds_total{disk}[{window}]))",
        "node_up": f"sum(up{node})",
    }


def render_report(args: argparse.Namespace, results: dict[str, QueryResult]) -> str:
    read_version = results["grpc_read_version_requests"].scalar_sum()
    batch_read_version = results["grpc_batch_read_version_requests"].scalar_sum()
    total_rpc = (read_version or 0.0) + (batch_read_version or 0.0)
    batch_ratio = batch_read_version / total_rpc if total_rpc else math.nan
    total_batches, avg_batch_size, single_item_ratio, distribution = batch_distribution(
        results["coalescer_batches_by_item_count"].samples
    )

    lines = [
        f"## backlog#2007 coalescer cost report: {args.profile}",
        "",
        f"- Window: `{args.window}`",
        f"- RustFS selector: `{args.rustfs_selector or '<none>'}`",
        f"- Coalescer selector: `{args.coalescer_selector or '<none>'}`",
        f"- Node selector: `{args.node_selector or '<none>'}`",
        "",
        "| Signal | Value |",
        "|---|---:|",
        f"| outgoing grpc_read_version requests | {fmt_count(read_version)} |",
        f"| outgoing grpc_batch_read_version requests | {fmt_count(batch_read_version)} |",
        f"| batch RPC share | {fmt_value(batch_ratio * 100 if not math.isnan(batch_ratio) else math.nan, '%')} |",
        f"| coalescer batches | {fmt_count(total_batches)} |",
        f"| avg coalesced batch size | {fmt_value(avg_batch_size)} |",
        f"| single-item batch ratio | {fmt_value(single_item_ratio * 100 if not math.isnan(single_item_ratio) else math.nan, '%')} |",
        f"| coalescer_wait p99 | {fmt_value(results['coalescer_wait_p99_ms'].scalar_sum(), ' ms')} |",
        f"| batch rpc_roundtrip p99 | {fmt_value(results['batch_rpc_roundtrip_p99_ms'].scalar_sum(), ' ms')} |",
        f"| batch disk_read p99 | {fmt_value(results['batch_disk_read_p99_ms'].scalar_sum(), ' ms')} |",
        f"| batch response_map p99 | {fmt_value(results['batch_response_map_p99_ms'].scalar_sum(), ' ms')} |",
        f"| node CPU busy | {fmt_value(results['node_cpu_busy_percent'].scalar_sum(), '%')} |",
        f"| node network RX | {fmt_value(results['node_network_receive_bytes_per_sec'].scalar_sum(), ' B/s')} |",
        f"| node network TX | {fmt_value(results['node_network_transmit_bytes_per_sec'].scalar_sum(), ' B/s')} |",
        f"| node disk read await | {fmt_value(results['node_disk_read_await_ms'].scalar_sum(), ' ms')} |",
        f"| node disk avg queue depth | {fmt_value(results['node_disk_avg_queue_depth'].scalar_sum())} |",
        f"| node disk util | {fmt_value(results['node_disk_util_percent'].scalar_sum(), '%')} |",
        f"| node-exporter up series | {fmt_count(results['node_up'].scalar_sum())} |",
        "",
        "### Batch distribution",
        "",
        "| item_count | batches |",
        "|---:|---:|",
    ]
    if distribution:
        lines.extend(f"| {item_count} | {fmt_count(count)} |" for item_count, count in distribution)
    else:
        lines.append("| UNAVAILABLE | UNAVAILABLE |")

    unavailable = [result for result in results.values() if result.error or not result.samples]
    if unavailable:
        lines.extend(["", "### Unavailable queries", ""])
        for result in unavailable:
            reason = result.error or "no series returned"
            lines.append(f"- `{result.name}`: {reason}")

    if args.show_queries:
        lines.extend(["", "### PromQL", ""])
        for result in results.values():
            lines.append(f"- `{result.name}`: `{result.query}`")

    return "\n".join(lines)


def run(args: argparse.Namespace) -> int:
    headers = {"Accept": "application/json"}
    if args.bearer:
        headers["Authorization"] = f"Bearer {args.bearer}"
    if args.basic:
        headers["Authorization"] = "Basic " + base64.b64encode(args.basic.encode()).decode()

    endpoint = query_url(args.query_url)
    queries = build_queries(args.window, args.rustfs_selector, args.coalescer_selector, args.node_selector)
    results = {
        name: run_query(endpoint, headers, args.timeout, name, query)
        for name, query in queries.items()
    }
    print(render_report(args, results))
    return 0


def self_test() -> None:
    assert query_url("http://prom:9090") == "http://prom:9090/api/v1/query"
    assert query_url("http://prom:9090/api/v1/query") == "http://prom:9090/api/v1/query"
    assert braces('server=~"node[5-8]"', ("operation", "grpc_batch_read_version")) == (
        '{server=~"node[5-8]",operation="grpc_batch_read_version"}'
    )
    assert braces_with_raw("", 'device!~"lo"') == '{device!~"lo"}'
    assert braces_with_raw('instance=~"node.*"', 'device!~"lo"') == '{instance=~"node.*",device!~"lo"}'
    payload = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {"item_count": "1"}, "value": [1, "2"]},
                {"metric": {"item_count": "4"}, "value": [1, "3"]},
            ],
        },
    }
    samples = parse_vector(payload)
    total_batches, avg_batch_size, single_item_ratio, rows = batch_distribution(samples)
    assert total_batches == 5
    assert avg_batch_size == 2.8
    assert single_item_ratio == 0.4
    assert rows == [(1, 2.0), (4, 3.0)]
    queries = build_queries("5m", "", "", "")
    assert "increase(rustfs_get_metadata_read_version_coalescer_total" in queries["coalescer_batches_by_item_count"]
    print("PASS: self-test")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-url", help="Prometheus base URL or /api/v1/query endpoint")
    parser.add_argument("--profile", default="unknown", help="Run label printed in the report, e.g. delay-200us or delay-50us")
    parser.add_argument("--window", default="5m", help="PromQL range selector covering the measured workload window")
    parser.add_argument("--rustfs-selector", default="", help=RUSTFS_SELECTOR_HELP)
    parser.add_argument(
        "--coalescer-selector",
        default="",
        help="PromQL label selector for rustfs_get_metadata_read_version_coalescer_total; leave empty if it has no server labels",
    )
    parser.add_argument("--node-selector", default="", help=NODE_SELECTOR_HELP)
    parser.add_argument("--bearer")
    parser.add_argument("--basic", help="username:password; prefer --bearer in shared shells")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--show-queries", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.query_url:
        parser.error("--query-url is required unless --self-test is used")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
