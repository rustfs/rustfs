#!/usr/bin/env python3
"""Read-only Prometheus smoke checks for backlog #1649 metric dimensions.

The harness queries Prometheus' instant-query API. It never writes to RustFS,
Prometheus, or the scrape targets. A check is ``metric|label=value,...``;
``--require-labels`` accepts ``metric|label1,label2``. ``--retired`` checks
that an exact label set is absent after the scheduler's retirement window.
"""

import argparse
import base64
import json
import sys
from dataclasses import dataclass
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class Series:
    labels: dict[str, str]


def query_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.path.rstrip("/").endswith("/api/v1/query"):
        return value
    return value.rstrip("/") + "/api/v1/query"


def parse_spec(spec: str, separator: str = "|") -> tuple[str, dict[str, str]]:
    metric, _, labels = spec.partition(separator)
    if not metric or any(c in metric for c in "{} \t"):
        raise ValueError(f"invalid metric check: {spec!r}")
    expected: dict[str, str] = {}
    if labels:
        for pair in labels.split(","):
            key, sep, value = pair.partition("=")
            if not sep or not key or not value:
                raise ValueError(f"invalid label selector in {spec!r}")
            if key in expected:
                raise ValueError(f"duplicate label {key!r} in {spec!r}")
            expected[key] = value
    return metric, expected


def parse_label_names(spec: str) -> tuple[str, list[str]]:
    metric, separator, labels = spec.partition("|")
    names = [item for item in labels.split(",") if item] if separator else []
    if not metric or any(c in metric for c in "{} \t") or not names or any(
        "=" in item or not item.replace("_", "a").isalnum() for item in names
    ):
        raise ValueError(f"invalid label-name check: {spec!r}")
    return metric, names


def fetch_series(endpoint: str, metric: str, headers: dict[str, str], timeout: float) -> list[Series]:
    query = f"{metric}{{}}" if "{" not in metric else metric
    request = Request(f"{endpoint}?{urlencode({'query': query})}", headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError) as error:
        raise RuntimeError(f"Prometheus query failed for {query!r}: {error}") from error
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus returned non-success for {query!r}: {payload}")
    data = payload.get("data", {})
    if data.get("resultType") != "vector":
        raise RuntimeError(f"Prometheus query did not return an instant vector: {query!r}")
    return [Series(dict(item.get("metric", {}))) for item in data.get("result", [])]


def has_labels(series: Iterable[Series], expected: dict[str, str]) -> bool:
    return any(all(item.labels.get(key) == value for key, value in expected.items()) for item in series)


def run(args: argparse.Namespace) -> int:
    headers = {"Accept": "application/json"}
    if args.bearer:
        headers["Authorization"] = f"Bearer {args.bearer}"
    if args.basic:
        headers["Authorization"] = "Basic " + base64.b64encode(args.basic.encode()).decode()
    endpoint = query_url(args.query_url)
    required = list(args.require)
    labels = list(args.require_labels)
    retired = list(args.retired)
    if args.profile == "backlog-1649":
        required += [
            "rustfs_system_drive_total_bytes",
            "rustfs_system_drive_writes_total",
            "rustfs_system_drive_deletes_total",
            "rustfs_scanner_source_work_total",
            "rustfs_scanner_active_bucket_drive_scans",
            "rustfs_scanner_bucket_drive_result_total",
            "rustfs_ilm_action_tasks",
            "rustfs_ilm_tasks",
            "rustfs_ilm_task_events_total",
            "rustfs_ilm_queue_backpressure_total",
            "rustfs_ilm_versions_scanned_by_server",
            "rustfs_notification_current_send_in_progress_by_server",
            "rustfs_notification_events_errors_total_by_server",
            "rustfs_notification_events_sent_total_by_server",
            "rustfs_notification_events_skipped_total_by_server",
            "rustfs_audit_failed_messages_by_server",
            "rustfs_audit_target_queue_length_by_server",
            "rustfs_audit_total_messages_by_server",
            "rustfs_notification_events_errors_total",
            "rustfs_notification_events_sent_total",
            "rustfs_notification_events_skipped_total",
            "rustfs_audit_failed_messages",
            "rustfs_audit_target_queue_length",
            "rustfs_audit_total_messages",
        ]
        labels += [
            "rustfs_system_drive_total_bytes|server,drive",
            "rustfs_system_drive_writes_total|server,drive",
            "rustfs_system_drive_deletes_total|server,drive",
            "rustfs_scanner_source_work_total|server,source,state",
            "rustfs_scanner_active_bucket_drive_scans|server,source,bucket,drive",
            "rustfs_scanner_bucket_drive_result_total|server,bucket,drive,result",
            "rustfs_ilm_action_tasks|server,action,state",
            "rustfs_ilm_tasks|server,action,queue_state",
            "rustfs_ilm_task_events_total|server,action,result",
            "rustfs_ilm_queue_backpressure_total|server,action,reason",
            "rustfs_ilm_versions_scanned_by_server|server,source",
            "rustfs_notification_current_send_in_progress_by_server|server",
            "rustfs_notification_events_errors_total_by_server|server",
            "rustfs_notification_events_sent_total_by_server|server",
            "rustfs_notification_events_skipped_total_by_server|server",
            "rustfs_audit_failed_messages_by_server|server,target_id",
            "rustfs_audit_target_queue_length_by_server|server,target_id",
            "rustfs_audit_total_messages_by_server|server,target_id",
            "rustfs_audit_failed_messages|target_id",
            "rustfs_audit_target_queue_length|target_id",
            "rustfs_audit_total_messages|target_id",
        ]
    if not required and not labels and not retired:
        raise ValueError("provide --profile backlog-1649 or at least one check")
    failures: list[str] = []
    cache: dict[str, list[Series]] = {}

    def get(metric: str) -> list[Series]:
        if metric not in cache:
            cache[metric] = fetch_series(endpoint, metric, headers, args.timeout)
        return cache[metric]

    def missing_servers(series: list[Series]) -> list[str]:
        if not args.server or not any("server" in item.labels for item in series):
            return []
        observed = {item.labels["server"] for item in series if "server" in item.labels}
        return sorted(set(args.server) - observed)

    for spec in required:
        metric, expected = parse_spec(spec)
        series = get(metric)
        if not series:
            failures.append(f"{metric}: no series returned")
        elif expected and not has_labels(series, expected):
            failures.append(f"{metric}: no series has labels {expected}; observed {len(series)} series")
        elif missing_servers(series):
            failures.append(f"{metric}: missing requested server series {missing_servers(series)}")
    for spec in labels:
        metric, required_labels = parse_label_names(spec)
        series = get(metric)
        if not series:
            failures.append(f"{metric}: aggregate series absent")
        elif any(not all(label in item.labels for label in required_labels) for item in series):
            failures.append(f"{metric}: at least one series is missing labels {required_labels}")
        elif missing_servers(series):
            failures.append(f"{metric}: missing requested server series {missing_servers(series)}")
    for spec in retired:
        metric, expected = parse_spec(spec)
        if has_labels(get(metric), expected):
            failures.append(f"{metric}: retired series still present with labels {expected}")
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        return 1
    print(f"PASS: {len(required)} required, {len(labels)} label, {len(retired)} retirement checks")
    return 0


def self_test() -> None:
    assert parse_spec("metric|server=node1,drive=d1") == ("metric", {"server": "node1", "drive": "d1"})
    assert parse_label_names("metric|server,drive") == ("metric", ["server", "drive"])
    assert has_labels([Series({"server": "node1", "drive": "d1"})], {"server": "node1"})
    assert not has_labels([Series({"server": "node1"})], {"server": "node2"})
    try:
        parse_spec("metric|server")
    except ValueError:
        pass
    else:
        raise AssertionError("malformed value selector accepted")
    print("PASS: self-test")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-url", help="Prometheus base URL or /api/v1/query endpoint")
    parser.add_argument("--profile", choices=["backlog-1649"])
    parser.add_argument("--server", action="append", default=[], help="server label value required in every server-scoped check")
    parser.add_argument("--require", action="append", default=[], metavar="METRIC|k=v,...")
    parser.add_argument("--require-labels", action="append", default=[], metavar="METRIC|k1,k2")
    parser.add_argument("--retired", action="append", default=[], metavar="METRIC|k=v,...")
    parser.add_argument("--bearer")
    parser.add_argument("--basic", help="username:password; prefer --bearer in shared shells")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.query_url:
        parser.error("--query-url is required unless --self-test is used")
    try:
        return run(args)
    except (RuntimeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
