#!/usr/bin/env python3
"""Summarize per-round PUT service metric deltas from before/after snapshots."""

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path


PROM_LINE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-csv", required=True, help="aggregate_service_metrics_captures.csv path")
    parser.add_argument("--delta-csv", required=True, help="Output generic metric delta CSV")
    parser.add_argument("--path-summary-csv", required=True, help="Output PUT path delta summary CSV")
    parser.add_argument("--stage-summary-csv", required=True, help="Output PUT stage duration summary CSV")
    return parser.parse_args()


def split_label_items(labels: str) -> list[str]:
    items: list[str] = []
    start = 0
    in_quotes = False
    escaped = False
    for index, char in enumerate(labels):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == '"':
            in_quotes = not in_quotes
            continue
        if char == "," and not in_quotes:
            items.append(labels[start:index])
            start = index + 1
    items.append(labels[start:])
    return [item.strip() for item in items if item.strip()]


def parse_labels(labels: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for item in split_label_items(labels):
        key, sep, value = item.partition("=")
        if not sep:
            continue
        value = value.strip()
        if value.startswith('"') and value.endswith('"') and len(value) >= 2:
            value = value[1:-1]
            value = value.replace(r"\"", '"').replace(r"\\", "\\")
        parsed[key.strip()] = value
    return parsed


def canonical_labels(labels: dict[str, str]) -> str:
    return ",".join(f'{key}="{escape_label_value(labels[key])}"' for key in sorted(labels))


def escape_label_value(value: str) -> str:
    return value.replace("\\", r"\\").replace('"', r"\"").replace("\n", r"\n")


def parse_prom(path: Path) -> dict[tuple[str, str], float]:
    metrics: dict[tuple[str, str], float] = {}
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            match = PROM_LINE_RE.match(line)
            if not match:
                continue
            metric, raw_labels, raw_value = match.groups()
            labels = canonical_labels(parse_labels(raw_labels or ""))
            try:
                value = float(raw_value)
            except ValueError:
                continue
            metrics[(metric, labels)] = value
    return metrics


def classify_metric(metric: str) -> str:
    if metric == "rustfs_s3_put_object_path_total":
        return "path_total"
    if metric == "rustfs_s3_put_object_diagnostics_total":
        return "diagnostics_total"
    if metric.endswith("_bucket"):
        return "histogram_bucket"
    if metric.endswith("_count"):
        return "histogram_count"
    if metric.endswith("_sum"):
        return "histogram_sum"
    if metric.endswith("_total"):
        return "counter_total"
    return "gauge_delta"


def is_monotonic_metric(metric: str) -> bool:
    return (
        metric.endswith("_total")
        or metric.endswith("_bucket")
        or metric.endswith("_count")
        or metric.endswith("_sum")
    )


def group_capture_rows(capture_csv: Path) -> dict[tuple[str, str, str, str, str, str], dict[str, dict[str, str]]]:
    groups: dict[tuple[str, str, str, str, str, str], dict[str, dict[str, str]]] = defaultdict(dict)
    with capture_csv.open("r", encoding="utf-8", newline="") as file:
        for row in csv.DictReader(file):
            phase = row.get("phase", "")
            if phase not in {"before", "after"}:
                continue
            key = (
                row.get("concurrency", ""),
                row.get("run_dir", ""),
                row.get("size", ""),
                row.get("tool", ""),
                row.get("round", ""),
                row.get("attempt", ""),
            )
            groups[key][phase] = row
    return groups


def label_value(labels: str, name: str) -> str:
    return parse_labels(labels).get(name, "")


def write_outputs(args: argparse.Namespace) -> None:
    capture_csv = Path(args.capture_csv)
    delta_csv = Path(args.delta_csv)
    path_summary_csv = Path(args.path_summary_csv)
    stage_summary_csv = Path(args.stage_summary_csv)
    for output in (delta_csv, path_summary_csv, stage_summary_csv):
        output.parent.mkdir(parents=True, exist_ok=True)

    groups = group_capture_rows(capture_csv)

    delta_fields = [
        "concurrency",
        "run_dir",
        "size",
        "tool",
        "round",
        "attempt",
        "source",
        "before_status",
        "after_status",
        "metric",
        "labels",
        "before",
        "after",
        "delta",
        "classification",
    ]
    path_fields = [
        "concurrency",
        "run_dir",
        "size",
        "tool",
        "round",
        "attempt",
        "path",
        "delta",
        "before_status",
        "after_status",
    ]
    stage_fields = [
        "concurrency",
        "run_dir",
        "size",
        "tool",
        "round",
        "attempt",
        "stage",
        "count_delta",
        "sum_delta",
        "avg_ms",
        "before_status",
        "after_status",
    ]

    with (
        delta_csv.open("w", encoding="utf-8", newline="") as delta_file,
        path_summary_csv.open("w", encoding="utf-8", newline="") as path_file,
        stage_summary_csv.open("w", encoding="utf-8", newline="") as stage_file,
    ):
        delta_writer = csv.DictWriter(delta_file, fieldnames=delta_fields)
        path_writer = csv.DictWriter(path_file, fieldnames=path_fields)
        stage_writer = csv.DictWriter(stage_file, fieldnames=stage_fields)
        delta_writer.writeheader()
        path_writer.writeheader()
        stage_writer.writeheader()

        for key in sorted(groups):
            phases = groups[key]
            before_row = phases.get("before")
            after_row = phases.get("after")
            if not before_row or not after_row:
                continue

            before_path = Path(before_row.get("snapshot_file", ""))
            after_path = Path(after_row.get("snapshot_file", ""))
            if before_row.get("status") != "ok" or after_row.get("status") != "ok":
                continue
            if not before_path.is_file() or not after_path.is_file():
                continue

            before = parse_prom(before_path)
            after = parse_prom(after_path)
            concurrency, run_dir, size, tool, round_no, attempt = key
            source = after_row.get("source", before_row.get("source", ""))
            before_status = before_row.get("status", "")
            after_status = after_row.get("status", "")

            stage_counts: dict[str, float] = defaultdict(float)
            stage_sums: dict[str, float] = defaultdict(float)

            for metric_key in sorted(after):
                metric, labels = metric_key
                before_value = before.get(metric_key, 0.0)
                after_value = after.get(metric_key, 0.0)
                delta = after_value - before_value
                if is_monotonic_metric(metric) and delta < 0:
                    continue
                if delta == 0:
                    continue

                classification = classify_metric(metric)
                delta_writer.writerow(
                    {
                        "concurrency": concurrency,
                        "run_dir": run_dir,
                        "size": size,
                        "tool": tool,
                        "round": round_no,
                        "attempt": attempt,
                        "source": source,
                        "before_status": before_status,
                        "after_status": after_status,
                        "metric": metric,
                        "labels": labels,
                        "before": f"{before_value:.12g}",
                        "after": f"{after_value:.12g}",
                        "delta": f"{delta:.12g}",
                        "classification": classification,
                    }
                )

                if metric == "rustfs_s3_put_object_path_total":
                    path_writer.writerow(
                        {
                            "concurrency": concurrency,
                            "run_dir": run_dir,
                            "size": size,
                            "tool": tool,
                            "round": round_no,
                            "attempt": attempt,
                            "path": label_value(labels, "path"),
                            "delta": f"{delta:.12g}",
                            "before_status": before_status,
                            "after_status": after_status,
                        }
                    )

                if metric.endswith("_stage_duration_ms_count"):
                    stage = label_value(labels, "stage")
                    if stage:
                        stage_counts[stage] += delta
                elif metric.endswith("_stage_duration_ms_sum"):
                    stage = label_value(labels, "stage")
                    if stage:
                        stage_sums[stage] += delta
                elif metric.endswith("_stage_duration_seconds_count"):
                    stage = label_value(labels, "stage")
                    if stage:
                        stage_counts[stage] += delta
                elif metric.endswith("_stage_duration_seconds_sum"):
                    stage = label_value(labels, "stage")
                    if stage:
                        stage_sums[stage] += delta * 1000.0

            for stage in sorted(set(stage_counts) | set(stage_sums)):
                count_delta = stage_counts.get(stage, 0.0)
                if count_delta <= 0:
                    continue
                sum_delta = stage_sums.get(stage, 0.0)
                avg_ms = sum_delta / count_delta if count_delta else 0.0
                stage_writer.writerow(
                    {
                        "concurrency": concurrency,
                        "run_dir": run_dir,
                        "size": size,
                        "tool": tool,
                        "round": round_no,
                        "attempt": attempt,
                        "stage": stage,
                        "count_delta": f"{count_delta:.12g}",
                        "sum_delta": f"{sum_delta:.12g}",
                        "avg_ms": f"{avg_ms:.6f}",
                        "before_status": before_status,
                        "after_status": after_status,
                    }
                )


def main() -> None:
    write_outputs(parse_args())


if __name__ == "__main__":
    main()
