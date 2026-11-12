#!/usr/bin/env python3
"""Check recorded gRPC measurement windows for process and maintenance stability.

Read-only: consume OTLP JSON snapshots and independently recorded process facts.
This checks observations, not throughput, RPC coverage, or an application SLO.
"""

import argparse
import hashlib
import json
import math
from pathlib import Path
import sys
import uuid

from scanner_otlp_window import attributes, counter_point, identity, load, require, uint, unique_object


REQUIRED_GAUGES = frozenset({
    "rustfs_heal_mrf_queue_depth",
    "rustfs_memory_allocator_reclaim_heal_activity_current",
    "rustfs_memory_allocator_reclaim_scanner_activity_current",
})
OPTIONAL_GAUGES = frozenset({"rustfs_heal_task_running"})
COUNTERS = frozenset({"rustfs_heal_task_start_total"})
METRICS = REQUIRED_GAUGES | OPTIONAL_GAUGES | COUNTERS


def processes(rows):
    require(isinstance(rows, list) and 0 < len(rows) <= 512, "invalid process inventory")
    result = {}
    for row in rows:
        node = row.get("node")
        require(isinstance(node, str) and node and node not in result, "duplicate or missing node identity")
        pid = uint(row.get("pid"), "process pid")
        start = uint(row.get("start_ticks"), "process start ticks")
        boot = row.get("boot_id")
        require(isinstance(boot, str) and uuid.UUID(boot).int > 0, "missing process boot identity")
        require(pid > 0 and start > 0, "missing process identity")
        result[node] = (pid, start, boot)
    return result


def resource_process(resource):
    values = attributes(resource.get("attributes", []))
    node = values.get("service.instance.id", {}).get("stringValue")
    pid = values.get("process.pid", {})
    if set(pid) == {"intValue"}:
        pid = uint(pid["intValue"], "resource process pid")
    elif set(pid) == {"stringValue"}:
        pid = uint(pid["stringValue"], "resource process pid")
    else:
        pid = None
    return node, pid


def gauge_point(point):
    require(uint(point.get("flags", 0), "point flags") == 0, "gauge has no recorded value or unknown flags")
    require(set(point) & {"asInt", "asDouble"} in ({"asInt"}, {"asDouble"}), "ambiguous or missing gauge value")
    value = uint(point["asInt"], "gauge value") if "asInt" in point else point["asDouble"]
    require(type(value) in (int, float) and math.isfinite(value) and value >= 0, "invalid gauge value")
    return value


def observations(document, inventory, captured, max_age):
    require(isinstance(document, dict), "snapshot must be an OTLP export object")
    found = set()
    result = {}
    ignored = 0
    for resource_metrics in document.get("resourceMetrics", []):
        resource = resource_metrics.get("resource", {})
        node, pid = resource_process(resource)
        if node not in inventory or pid != inventory[node][0]:
            ignored += 1
            continue
        for scope_metrics in resource_metrics.get("scopeMetrics", []):
            scope = scope_metrics.get("scope", {})
            for metric in scope_metrics.get("metrics", []):
                name = metric.get("name")
                if name not in METRICS:
                    continue
                kind = "sum" if name in COUNTERS else "gauge"
                require(set(metric) & {"sum", "gauge", "histogram", "summary", "exponentialHistogram"} == {kind},
                        "unexpected maintenance metric type")
                data = metric[kind]
                if kind == "sum":
                    require(data.get("isMonotonic") is True and data.get("aggregationTemporality") in
                            (2, "AGGREGATION_TEMPORALITY_CUMULATIVE"), "maintenance counter must be cumulative")
                for point in data.get("dataPoints", []):
                    require(uint(point.get("droppedAttributesCount", 0), "point dropped attributes") == 0,
                            "maintenance point identity has dropped attributes")
                    end = uint(point.get("timeUnixNano"), "metric timestamp")
                    require(0 < end <= captured and captured - end <= max_age, "stale or future maintenance observation")
                    descriptor = identity(resource, scope, metric, point,
                                          resource_metrics.get("schemaUrl", ""), scope_metrics.get("schemaUrl", ""))
                    key = json.dumps(descriptor, sort_keys=True, allow_nan=False)
                    require(key not in result, "duplicate maintenance series")
                    value = counter_point(point) if kind == "sum" else {"value": gauge_point(point), "end": end}
                    result[key] = {**value, "node": node, "name": name, "kind": kind}
                    found.add((node, name))
    require(all((node, name) in found for node in inventory for name in REQUIRED_GAUGES),
            "missing current-process maintenance observations")
    return result, ignored


def check(manifest, root, expected_nodes=4):
    require(isinstance(manifest, dict) and manifest.get("scope") == "measurement",
            "a measurement-only window is required; command/prepare/cleanup windows cannot qualify")
    inventory = processes(manifest.get("processes"))
    require(type(expected_nodes) is int and 0 < expected_nodes <= 512 and len(inventory) == expected_nodes,
            "process inventory does not match the declared fleet size")
    start = uint(manifest.get("start_ns"), "window start")
    end = uint(manifest.get("end_ns"), "window end")
    gap = uint(manifest.get("max_sample_gap_ns"), "maximum sample gap")
    age = uint(manifest.get("max_export_age_ns"), "maximum export age")
    require(0 < start < end and gap > 0 and age > 0, "invalid observation window or sampling budget")
    samples = manifest.get("samples")
    require(isinstance(samples, list) and 3 <= len(samples) <= 4096, "at least three bounded observations are required")
    previous = None
    previous_time = None
    ignored = 0
    violations = []
    paths = set()
    receipts = []
    for index, sample in enumerate(samples):
        require(sample.get("phase") == "measurement", "observation belongs to a different workload phase")
        require(processes(sample.get("processes")) == inventory, "process restarted, changed, or was not observed")
        captured = uint(sample.get("captured_ns"), "capture timestamp")
        require(start <= captured <= end, "observation outside the measurement window")
        if previous_time is not None:
            require(0 < captured - previous_time <= gap, "duplicate, reversed, or missing observations")
        else:
            require(captured == start, "missing measurement-start observation")
        require(isinstance(sample.get("path"), str) and sample["path"], "missing snapshot path")
        path = (root / sample["path"]).resolve()
        require(path not in paths, "snapshot file reused across observations")
        paths.add(path)
        document, digest = load(path, "json")
        receipts.append({"path": sample["path"], "sha256": digest, "captured_ns": captured})
        current, skipped = observations(document, inventory, captured, age)
        ignored += skipped
        for key, point in current.items():
            if point["kind"] == "gauge":
                if point["value"] != 0:
                    violations.append({"sample": index, "node": point["node"], "metric": point["name"], "reason": "nonzero"})
            elif previous is not None:
                before = previous.get(key)
                if before is None:
                    if point["value"] != 0:
                        violations.append({"sample": index, "node": point["node"], "metric": point["name"],
                                           "reason": "new_nonzero_counter"})
                else:
                    require(before["start"] == point["start"] and point["value"] >= before["value"],
                            "maintenance counter reset within measurement window")
                    if point["value"] > before["value"]:
                        violations.append({"sample": index, "node": point["node"], "metric": point["name"],
                                           "reason": "counter_increased"})
            if previous is not None and key in previous:
                require(point["end"] > previous[key]["end"], "cached maintenance point repeated across captures")
        if previous is not None:
            require(previous.keys() <= current.keys(), "maintenance series disappeared within measurement window")
        previous, previous_time = current, captured
    require(previous_time == end, "missing measurement-end observation")
    return {"observed_stable": not violations, "scope": "measurement", "start_ns": start, "end_ns": end,
            "samples": len(samples), "processes": len(inventory), "max_sample_gap_ns": gap,
            "max_export_age_ns": age, "ignored_resource_exports": ignored, "violations": violations,
            "snapshots": receipts,
            "limits": "Sampled maintenance observations only; RPC coverage, per-request timing, drift and SLO gates remain required."}, paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--expected-nodes", type=int, default=4)
    args = parser.parse_args()
    try:
        require(args.manifest.stat().st_size <= 1024 * 1024, "oversized window manifest")
        raw = args.manifest.read_bytes()
        manifest = json.loads(raw, object_pairs_hook=unique_object)
        result, inputs = check(manifest, args.manifest.resolve().parent, args.expected_nodes)
        require(args.out.resolve() not in inputs | {args.manifest.resolve()}, "output would overwrite raw evidence")
        if args.out.exists():
            require(not any(args.out.samefile(path) for path in inputs | {args.manifest.resolve()}),
                    "output aliases raw evidence")
        result["manifest_sha256"] = hashlib.sha256(raw).hexdigest()
        args.out.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
        return 0 if result["observed_stable"] else 1
    except (ValueError, OSError, TypeError, KeyError, AttributeError) as error:
        print(f"Invalid gRPC measurement evidence: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
