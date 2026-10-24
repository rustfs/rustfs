#!/usr/bin/env python3
"""Extract histogram and integer counter windows without filling missing data."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import sys

MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON field")
        result[key] = value
    return result


def uint(value, name):
    # OTLP JSON encodes uint64 as strings; accepting floats loses nanoseconds.
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        value = int(value)
    require(type(value) is int and 0 <= value < 2**64, f"invalid {name}")
    return value


def attributes(value):
    require(isinstance(value, list), "attributes must be an array")
    result = {}
    for entry in value:
        require(isinstance(entry, dict), "invalid attribute")
        key = entry.get("key")
        require(isinstance(key, str) and key and key not in result, "duplicate or missing attribute key")
        require(isinstance(entry.get("value"), dict) and entry["value"], "missing attribute value")
        result[key] = entry["value"]
    return result


def identity(resource, scope, metric, point, resource_schema, scope_schema):
    require(uint(resource.get("droppedAttributesCount", 0), "resource dropped attributes") == 0,
            "resource identity has dropped attributes")
    require(uint(scope.get("droppedAttributesCount", 0), "scope dropped attributes") == 0,
            "scope identity has dropped attributes")
    return {
        "resource_schema": resource_schema,
        "resource": attributes(resource.get("attributes", [])),
        "scope_schema": scope_schema,
        "scope": {"name": scope.get("name", ""), "version": scope.get("version", ""),
                  "attributes": attributes(scope.get("attributes", []))},
        "name": metric["name"],
        "unit": metric.get("unit", ""),
        "attributes": attributes(point.get("attributes", [])),
    }


def point_times(point):
    require(uint(point.get("flags", 0), "point flags") == 0, "point has no recorded value or unknown flags")
    start = uint(point.get("startTimeUnixNano"), "startTimeUnixNano")
    end = uint(point.get("timeUnixNano"), "timeUnixNano")
    require(0 < start <= end, "missing or reversed point timestamps")
    return {"start": start, "end": end}


def counter_point(point):
    require(set(point) & {"asInt", "asDouble"} == {"asInt"}, "counter must have one integer value")
    value = uint(point.get("asInt"), "counter value")
    require(value < 2**63, "counter exceeds OTLP signed int64 range")
    return {**point_times(point), "kind": "sum", "value": value}


def histogram_point(point):
    times = point_times(point)
    bounds = point.get("explicitBounds", [])
    buckets = point.get("bucketCounts", [])
    require(isinstance(bounds, list) and isinstance(buckets, list), "invalid histogram buckets")
    require(all(type(v) in (float, int) and math.isfinite(v) for v in bounds), "non-finite histogram boundary")
    require(all(a < b for a, b in zip(bounds, bounds[1:])), "unordered histogram boundaries")
    require(len(buckets) == len(bounds) + 1, "missing or inconsistent histogram buckets")
    buckets = [uint(value, "bucket count") for value in buckets]
    count = uint(point.get("count", 0), "histogram count")
    require(sum(buckets) == count, "histogram count does not match buckets")
    return {**times, "kind": "histogram", "count": count, "bounds": bounds, "buckets": buckets}


def snapshot(document, names):
    require(isinstance(document, dict), "snapshot must be an OTLP export object")
    series = {}
    seen = set()
    for resource_metrics in document.get("resourceMetrics", []):
        resource = resource_metrics.get("resource", {})
        for scope_metrics in resource_metrics.get("scopeMetrics", []):
            scope = scope_metrics.get("scope", {})
            for metric in scope_metrics.get("metrics", []):
                if metric.get("name") not in names:
                    continue
                kinds = set(metric) & {"histogram", "exponentialHistogram", "sum", "gauge", "summary"}
                require(kinds in ({"histogram"}, {"sum"}), "ambiguous or unsupported metric type")
                kind = next(iter(kinds))
                data = metric[kind]
                require(isinstance(data, dict), f"{metric['name']} has invalid metric data")
                require(data.get("aggregationTemporality") in (2, "AGGREGATION_TEMPORALITY_CUMULATIVE"),
                        f"{metric['name']} is not cumulative")
                if kind == "sum":
                    require(data.get("isMonotonic") is True, "counter sum must be monotonic")
                for point in data.get("dataPoints", []):
                    key = identity(resource, scope, metric, point,
                                   resource_metrics.get("schemaUrl", ""), scope_metrics.get("schemaUrl", ""))
                    encoded = json.dumps(key, sort_keys=True, allow_nan=False)
                    require(encoded not in series, "duplicate metric series in snapshot")
                    series[encoded] = (key, histogram_point(point) if kind == "histogram" else counter_point(point))
                    seen.add(metric["name"])
    require(seen == set(names), "snapshot is missing requested metric observations")
    return series


def quantile_bounds(buckets, bounds, numerator):
    count = sum(buckets)
    if count == 0:
        return None
    rank = (count * numerator + 99) // 100
    cumulative = 0
    for index, bucket in enumerate(buckets):
        cumulative += bucket
        if cumulative >= rank:
            # The bucket only bounds the quantile; interpolation invents precision.
            return {"lower_exclusive": bounds[index - 1] if index else None,
                    "upper_inclusive": bounds[index] if index < len(bounds) else None}
    raise ValueError("quantile rank exceeds histogram count")


def window(before, after, names, expected_start=None, expected_end=None):
    require(names and len(names) == len(set(names)), "metric names must be nonempty and unique")
    require((expected_start is None) == (expected_end is None), "both expected window boundaries are required")
    if expected_start is not None:
        expected_start = uint(expected_start, "expected start")
        expected_end = uint(expected_end, "expected end")
        require(0 < expected_start < expected_end, "invalid expected window")
    first, last = snapshot(before, names), snapshot(after, names)
    require(first.keys() == last.keys(), "metric series identity changed or is missing at a boundary")
    rows = []
    for key in sorted(first):
        descriptor, a = first[key]
        _, b = last[key]
        require(a["kind"] == b["kind"], "metric type changed within measurement window")
        require(a["start"] == b["start"], "metric reset within measurement window")
        require(a["end"] < b["end"], "duplicate or reversed snapshot time")
        if expected_start is not None:
            require((a["end"], b["end"]) == (expected_start, expected_end),
                    "metric does not match the requested measurement window")
        row = {**descriptor, "kind": a["kind"], "window_start_ns": a["end"], "window_end_ns": b["end"]}
        if a["kind"] == "sum":
            delta = b["value"] - a["value"]
            require(delta >= 0, "integer counter decreased")
            rows.append({**row, "delta": delta})
            continue
        require(a["bounds"] == b["bounds"], "histogram boundaries changed")
        buckets = [end - start for start, end in zip(a["buckets"], b["buckets"])]
        require(all(value >= 0 for value in buckets), "histogram bucket counter decreased")
        count = b["count"] - a["count"]
        require(count == sum(buckets), "window count does not match buckets")
        rows.append({**row,
                     "count": count, "explicit_bounds": a["bounds"], "bucket_counts": buckets,
                     "p95": quantile_bounds(buckets, a["bounds"], 95),
                     "p99": quantile_bounds(buckets, a["bounds"], 99)})
    return {"window_checked": expected_start is not None, "series": rows}


def load(path, input_format):
    require(path.stat().st_size <= MAX_SNAPSHOT_BYTES, "oversized OTLP snapshot")
    raw = path.read_bytes()
    if input_format == "protobuf":
        try:
            from google.protobuf.json_format import MessageToDict
            from google.protobuf.message import DecodeError
            from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
        except ImportError as error:
            raise ValueError("protobuf input requires opentelemetry-proto in the active Python environment") from error
        request = ExportMetricsServiceRequest()
        try:
            request.ParseFromString(raw)
        except DecodeError as error:
            raise ValueError("malformed OTLP protobuf") from error
        document = MessageToDict(request)
    else:
        document = json.loads(raw, object_pairs_hook=unique_object)
    return document, hashlib.sha256(raw).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--metric", action="append", required=True)
    parser.add_argument("--format", choices=("json", "protobuf"), default="json")
    parser.add_argument("--start-ns", type=int)
    parser.add_argument("--end-ns", type=int)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    try:
        before, before_sha = load(args.before, args.format)
        after, after_sha = load(args.after, args.format)
        result = window(before, after, args.metric, args.start_ns, args.end_ns)
        result["snapshots"] = {"before_sha256": before_sha, "after_sha256": after_sha}
        with args.out.open("x", encoding="utf-8") as output:
            output.write(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")
    except (ValueError, OSError, TypeError, AttributeError) as error:
        print(f"OTLP window rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
