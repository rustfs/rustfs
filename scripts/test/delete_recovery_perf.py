#!/usr/bin/env python3
"""Compare strict DELETE stages using isolated, resource-matched ABBA cohorts.

Both binaries require a JSON receipt with binary_sha256, source_head, and an
identical build object containing profile=release, opt_level=3, target, rustc,
features, default_features, lto, and codegen_units. Disk operation service times
include concurrent work; they are not an additive wall-clock decomposition.
"""

import argparse
import hashlib
import json
from pathlib import Path
import random
import re
import statistics
import sys
import time
import uuid

import delete_recovery_docker as harness


def binary_digest(binary):
    digest = hashlib.sha256()
    with binary.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def binary_receipt(binary, receipt):
    value = json.loads(receipt.read_text())
    if value.get("binary_sha256") != binary_digest(binary):
        raise ValueError(f"binary does not match its receipt: {binary}")
    if not re.fullmatch(r"[0-9a-f]{40}", value.get("source_head", "")):
        raise ValueError("receipt requires an exact source_head")
    build = value["build"]
    required = {"profile", "opt_level", "target", "rustc", "features", "default_features", "lto", "codegen_units"}
    if not required <= build.keys() or build["profile"] != "release" or build["opt_level"] != 3:
        raise ValueError("receipts must describe optimized release builds")
    if not isinstance(build["features"], list) or "e2e-test-hooks" in build["features"]:
        raise ValueError("performance receipts must exclude test-only hooks")
    return value


def operation_snapshot(client):
    _, body = client.request("GET", "rustfs/admin/v3/storageinfo", expected=200)
    found = []

    def visit(value):
        if isinstance(value, dict):
            if "api_calls" in value and "last_minute" in value:
                found.append(value)
            else:
                for item in value.values():
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(json.loads(body))
    if len(found) != 4:
        raise AssertionError("the stage probe requires four complete local disk metrics")
    result = {}
    for metric in found:
        for operation, sample in metric["last_minute"].items():
            item = result.setdefault(operation, {"count": 0, "ns": 0})
            item["count"] += sample["count"]
            item["ns"] += sample["acc_time_ns"]
    return result, [metric["last_minute"].get("discover_delete_transactions", {}).get("count", 0) for metric in found]


def stage_delta(before, after):
    result = {}
    for operation in before.keys() | after.keys():
        old = before.get(operation, {"count": 0, "ns": 0})
        new = after.get(operation, {"count": 0, "ns": 0})
        count, elapsed = new["count"] - old["count"], new["ns"] - old["ns"]
        if count < 0 or elapsed < 0:
            raise ValueError("the rolling metric window expired; stage attribution is invalid")
        if count:
            result[operation] = {"calls": count, "service_ms": elapsed / 1_000_000}
    return result


def drift_percent(first, second):
    if min(first, second) <= 0:
        raise ValueError("benchmark durations must be positive")
    return abs(first - second) / min(first, second) * 100


def measure(cluster, case, count, size, batch, repeat):
    client = cluster.clients[0]
    bucket = f"delete-perf-{case}-{repeat}"
    payload = random.Random(2849 + size).randbytes(size)
    keys = [f"object-{index:04d}" for index in range(count)]
    client.put_bucket(bucket)
    for key in keys:
        client.put(bucket, key, payload)
        etag = client.last_headers.get("etag")
        assert etag, "PUT must acknowledge the object's ETag"
        assert client.get(bucket, key) == payload, "complete bytes must match before deletion"
        assert client.last_headers.get("etag") == etag
    assert sorted(client.list(bucket)) == keys
    inventory = harness.residue_inventory(cluster, bucket, keys[0])
    for node in inventory:
        for drive in node:
            external = any(path.rsplit("/", 1)[-1] == "part.1" for path in drive["files"])
            assert external == (size > 128 * 1024), "the fixture must exercise its declared physical layout"
    harness.docker("stop", "--time", "30", cluster.names[0])
    restart_started = time.perf_counter()
    cluster.restart()
    ready_seconds = time.perf_counter() - restart_started
    deadline = time.monotonic() + 90
    while True:
        startup_operations, completed = operation_snapshot(client)
        if all(count >= 1 for count in completed):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("clean restart did not complete raw discovery on all four disks")
        time.sleep(0.02)
    # Each fixture has one bucket and fewer than 4096 raw entries per disk,
    # so its complete initial sweep uses one bounded page on each disk.
    assert completed == [1] * 4, "unexpected discovery scopes invalidate startup attribution"
    startup = {
        "ready_seconds": ready_seconds,
        "initial_discovery_complete_seconds": time.perf_counter() - restart_started,
        "discovery_within_30_seconds": time.perf_counter() - restart_started <= 30,
        "raw_discovery_operations": startup_operations["discover_delete_transactions"],
    }
    assert sorted(client.list(bucket)) == keys, "the clean restart must preserve the fixture"
    # Compare DELETE at the same process age. A faster startup must not move
    # only the candidate into earlier periodic maintenance phases.
    time.sleep(max(0, 35 - (time.perf_counter() - restart_started)))
    before, _ = operation_snapshot(client)
    latencies = []
    started = time.perf_counter()
    if batch:
        client.batch_delete(bucket, keys)
    else:
        for key in keys:
            tick = time.perf_counter()
            client.delete(bucket, key)
            latencies.append(time.perf_counter() - tick)
    elapsed = time.perf_counter() - started
    after, _ = operation_snapshot(client)
    if time.perf_counter() - restart_started >= 59:
        raise ValueError("the process-age bound no longer protects the rolling operation window")
    assert client.list(bucket) == []
    leftovers = cluster.inspect_data(
        "from pathlib import Path\nimport json\n"
        f"keys={keys!r};bucket={bucket!r}\n"
        "print(json.dumps([str(d / bucket / key) for d in Path('/data').glob('d*') "
        "for key in keys if (d / bucket / key).exists()]))"
    )
    assert all(not node for node in leftovers), "all requested object directories must converge physically"
    client.delete_bucket(bucket)
    return {
        "case": case, "repeat": repeat, "objects": count, "object_bytes": size,
        "wall_seconds": elapsed, "operations": stage_delta(before, after), "clean_restart": startup,
        "single_median_ms": statistics.median(latencies) * 1000 if latencies else None,
        "complete_bytes_etag_and_physical_cleanup": True,
    }


def comparison_report(samples, drift_limit):
    medians = {cohort: {case: statistics.median(item["wall_seconds"] for item in values if item["case"] == case)
                       for case in ("inline-batch", "inline-single", "external-batch")}
               for cohort, values in samples.items()}
    drifts = {case: {"baseline_percent": drift_percent(medians["A1"][case], medians["A2"][case]),
                     "candidate_percent": drift_percent(medians["B1"][case], medians["B2"][case])}
              for case in medians["A1"]}
    startup_ok = all(sample["clean_restart"]["discovery_within_30_seconds"]
                     for cohort in ("B1", "B2") for sample in samples[cohort])
    valid = startup_ok and all(value <= drift_limit for pair in drifts.values() for value in pair.values())
    report = {"verdict": "comparable" if valid else "inconclusive", "medians_seconds": medians,
              "drift": drifts, "candidate_discovery_within_30_seconds": startup_ok}
    if valid:
        report["wall_time_reduction_percent"] = {
            case: (1 - statistics.median([medians["B1"][case], medians["B2"][case]]) /
                   statistics.median([medians["A1"][case], medians["A2"][case]])) * 100
            for case in medians["A1"]}
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for variant in ("baseline", "candidate"):
        parser.add_argument(f"--{variant}-binary", type=Path, required=True)
        parser.add_argument(f"--{variant}-receipt", type=Path, required=True)
    parser.add_argument("--image", required=True, help="Existing local image with Python 3 and curl")
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--cpuset-cpus", required=True)
    parser.add_argument("--memory", default="4g")
    parser.add_argument("--port", type=int, default=19640)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--drift-limit-percent", type=float, default=10)
    args = parser.parse_args()
    if args.repetitions < 3 or not 0 < args.drift_limit_percent <= 10:
        parser.error("at least three repetitions and a drift limit in (0, 10] are required")
    if not 1024 <= args.port <= 65532 or not re.fullmatch(r"[0-9,-]+", args.cpuset_cpus):
        parser.error("invalid port or CPU set")
    baseline = binary_receipt(args.baseline_binary, args.baseline_receipt)
    candidate = binary_receipt(args.candidate_binary, args.candidate_receipt)
    if baseline["build"] != candidate["build"]:
        parser.error("baseline and candidate build configurations differ")
    args.artifact_dir.mkdir(parents=True, exist_ok=False)
    inputs = {"baseline": baseline, "candidate": candidate, "configuration": vars(args).copy()}
    (args.artifact_dir / "inputs.json").write_text(json.dumps(inputs, default=str, indent=2))
    image = harness.docker("image", "inspect", "--format", "{{.Id}}", args.image).stdout.decode().strip()
    original_docker = harness.docker

    def resource_matched_docker(*command, check=True):
        if command and command[0] == "run" and "-d" in command:
            command = (command[0], "--cpuset-cpus", args.cpuset_cpus, "--memory", args.memory, *command[1:])
        return original_docker(*command, check=check)

    harness.docker = resource_matched_docker
    samples = {}
    for cohort, binary in (("A1", args.baseline_binary), ("B1", args.candidate_binary),
                           ("B2", args.candidate_binary), ("A2", args.baseline_binary)):
        root = args.artifact_dir / cohort
        root.mkdir()
        harness.ROOT, harness.PREFIX = root, f"delete-perf-{uuid.uuid4().hex[:12]}"
        harness.IMAGE, harness.PORT = image, args.port
        harness.CANDIDATE, harness.BASELINE = binary.resolve(), None
        # Keep normal scanner/heal work out of stage attribution. Automatic
        # transaction recovery is initialized by ECStore and remains enabled.
        cluster = harness.Cluster("strict", drives=4, extra_env={
            "RUSTFS_SCANNER_ENABLED": "false", "RUSTFS_HEAL_ENABLED": "false",
        })
        samples[cohort] = []
        try:
            cluster.create()
            for repeat in range(args.repetitions):
                for case, count, size, batch in (("inline-batch", 500, 4096, True),
                                                ("inline-single", 32, 4096, False),
                                                ("external-batch", 32, 1024 * 1024, True)):
                    result = measure(cluster, case, count, size, batch, repeat)
                    samples[cohort].append(result)
                    (root / "samples.json").write_text(json.dumps(samples[cohort], indent=2))
                    print(json.dumps({"cohort": cohort, **result}), flush=True)
        finally:
            cluster.remove()
    binary_receipt(args.baseline_binary, args.baseline_receipt)
    binary_receipt(args.candidate_binary, args.candidate_receipt)
    report = comparison_report(samples, args.drift_limit_percent)
    report["resource_contract"] = {
        "cpuset_cpus": args.cpuset_cpus, "memory": args.memory, "image": image,
        "topology": "1 process / 4 logical drives on one Docker host filesystem",
    }
    (args.artifact_dir / "result.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report), flush=True)
    return 0 if report["verdict"] == "comparable" else 2


if __name__ == "__main__":
    sys.exit(main())
