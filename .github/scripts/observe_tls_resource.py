#!/usr/bin/env python3
"""Collect bounded, allowlisted local metrics for one fixed TLS e2e case."""

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import xml.etree.ElementTree as ET

SOURCE = "fedc9666123076f2b985d75a9c290f6496ef8717"
TREE = "1ea486f6d6a86ce488b83cab4a1b232188ab9708"
CASE = "distributed::internode_tls_test::four_node_tls_large_and_range_gets_preserve_bytes"
FILTER = f"test(={CASE})"
INTERVAL_SECONDS = 2
MAX_SAMPLES = 900
MAX_SCAN_PIDS = 2048
SCAN_EVERY_SAMPLES = 5
MAX_PROCESS_SCANS = 120
MAX_ANCESTRY_DEPTH = 32
PRIVATE = Path(os.environ["RUNNER_TEMP"]) / "tls-private"
EVIDENCE = Path(os.environ["RUNNER_TEMP"]) / "tls-evidence"
REPORT = PRIVATE / "acceptance.json"
PIDS = PRIVATE / "acceptance.pids.json"
JUNIT = Path("target/nextest/e2e-distributed/junit.xml")
TEST_LOG = PRIVATE / "test.log"


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(name, value):
    (EVIDENCE / name).write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")


def read_text(path):
    try:
        return Path(path).read_text(errors="replace"), "available"
    except PermissionError:
        return None, "denied"
    except FileNotFoundError:
        return None, "missing"
    except OSError:
        return None, "unavailable"


def process_stat(pid):
    raw, state = read_text(f"/proc/{pid}/stat")
    if raw is None:
        return None, state
    try:
        # comm can contain spaces and parentheses; no name or command is retained.
        fields = raw[raw.rfind(")") + 2 :].split()
        data = {
            "state_code": fields[0],
            "ppid": int(fields[1]),
            "user_ticks": int(fields[11]),
            "system_ticks": int(fields[12]),
            "start_ticks": int(fields[19]),
            "rss_pages": int(fields[21]),
        }
        if data["start_ticks"] < 0 or data["rss_pages"] < 0:
            raise ValueError
        return data, "available"
    except (IndexError, ValueError):
        return None, "malformed"


def process_io(pid):
    raw, state = read_text(f"/proc/{pid}/io")
    if raw is None:
        return {"state": state, "values": None}
    wanted = {"rchar", "wchar", "read_bytes", "write_bytes", "cancelled_write_bytes"}
    values = {}
    try:
        for line in raw.splitlines():
            key, value = line.split(":", 1)
            if key in wanted:
                values[key] = int(value.strip())
        if set(values) != wanted or any(v < 0 for v in values.values()):
            raise ValueError
        return {"state": "available", "values": values}
    except ValueError:
        return {"state": "malformed", "values": None}


def sample_process(identity):
    if identity is None:
        return {"state": "not_observed", "pid": None, "start_ticks": None, "cpu": None, "rss_bytes": None, "io": None}
    pid, expected_start = identity
    stat, state = process_stat(pid)
    if stat is None:
        return {"state": state, "pid": pid, "start_ticks": expected_start, "cpu": None, "rss_bytes": None, "io": None}
    if stat["start_ticks"] != expected_start:
        return {"state": "pid_reused", "pid": pid, "start_ticks": expected_start, "cpu": None, "rss_bytes": None, "io": None}
    io = process_io(pid)
    again, after_state = process_stat(pid)
    if again is None or again["start_ticks"] != expected_start:
        return {"state": "raced_" + after_state, "pid": pid, "start_ticks": expected_start, "cpu": None, "rss_bytes": None, "io": None}
    state = "zombie" if stat["state_code"] in ("Z", "X") else "alive"
    return {
        "state": state,
        "pid": pid,
        "start_ticks": expected_start,
        "cpu": {"user_ticks": stat["user_ticks"], "system_ticks": stat["system_ticks"]},
        "rss_bytes": stat["rss_pages"] * os.sysconf("SC_PAGE_SIZE"),
        "io": io,
    }


def executable_matches(pid, executable):
    try:
        actual = os.stat(f"/proc/{pid}/exe")
        return (actual.st_dev, actual.st_ino) == executable
    except OSError:
        return False


def process_tree(root):
    try:
        pids = [int(name) for name in os.listdir("/proc") if name.isdigit()]
    except OSError:
        return {}, "unavailable"
    if len(pids) > MAX_SCAN_PIDS:
        return {}, "too_many_processes"
    children_by_parent = {}
    for pid in pids:
        stat, _ = process_stat(pid)
        if stat is not None:
            children_by_parent.setdefault(stat["ppid"], []).append(pid)
    children, pending = set(), [root]
    while pending:
        current = pending.pop()
        if current not in children:
            children.add(current)
            pending.extend(children_by_parent.get(current, []))
    return children, "available"


def find_test(root, descendants, executable, scan_state):
    candidates = []
    for pid in descendants:
        before, _ = process_stat(pid)
        if before is not None and executable_matches(pid, executable) and is_descendant(pid, root):
            after, _ = process_stat(pid)
            if after is not None and before["start_ticks"] == after["start_ticks"]:
                candidates.append((pid, after["start_ticks"]))
    if len(candidates) == 1:
        return candidates[0], scan_state
    return None, "ambiguous" if len(candidates) > 1 else scan_state


def is_descendant(pid, root):
    seen = set()
    for _ in range(MAX_ANCESTRY_DEPTH):
        if pid == root:
            return True
        if pid in seen:
            return False
        seen.add(pid)
        stat, _ = process_stat(pid)
        if stat is None:
            return False
        pid = stat["ppid"]
    return False


def identify_nodes(node_ids, root, executable, known):
    if node_ids is None:
        raw, _ = read_text(PIDS)
        try:
            candidate = json.loads(raw) if raw is not None else None
            if (isinstance(candidate, list) and len(candidate) == 4
                    and all(type(pid) is int and pid > 0 for pid in candidate)
                    and len(set(candidate)) == 4):
                node_ids = candidate
        except (ValueError, TypeError):
            pass
    if node_ids is None:
        return None, known, "not_observed"
    if all(identity is not None for identity in known):
        return node_ids, known, "available"
    for index, pid in enumerate(node_ids):
        if known[index] is None:
            before, _ = process_stat(pid)
            if before is not None and executable_matches(pid, executable) and is_descendant(pid, root):
                after, _ = process_stat(pid)
                if after is not None and after["start_ticks"] == before["start_ticks"]:
                    known[index] = (pid, after["start_ticks"])
    return node_ids, known, "available" if all(identity is not None for identity in known) else "partial"


def host_cpu():
    raw, state = read_text("/proc/stat")
    if raw is None:
        return {"state": state, "values": None}
    try:
        line = next(line for line in raw.splitlines() if line.startswith("cpu "))
        numbers = [int(value) for value in line.split()[1:9]]
        if len(numbers) != 8 or any(value < 0 for value in numbers):
            raise ValueError
        return {"state": "available", "values": dict(zip(("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"), numbers))}
    except (StopIteration, ValueError):
        return {"state": "malformed", "values": None}


def host_io():
    raw, state = read_text("/proc/vmstat")
    if raw is None:
        return {"state": state, "values": None}
    wanted = {"pgpgin", "pgpgout", "pswpin", "pswpout"}
    try:
        values = {key: int(value) for line in raw.splitlines() for key, value in [line.split()] if key in wanted}
        if set(values) != wanted or any(value < 0 for value in values.values()):
            raise ValueError
        return {"state": "available", "values": values}
    except ValueError:
        return {"state": "malformed", "values": None}


def pressure(resource):
    raw, state = read_text(f"/proc/pressure/{resource}")
    if raw is None:
        return {"state": state, "values": None}
    try:
        values = {}
        for line in raw.splitlines():
            parts = line.split()
            if parts[0] not in ("some", "full"):
                continue
            fields = dict(part.split("=", 1) for part in parts[1:])
            avg10 = float(fields["avg10"])
            total = int(fields["total"])
            if not (0 <= avg10 <= 100 and total >= 0):
                raise ValueError
            values[parts[0]] = {"avg10_percent": avg10, "total_us": total}
        if "some" not in values:
            raise ValueError
        # Some kernels do not provide full CPU pressure.
        return {"state": "available", "values": {"some": values["some"], "full": values.get("full")}}
    except (ValueError, KeyError, IndexError):
        return {"state": "malformed", "values": None}


def nullable_number(value):
    return value if type(value) is int and 0 <= value <= 2**63 - 1 else None


def clock_fields(clock):
    if not isinstance(clock, dict):
        clock = {}
    return {key: nullable_number(clock.get(key)) for key in ("unix_ms", "monotonic_before_ms", "monotonic_after_ms")}


def progress_evidence():
    prefix = "TLS acceptance: concurrent GET failure: "
    snapshots = []
    if not TEST_LOG.exists():
        return {"state": "missing_test_output", "snapshots": snapshots}
    with TEST_LOG.open(errors="replace") as lines:
        for line in lines:
            if prefix not in line or len(line) > 100000:
                continue
            try:
                value = json.loads(line.split(prefix, 1)[1])
                if value.get("kind") not in ("read_failed", "task_failed") or not isinstance(value.get("streams"), list):
                    continue
                streams = []
                for stream in value["streams"][:4]:
                    if not isinstance(stream, dict):
                        continue
                    stage = stream.get("last_stage")
                    terminal = stream.get("terminal")
                    snapshot = stream.get("snapshot")
                    streams.append({
                        "node": nullable_number(stream.get("node")),
                        "snapshot": snapshot if snapshot in ("available", "busy", "poisoned") else "unknown",
                        "stage": stage if stage in ("queued", "waiting_headers", "headers_received", "reading_body", "complete") else "unknown",
                        "terminal": terminal if terminal in ("success", "failed", "panicked", "cancelled") else "unknown",
                        "sampled_clock": clock_fields(stream.get("sampled_clock")),
                        **{key: nullable_number(stream.get(key)) for key in ("started_ms", "headers_ms", "body_started_ms", "completed_ms", "received_bytes", "last_progress_ms", "terminal_ms")},
                    })
                snapshots.append({
                    "kind": value["kind"], "failed_node": nullable_number(value.get("failed_node")),
                    "elapsed_ms": nullable_number(value.get("elapsed_ms")),
                    "expected_bytes": nullable_number(value.get("expected_bytes")),
                    "stage_start_clock": clock_fields(value.get("stage_start_clock_sample")), "streams": streams,
                })
            except (ValueError, TypeError, AttributeError):
                continue
    return {"state": "available" if snapshots else "not_observed", "snapshots": snapshots[:4]}


def sanitize_junit():
    if not JUNIT.exists():
        return "missing"
    try:
        cases = list(ET.parse(JUNIT).getroot().iter("testcase"))
        if len(cases) != 1 or cases[0].get("name") != CASE or cases[0].get("classname") != "e2e_test":
            return "unexpected_case_set"
        source = cases[0]
        outcome = "failure" if source.find("failure") is not None else "error" if source.find("error") is not None else "skipped" if source.find("skipped") is not None else "passed"
        duration = float(source.get("time", ""))
        if not 0 <= duration < 86400:
            raise ValueError
        root = ET.Element("testsuites")
        suite = ET.SubElement(root, "testsuite", {"name": "e2e_test", "tests": "1", "failures": str(int(outcome == "failure")), "errors": str(int(outcome == "error")), "skipped": str(int(outcome == "skipped")), "time": str(duration)})
        case = ET.SubElement(suite, "testcase", {"classname": "e2e_test", "name": CASE, "time": str(duration)})
        if outcome != "passed":
            ET.SubElement(case, outcome, {"message": "Details withheld"})
        ET.ElementTree(root).write(EVIDENCE / "junit.xml", encoding="utf-8", xml_declaration=True)
        return outcome
    except (ET.ParseError, ValueError, OSError):
        return "malformed"


def listing_and_identity():
    listing = PRIVATE / "listing.json"
    with listing.open("wb") as output, (PRIVATE / "list.log").open("wb") as errors:
        result = subprocess.run(["cargo", "nextest", "list", "--locked", "--profile", "e2e-distributed", "-p", "e2e_test", "-E", FILTER, "--message-format", "json"], stdout=output, stderr=errors, check=False)
    if result.returncode != 0:
        raise ValueError("listing_failed")
    data = json.loads(listing.read_text())
    matches = [(suite_id, name, test) for suite_id, suite in data["rust-suites"].items() for name, test in suite["testcases"].items() if test.get("filter-match", {}).get("status") == "matches"]
    if len(matches) != 1 or matches[0][0:2] != ("e2e_test", CASE) or matches[0][2].get("ignored") is not False:
        raise ValueError("unexpected_selection")
    suite = data["rust-suites"]["e2e_test"]
    if suite.get("binary-id") != "e2e_test" or suite.get("package-name") != "e2e_test":
        raise ValueError("unexpected_binary")
    binary = Path(suite["binary-path"])
    test_stat = binary.stat()
    server = Path("target/debug/rustfs")
    server_stat = server.stat()
    sidecar = json.loads(Path("target/debug/rustfs.e2e.json").read_text())
    if sidecar["source"]["head"] != SOURCE or sidecar["requested_features"] != ["e2e-test-hooks"] or sidecar["profile"] != "debug" or digest(server) != sidecar["binary_sha256"]:
        raise ValueError("server_identity_mismatch")
    manifest = {
        "source_commit": SOURCE, "source_tree": TREE,
        "source_inventory_sha256": sidecar["source"]["sha256"],
        "server_binary_sha256": sidecar["binary_sha256"],
        "server_requested_features": sidecar["requested_features"],
        "server_resolved_features": sidecar["features"], "server_profile": sidecar["profile"],
        "test_binary_sha256": digest(binary), "suite": "e2e_test", "test": CASE, "selected_count": 1,
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "interval_seconds": INTERVAL_SECONDS,
        "max_samples": MAX_SAMPLES, "max_process_scans": MAX_PROCESS_SCANS,
        "process_scan_interval_samples": SCAN_EVERY_SAMPLES, "max_scan_pids": MAX_SCAN_PIDS,
        "max_ancestry_depth": MAX_ANCESTRY_DEPTH, "workflow_commit": os.environ.get("WORKFLOW_SHA"),
        "observer_sha256": digest(Path(__file__)),
        "metric_scope": "host_and_observed_processes",
        "run_id": nullable_number(int(os.environ["RUN_ID"])), "run_attempt": nullable_number(int(os.environ["RUN_ATTEMPT"])),
    }
    return binary, (test_stat.st_dev, test_stat.st_ino), (server_stat.st_dev, server_stat.st_ino), manifest


def main():
    os.umask(0o077)
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    summary = {"state": "preparing", "test_exit_code": None, "junit": "not_observed", "samples": 0, "samples_truncated": False}
    write_json("summary.json", summary)
    binary, test_exe, server_exe, manifest = listing_and_identity()
    write_json("identity.json", manifest)
    if JUNIT.exists() or JUNIT.is_symlink():
        summary["state"] = "preexisting_junit_preserved"
        summary["junit"] = "preexisting_preserved"
        write_json("summary.json", summary)
        print("Pre-existing JUnit preserved; test not started.")
        return 1
    env = dict(os.environ, RUSTFS_INTERNODE_ACCEPTANCE_REPORT=str(REPORT))
    command = ["python3", "scripts/e2e_binary.py", "run", "--features", "e2e-test-hooks", "--", "cargo", "nextest", "run", "--locked", "--profile", "e2e-distributed", "-p", "e2e_test", "-E", FILTER, "--no-tests=fail", "--retries", "0", "--failure-output", "immediate", "--success-output", "never", "--color", "never", "--no-output-indent"]
    test_identity = None
    node_ids, nodes = None, [None] * 4
    process_scans = 0
    with TEST_LOG.open("wb") as output, (EVIDENCE / "samples.jsonl").open("w") as samples:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, env=env, start_new_session=True)
        try:
            # Lower only the observer's priority after the test wrapper starts.
            try:
                os.nice(10)
                summary["observer_nice_adjusted"] = True
            except OSError:
                summary["observer_nice_adjusted"] = False
            while process.poll() is None:
                sample_started = time.monotonic()
                if summary["samples"] < MAX_SAMPLES:
                    before = time.time_ns() // 1_000_000
                    mono = time.monotonic_ns() // 1_000_000
                    scan_state = "not_needed"
                    if test_identity is None:
                        if process_scans >= MAX_PROCESS_SCANS:
                            scan_state = "scan_limit"
                        elif summary["samples"] % SCAN_EVERY_SAMPLES:
                            scan_state = "rate_limited"
                        else:
                            descendants, scan_state = process_tree(process.pid)
                            process_scans += 1
                            test_identity, scan_state = find_test(process.pid, descendants, test_exe, scan_state)
                    node_ids, nodes, node_scan = identify_nodes(node_ids, process.pid, server_exe, nodes)
                    node_samples = [{"index": index, "reported_pid": node_ids[index] if node_ids is not None else None,
                                     **sample_process(identity)} for index, identity in enumerate(nodes)]
                    record = {"unix_before_ms": before, "monotonic_ms": mono, "test_discovery": scan_state, "node_discovery": node_scan,
                              "test": sample_process(test_identity), "nodes": node_samples,
                              "host_cpu": host_cpu(), "host_io": host_io(),
                              "host_psi": {key: pressure(key) for key in ("cpu", "memory", "io")}, "unix_after_ms": time.time_ns() // 1_000_000}
                    samples.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
                    samples.flush()
                    summary["samples"] += 1
                else:
                    summary["samples_truncated"] = True
                try:
                    delay = max(0.25, INTERVAL_SECONDS - (time.monotonic() - sample_started))
                    process.wait(timeout=delay)
                except subprocess.TimeoutExpired:
                    pass
        finally:
            if process.poll() is None:
                # e2e_binary.py handles SIGINT by stopping its nextest group.
                try:
                    os.killpg(process.pid, signal.SIGINT)
                    process.wait(timeout=10)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except OSError:
                        pass
                    process.wait()
            summary["test_exit_code"] = process.returncode
            summary["state"] = "processing"
            write_json("summary.json", summary)
    summary["test_exit_code"] = process.returncode
    summary["junit"] = sanitize_junit()
    summary["test_process_observed"] = test_identity is not None
    summary["process_scans"] = process_scans
    summary["node_processes_observed"] = sum(identity is not None for identity in nodes)
    summary["node_pid_file_observed"] = node_ids is not None
    summary["test_binary_unchanged"] = digest(binary) == manifest["test_binary_sha256"]
    summary["state"] = "complete" if process.returncode == 0 and summary["junit"] == "passed" and summary["test_binary_unchanged"] else "failed"
    write_json("progress.json", progress_evidence())
    write_json("summary.json", summary)
    return 0 if summary["state"] == "complete" else 1


if __name__ == "__main__":
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    try:
        raise SystemExit(main())
    except (Exception, KeyboardInterrupt):
        try:
            prior = json.loads((EVIDENCE / "summary.json").read_text())
            exit_code = prior.get("test_exit_code")
            if type(exit_code) is not int or not -255 <= exit_code <= 255:
                exit_code = None
            write_json("summary.json", {"state": "observer_error", "test_exit_code": exit_code, "junit": "not_observed"})
        except (OSError, ValueError, AttributeError):
            pass
        print("Observation failed; raw output withheld.")
        raise SystemExit(1)
