#!/usr/bin/env python3
"""SSE black-box suite: register server binaries, run the cases against one, and compare two runs.

    uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py bin add --name head-default --path target/debug/rustfs --commit <sha> --build default
    uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py run --binary head-default
    uv run --python 3.12 --no-project scripts/test/sse_blackbox/suite.py diff <run-a> <run-b>
"""

from __future__ import annotations

import argparse
import atexit
import datetime
import fcntl
import json
import shutil
import sys
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cases_copy_multipart  # noqa: F401
import cases_negative_authz  # noqa: F401
import cases_roundtrip  # noqa: F401
import cases_state  # noqa: F401
import diffing
import framework as fw
import harness as h
import observe
import selftest

BIN_DIR = h.STATE_DIR / "bin"
RUNS_DIR = h.STATE_DIR / "runs"
KEEP_RUNS = 5


# ---------------------------------------------------------------------------
# Binary registry
# ---------------------------------------------------------------------------

def cmd_bin_add(args) -> int:
    source = Path(args.path).resolve()
    if not source.is_file():
        raise SystemExit(f"no such binary: {source}")
    target_dir = BIN_DIR / args.name
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "rustfs"
    shutil.copy2(source, target)
    identity = h.binary_identity(target)
    identity.update({"name": args.name, "commit": args.commit, "build": args.build, "source": str(source),
                     "master_key_event": not args.no_master_key_event})
    (target_dir / "identity.json").write_text(json.dumps(identity, indent=1) + "\n")
    print(f"registered {args.name}: {identity['version']} sha256 {identity['sha256'][:12]} build {args.build}")
    return 0


def cmd_bin_list(_args) -> int:
    for identity_file in sorted(BIN_DIR.glob("*/identity.json")):
        identity = json.loads(identity_file.read_text())
        print(f"{identity['name']:24} commit {identity['commit']:12} build {identity['build']:8} sha256 {identity['sha256'][:12]}  {identity['version']}")
    return 0


def load_binary(name: str) -> tuple[Path, dict]:
    """A registered binary must still be the bytes that were registered."""
    identity_file = BIN_DIR / name / "identity.json"
    if not identity_file.exists():
        raise SystemExit(f"binary {name!r} is not registered; see `suite.py bin add`")
    identity = json.loads(identity_file.read_text())
    binary = BIN_DIR / name / "rustfs"
    actual = h.sha256_hex(binary.read_bytes())
    if actual != identity["sha256"]:
        raise SystemExit(f"binary {name!r} changed since registration: sha256 {actual[:12]} != {identity['sha256'][:12]}")
    return binary, identity


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def _prune_runs() -> None:
    """Keep the most recent runs and every run that did not succeed."""
    finished = sorted((d for d in RUNS_DIR.glob("*") if (d / "DONE").exists()), key=lambda d: d.name)
    succeeded = [d for d in finished if (d / "DONE").read_text().strip() == "success"]
    for old in succeeded[:-KEEP_RUNS]:
        shutil.rmtree(old / "sites", ignore_errors=True)


def _provision(site: h.Site, config: str) -> None:
    if config in fw.KMS_CONFIGS:
        h.wait_kms_healthy(site)
        for name in (h.KEY_1, h.KEY_2):
            h.ensure_key(site, name)


def _run_case(case: fw.Case, site: h.Site, config: str, identity: dict) -> tuple[dict, list[dict], list[dict]]:
    ctx = fw.Ctx(site=site, config=config, case_id=case.case_id, master_key_event=identity.get("master_key_event", True))
    started = time.monotonic()
    verdict, reasons = fw.PASS, []
    observe.begin_case(config, case.case_id)
    try:
        if not h.is_running(site):
            raise h.EnvError(f"{site.name} is not running")
        case.fn(ctx)
        reasons = list(ctx.failures)
    except h.EnvError as err:
        verdict, reasons = fw.ENV_ERROR, [str(err)]
    except h.CheckFailed as err:
        reasons = [*ctx.failures, str(err)]
    except Exception:  # noqa: BLE001 - an unexpected response shape must surface with its traceback, not vanish
        reasons = [*ctx.failures, "unexpected exception:\n" + traceback.format_exc(limit=6)]
    finally:
        observe.end_case()
    if verdict != fw.ENV_ERROR:
        if reasons:
            verdict = fw.FAIL
        elif ctx.fixed:
            verdict, reasons = fw.FIXED, [f"now holds: {text}" for text in ctx.fixed]
        elif ctx.known:
            verdict, reasons = fw.KNOWN, [f"does not hold: {text}" for text in ctx.known]
    base = {"config": config, "case": case.case_id, "level": case.level, "layer": case.layer}
    result = {**base, "row": "", "title": case.title, "verdict": verdict, "reasons": reasons,
              "seconds": round(time.monotonic() - started, 2)}
    rows = [] if verdict == fw.ENV_ERROR else [{**base, **row} for row in ctx.rows]
    return result, rows, observe.drain()


def _build_self_check(identity: dict, observations: list[dict]) -> str | None:
    """A binary's stored form must match the build it was registered as; otherwise the run tested the wrong binary."""
    stored = [o["value"] for o in observations if o["kind"] == "stored-keys" and o["value"]]
    managed = [keys for keys in stored if any("encryption-key" in k or "sealed-key" in k for k in keys)]
    if not managed:
        return None
    sealed = any("sealed-key" in k for keys in managed for k in keys)
    declared_sealed = identity["build"] == "rio-v2"
    if sealed != declared_sealed:
        return (f"binary registered as build {identity['build']!r} but its objects "
                f"{'carry' if sealed else 'do not carry'} sealed object-key metadata")
    return None


def cmd_run(args) -> int:
    binary, identity = load_binary(args.binary)
    h.STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock = (h.STATE_DIR / "lock").open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("another run holds the lock; only one driver may run at a time")

    selected = set(args.cases.split(",")) if args.cases else None
    configs = [c for c in fw.CONFIG_ORDER if not args.configs or c in args.configs.split(",")]
    run_as = dict(pair.split("=", 1) for pair in args.run_as.split(",")) if args.run_as else {}
    for name in [*run_as, *run_as.values(), *(args.configs.split(",") if args.configs else [])]:
        if name not in fw.CONFIGS:
            raise SystemExit(f"unknown configuration {name!r}; known: {', '.join(fw.CONFIG_ORDER)}")

    run_id = f"{datetime.datetime.now().strftime('%Y%m%dT%H%M%S')}-{identity['sha256'][:8]}"
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True)
    _prune_runs()
    meta = {"run_id": run_id, "level": args.level, "binary": identity, "port": args.port, "configs": configs,
            "run_as": run_as, "sabotage": args.sabotage, "cases": sorted(selected) if selected else "all",
            "started": datetime.datetime.now().isoformat(timespec="seconds"),
            "harness": {p.name: h.sha256_hex(p.read_bytes())[:16] for p in sorted(h.HERE.glob("*.py"))}}
    print(f"run {run_id}: binary {identity['name']} ({identity['version']}, build {identity['build']}), port {args.port}")

    results, observations = [], []
    for config in configs:
        cases = [c for c in fw.CASES if config in c.configs and c.level == args.level
                 and (selected is None or c.case_id in selected)]
        cases.sort(key=lambda c: c.last)
        if not cases:
            continue
        site = h.Site(name=config, root=run_dir / "sites" / config, port=args.port)
        atexit.register(h.stop_site, site)
        env_config = run_as.get(config, config)
        print(f"-- {config}" + (f" (started as {env_config})" if env_config != config else ""))
        startup_error = None
        try:
            if env_config in fw.KMS_CONFIGS:
                h.write_local_key_file(site.key_dir, h.DEFAULT_KEY)
            h.start_site(site, binary, fw.CONFIGS[env_config](site))
            _provision(site, env_config)
            if args.sabotage == "kill-site":
                h.stop_site(site)
        except h.EnvError as err:
            startup_error = str(err)
            print(f"   environment error: {startup_error.splitlines()[0]}")
        for case in cases:
            if startup_error is not None:
                result = {"config": config, "case": case.case_id, "level": case.level, "layer": case.layer, "row": "",
                          "title": case.title, "verdict": fw.ENV_ERROR, "reasons": [startup_error], "seconds": 0.0}
                rows, obs = [], []
            else:
                result, rows, obs = _run_case(case, site, config, identity)
            results.extend([result, *rows])
            observations.extend(obs)
            print(f"   {result['verdict']:9} {case.case_id:8} {case.title}  ({result['seconds']}s)")
            for reason in result["reasons"][:6]:
                print(f"             - {reason.splitlines()[0][:260]}")
            if args.fail_fast and result["verdict"] not in (fw.PASS, fw.KNOWN):
                break
        h.stop_site(site)

    self_check = _build_self_check(identity, observations)
    case_results = [r for r in results if r["row"] == ""]
    counts = {v: sum(1 for r in case_results if r["verdict"] == v)
              for v in (fw.PASS, fw.FAIL, fw.ENV_ERROR, fw.SKIP, fw.KNOWN, fw.FIXED)}
    status = "success"
    if counts[fw.ENV_ERROR] or self_check:
        status = "environment-error"
    elif counts[fw.FAIL] or counts[fw.FIXED]:
        # A deviation that no longer reproduces fails the run until its marker is turned into a plain check.
        status = "failed"
    elif not case_results:
        status = "environment-error"  # nothing ran: never a success
    meta.update({"finished": datetime.datetime.now().isoformat(timespec="seconds"), "counts": counts, "status": status,
                 "build_self_check": self_check or "ok"})

    (run_dir / "run.json").write_text(json.dumps(meta, indent=1) + "\n")
    (run_dir / "results.jsonl").write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in results))
    (run_dir / "observations.jsonl").write_text("".join(json.dumps(o, sort_keys=True) + "\n" for o in observations))
    report = [f"# SSE black-box run {run_id}", "",
              f"- binary: {identity['name']} ({identity['version']}), commit {identity['commit']}, build {identity['build']}, sha256 {identity['sha256'][:16]}",
              f"- status: {status}; " + ", ".join(f"{k} {v}" for k, v in counts.items()),
              f"- build self-check: {self_check or 'ok'}", "", "| config | case | verdict | seconds | reasons |", "|---|---|---|---|---|"]
    for r in case_results:
        reasons = "<br>".join(reason.splitlines()[0][:200].replace("|", "\\|") for reason in r["reasons"])
        report.append(f"| {r['config']} | {r['case']} | {r['verdict']} | {r['seconds']} | {reasons} |")
    (run_dir / "report.md").write_text("\n".join(report) + "\n")
    (run_dir / "DONE").write_text(status + "\n")

    print(f"{status}: " + ", ".join(f"{k} {v}" for k, v in counts.items()) + f", {len(observations)} observations")
    if self_check:
        print(f"build self-check failed: {self_check}")
    print(f"run directory: {run_dir}")
    return 0 if status == "success" else 1


# ---------------------------------------------------------------------------
# Cross: one binary writes, another reads the same data directory
# ---------------------------------------------------------------------------

def _cross_objects(config: str) -> list[tuple[str, dict | None, dict | None, dict, list[bytes]]]:
    """(key, write headers, read headers, projection, parts) for every stored form the configuration can produce."""
    key_a = h.derived_key("ssec a")
    schemes = [("sse-c", h.ssec(key_a), h.ssec(key_a), h.projection_c(key_a)), ("sse-s3", h.sse_s3(), None, h.projection_s3())]
    if config == "e2":
        schemes.append(("sse-kms", h.sse_kms(h.KEY_1), None, h.projection_kms(h.KEY_1)))
    objects = [("plain", None, None, {}, [h.plain("cross plain", 70 * h.KIB)])]
    for name, write_headers, read_headers, want in schemes:
        # Sizes on both sides of the inline threshold and of the 1 MiB block boundary.
        for size in (0, 1, 32 * h.KIB, 70 * h.KIB, 200_000, h.MIB, 3 * h.MIB + 1):
            objects.append((f"{name}-{size}", write_headers, read_headers, want, [h.plain(f"cross {name} {size}", size)]))
        parts = [h.plain(f"cross {name} part 1", 5 * h.MIB), h.plain(f"cross {name} part 2", h.MIB + 17)]
        objects.append((f"{name}-multipart", write_headers, read_headers, want, parts))
    return objects


def _cross_write(site: h.Site, bucket: str, objects) -> None:
    """Setup done by the writer binary; a failure here is not the reader's verdict."""
    for key, write_headers, _, _, parts in objects:
        part_headers = write_headers if write_headers and any(k.startswith(h.SSEC) for k in write_headers) else None
        if len(parts) == 1:
            status, _, body = h.put(site, bucket, key, parts[0], write_headers)
        else:
            status, _, upload_id, body = h.create_multipart(site, bucket, key, write_headers)
            etags = []
            for number, data in enumerate(parts, start=1):
                if status != 200:
                    break
                status, part_response, body = h.upload_part(site, bucket, key, upload_id, number, data, part_headers)
                etags.append((number, part_response.get("etag", "")))
            if status == 200:
                status, _, body = h.complete_multipart(site, bucket, key, upload_id, etags, part_headers)
        if status != 200:
            raise h.EnvError(f"writer could not store {key}: HTTP {status} {h.s3_error(body)}")


def cmd_cross(args) -> int:
    writer, writer_identity = load_binary(args.writer)
    reader, reader_identity = load_binary(args.reader)
    lock = (h.STATE_DIR / "lock").open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("another run holds the lock; only one driver may run at a time") from None
    run_id = f"{datetime.datetime.now().strftime('%Y%m%dT%H%M%S')}-cross-{writer_identity['sha256'][:8]}-{reader_identity['sha256'][:8]}"
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True)
    print(f"cross {run_id}: {writer_identity['name']} writes, {reader_identity['name']} reads")
    results, observations = [], []
    for config in ("e1", "e2"):
        site = h.Site(name=config, root=run_dir / "sites" / config, port=args.port)
        atexit.register(h.stop_site, site)
        objects = _cross_objects(config)
        bucket = f"bb-cross-{config}"
        case = fw.Case(f"CROSS-{config}", f"{writer_identity['name']} writes, {reader_identity['name']} reads", "p1", "L1",
                       (config,), None)

        def read_all(c: fw.Ctx, objects=objects, bucket=bucket) -> None:
            for key, _, read_headers, want, parts in objects:
                with c.row(key):
                    c.read_back(bucket, key, b"".join(parts), read_headers, want, key)
                    status, head_headers = h.head(c.site, bucket, key, read_headers)
                    if c.check(status == 200, f"{key} HEAD: HTTP {status}, expected 200"):
                        c.projection(head_headers, want, f"{key} HEAD")
                    c.stored(bucket, key)

        case.fn = read_all
        try:
            if config in fw.KMS_CONFIGS:
                h.write_local_key_file(site.key_dir, h.DEFAULT_KEY)
            h.start_site(site, writer, fw.CONFIGS[config](site))
            _provision(site, config)
            h.make_bucket(site, bucket)
            _cross_write(site, bucket, objects)
            h.stop_site(site)
            h.start_site(site, reader, fw.CONFIGS[config](site))
            _provision(site, config)
            result, rows, obs = _run_case(case, site, config, reader_identity)
        except h.EnvError as err:
            result = {"config": config, "case": case.case_id, "level": "p1", "layer": "L1", "row": "", "title": case.title,
                      "verdict": fw.ENV_ERROR, "reasons": [str(err)], "seconds": 0.0}
            rows, obs = [], []
        h.stop_site(site)
        results.extend([result, *rows])
        observations.extend(obs)
        print(f"   {result['verdict']:9} {case.case_id}: {sum(1 for r in rows if r['verdict'] == fw.PASS)}/{len(objects)} objects read back")
        for reason in result["reasons"][:8]:
            print(f"             - {reason.splitlines()[0][:260]}")
    verdicts = {r["verdict"] for r in results if r["row"] == ""}
    status = "success" if verdicts == {fw.PASS} else ("environment-error" if fw.ENV_ERROR in verdicts else "failed")
    meta = {"run_id": run_id, "kind": "cross", "binary": reader_identity, "writer": writer_identity, "status": status}
    (run_dir / "run.json").write_text(json.dumps(meta, indent=1) + "\n")
    (run_dir / "results.jsonl").write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in results))
    (run_dir / "observations.jsonl").write_text("".join(json.dumps(o, sort_keys=True) + "\n" for o in observations))
    (run_dir / "DONE").write_text(status + "\n")
    print(f"{status}\nrun directory: {run_dir}")
    return 0 if status == "success" else 1


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------

def _resolve_run(value: str) -> Path:
    path = Path(value)
    return path if path.is_dir() else RUNS_DIR / value


def cmd_selftest(_args) -> int:
    failures = selftest.run()
    for failure in failures:
        print(f"selftest failed: {failure}")
    print("selftest: " + ("ok" if not failures else f"{len(failures)} failed"))
    return 1 if failures else 0


def cmd_diff(args) -> int:
    run_a, run_b = diffing.load_run(_resolve_run(args.a)), diffing.load_run(_resolve_run(args.b))
    outcome = diffing.compare(run_a, run_b)
    text = diffing.render(run_a, run_b, outcome)
    print(text, end="")
    if args.out:
        Path(args.out).write_text(text)
    if outcome["compared"]["exchanges"] == 0:
        print("nothing was compared: the two runs share no HTTP exchange")
        return 2
    return 1 if outcome["blocking"] else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    bin_parser = sub.add_parser("bin", help="register or list server binaries")
    bin_sub = bin_parser.add_subparsers(dest="bin_command", required=True)
    add = bin_sub.add_parser("add")
    add.add_argument("--name", required=True)
    add.add_argument("--path", required=True)
    add.add_argument("--commit", required=True)
    add.add_argument("--build", required=True, choices=("default", "rio-v2"))
    add.add_argument("--no-master-key-event", action="store_true",
                     help="the binary predates the sse_local_master_key startup event")
    add.set_defaults(fn=cmd_bin_add)
    bin_sub.add_parser("list").set_defaults(fn=cmd_bin_list)

    run = sub.add_parser("run", help="run the cases against a registered binary")
    run.add_argument("--binary", required=True)
    run.add_argument("--level", default="p0", choices=("p0",))
    run.add_argument("--configs", help="comma-separated subset of: " + ", ".join(fw.CONFIG_ORDER))
    run.add_argument("--cases", help="comma-separated case ids")
    run.add_argument("--port", type=int, default=h.DEFAULT_PORT)
    run.add_argument("--fail-fast", action="store_true")
    run.add_argument("--run-as", help="self-test: start configuration X with the environment of Y, as X=Y[,...]")
    run.add_argument("--sabotage", choices=("kill-site",), help="self-test: break the environment on purpose")
    run.set_defaults(fn=cmd_run)

    cross = sub.add_parser("cross", help="one binary writes every stored form, another reads the same data directory")
    cross.add_argument("--writer", required=True)
    cross.add_argument("--reader", required=True)
    cross.add_argument("--port", type=int, default=h.DEFAULT_PORT)
    cross.set_defaults(fn=cmd_cross)

    sub.add_parser("selftest", help="check offline that the suite tells apart what it must").set_defaults(fn=cmd_selftest)

    diff = sub.add_parser("diff", help="compare two finished runs")
    diff.add_argument("a")
    diff.add_argument("b")
    diff.add_argument("--out")
    diff.set_defaults(fn=cmd_diff)

    args = parser.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
