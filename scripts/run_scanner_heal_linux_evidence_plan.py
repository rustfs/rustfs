#!/usr/bin/env python3
"""Plan the Scanner/Heal Linux release-evidence validation flow.

The planner is intentionally not a release-evidence producer. It writes a
machine-readable execution manifest for the existing measured runners and can
run only their lightweight preflight checks. Long-running Linux, distributed,
mixed-version, ABBA, and profile lanes remain explicit operator actions.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / ".config" / "scanner-heal-required-tests.json"
DEFAULT_OUTPUT_ROOT = ROOT / "target" / "scanner-heal-linux-evidence-plan"
REQUIRED_GATES = {
    "G01",
    "G02",
    "G03",
    "G04",
    "G05",
    "G06",
    "G07",
    "G08",
    "G09",
    "G10",
    "G11",
    "G12",
    "G13",
    "G14",
    "P1",
    "P2",
    "P3",
    "P4",
    "R-D",
    "R-E",
    "R-L",
}


def command(*parts: str) -> list[str]:
    return list(parts)


def expected_outputs(*parts: str) -> list[str]:
    return list(parts)


def git_output(*args: str) -> str:
    return subprocess.check_output(command("git", *args), cwd=ROOT, text=True).strip()


def source_revision(explicit: str | None) -> str:
    if explicit:
        if len(explicit) != 40 or any(char not in "0123456789abcdef" for char in explicit):
            raise ValueError("--source-revision must be a 40-character lowercase Git SHA")
        return explicit
    return git_output("rev-parse", "HEAD")


def load_registry() -> dict[str, Any]:
    with REGISTRY.open() as stream:
        registry = json.load(stream)
    if registry.get("schema") != 2:
        raise ValueError("Scanner/Heal release registry must use schema 2")
    return registry


def validate_registry(registry: dict[str, Any]) -> None:
    gates = {item["gate"] for item in registry.get("release_requirements", [])}
    missing = sorted(REQUIRED_GATES - gates)
    if missing:
        raise ValueError(f"release registry is missing gates: {', '.join(missing)}")
    lanes = registry.get("release_lanes")
    if not isinstance(lanes, dict) or not lanes:
        raise ValueError("release registry is missing release_lanes")
    lane_gates = set()
    for lane_name, lane in lanes.items():
        if not isinstance(lane, dict):
            raise ValueError(f"release lane {lane_name} must be an object")
        lane_gates.update(lane.get("gates", []))
    missing_from_lanes = sorted(gates - lane_gates)
    if missing_from_lanes:
        raise ValueError(f"release lanes do not cover gates: {', '.join(missing_from_lanes)}")


def existing_script_command(*parts: str) -> dict[str, Any]:
    for part in parts:
        if part.startswith("scripts/") and "$" not in part and not (ROOT / part).is_file():
            raise ValueError(f"missing planned script: {part}")
    return {"command": command(*parts), "script": parts[0]}


def preflight_steps() -> list[dict[str, Any]]:
    return [
        {
            "id": "repo-source-clean",
            "description": "Verify the checkout revision and tracked-source cleanliness before measured evidence.",
            "commands": [
                {"command": command("git", "rev-parse", "HEAD")},
                {"command": command("git", "status", "--porcelain", "--untracked-files=no")},
            ],
        },
        {
            "id": "release-registry-self-test",
            "description": "Exercise the release registry, descriptor, bundle, and negative parser cases.",
            "commands": [existing_script_command("scripts/python_bin.sh", "scripts/check_test_wiring.py", "--self-test")],
            "preflight_runnable": True,
        },
        {
            "id": "runner-self-tests",
            "description": "Run lightweight CLI/schema tests for every Scanner/Heal evidence runner.",
            "commands": [
                existing_script_command("scripts/run_scanner_heal_evidence_case.sh", "--self-test"),
                existing_script_command("scripts/test_scanner_heal_authority_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_checkpoint_crash_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_status_outcome_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_scoped_ack_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_legacy_rollback_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_g14_multiset_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_g09_upgrade_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_scheduler_pressure_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_maintenance_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_w13_mrf_evidence.sh"),
                existing_script_command("scripts/test_scanner_heal_w16_recovery_evidence.sh"),
            ],
            "preflight_runnable": True,
        },
    ]


def concrete_case_steps(registry: dict[str, Any]) -> list[dict[str, Any]]:
    cases = registry["cases"]
    ordered_cases = [
        "background-target-restart",
        "background-target-crash",
        "background-target-restart-ec8-4",
        "background-target-crash-ec8-4",
        "ec84-target-drive-restart",
        "background-target-restart-ec8-4-multi-set",
        "background-target-crash-ec8-4-multi-pool",
    ]
    missing = [case for case in ordered_cases if case not in cases]
    if missing:
        raise ValueError(f"release registry is missing concrete cases: {', '.join(missing)}")
    return [
        {
            "id": f"case-{case_id}",
            "description": cases[case_id]["scope"],
            "covers": {
                "gate": cases[case_id]["gate"],
                "task": cases[case_id]["task"],
                "lane": cases[case_id]["lane"],
                "evidence": cases[case_id]["evidence"],
            },
            "commands": [
                existing_script_command(
                    "scripts/run_scanner_heal_evidence_case.sh",
                    "--case",
                    case_id,
                    "--run-dir",
                    f"$RUN_ROOT/cases/{case_id}",
                )
            ],
            "expected_outputs": expected_outputs(
                f"$RUN_ROOT/cases/{case_id}/run.json",
                f"$RUN_ROOT/cases/{case_id}/execution.json",
                f"$RUN_ROOT/cases/{case_id}/listing.json",
                f"$RUN_ROOT/cases/{case_id}/junit.xml",
                f"$RUN_ROOT/cases/{case_id}/{cases[case_id]['oracle']}",
                f"$RUN_ROOT/cases/{case_id}/release-status.json",
            ),
        }
        for case_id in ordered_cases
    ]


def descriptor_steps() -> list[dict[str, Any]]:
    return [
        {
            "id": "authority-coverage",
            "description": "Assemble the G01 authority descriptor from operator-collected root and quota authority artifacts.",
            "covers": {"gates": ["G01"], "issues": ["2270"]},
            "commands": [
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_authority_evidence.py",
                    "--root-authority-json",
                    "$RUN_ROOT/raw/authority/root-authority.json",
                    "--quota-authority-json",
                    "$RUN_ROOT/raw/authority/quota-authority.json",
                    "--out-dir",
                    "$RUN_ROOT/descriptors/authority",
                )
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/descriptors/authority/release-bundle-authority.json"),
        },
        {
            "id": "checkpoint-and-restart",
            "description": "Assemble G02/R-E bounded checkpoint and restart descriptors from measured diagnostic reports.",
            "covers": {"gates": ["G02", "R-E"], "issues": ["2269"]},
            "commands": [
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_checkpoint_crash_evidence.py",
                    "--diagnostic-dir",
                    "$RUN_ROOT/raw/checkpoint",
                    "--out-dir",
                    "$RUN_ROOT/descriptors/checkpoint",
                )
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/descriptors/checkpoint/release-bundle-checkpoint-crash.json"),
        },
        {
            "id": "status-and-outcome",
            "description": "Collect live G05/G06/R-D raw observations, then assemble their descriptor.",
            "covers": {"gates": ["G05", "G06", "R-D"], "issues": ["2278"]},
            "commands": [
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_status_outcome_probe.py",
                    "--observations-json",
                    "$RUN_ROOT/raw/status-outcome/observations.json",
                    "--out-dir",
                    "$RUN_ROOT/raw/status-outcome",
                ),
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_status_outcome_evidence.py",
                    "--status-outcome-json",
                    "$RUN_ROOT/raw/status-outcome/status-outcome.json",
                    "--status-compat-json",
                    "$RUN_ROOT/raw/status-outcome/status-compat.json",
                    "--disposition-json",
                    "$RUN_ROOT/raw/status-outcome/disposition.json",
                    "--out-dir",
                    "$RUN_ROOT/descriptors/status-outcome",
                ),
            ],
            "expected_outputs": expected_outputs(
                "$RUN_ROOT/raw/status-outcome/status-outcome.json",
                "$RUN_ROOT/raw/status-outcome/status-compat.json",
                "$RUN_ROOT/raw/status-outcome/disposition.json",
                "$RUN_ROOT/descriptors/status-outcome/release-bundle-status-outcome.json",
            ),
        },
        {
            "id": "ec8-4-multiset-descriptor",
            "description": "Assemble G14 EC8+4 multi-set and multi-pool descriptors from the measured case directories.",
            "covers": {"gates": ["G14"], "issues": ["2266", "2269"]},
            "commands": [
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_g14_multiset_evidence.py",
                    "--case-dir",
                    "multi-set=$RUN_ROOT/cases/background-target-restart-ec8-4-multi-set",
                    "--case-dir",
                    "multi-pool=$RUN_ROOT/cases/background-target-crash-ec8-4-multi-pool",
                    "--out-dir",
                    "$RUN_ROOT/descriptors/g14",
                )
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/descriptors/g14/release-bundle-g14.json"),
        },
        {
            "id": "w16-recovery-intent",
            "description": "Run the G04/G12 recovery-intent and quota-authority lanes.",
            "covers": {"gates": ["G04", "G12"], "issues": ["2279"]},
            "commands": [
                existing_script_command(
                    "scripts/run_scanner_heal_w16_recovery_evidence.sh",
                    "--run-dir",
                    "$RUN_ROOT/w16",
                )
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/w16/release-bundle-w16.json"),
        },
        {
            "id": "mrf-responsibility",
            "description": "Run or package G07/G08/P4 durable MRF responsibility evidence.",
            "covers": {"gates": ["G07", "G08", "P4"], "issues": ["2277", "2278"]},
            "commands": [
                existing_script_command(
                    "scripts/run_scanner_heal_w13_mrf_evidence.sh",
                    "--run-dir",
                    "$RUN_ROOT/w13",
                )
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/w13/release-bundle-w13.json"),
        },
        {
            "id": "mixed-version-rollback",
            "description": "Run G09 mixed-version/rollback, then assemble G03 and R-L operator descriptors.",
            "covers": {"gates": ["G03", "G09", "R-L"], "issues": ["2269", "2281"]},
            "commands": [
                existing_script_command(
                    "scripts/run_scanner_heal_g09_upgrade_evidence.sh",
                    "--run-dir",
                    "$RUN_ROOT/g09",
                ),
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_scoped_ack_evidence.py",
                    "--proof-json",
                    "$RUN_ROOT/raw/scoped-ack/scoped-ack-proof.json",
                    "--out-dir",
                    "$RUN_ROOT/descriptors/scoped-ack",
                ),
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_legacy_rollback_evidence.py",
                    "--proof-json",
                    "$RUN_ROOT/raw/legacy-rollback/legacy-rollback-proof.json",
                    "--out-dir",
                    "$RUN_ROOT/descriptors/legacy-rollback",
                ),
            ],
            "expected_outputs": expected_outputs(
                "$RUN_ROOT/g09/release-bundle-g09.json",
                "$RUN_ROOT/descriptors/scoped-ack/release-bundle-scoped-ack.json",
                "$RUN_ROOT/descriptors/legacy-rollback/release-bundle-legacy-rollback.json",
            ),
        },
        {
            "id": "maintenance-producers",
            "description": "Assemble G11/G13 producer coverage, quorum-minus-one, and remount descriptors.",
            "covers": {"gates": ["G11", "G13"], "issues": ["2272", "2280"]},
            "commands": [
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_maintenance_evidence.py",
                    "--proof-json",
                    "$RUN_ROOT/raw/maintenance/maintenance-proof.json",
                    "--out-dir",
                    "$RUN_ROOT/descriptors/maintenance",
                )
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/descriptors/maintenance/release-bundle-maintenance.json"),
        },
    ]


def performance_steps() -> list[dict[str, Any]]:
    return [
        {
            "id": "scanner-heal-abba",
            "description": "Run the isolated EC8+4 Scanner/Heal ABBA matrix through the deployment adapter.",
            "covers": {"gates": ["G10", "P1", "P2", "P3"], "issues": ["2266", "2273", "2274", "2275"]},
            "commands": [
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/scanner_abba.py",
                    "--manifest",
                    "$RUN_ROOT/abba/manifest.json",
                    "--adapter",
                    "$RUN_ROOT/abba/adapter.py",
                    "--out-dir",
                    "$RUN_ROOT/abba/out",
                    "--data-root",
                    "$RUN_ROOT/abba/data",
                )
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/abba/out/report.json"),
        },
        {
            "id": "scheduler-pressure",
            "description": "Assemble scheduler-pressure, profile, RSS, throughput, and latency descriptors.",
            "covers": {"gates": ["G10", "P1", "P2", "P3"], "issues": ["2266", "2274", "2275"]},
            "commands": [
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/run_scanner_heal_scheduler_pressure_evidence.py",
                    "--abba-dir",
                    "$RUN_ROOT/abba/out",
                    "--recovery-window-json",
                    "$RUN_ROOT/raw/scheduler/recovery-window.json",
                    "--profile-artifact",
                    "allocation-profile=$RUN_ROOT/profile/allocation-profile.json",
                    "--profile-artifact",
                    "flamegraph=$RUN_ROOT/profile/flamegraph.svg",
                    "--profile-artifact",
                    "rss-samples=$RUN_ROOT/profile/rss-samples.json",
                    "--profile-artifact",
                    "save-frequency=$RUN_ROOT/profile/save-frequency.json",
                    "--out-dir",
                    "$RUN_ROOT/descriptors/scheduler-pressure",
                )
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/descriptors/scheduler-pressure/release-bundle-scheduler-pressure.json"),
        },
    ]


def bundle_steps() -> list[dict[str, Any]]:
    return [
        {
            "id": "assemble-release-bundle",
            "description": "Assemble every measured lane descriptor into the final release bundle.",
            "covers": {"gates": sorted(REQUIRED_GATES), "issues": ["2240", "2428"]},
            "commands": [
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/check_test_wiring.py",
                    "--assemble-scanner-heal-release-bundle",
                    "$RUN_ROOT/descriptors/authority/release-bundle-authority.json",
                    "$RUN_ROOT/descriptors/checkpoint/release-bundle-checkpoint-crash.json",
                    "$RUN_ROOT/descriptors/status-outcome/release-bundle-status-outcome.json",
                    "$RUN_ROOT/w16/release-bundle-w16.json",
                    "$RUN_ROOT/w13/release-bundle-w13.json",
                    "$RUN_ROOT/g09/release-bundle-g09.json",
                    "$RUN_ROOT/descriptors/scoped-ack/release-bundle-scoped-ack.json",
                    "$RUN_ROOT/descriptors/legacy-rollback/release-bundle-legacy-rollback.json",
                    "$RUN_ROOT/descriptors/maintenance/release-bundle-maintenance.json",
                    "$RUN_ROOT/descriptors/g14/release-bundle-g14.json",
                    "$RUN_ROOT/descriptors/scheduler-pressure/release-bundle-scheduler-pressure.json",
                    "$RUN_ROOT/release-bundle",
                ),
                existing_script_command(
                    "scripts/python_bin.sh",
                    "scripts/check_test_wiring.py",
                    "--check-scanner-heal-release-bundle",
                    "$RUN_ROOT/release-bundle/release-evidence.json",
                ),
            ],
            "expected_outputs": expected_outputs("$RUN_ROOT/release-bundle/release-evidence.json"),
        }
    ]


def build_plan(registry: dict[str, Any], revision: str, phases: set[str]) -> dict[str, Any]:
    stage_defs = [
        ("preflight", "Preflight", preflight_steps()),
        ("functional", "Functional And Durable Evidence", concrete_case_steps(registry) + descriptor_steps()),
        ("performance", "ABBA And Profile Evidence", performance_steps()),
        ("bundle", "Release Bundle Assembly", bundle_steps()),
    ]
    stages = []
    for key, title, steps in stage_defs:
        if "all" not in phases and key not in phases:
            continue
        stages.append({"id": key, "title": title, "steps": steps})
    return {
        "schema": 1,
        "kind": "scanner-heal-linux-evidence-plan",
        "evidence_type": "plan_only",
        "source_revision": revision,
        "registry": str(REGISTRY.relative_to(ROOT)),
        "run_root_env": "RUN_ROOT",
        "requirements": {
            "base_branch": "release",
            "platform": "Linux",
            "tracked_source_clean": True,
            "measured_evidence_required": True,
            "synthetic_evidence_rejected": True,
            "stop_on_product_failure": True,
        },
        "stages": stages,
    }


def write_plan(out_dir: Path, plan: dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "scanner-heal-linux-evidence-plan.json"
    path.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    return path


def text_plan(plan: dict[str, Any]) -> str:
    lines = [
        f"kind={plan['kind']}",
        f"source_revision={plan['source_revision']}",
        f"registry={plan['registry']}",
        "evidence_type=plan_only",
    ]
    for stage in plan["stages"]:
        lines.append(f"stage={stage['id']} steps={len(stage['steps'])}")
        for step in stage["steps"]:
            lines.append(f"  step={step['id']}")
            for entry in step["commands"]:
                lines.append("    command=" + " ".join(entry["command"]))
    return "\n".join(lines)


def iter_preflight_commands(plan: dict[str, Any]) -> list[list[str]]:
    commands: list[list[str]] = []
    for stage in plan["stages"]:
        if stage["id"] != "preflight":
            continue
        for step in stage["steps"]:
            if not step.get("preflight_runnable"):
                continue
            commands.extend(entry["command"] for entry in step["commands"])
    return commands


def render_run_root_path(value: str, run_root: Path) -> Path:
    rendered = value.replace("$RUN_ROOT", str(run_root))
    return Path(rendered)


def check_expected_output(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "status": "missing"}
    if not path.is_file():
        return {"path": str(path), "status": "invalid", "error": "expected a file"}
    size = path.stat().st_size
    if size <= 0:
        return {"path": str(path), "status": "empty", "bytes": size}
    return {"path": str(path), "status": "present", "bytes": size}


def covered_gates(step: dict[str, Any]) -> list[str]:
    covers = step.get("covers")
    if not isinstance(covers, dict):
        return []
    gate = covers.get("gate")
    gates = covers.get("gates")
    if isinstance(gate, str):
        return [gate]
    if isinstance(gates, list):
        return [item for item in gates if isinstance(item, str)]
    return []


def build_status(plan: dict[str, Any], run_root: Path) -> dict[str, Any]:
    run_root = run_root.resolve()
    stage_statuses = []
    totals = {"present": 0, "missing": 0, "empty": 0, "invalid": 0, "expected": 0}
    pending_gates: set[str] = set()
    next_step: dict[str, Any] | None = None
    for stage in plan["stages"]:
        step_statuses = []
        for step in stage["steps"]:
            outputs = [
                check_expected_output(render_run_root_path(path, run_root))
                for path in step.get("expected_outputs", [])
            ]
            counts = {"present": 0, "missing": 0, "empty": 0, "invalid": 0}
            for output in outputs:
                counts[output["status"]] += 1
            for key in counts:
                totals[key] += counts[key]
            totals["expected"] += len(outputs)
            if not outputs:
                status = "not_tracked"
            elif counts["invalid"] or counts["empty"]:
                status = "invalid"
            elif counts["missing"]:
                status = "pending" if counts["present"] == 0 else "partial"
            else:
                status = "complete"
            if status in {"pending", "partial", "invalid"}:
                pending_gates.update(covered_gates(step))
                if next_step is None:
                    next_step = {
                        "stage": stage["id"],
                        "step": step["id"],
                        "status": status,
                        "commands": step["commands"],
                    }
            step_statuses.append({
                "id": step["id"],
                "status": status,
                "covers": step.get("covers", {}),
                "outputs": outputs,
            })
        tracked = [step for step in step_statuses if step["status"] != "not_tracked"]
        if not tracked:
            stage_state = "not_tracked"
        elif all(step["status"] == "complete" for step in tracked):
            stage_state = "complete"
        elif any(step["status"] == "invalid" for step in tracked):
            stage_state = "invalid"
        elif any(step["status"] in {"complete", "partial"} for step in tracked):
            stage_state = "partial"
        else:
            stage_state = "pending"
        stage_statuses.append({"id": stage["id"], "status": stage_state, "steps": step_statuses})
    if totals["invalid"] or totals["empty"]:
        decision = "invalid"
    elif totals["missing"]:
        decision = "blocked"
    else:
        decision = "complete"
    return {
        "schema": 1,
        "kind": "scanner-heal-linux-evidence-status",
        "decision": decision,
        "release_approved": False,
        "source_revision": plan["source_revision"],
        "run_root": str(run_root),
        "artifact_totals": totals,
        "pending_gates": sorted(pending_gates),
        "next_step": next_step,
        "stages": stage_statuses,
    }


def run_preflight(plan: dict[str, Any]) -> int:
    commands = iter_preflight_commands(plan)
    if not commands:
        raise ValueError("--run-preflight requires the preflight stage")
    failures = []
    for args in commands:
        result = subprocess.run(args, cwd=ROOT)
        if result.returncode != 0:
            failures.append({"command": args, "exit_code": result.returncode})
            break
    if failures:
        print(json.dumps({"status": "failed", "failures": failures}, indent=2), file=sys.stderr)
        return 1
    print(json.dumps({"status": "passed", "commands": len(commands)}, indent=2))
    return 0


def self_test() -> None:
    registry = load_registry()
    validate_registry(registry)
    revision = git_output("rev-parse", "HEAD")
    plan = build_plan(registry, revision, {"all"})
    assert plan["evidence_type"] == "plan_only"
    stages = {stage["id"]: stage for stage in plan["stages"]}
    assert set(stages) == {"preflight", "functional", "performance", "bundle"}
    commands = [
        " ".join(entry["command"])
        for stage in plan["stages"]
        for step in stage["steps"]
        for entry in step["commands"]
    ]
    assert not any("--case release" in item for item in commands)
    assert any("run_scanner_heal_g09_upgrade_evidence.sh" in item for item in commands)
    assert any("scanner_abba.py" in item for item in commands)
    assert any("--check-scanner-heal-release-bundle" in item for item in commands)
    preflight = iter_preflight_commands(plan)
    assert preflight
    assert all(args[0].startswith("scripts/") for args in preflight)
    out_dir = Path(os.environ.get("TMPDIR", "/tmp")) / "rustfs-scanner-heal-linux-plan-self-test"
    path = write_plan(out_dir, plan)
    loaded = json.loads(path.read_text())
    assert loaded["source_revision"] == revision
    status = build_status(plan, out_dir / "empty-run")
    assert status["decision"] == "blocked"
    assert status["release_approved"] is False
    assert status["pending_gates"]
    complete_root = out_dir / "complete-run"
    for stage in plan["stages"]:
        for step in stage["steps"]:
            for output in step.get("expected_outputs", []):
                path = render_run_root_path(output, complete_root)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}\n")
    complete = build_status(plan, complete_root)
    assert complete["decision"] == "complete"
    assert complete["release_approved"] is False
    print("PASS: scanner/heal Linux evidence plan self-test")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", action="append", choices=("all", "preflight", "functional", "performance", "bundle"), default=[])
    parser.add_argument("--source-revision")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--write-plan", action="store_true")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--run-preflight", action="store_true")
    parser.add_argument("--status-root", type=Path)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return 0
    try:
        registry = load_registry()
        validate_registry(registry)
        phases = set(args.phase or ["all"])
        if "all" in phases and len(phases) > 1:
            raise ValueError("--phase all cannot be combined with another phase")
        if args.status_root is not None and (args.write_plan or args.run_preflight):
            raise ValueError("--status-root cannot be combined with --write-plan or --run-preflight")
        plan = build_plan(registry, source_revision(args.source_revision), phases)
        if args.status_root is not None:
            status = build_status(plan, args.status_root)
            print(json.dumps(status, indent=2, sort_keys=True))
            return 0 if status["decision"] == "complete" else 3
        if args.write_plan:
            path = write_plan(args.out_dir.resolve(), plan)
            print(path)
        elif args.run_preflight:
            pass
        elif args.format == "json":
            print(json.dumps(plan, indent=2, sort_keys=True))
        else:
            print(text_plan(plan))
        if args.run_preflight:
            return run_preflight(plan)
        return 0
    except (AssertionError, ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
