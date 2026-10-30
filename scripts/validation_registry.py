#!/usr/bin/env python3
"""One responsibility registry for suite selection and periodic health monitors."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / ".github/scheduled-validations.json"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load_registry(path=REGISTRY):
    value = json.loads(Path(path).read_text())
    require(isinstance(value, dict) and type(value.get("schema")) is int and value["schema"] == 1,
            "unsupported validation registry")
    require(set(value.get("plans", {})) == {"nightly", "weekly", "monthly"}, "missing validation cadence")
    for plan in value["plans"].values():
        require(isinstance(plan, dict) and plan.get("timezone") == "UTC"
                and isinstance(plan.get("cadences"), list) and plan["cadences"]
                and set(plan["cadences"]) <= {"nightly", "weekly", "monthly"}, "invalid cadence plan")
    workflows = value.get("workflows")
    suites = value.get("suites")
    require(isinstance(workflows, list) and workflows and isinstance(suites, list) and suites,
            "empty validation responsibilities")
    paths = set()
    for item in workflows:
        require(isinstance(item, dict), "invalid workflow responsibility")
        path = item.get("workflow")
        require(isinstance(path, str) and re.fullmatch(r"\.github/workflows/[a-z0-9-]+\.yml", path)
                and path not in paths, "invalid or duplicate workflow responsibility")
        paths.add(path)
        require(item.get("owner") and item.get("name") and item.get("roles") and item.get("events"),
                f"incomplete responsibility: {path}")
        require(item.get("timezone") == "UTC" and item.get("policy") in ("active", "paused", "disabled"),
                f"invalid execution policy: {path}")
        require(item.get("monitor") in ("workflow", "complete-chain", "functional-chain", "none"),
                f"invalid monitor: {path}")
        if item["monitor"] in ("workflow", "complete-chain"):
            require(type(item.get("max_age_hours")) is int and item["max_age_hours"] > 0,
                    f"missing freshness budget: {path}")
        if item["monitor"] == "none":
            require(item.get("monitor_reason"), f"missing monitor exclusion: {path}")
    ids = []
    for suite in suites:
        require(isinstance(suite, dict) and isinstance(suite.get("id"), str)
                and re.fullmatch(r"[a-z][a-z0-9-]*", suite["id"]) and suite["id"] not in ids,
                "invalid or duplicate suite")
        ids.append(suite["id"])
        require(suite.get("workflow") in paths and suite.get("cadence") in ("nightly", "weekly", "monthly")
                and suite.get("timezone") == "UTC" and suite.get("resource_lock") and suite.get("environment")
                and suite.get("owner") and suite.get("evidence") and type(suite.get("required")) is bool
                and isinstance(suite.get("dependencies"), list), "incomplete suite responsibility")
        require(suite["required"] or suite.get("optional_reason"), "optional suite needs a reason")
    for suite in suites:
        require(set(suite["dependencies"]) <= set(ids) and suite["id"] not in suite["dependencies"],
                "unknown or self-dependent suite")
    return value


def suite_ids(registry=None):
    return tuple(item["id"] for item in (registry or load_registry())["suites"])


def workflow_entry(path, registry=None):
    entries = [item for item in (registry or load_registry())["workflows"] if item["workflow"] == path]
    require(len(entries) == 1, f"unregistered workflow: {path}")
    return entries[0]


def monitored_workflows(registry=None):
    return [item for item in (registry or load_registry())["workflows"]
            if item["monitor"] in ("workflow", "complete-chain")]


def select_plan(registry, cadence="nightly", requested="", event="workflow_dispatch"):
    require(cadence in registry["plans"], "unknown validation plan")
    require(event in ("workflow_dispatch", "workflow_run", "schedule"), "unsupported plan event")
    require(not requested or event == "workflow_dispatch", "automatic plans cannot override suite selection")
    by_id = {item["id"]: item for item in registry["suites"]}
    explicit = bool(requested and requested != "all")
    names = requested.split(",") if explicit else [item["id"] for item in registry["suites"]
              if item["cadence"] in registry["plans"][cadence]["cadences"]]
    require(names and all(name == name.strip() and name in by_id for name in names)
            and len(names) == len(set(names)), "unknown, empty or duplicate selected suite")
    selected = set()

    def add(name, ancestors=()):
        require(name not in ancestors, "cyclic suite dependency")
        for dependency in by_id[name]["dependencies"]:
            add(dependency, ancestors + (name,))
        selected.add(name)

    for name in names:
        add(name)
    ordered = [name for name in by_id if name in selected]
    for name in ordered:
        require(workflow_entry(by_id[name]["workflow"], registry)["policy"] == "active",
                f"selected suite is disabled or paused: {name}")
    return {"schema": 1, "cadence": cadence, "timezone": registry["plans"][cadence]["timezone"],
            "suites": ordered, "optional_suites": [] if explicit else [name for name in ordered if not by_id[name]["required"]],
            "environments": sorted({by_id[name]["environment"] for name in ordered}),
            "resource_locks": sorted({by_id[name]["resource_lock"] for name in ordered}),
            "periodic_workflows": [item["workflow"] for item in registry["workflows"]
                                   if item["cadence"] in registry["plans"][cadence]["cadences"]
                                   and item["policy"] == "active" and item["monitor"] in ("workflow", "complete-chain")]}


def validate_plan(plan, registry=None):
    registry = registry or load_registry()
    require(isinstance(plan, dict) and plan.get("schema") == 1, "invalid suite selection plan")
    # Explicit single-suite invocations have no optional lanes; an automatic
    # plan can omit only optional lanes declared in the registry.
    full = select_plan(registry, plan.get("cadence", ""))
    selected = plan.get("suites")
    require(isinstance(selected, list) and selected
            and selected == [name for name in suite_ids(registry) if name in selected], "invalid selected suites")
    optional = plan.get("optional_suites")
    require(isinstance(optional, list) and len(optional) == len(set(optional))
            and set(optional) <= set(full["optional_suites"]) & set(selected), "invalid optional suites")
    for suite in registry["suites"]:
        if suite["id"] in selected:
            require(set(suite["dependencies"]) <= set(selected), "plan omits a suite dependency")
    return plan


def watchdog_event(event, registry=None):
    run = event.get("workflow_run", {})
    matches = [item for item in monitored_workflows(registry)
               if item["workflow"] == run.get("path") and item["name"] == run.get("name")]
    if len(matches) != 1:
        return False
    item = matches[0]
    expected_event = "workflow_run" if item["monitor"] == "complete-chain" else "schedule"
    repository = event.get("repository", {})
    return (item["policy"] == "active" and run.get("event") == expected_event
            and run.get("head_branch") == repository.get("default_branch")
            and (run.get("head_repository") or {}).get("full_name") == repository.get("full_name")
            and run.get("conclusion") != "success"
            and (run.get("conclusion") != "failure" or item.get("failure_owner") == "watchdog"))


def check_wiring(registry, root=ROOT):
    declared = {item["workflow"] for item in registry["workflows"]}
    actual = {str(path.relative_to(root)) for path in (root / ".github/workflows").glob("*.yml")}
    require(declared == actual, "registry workflow coverage differs: " + ", ".join(sorted(declared ^ actual)))
    for item in registry["workflows"]:
        source = (root / item["workflow"]).read_text()
        name = re.search(r"^name: (.+)$", source, re.M)
        require(name and name.group(1).strip("\"'") == item["name"], "workflow name differs from registry")
        trigger = re.search(r"^on:\n(.*?)(?=^[^ #\n]|\Z)", source, re.M | re.S)
        require(trigger is not None, "workflow has no event declaration")
        events = re.findall(r"^  (\w+):", trigger.group(1), re.M)
        schedules = re.findall(r"^\s+- cron:\s*['\"]([^'\"]+)", trigger.group(1), re.M)
        require(events == item["events"] and schedules == item["schedule"],
                "workflow events or cadence differ from registry: " + item["workflow"])
    for suite in registry["suites"]:
        source = (root / suite["workflow"]).read_text()
        require("repository_dispatch" not in source and "Continue functional chain" not in source,
                "retired functional relay remains wired")
        require("group: " + suite["resource_lock"] in source and "cancel-in-progress: false" in source,
                "suite resource lock differs from registry")
        require("functional_chain_evidence.py record --suite " + suite["id"] in source,
                "suite evidence exit differs from registry")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("check", "select", "watchdog"))
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--cadence", default=os.environ.get("VALIDATION_PLAN") or "nightly")
    parser.add_argument("--suites", default=os.environ.get("VALIDATION_SUITES", ""))
    parser.add_argument("--event", default=os.environ.get("GITHUB_EVENT_NAME", "workflow_dispatch"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    registry = load_registry(args.registry)
    if args.mode == "check":
        check_wiring(registry)
        print(f"Validated {len(registry['workflows'])} workflow responsibilities and {len(registry['suites'])} suites")
    elif args.mode == "watchdog":
        selected = watchdog_event(json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()), registry)
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("selected=" + str(selected).lower() + "\n")
    else:
        plan = select_plan(registry, args.cadence, args.suites, args.event)
        encoded = json.dumps(plan, separators=(",", ":"))
        if args.output:
            args.output.write_text(encoded + "\n")
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a") as output:
                output.write("plan=" + encoded + "\n")
                for key in ("suites", "environments", "optional_suites"):
                    output.write(key + "=" + json.dumps(plan[key], separators=(",", ":")) + "\n")
        print(encoded)


if __name__ == "__main__":
    main()
