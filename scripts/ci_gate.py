#!/usr/bin/env python3
"""Select conservative PR scopes and verify the complete required job set."""
from __future__ import annotations

import fnmatch
import json
import posixpath
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tomllib
import unittest

ROOT = Path(__file__).resolve().parent.parent
ALWAYS_JOBS = ("classify-changes", "typos", "quick-checks")
CODE_JOBS = (
    "test-and-lint", "workspace-tests", "core-contract", "connect-feature-boundaries", "test-and-lint-rio-v2",
    "test-and-lint-protocols", "build-rustfs-debug-binary", "uring-integration",
    "e2e-tests", "s3-implemented-tests", "s3-lifecycle-behavior-tests",
)
# Specialized boundary, hardware and alternate-feature lanes do not gate a
# merge. They run only in the scheduled (nightly) and manual full-validation
# runs, which report a regression within a day; repeating them on every PR
# revision and merge group bought no earlier signal worth the runner occupancy
# (Connect alone measured 53.5 median minutes per run and occasionally set the
# wall). Workspace lint and tests, the short core contract, the debug build,
# E2E smoke and the S3 lanes stay on the pull-request path because they cover
# the product's core correctness and S3 compatibility surface.
NIGHTLY_JOBS = ("connect-feature-boundaries", "uring-integration", "test-and-lint-rio-v2", "test-and-lint-protocols")
# These jobs still exercise the server and its black-box harness when only E2E
# Rust sources change. Production code, manifests and shared test configuration
# always select the full matrix. No production package depends on e2e_test.
E2E_JOBS = (
    "test-and-lint", "workspace-tests", "core-contract", "build-rustfs-debug-binary", "e2e-tests",
    "s3-implemented-tests", "s3-lifecycle-behavior-tests",
)
E2E_SELECTION_FILES = {
    f".config/{profile}-selection.txt" for profile in (
        "e2e-smoke", "e2e-full", "e2e-nightly", "e2e-repl-nightly",
        "e2e-distributed", "e2e-protocols", "e2e-odm-interop",
    )
}
SHARD_JOBS = ("workspace-shards", "workspace-shard-evidence")
OPTIONAL_JOBS = ("build-rustfs-debug-binary-rio-v2", "e2e-tests-rio-v2", "e2e-full", *SHARD_JOBS)
NON_VALIDATION_JOBS = {"required-checks", "cancel-closed-pr-runs", "alert-on-failure"}


def documentation_path(path: str) -> bool:
    parts = PurePosixPath(path).parts
    if not parts or path.startswith("/") or any(part in (".", "..") for part in parts) or any(ord(c) < 32 for c in path):
        return False
    if parts[-1] == "AGENTS.md":
        return True
    if len(parts) == 1 and (path.endswith(".md") or path == "LICENSE" or path.startswith("LICENSE-")):
        return True
    if path.startswith(("docs/", ".agents/skills/")) and path.endswith(".md"):
        return True
    return path.startswith("docs/") and path.endswith((".png", ".jpg", ".svg"))


POLICY_PATH = ".config/ci-plan.json"
SHA = re.compile(r"[0-9a-f]{40}")
EVENTS = ("pull_request", "push", "merge_group", "schedule", "workflow_dispatch")


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=root, stderr=subprocess.PIPE).decode("utf-8")


def git_file(root: Path, revision: str, path: str) -> str:
    if not SHA.fullmatch(revision):
        raise ValueError("revision must be a full commit SHA")
    return git(root, "show", f"{revision}:{path}")


def dependency_tables(document: dict):
    for key, value in document.items():
        if key in ("dependencies", "dev-dependencies", "build-dependencies"):
            if not isinstance(value, dict):
                raise ValueError("dependency table is not an object")
            yield value
        elif key == "target":
            for target in value.values():
                yield from dependency_tables(target)


def cargo_graph(root: Path, revision: str) -> tuple[dict[str, str], dict[str, set[str]]]:
    """Read Cargo inputs without resolving the network or executing candidate code.

    All dependency kinds and target predicates are unioned. This intentionally
    over-selects optional/target dependencies instead of guessing active cfgs.
    """
    workspace = tomllib.loads(git_file(root, revision, "Cargo.toml"))
    files = set(git(root, "ls-tree", "-r", "--name-only", "-z", revision).rstrip("\0").split("\0"))
    manifests = {path for path in files if path.endswith("/Cargo.toml")}
    members = workspace.get("workspace", {}).get("members", [])
    excluded = workspace.get("workspace", {}).get("exclude", [])
    selected = {path for path in manifests if any(fnmatch.fnmatchcase(path[:-11], member) for member in members)
                and not any(fnmatch.fnmatchcase(path[:-11], pattern) for pattern in excluded)}
    if "package" in workspace:
        selected.add("Cargo.toml")
    if not selected:
        raise ValueError("workspace has no packages")
    for member in members:
        if not any(fnmatch.fnmatchcase(path[:-11], member) for path in manifests):
            raise ValueError(f"workspace member has no manifest: {member}")
    shared = workspace.get("workspace", {}).get("dependencies", {})
    packages, graph, documents = {}, {}, {}
    pending = set(selected)
    while pending:
        manifest = pending.pop()
        if manifest in documents:
            continue
        document = tomllib.loads(git_file(root, revision, manifest))
        directory = posixpath.dirname(manifest)
        name = document["package"]["name"]
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", name):
            raise ValueError("invalid Cargo package name")
        if name in packages and packages[name] != directory:
            raise ValueError(f"duplicate workspace package {name}")
        packages[name] = directory
        documents[manifest] = (name, document)
        graph[name] = set()
        for table in dependency_tables(document):
            for alias, value in table.items():
                dependency = shared.get(alias) if isinstance(value, dict) and value.get("workspace") is True else value
                if not isinstance(dependency, dict) or "path" not in dependency:
                    continue
                owner = "" if isinstance(value, dict) and value.get("workspace") is True else directory
                path = posixpath.normpath(posixpath.join(owner, dependency["path"], "Cargo.toml"))
                if path.startswith("../") or path.startswith("/") or path not in files:
                    raise ValueError(f"unresolved workspace path dependency: {path}")
                pending.add(path)
    by_directory = {directory: name for name, directory in packages.items()}
    for manifest, (name, document) in documents.items():
        for table in dependency_tables(document):
            for alias, value in table.items():
                inherited = isinstance(value, dict) and value.get("workspace") is True
                dependency = shared.get(alias) if inherited else value
                if inherited and dependency is None:
                    raise ValueError(f"missing inherited dependency: {alias}")
                if isinstance(dependency, dict) and "path" in dependency:
                    owner = "" if inherited else posixpath.dirname(manifest)
                    directory = posixpath.normpath(posixpath.join(owner, dependency["path"]))
                    graph[name].add(by_directory[directory])
                else:
                    dependency_name = dependency.get("package", alias) if isinstance(dependency, dict) else alias
                    if dependency_name in packages:
                        graph[name].add(dependency_name)
    return packages, graph


def closure(seeds: set[str], graph: dict[str, set[str]], reverse: bool = False) -> set[str]:
    reached = set(seeds)
    while True:
        expanded = reached | ({name for name, deps in graph.items() if deps & reached} if reverse
                              else {dep for name in reached for dep in graph.get(name, set())})
        if expanded == reached:
            return reached
        reached = expanded


def expected_results(mode: str, event: str, ref: str, connect: bool = True, action: str = "", rehearse_shards: bool = False) -> dict[str, str]:
    if event not in EVENTS:
        raise ValueError(f"unsupported CI event: {event!r}")
    if mode not in ("docs", "e2e", "full") or (mode != "full" and event != "pull_request"):
        raise ValueError(f"invalid CI selection: {mode!r} for {event!r}")
    if type(rehearse_shards) is not bool or (rehearse_shards and event != "workflow_dispatch"):
        raise ValueError("shard rehearsal requires an explicit manual event")
    expected = {job: "success" for job in ALWAYS_JOBS}
    expected.update({job: "success" if mode == "full" or (mode == "e2e" and job in E2E_JOBS) else "skipped" for job in CODE_JOBS})
    nightly = mode == "full" and event in ("schedule", "workflow_dispatch")
    expected.update({job: "success" if nightly else "skipped" for job in NIGHTLY_JOBS})
    expected["connect-feature-boundaries"] = "success" if nightly and connect else "skipped"
    expected.update({job: "success" if nightly else "skipped" for job in OPTIONAL_JOBS[:2]})
    full = mode == "full" and (event in ("merge_group", "workflow_dispatch") or (event == "push" and ref == "refs/heads/main"))
    expected["e2e-full"] = "success" if full else "skipped"
    expected.update({job: "success" if rehearse_shards else "skipped" for job in SHARD_JOBS})
    if event == "pull_request" and action == "closed":
        return {job: "skipped" for job in expected}
    return expected


def read_policy(root: Path, revision: str) -> dict:
    policy = json.loads(git_file(root, revision, POLICY_PATH))
    if policy.get("schema_version") != 1 or policy.get("rollout") not in ("shadow", "enforced"):
        raise ValueError("unsupported CI policy")
    for field in ("core_packages", "connect_packages", "full_paths"):
        if not isinstance(policy.get(field), list) or not all(isinstance(value, str) and value for value in policy[field]):
            raise ValueError(f"invalid CI policy {field}")
    return policy


def configurations(jobs: dict[str, str]) -> list[dict]:
    values = [
        ("workspace-lint", "test-and-lint", "dev", []),
        ("workspace-tests", "test-and-lint", "test", []),
        ("e2e-harness", "build-rustfs-debug-binary", "test", []),
        ("uring-tests", "uring-integration", "test", []),
        ("server-e2e-hooks", "build-rustfs-debug-binary", "dev", ["e2e-test-hooks"]),
        ("connect-debug", "connect-feature-boundaries", "test", ["connect-e2e-short-credentials"]),
        ("connect-offline-cli", "connect-feature-boundaries", "dev", ["offline-enrollment-e2e-root"]),
        ("connect-offline-tests", "connect-feature-boundaries", "test", ["offline-enrollment-e2e-root"]),
        ("connect-release", "connect-feature-boundaries", "release", []),
        ("connect-release-rejection", "connect-feature-boundaries", "release", ["connect-e2e-short-credentials"]),
        ("rio-v2-lint", "test-and-lint-rio-v2", "dev", ["rio-v2"]),
        ("rio-v2-tests", "test-and-lint-rio-v2", "test", ["rio-v2"]),
        ("protocol-swift-lint", "test-and-lint-protocols", "dev", ["swift"]),
        ("protocol-swift-tests", "test-and-lint-protocols", "test", ["swift"]),
        ("protocol-sftp-lint", "test-and-lint-protocols", "dev", ["sftp"]),
        ("protocol-sftp-tests", "test-and-lint-protocols", "test", ["sftp"]),
        ("protocol-http3-lint", "test-and-lint-protocols", "dev", ["http3", "swift"]),
        ("protocol-http3-tests", "test-and-lint-protocols", "test", ["http3", "swift"]),
        ("server-rio-v2", "build-rustfs-debug-binary-rio-v2", "dev", ["e2e-test-hooks", "rio-v2"]),
    ]
    return [{"id": identity, "job": job, "target": "x86_64-unknown-linux-gnu", "profile": profile,
             "default_features": True, "features": features} for identity, job, profile, features in values if jobs[job] == "success"]


def select_plan(event: str, base: str, head: str, ref: str, root: Path, action: str = "", rehearse_shards: bool = False) -> dict:
    if event not in EVENTS or not SHA.fullmatch(head):
        raise ValueError("unsupported event or missing candidate SHA")
    tree = git(root, "rev-parse", f"{head}^{{tree}}").strip()
    if not SHA.fullmatch(tree):
        raise ValueError("candidate tree cannot be resolved")
    head_packages, head_graph = cargo_graph(root, head)
    all_packages = sorted(set(head_packages) - {"e2e_test"})
    if not all_packages:
        raise ValueError("candidate has no workspace validation packages")
    policy_revision = base if event == "pull_request" and SHA.fullmatch(base) else head
    reasons, changed, mode = [], [], "full"
    policy = None
    full = event != "pull_request"
    try:
        policy = read_policy(root, policy_revision)
        base_packages, base_graph = cargo_graph(root, base) if SHA.fullmatch(base) else (head_packages, head_graph)
        if event == "pull_request":
            if not SHA.fullmatch(base):
                raise ValueError("missing base SHA")
            changed = git(root, "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--name-only", "-z", base, head, "--").rstrip("\0").split("\0")
            if not changed or changed == [""]:
                raise ValueError("empty change range")
        else:
            reasons.append(f"{event} validates the complete candidate")
    except (OSError, subprocess.CalledProcessError, UnicodeError, KeyError, TypeError, ValueError) as error:
        full = True
        base_packages, base_graph = head_packages, head_graph
        reasons.append(f"conservative fallback: {type(error).__name__}")
    graph = {name: head_graph.get(name, set()) | base_graph.get(name, set()) for name in set(head_graph) | set(base_graph)}
    affected = set()
    if not full and policy is not None:
        for path in changed:
            if documentation_path(path):
                continue
            if path in E2E_SELECTION_FILES:
                affected.add("e2e_test")
                continue
            # Cargo's graph cannot describe include_bytes!, fixture readers or
            # build-script inputs shared across packages. Narrow only Rust
            # sources; all other package-owned inputs retain the full gate.
            if (not path.endswith(".rs") or PurePosixPath(path).name == "build.rs"
                    or "fixtures" in PurePosixPath(path).parts
                    or any(fnmatch.fnmatchcase(path, pattern) for pattern in policy["full_paths"])):
                full = True
                reasons.append(f"shared build or policy input: {path}")
                continue
            owners = {name for packages in (base_packages, head_packages) for name, directory in packages.items()
                      if directory and path.startswith(directory + "/")}
            if not owners or path.startswith("/") or any(part in (".", "..") for part in path.split("/")) or any(ord(c) < 32 for c in path):
                full = True
                reasons.append(f"unmapped input: {path}")
            affected.update(owners)
        if not affected and not full:
            mode = "docs"
            reasons.append("all changes are documentation")
        elif affected == {"e2e_test"} and not full and not (closure({"e2e_test"}, graph, reverse=True) - {"e2e_test"}) and all(
                documentation_path(path) or path in E2E_SELECTION_FILES or
                (path.startswith("crates/e2e_test/src/") and path.endswith(".rs")) for path in changed):
            mode = "e2e"
            reasons.append("isolated E2E harness changes")
    core = set(policy["core_packages"]) if policy else set()
    try:
        # Union both inventories: a PR may add tests, but cannot remove baseline responsibility.
        for revision in {policy_revision, head}:
            inventory = json.loads(git_file(root, revision, ".config/ecstore-required-tests.json"))
            if not inventory["tests"]:
                raise ValueError("empty core test inventory")
            core.update(test["suite"].split("::", 1)[0] for test in inventory["tests"])
        if not core <= set(head_packages):
            raise ValueError("core package missing from candidate")
    except (subprocess.CalledProcessError, KeyError, TypeError, ValueError):
        full = True
        mode = "full"
        reasons.append("core inventory cannot be resolved; require the full workspace")
    selected = (closure(affected, graph, reverse=True) | core) & set(head_packages) - {"e2e_test"}
    if full:
        selected = set(all_packages)
    if mode == "docs":
        selected = set()
    connect_inputs = closure(set(policy["connect_packages"]), graph) if policy else set(head_packages)
    connect = mode == "full" and (full or "rustfs" in affected or bool(affected & connect_inputs))
    reasons.append("selected changed packages, reverse dependencies and fixed core contracts")
    reasons.append("Connect boundary or a shared input changed" if connect else "Connect has no changed credential/configuration dependency")
    proposed = {"packages": sorted(selected), "connect": connect, "full_validation": full, "reasons": reasons,
                "jobs": expected_results(mode, event, ref, connect, action, rehearse_shards)}
    shadow = policy is None or policy["rollout"] == "shadow"
    effective_packages = all_packages if shadow and mode != "docs" else sorted(selected)
    effective_connect = mode == "full" if shadow else connect
    effective_full = mode == "full" if shadow else full
    jobs = expected_results(mode, event, ref, effective_connect, action, rehearse_shards)
    plan = {"schema_version": 1, "event": event, "action": action, "ref": ref, "base": base if SHA.fullmatch(base) else "",
            "head": head, "tree": tree, "policy": {"revision": policy_revision, "rollout": "shadow" if shadow else "enforced"},
            "mode": mode, "packages": effective_packages, "full_validation": effective_full, "connect": effective_connect,
            "rehearse_shards": rehearse_shards,
            "configurations": configurations(jobs), "jobs": jobs, "proposed": proposed,
            "reasons": ["shadow rollout preserves current validation responsibility"] + reasons if shadow else reasons,
            "not_applicable": {job: f"not required for {mode}/{event}" for job, result in jobs.items() if result == "skipped"}}
    return plan


def validate_plan(plan: object, event: str, ref: str, head: str = "", tree: str = "", base: str | None = None, action: str | None = None) -> dict[str, str]:
    if not isinstance(plan, dict) or type(plan.get("schema_version")) is not int or plan["schema_version"] != 1:
        raise ValueError("invalid plan schema")
    if plan.get("event") != event or plan.get("ref") != ref:
        raise ValueError("plan event/ref does not match this run")
    if not isinstance(plan.get("base"), str) or plan["base"] and not SHA.fullmatch(plan["base"]):
        raise ValueError("invalid plan base identity")
    if not isinstance(plan.get("action"), str):
        raise ValueError("invalid plan event action")
    if (base is not None and plan["base"] != base) or (action is not None and plan["action"] != action):
        raise ValueError("plan base/action does not match this run")
    for field in ("head", "tree"):
        if not isinstance(plan.get(field), str) or not SHA.fullmatch(plan[field]):
            raise ValueError(f"invalid plan {field}")
    if (head and plan["head"] != head) or (tree and plan["tree"] != tree):
        raise ValueError("plan candidate identity does not match this run")
    if any(type(plan.get(name)) is not bool for name in ("connect", "full_validation", "rehearse_shards")):
        raise ValueError("invalid plan selection flags")
    policy = plan.get("policy")
    if not isinstance(policy, dict) or policy.get("rollout") not in ("shadow", "enforced") or not isinstance(policy.get("revision"), str) or not SHA.fullmatch(policy["revision"]):
        raise ValueError("invalid plan policy identity")
    if policy["rollout"] == "shadow" and plan.get("mode") == "full" and (not plan["connect"] or not plan["full_validation"]):
        raise ValueError("shadow plan cannot reduce full validation responsibility")
    packages = plan.get("packages")
    if not isinstance(packages, list) or not all(isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_-]+", name) for name in packages) or len(packages) != len(set(packages)):
        raise ValueError("invalid plan packages")
    if plan.get("mode") != "docs" and not packages:
        raise ValueError("code validation plan must not be empty")
    expected = expected_results(plan.get("mode"), event, ref, plan["connect"], plan.get("action", ""), plan["rehearse_shards"])
    if plan.get("jobs") != expected:
        raise ValueError("plan job contract differs from selected responsibilities")
    if plan.get("configurations") != configurations(expected):
        raise ValueError("plan configuration matrix is incomplete")
    if not isinstance(plan.get("reasons"), list) or not plan["reasons"] or not all(isinstance(reason, str) and reason for reason in plan["reasons"]):
        raise ValueError("plan selection has no explanation")
    not_applicable = plan.get("not_applicable")
    if not isinstance(not_applicable, dict) or not all(isinstance(reason, str) and reason for reason in not_applicable.values()) or set(not_applicable) != {job for job, value in expected.items() if value == "skipped"}:
        raise ValueError("plan skips lack explicit not-applicable reasons")
    return expected


def verify_results(needs: object, event: str, ref: str, head: str = "", tree: str = "", base: str | None = None, action: str | None = None) -> list[str]:
    if not isinstance(needs, dict):
        return ["needs must be a job-result object"]
    selection = needs.get("classify-changes", {})
    outputs = selection.get("outputs", {}) if isinstance(selection, dict) else {}
    try:
        raw = outputs.get("plan") if isinstance(outputs, dict) else None
        if raw:
            plan = json.loads(raw)
            expected = validate_plan(plan, event, ref, head, tree, base, action)
            if outputs.get("mode") != plan["mode"]:
                raise ValueError("plan mode differs from scheduling output")
            for name in ("connect", "full_validation", "rehearse_shards"):
                if name in outputs and outputs[name] != str(plan[name]).lower():
                    raise ValueError(f"plan {name} differs from scheduling output")
            if outputs.get("packages") and json.loads(outputs["packages"]) != plan["packages"]:
                raise ValueError("plan packages differ from scheduling output")
        else:
            # During adoption the trusted base script only emits `mode`. No
            # legacy output may authorize an omitted job in the new workflow.
            if not isinstance(outputs, dict) or outputs.get("mode") != "full":
                raise ValueError("missing plan requires conservative full mode")
            expected = expected_results("full", event, ref)
    except (TypeError, ValueError) as error:
        return [str(error)]
    errors = []
    if set(needs) != set(expected):
        errors.append(f"job set differs: missing={sorted(set(expected) - set(needs))}, unexpected={sorted(set(needs) - set(expected))}")
    for job, required in expected.items():
        result = needs.get(job, {})
        actual = result.get("result") if isinstance(result, dict) else None
        if actual != required:
            errors.append(f"{job}: expected {required}, got {actual!r}")
    return errors


def workflow_document(text: str) -> dict:
    try:
        import yaml
    except ImportError as error:
        raise ValueError("workflow checking requires PyYAML") from error

    class UniqueLoader(yaml.BaseLoader):
        pass

    def mapping(loader, node):
        result = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node)
            if key in result:
                raise ValueError(f"duplicate YAML key: {key}")
            result[key] = loader.construct_object(value_node)
        return result

    UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    try:
        value = yaml.load(text, Loader=UniqueLoader)
    except yaml.YAMLError as error:
        raise ValueError(f"invalid workflow YAML: {error}") from error
    if not isinstance(value, dict):
        raise ValueError("workflow must be a mapping")
    return value


def condition(expression: str | bool, context: dict) -> bool:
    """Evaluate the finite condition grammar used by CI, never Python eval."""
    if isinstance(expression, bool):
        return expression
    expression = expression.strip()
    if expression.startswith("${{") and expression.endswith("}}"):
        expression = expression[3:-2].strip()
    token = re.compile(r"\s*(?:('(?:[^']|'')*')|([A-Za-z_][A-Za-z_0-9.-]*)|(&&|\|\||==|!=|[!(),]))")
    values, cursor = [], 0
    while cursor < len(expression):
        match = token.match(expression, cursor)
        if not match:
            raise ValueError(f"unsupported condition near {expression[cursor:]!r}")
        values.append(next(value for value in match.groups() if value is not None))
        cursor = match.end()
    position = 0

    def atom():
        nonlocal position
        if position >= len(values):
            raise ValueError("incomplete condition")
        value = values[position]
        position += 1
        if value == "!":
            return not atom()
        if value == "(":
            result = parse(0)
            if position >= len(values) or values[position] != ")":
                raise ValueError("unclosed condition")
            position += 1
            return result
        if value.startswith("'"):
            return value[1:-1].replace("''", "'")
        if position < len(values) and values[position] == "(":
            position += 1
            arguments = []
            while position < len(values) and values[position] != ")":
                arguments.append(parse(0))
                if position < len(values) and values[position] == ",":
                    position += 1
                else:
                    break
            if position >= len(values) or values[position] != ")":
                raise ValueError("unclosed function")
            position += 1
            if value == "contains" and len(arguments) == 2:
                return arguments[1] in arguments[0]
            if value == "fromJSON" and len(arguments) == 1:
                return json.loads(arguments[0])
            if value in ("always", "success", "failure", "cancelled") and not arguments:
                return True if value == "always" else context.get(value, False)
            raise ValueError(f"unsupported condition function: {value}")
        if value in ("true", "false"):
            return value == "true"
        if value not in context:
            raise ValueError(f"unknown condition input: {value}")
        return context[value]

    precedence = {"||": 1, "&&": 2, "==": 3, "!=": 3}

    def parse(minimum):
        nonlocal position
        left = atom()
        while position < len(values) and precedence.get(values[position], 0) > minimum:
            operator = values[position]
            position += 1
            right = parse(precedence[operator])
            if operator == "||":
                left = left or right
            elif operator == "&&":
                left = left and right
            elif operator == "==":
                left = left == right
            else:
                left = left != right
        return left

    result = parse(0)
    if position != len(values):
        raise ValueError("unexpected condition tokens")
    return bool(result)


def check_workflow(root: Path) -> list[str]:
    try:
        document = workflow_document((root / ".github/workflows/ci.yml").read_text())
        jobs = document["jobs"]
        if not isinstance(jobs, dict):
            raise ValueError("jobs must be a mapping")
    except (OSError, KeyError, TypeError, ValueError) as error:
        return [str(error)]
    errors = []
    required = set(ALWAYS_JOBS + CODE_JOBS + OPTIONAL_JOBS)
    if set(jobs) - NON_VALIDATION_JOBS != required:
        errors.append("CI verification jobs and the required gate contract differ")
    for job in required | {"required-checks"}:
        block = jobs.get(job, {})
        for item in [block, *block.get("steps", [])]:
            if item.get("continue-on-error", "false") != "false":
                errors.append(f"{job} cannot convert a validation failure into success")
    gate = jobs.get("required-checks", {})
    dependencies = gate.get("needs", [])
    if not isinstance(dependencies, list) or set(dependencies) != required or len(dependencies) != len(required):
        errors.append("required-checks must directly depend on every verification job exactly once")
    if gate.get("name") != "Test and Lint" or sum(block.get("name") == "Test and Lint" for block in jobs.values()) != 1:
        errors.append("Test and Lint must uniquely name the aggregate gate")
    for job, block in jobs.items():
        if not isinstance(block.get("name"), str):
            errors.append(f"{job} needs a stable display name")
        name = block.get("name", "")
        if "${{" in name and any(value.strip() not in ("matrix.features.name", "matrix.shard") for value in re.findall(r"\$\{\{(.*?)\}\}", name)):
            errors.append(f"{job} has an unverifiable dynamic display name")
    verifiers = [step for step in gate.get("steps", []) if "scripts/ci_gate.py verify" in step.get("run", "")]
    if len(verifiers) != 1 or (verifiers[0].get("run", "").strip() != "python3 scripts/ci_gate.py verify"
                              or verifiers[0].get("env", {}).get("CI_NEEDS") != "${{ toJSON(needs) }}"
                              or verifiers[0].get("shell") != "bash" or "if" in verifiers[0]):
        errors.append("required-checks must unconditionally verify actual needs and preserve its exit status")
    lint_commands = ("cargo clippy --all-targets -- -D warnings", "python3 scripts/ci_test_artifact.py cargo lint --configuration workspace")
    clippy = [step for step in jobs.get("test-and-lint", {}).get("steps", [])
              if any(command in step.get("run", "") for command in lint_commands)]
    if not clippy or any("if" in step for step in clippy):
        errors.append("workspace Clippy must run on every code event, pull requests included")
    matrix = jobs.get("test-and-lint-protocols", {}).get("strategy", {}).get("matrix", {})
    features = matrix.get("features", [])
    if not isinstance(features, list) or {item.get("name"): item.get("flags") for item in features} != {
            "swift": "--features swift", "sftp": "--features sftp", "http3": "--features http3,swift"} or len(features) != 3:
        errors.append("protocol matrix must retain every required feature configuration")
    triggers = document.get("on", {})
    if isinstance(triggers, dict) and isinstance(triggers.get("pull_request"), dict) and any(key in triggers["pull_request"] for key in ("paths", "paths-ignore")):
        errors.append("all pull requests must enter the single CI workflow")
    if (root / ".github/workflows/ci-docs-only.yml").exists():
        errors.append("the duplicate required-status companion must be removed")
    # Truth tables protect behavior while allowing equivalent expression and YAML formatting.
    for event, rehearse_shards in [(event, rehearsal) for event in EVENTS
                                   for rehearsal in ((False, True) if event == "workflow_dispatch" else (False,))]:
        for mode in (("docs", "e2e", "full") if event == "pull_request" else ("full",)):
            for ref in ("refs/heads/main", "refs/heads/release"):
                for connect in (True, False) if event == "pull_request" and mode == "full" else (mode == "full",):
                    for action in ("opened", "closed") if event == "pull_request" else ("",):
                        expected = expected_results(mode, event, ref, connect, action, rehearse_shards)
                        context = {"github.event_name": event, "github.ref": ref, "github.event.action": action,
                                   "needs.classify-changes.outputs.mode": mode, "needs.classify-changes.outputs.connect": str(connect).lower(),
                                   "needs.classify-changes.outputs.rehearse_shards": str(rehearse_shards).lower(),
                                   "success": True}
                        evaluated, evaluating = {}, set()

                        def scheduled(job):
                            if job in evaluated:
                                return evaluated[job]
                            if job in evaluating or job not in jobs:
                                raise ValueError(f"cyclic or unknown job dependency: {job}")
                            evaluating.add(job)
                            block = jobs[job]
                            dependencies = block.get("needs", [])
                            if isinstance(dependencies, str):
                                dependencies = [dependencies]
                            dependency_results = [scheduled(dependency) for dependency in dependencies]
                            success = all(dependency_results)
                            expression = block.get("if", "success()")
                            unquoted = re.sub(r"'(?:[^']|'')*'", "", expression)
                            status_function = re.search(r"\b(always|success|failure|cancelled)\s*\(", unquoted)
                            actual = condition(expression, dict(context, success=success))
                            evaluated[job] = actual and (bool(status_function) or success)
                            evaluating.remove(job)
                            return evaluated[job]

                        for job in required | {"required-checks"}:
                            should_run = expected.get(job, "success") == "success" if job != "required-checks" else action != "closed"
                            try:
                                actual = scheduled(job)
                            except (KeyError, TypeError, ValueError) as error:
                                errors.append(f"{job}: {error}")
                                continue
                            if actual != should_run:
                                errors.append(f"{job} condition differs from plan for {event}/{mode}/{connect}/{action}/{ref}")
                        for status in ("failure", "cancelled"):
                            context.update(success=False, failure=status == "failure", cancelled=status == "cancelled")
                            try:
                                if condition(gate.get("if", "true"), context) != (action != "closed") or not re.search(r"\balways\s*\(", re.sub(r"'(?:[^']|'')*'", "", gate.get("if", ""))):
                                    errors.append("required-checks must run after failed or skipped dependencies")
                            except (TypeError, ValueError) as error:
                                errors.append(str(error))
    return sorted(set(errors))


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        suite = unittest.defaultTestLoader.discover(str(ROOT / "scripts"), pattern="test_ci_plan.py")
        return not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    if sys.argv[1:] == ["select"]:
        try:
            plan = select_plan(os.environ.get("GITHUB_EVENT_NAME", ""), os.environ.get("CI_BASE_SHA", ""),
                               os.environ.get("GITHUB_SHA", ""), os.environ.get("GITHUB_REF", ""),
                               Path.cwd(), os.environ.get("CI_EVENT_ACTION", ""), os.environ.get("CI_REHEARSE_SHARDS", "false") == "true")
            with open(os.environ["GITHUB_OUTPUT"], "a") as output:
                for name, value in (("mode", plan["mode"]), ("plan", json.dumps(plan, separators=(",", ":"))),
                                    ("connect", str(plan["connect"]).lower()), ("packages", json.dumps(plan["packages"], separators=(",", ":"))),
                                    ("full_validation", str(plan["full_validation"]).lower()),
                                    ("rehearse_shards", str(plan["rehearse_shards"]).lower())):
                    output.write(f"{name}={value}\n")
            print(json.dumps(plan, indent=2))
            if os.environ.get("GITHUB_STEP_SUMMARY"):
                with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
                    summary.write("### CI validation plan\n\n```json\n" + json.dumps(plan, indent=2) + "\n```\n")
            return 0
        except (OSError, subprocess.CalledProcessError, UnicodeError, KeyError, TypeError, ValueError) as error:
            print(f"ERROR: cannot safely plan CI: {error}", file=sys.stderr)
            return 1
    if sys.argv[1:] == ["verify"]:
        try:
            head = os.environ.get("GITHUB_SHA", "")
            tree = git(Path.cwd(), "rev-parse", f"{head}^{{tree}}").strip() if SHA.fullmatch(head) else ""
            event = os.environ.get("GITHUB_EVENT_NAME", "")
            base, action = None, None
            if os.environ.get("GITHUB_EVENT_PATH"):
                payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
                base = (payload["pull_request"]["base"]["sha"] if event == "pull_request" else
                        payload["merge_group"]["base_sha"] if event == "merge_group" else payload.get("before", ""))
                action = payload.get("action", "")
            errors = verify_results(json.loads(os.environ["CI_NEEDS"]), event, os.environ.get("GITHUB_REF", ""), head, tree, base, action)
        except (OSError, KeyError, TypeError, ValueError, subprocess.CalledProcessError) as error:
            errors = [str(error)]
    elif sys.argv[1:] == ["--check-workflow"]:
        errors = check_workflow(ROOT)
    else:
        print("usage: ci_gate.py {select|verify|--check-workflow|--self-test}", file=sys.stderr)
        return 2
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if not errors:
        print("CI contract passed")
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
