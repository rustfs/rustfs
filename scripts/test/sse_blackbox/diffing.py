"""Compare two finished runs: verdicts, observation vectors, and which cases exist on one side only."""

from __future__ import annotations

import json
from pathlib import Path

# Differences that a comparison between two builds is allowed to show. The list is closed: anything else fails it.
#   - the `sse_local_master_key` startup event exists only from the build that introduced it. The difference is
#     accepted only when one binary is registered as predating the event and that side recorded none; two binaries
#     that both have the event must agree on it.
ALLOWED_NOTES = {("startup-event", "sse_local_master_key")}


def _predates_event(run: dict) -> bool:
    return not run["meta"]["binary"].get("master_key_event", True)


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def load_run(run_dir: Path) -> dict:
    done = run_dir / "DONE"
    if not done.exists():
        raise SystemExit(f"{run_dir}: no DONE marker; the run did not finish and cannot be compared")
    return {
        "dir": run_dir,
        "meta": json.loads((run_dir / "run.json").read_text()),
        "results": _load_jsonl(run_dir / "results.jsonl"),
        "observations": _load_jsonl(run_dir / "observations.jsonl"),
    }


def _result_key(r: dict) -> tuple:
    return (r["config"], r["case"], r.get("row", ""))


def _observation_key(o: dict) -> tuple:
    return (o["config"], o["case"], o["kind"], o["seq"], o.get("name", ""))


def _field_differences(a: dict, b: dict) -> list[str]:
    out = []
    for name in sorted(set(a) | set(b)):
        if a.get(name) != b.get(name):
            out.append(f"{name}: {json.dumps(a.get(name), sort_keys=True)} != {json.dumps(b.get(name), sort_keys=True)}")
    return out


def compare(run_a: dict, run_b: dict) -> dict:
    """Returns {'blocking': [...], 'allowed': [...], 'compared': {...}}; an empty 'blocking' list means equal."""
    blocking: list[str] = []
    allowed: list[str] = []

    results_a = {_result_key(r): r for r in run_a["results"]}
    results_b = {_result_key(r): r for r in run_b["results"]}
    for key in sorted(set(results_a) | set(results_b)):
        label = "/".join(str(part) for part in key if part != "")
        if key not in results_a or key not in results_b:
            # A case that disappears would otherwise read as "fewer failures".
            blocking.append(f"result only in {'B' if key not in results_a else 'A'}: {label}")
        elif results_a[key]["verdict"] != results_b[key]["verdict"]:
            blocking.append(f"verdict differs: {label}: {results_a[key]['verdict']} != {results_b[key]['verdict']}")

    obs_a = {_observation_key(o): o for o in run_a["observations"]}
    obs_b = {_observation_key(o): o for o in run_b["observations"]}
    for key in sorted(set(obs_a) | set(obs_b), key=str):
        config, case, kind, seq, name = key
        label = f"{config}/{case} {kind} #{seq} {name}".rstrip()
        is_allowed = (kind, name) in ALLOWED_NOTES and any(
            _predates_event(run) and observations.get(key, {}).get("value") == []
            for run, observations in ((run_a, obs_a), (run_b, obs_b)))
        if key not in obs_a or key not in obs_b:
            side = "B" if key not in obs_a else "A"
            present = obs_b[key] if key not in obs_a else obs_a[key]
            what = present.get("request") or json.dumps(present.get("value"), sort_keys=True)
            (allowed if is_allowed else blocking).append(f"observation only in {side}: {label}: {what}")
            continue
        differences = _field_differences(obs_a[key], obs_b[key])
        if differences:
            what = obs_a[key].get("request") or obs_a[key].get("name", "")
            entry = f"observation differs: {label} [{obs_a[key].get('op', kind)} {what}]\n    " + "\n    ".join(differences)
            (allowed if is_allowed else blocking).append(entry)

    return {
        "blocking": blocking,
        "allowed": allowed,
        "compared": {
            "results": len(set(results_a) & set(results_b)),
            "observations": len(set(obs_a) & set(obs_b)),
            "exchanges": sum(1 for k in set(obs_a) & set(obs_b) if k[2] == "exchange"),
        },
    }


def render(run_a: dict, run_b: dict, outcome: dict) -> str:
    def identity(run: dict) -> str:
        binary = run["meta"]["binary"]
        return f"{binary.get('name')} (commit {binary.get('commit')}, build {binary.get('build')}, sha256 {binary.get('sha256', '')[:12]})"

    compared = outcome["compared"]
    lines = [
        f"A: {run_a['dir'].name}  {identity(run_a)}",
        f"B: {run_b['dir'].name}  {identity(run_b)}",
        f"compared: {compared['results']} verdicts, {compared['observations']} observations ({compared['exchanges']} HTTP exchanges)",
        f"blocking differences: {len(outcome['blocking'])}",
        f"allowed differences: {len(outcome['allowed'])}",
    ]
    for title, entries in (("BLOCKING", outcome["blocking"]), ("ALLOWED", outcome["allowed"])):
        if entries:
            lines.append(f"\n== {title} ==")
            lines.extend(f"- {entry}" for entry in entries)
    return "\n".join(lines) + "\n"
