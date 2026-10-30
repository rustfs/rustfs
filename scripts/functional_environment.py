#!/usr/bin/env python3
"""Quarantine shared fleet nodes until the owning attempt completes cleanup.

GitHub workflow concurrency owns mutual exclusion. Durable node markers keep
cancellation, runner loss and failed cleanup from making that environment
available to the next lock holder. Recovery must inspect the retained receipt,
clean the nodes under the same resource lock, then recover using the original
run/attempt/suite identity. Recovery also handles partially admitted fleets.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

from validation_registry import load_registry, require

# Execute the same small state transition on every node, without relying on
# runner-local storage surviving a job or being shared by different runners.
REMOTE = r'''
import base64,json,os,pathlib,stat,sys
mode,encoded=sys.argv[1:]
identity=json.loads(base64.b64decode(encoded))
root=pathlib.Path.home()/'.rustfs-functional-environment'
root.mkdir(mode=0o700,exist_ok=True)
info=root.lstat()
if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
    raise ValueError('unsafe environment quarantine directory')
path=root/'active.json'
if mode=='check':
    if path.exists() or path.is_symlink():
        raise ValueError('environment quarantined by an unfinished attempt: '+path.read_text())
elif mode=='enter':
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as out:
        out.write(json.dumps(identity,sort_keys=True)+'\n');out.flush();os.fsync(out.fileno())
elif mode in ('recover-verify','recover-finish') and not path.exists() and not path.is_symlink():
    pass
elif mode in ('verify','finish','recover-verify','recover-finish'):
    if path.is_symlink() or json.loads(path.read_text())!=identity:
        raise ValueError('quarantine belongs to another attempt')
    if mode in ('finish','recover-finish'):
        path.unlink()
else:
    raise ValueError('unknown environment operation')
fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
'''


def identity(suite, env):
    suites = {item["id"]: item for item in load_registry()["suites"]}
    require(suite in suites, "unknown environment suite")
    run_id, attempt = env.get("GITHUB_RUN_ID", ""), env.get("GITHUB_RUN_ATTEMPT", "")
    require(re.fullmatch(r"[1-9][0-9]*", run_id) and re.fullmatch(r"[1-9][0-9]*", attempt),
            "invalid quarantine run identity")
    return {"schema": 1, "run_id": int(run_id), "attempt": int(attempt), "suite": suite,
            "environment": suites[suite]["environment"], "resource_lock": suites[suite]["resource_lock"]}


def remote(node, user, mode, value):
    encoded = base64.b64encode(json.dumps(value, sort_keys=True).encode()).decode()
    command = shlex.join(["python3", "-c", REMOTE, mode, encoded])
    subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o",
                    "StrictHostKeyChecking=accept-new", user + "@" + node, command],
                   check=True, capture_output=True, text=True, timeout=30)


def transition(mode, suite, env, cleanup_outcome="", execute=remote):
    value = identity(suite, env)
    nodes = (env.get("RUSTFS_NODES") or "vm000 vm001 vm002").split()
    user = env.get("RUSTFS_SSH_USER") or "azureuser"
    require(nodes and len(nodes) == len(set(nodes)) and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", node) for node in nodes)
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", user), "invalid fleet SSH identity")
    receipt = {**value, "nodes": nodes, "mode": mode, "cleanup_outcome": cleanup_outcome,
               "state": "quarantined", "completed_nodes": []}
    try:
        require(mode in ("check", "enter", "finish", "recover"), "unknown fleet transition")
        if mode in ("finish", "recover"):
            require(cleanup_outcome == "success", "cleanup incomplete; fleet remains quarantined")
        # Preflight every node before the first state mutation. Any partial
        # failure deliberately leaves admission blocked until operator recovery.
        for node in nodes:
            execute(node, user, "check" if mode in ("check", "enter") else "recover-verify" if mode == "recover" else "verify", value)
        for node in ([] if mode == "check" else nodes):
            execute(node, user, "recover-finish" if mode == "recover" else mode, value)
            receipt["completed_nodes"].append(node)
        receipt["state"] = "owned" if mode == "enter" else "clean"
        return receipt
    finally:
        target = env.get("FUNCTIONAL_ENVIRONMENT_RECEIPT")
        if target:
            Path(target).write_text(json.dumps(receipt, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("check", "enter", "finish", "recover"))
    parser.add_argument("--suite", required=True)
    parser.add_argument("--cleanup-outcome", default="")
    args = parser.parse_args()
    print(json.dumps(transition(args.mode, args.suite, os.environ, args.cleanup_outcome), sort_keys=True))


if __name__ == "__main__":
    main()
