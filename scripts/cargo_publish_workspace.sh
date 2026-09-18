#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: scripts/cargo_publish_workspace.sh [options] [-- extra cargo publish args]

Generate and optionally execute the RustFS workspace cargo publish order.
The test-only e2e_test package is always excluded.

Options:
  --mode plan|dry-run|publish  Action to perform. Defaults to plan.
  --registry NAME             Registry to check and publish to. Defaults to crates-io.
  --wait-seconds N            Delay between successful publishes. Defaults to 60.
  --allow-dirty               Allow a dirty git worktree and pass --allow-dirty to cargo publish.
  --only PACKAGE              Publish only PACKAGE and its workspace dependencies. Repeatable.
  --exclude PACKAGE           Exclude PACKAGE from the plan. Repeatable.
  -h, --help                  Show this help.

Publish mode requires RUSTFS_CARGO_PUBLISH_CONFIRM=publish.
USAGE
}

mode="plan"
registry="crates-io"
wait_seconds="60"
allow_dirty=0
only_packages=()
exclude_packages=()
extra_cargo_args=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            [[ $# -ge 2 ]] || { echo "--mode requires a value" >&2; exit 2; }
            mode="$2"
            shift 2
            ;;
        --registry)
            [[ $# -ge 2 ]] || { echo "--registry requires a value" >&2; exit 2; }
            registry="$2"
            shift 2
            ;;
        --wait-seconds)
            [[ $# -ge 2 ]] || { echo "--wait-seconds requires a value" >&2; exit 2; }
            wait_seconds="$2"
            shift 2
            ;;
        --allow-dirty)
            allow_dirty=1
            shift
            ;;
        --only)
            [[ $# -ge 2 ]] || { echo "--only requires a package name" >&2; exit 2; }
            only_packages+=("$2")
            shift 2
            ;;
        --exclude)
            [[ $# -ge 2 ]] || { echo "--exclude requires a package name" >&2; exit 2; }
            exclude_packages+=("$2")
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            extra_cargo_args+=("$@")
            break
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

case "$mode" in
    plan|dry-run|publish) ;;
    *)
        echo "--mode must be one of: plan, dry-run, publish" >&2
        exit 2
        ;;
esac

if ! [[ "$wait_seconds" =~ ^[0-9]+$ ]]; then
    echo "--wait-seconds must be a non-negative integer" >&2
    exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "$repo_root"

if [[ "$allow_dirty" -eq 0 ]] && [[ -n "$(git status --porcelain)" ]]; then
    echo "worktree is dirty; commit, stash, or pass --allow-dirty when this is intentional" >&2
    exit 1
fi

if [[ "$mode" == "publish" && "${RUSTFS_CARGO_PUBLISH_CONFIRM:-}" != "publish" ]]; then
    echo "publish mode requires RUSTFS_CARGO_PUBLISH_CONFIRM=publish" >&2
    exit 2
fi

order_args=()
if [[ "${#only_packages[@]}" -gt 0 ]]; then
    for package in "${only_packages[@]}"; do
        order_args+=(--only "$package")
    done
fi
if [[ "${#exclude_packages[@]}" -gt 0 ]]; then
    for package in "${exclude_packages[@]}"; do
        order_args+=(--exclude "$package")
    done
fi

publish_plan_file="$(mktemp "${TMPDIR:-/tmp}/rustfs-cargo-publish-plan.XXXXXX")"
trap 'rm -f "$publish_plan_file"' EXIT

python_args=(-)
if [[ "${#order_args[@]}" -gt 0 ]]; then
    python_args+=("${order_args[@]}")
fi

python3 "${python_args[@]}" >"$publish_plan_file" <<'PY'
import argparse
import collections
import json
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--only", action="append", default=[])
parser.add_argument("--exclude", action="append", default=[])
args = parser.parse_args()
default_excluded = {"e2e_test"}

metadata = json.loads(
    subprocess.check_output(["cargo", "metadata", "--format-version", "1", "--no-deps"])
)
workspace_members = set(metadata["workspace_members"])
workspace_root = metadata["workspace_root"] + "/"
packages_by_id = {
    package["id"]: package
    for package in metadata["packages"]
    if package["id"] in workspace_members
}
packages_by_name = {package["name"]: package for package in packages_by_id.values()}

unknown = sorted((set(args.only) | set(args.exclude)) - set(packages_by_name))
if unknown:
    print(f"unknown workspace package(s): {', '.join(unknown)}", file=sys.stderr)
    sys.exit(2)

publishable = {
    name
    for name, package in packages_by_name.items()
    if package.get("publish") != []
}

if args.only:
    selected = set()
    visiting = set()

    def include_with_dependencies(name):
        if name in selected:
            return
        if name in visiting:
            print(f"dependency cycle while selecting {name}", file=sys.stderr)
            sys.exit(1)
        visiting.add(name)
        package = packages_by_name[name]
        for dependency in package["dependencies"]:
            if dependency.get("source") is None and dependency["name"] in packages_by_name:
                include_with_dependencies(dependency["name"])
        visiting.remove(name)
        selected.add(name)

    for name in args.only:
        include_with_dependencies(name)
else:
    selected = set(publishable)

selected -= default_excluded
selected -= set(args.exclude)
selected &= publishable

for name in sorted(selected):
    package = packages_by_name[name]
    for dependency in package["dependencies"]:
        dependency_name = dependency["name"]
        if dependency.get("source") is None and dependency_name in packages_by_name:
            if dependency_name not in publishable:
                print(
                    f"{name} depends on non-publishable workspace package {dependency_name}",
                    file=sys.stderr,
                )
                sys.exit(1)
            if dependency_name not in selected:
                print(
                    f"{name} depends on excluded workspace package {dependency_name}",
                    file=sys.stderr,
                )
                sys.exit(1)

edges = collections.defaultdict(set)
indegree = {name: 0 for name in selected}
for name in selected:
    package = packages_by_name[name]
    for dependency in package["dependencies"]:
        dependency_name = dependency["name"]
        if dependency.get("source") is None and dependency_name in selected:
            if name not in edges[dependency_name]:
                edges[dependency_name].add(name)
                indegree[name] += 1

ready = collections.deque(sorted(name for name, degree in indegree.items() if degree == 0))
ordered = []
while ready:
    name = ready.popleft()
    ordered.append(name)
    for dependent in sorted(edges[name]):
        indegree[dependent] -= 1
        if indegree[dependent] == 0:
            ready.append(dependent)

if len(ordered) != len(selected):
    remaining = ", ".join(sorted(name for name, degree in indegree.items() if degree > 0))
    print(f"workspace dependency cycle blocks publish order: {remaining}", file=sys.stderr)
    sys.exit(1)

for name in ordered:
    package = packages_by_name[name]
    manifest_path = package["manifest_path"].removeprefix(workspace_root)
    print(f"{name}\t{package['version']}\t{manifest_path}")
PY

publish_plan=()
while IFS= read -r line; do
    publish_plan+=("$line")
done <"$publish_plan_file"

if [[ "${#publish_plan[@]}" -eq 0 ]]; then
    echo "no publishable workspace packages selected" >&2
    exit 1
fi

printf 'Cargo publish plan (%s package(s)):\n' "${#publish_plan[@]}"
index=1
for entry in "${publish_plan[@]}"; do
    IFS=$'\t' read -r package version manifest_path <<<"$entry"
    printf '%02d. %s %s (%s)\n' "$index" "$package" "$version" "$manifest_path"
    index=$((index + 1))
done

if [[ "$mode" == "plan" ]]; then
    exit 0
fi

registry_display="$registry"

crate_version_exists() {
    local package="$1"
    local version="$2"

    if [[ "$registry" == "crates-io" ]]; then
        python3 - "$package" "$version" <<'PY'
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

package = sys.argv[1]
version = sys.argv[2]
url = "https://crates.io/api/v1/crates/{}/{}".format(
    urllib.parse.quote(package, safe=""),
    urllib.parse.quote(version, safe=""),
)
request = urllib.request.Request(
    url,
    headers={"User-Agent": "rustfs-cargo-publish-workspace"},
)

try:
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
except urllib.error.HTTPError as exc:
    if exc.code == 404:
        sys.exit(1)
    print(f"failed to verify {package} {version} on crates-io: HTTP {exc.code}", file=sys.stderr)
    sys.exit(2)
except Exception as exc:
    print(f"failed to verify {package} {version} on crates-io: {exc}", file=sys.stderr)
    sys.exit(2)

crate = payload.get("version", {}).get("crate")
num = payload.get("version", {}).get("num")
if crate == package and num == version:
    sys.exit(0)

print(f"unexpected crates-io response while verifying {package} {version}", file=sys.stderr)
sys.exit(2)
PY
        return "$?"
    fi

    local output
    if output="$(cargo info "$package@$version" --registry "$registry" 2>&1)"; then
        return 0
    fi

    if [[ "$output" == *"could not find"* || "$output" == *"no matching package"* || "$output" == *"failed to find"* ]]; then
        return 1
    fi

    printf '%s\n' "$output" >&2
    return 2
}

cargo_publish_base=(cargo publish --registry "$registry")
if [[ "$allow_dirty" -eq 1 ]]; then
    cargo_publish_base+=(--allow-dirty)
fi

index=1
total="${#publish_plan[@]}"
for entry in "${publish_plan[@]}"; do
    IFS=$'\t' read -r package version manifest_path <<<"$entry"
    printf '\n[%s/%s] %s %s (%s)\n' "$index" "$total" "$package" "$version" "$registry_display"

    set +e
    crate_version_exists "$package" "$version"
    exists_status="$?"
    set -e

    case "$exists_status" in
        0)
            printf 'skip: %s %s already exists on %s\n' "$package" "$version" "$registry_display"
            index=$((index + 1))
            continue
            ;;
        1)
            printf 'publish check: %s %s is not present on %s\n' "$package" "$version" "$registry_display"
            ;;
        *)
            printf 'error: could not verify whether %s %s exists on %s\n' "$package" "$version" "$registry_display" >&2
            exit 1
            ;;
    esac

    if [[ "$mode" == "dry-run" ]]; then
        cargo_publish_cmd=("${cargo_publish_base[@]}" -p "$package" --dry-run)
    else
        cargo_publish_cmd=("${cargo_publish_base[@]}" -p "$package")
    fi

    if [[ "${#extra_cargo_args[@]}" -gt 0 ]]; then
        cargo_publish_cmd+=("${extra_cargo_args[@]}")
    fi

    "${cargo_publish_cmd[@]}"

    if [[ "$mode" == "publish" ]]; then
        if [[ "$index" -lt "$total" && "$wait_seconds" -gt 0 ]]; then
            sleep "$wait_seconds"
        fi
    fi

    index=$((index + 1))
done
