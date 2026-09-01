#!/usr/bin/env bash
# Resolve a Python interpreter new enough for this repository's helper scripts
# and exec it with the given arguments.
#
# Why this exists: scripts/check_test_wiring.py and
# scripts/check_security_coverage.py import `tomllib`, which only landed in
# Python 3.11. macOS ships /usr/bin/python3 at 3.9, so a plain
# `python3 ./scripts/check_test_wiring.py` makes `make pre-commit` fail on a
# clean machine with `ModuleNotFoundError: No module named 'tomllib'` even
# though the checker itself is fine. Make targets go through this wrapper so
# the gate works wherever a new-enough interpreter (or `uv`) is reachable.
#
# CI runners already provide Python 3.11+ as `python3`, so .github/workflows
# keeps calling `python3` directly.
#
# Resolution order:
#   1. $RUSTFS_PYTHON, if set (must itself be >= the minimum version)
#   2. python3.14 / python3.13 / python3.12 / python3.11 / python3 / python
#   3. `uv run --python <spec> --no-project python`
#
# Usage:
#   ./scripts/python_bin.sh ./scripts/check_test_wiring.py --self-test
#   ./scripts/python_bin.sh --print-interpreter   # report what would be used

set -euo pipefail

MIN_MAJOR=3
MIN_MINOR=11
UV_PYTHON_SPEC="${RUSTFS_UV_PYTHON:-3.12}"

version_ok() {
    "$1" -c "import sys; raise SystemExit(0 if sys.version_info >= (${MIN_MAJOR}, ${MIN_MINOR}) else 1)" \
        >/dev/null 2>&1
}

if [ "${1:---help}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    cat <<USAGE
Usage: scripts/python_bin.sh [--print-interpreter] [python arguments...]

Execs a Python >= ${MIN_MAJOR}.${MIN_MINOR} interpreter (the repository's checkers import
tomllib) with the given arguments. Resolution order:
  1. \$RUSTFS_PYTHON, if set
  2. python3.14 / python3.13 / python3.12 / python3.11 / python3 / python
  3. uv run --python \${RUSTFS_UV_PYTHON:-3.12} --no-project python

  --print-interpreter   Print the interpreter that would be used and exit.
USAGE
    exit 0
fi

print_only=0
if [ "${1:-}" = "--print-interpreter" ]; then
    print_only=1
    shift
fi

if [ -n "${RUSTFS_PYTHON:-}" ]; then
    if ! command -v "${RUSTFS_PYTHON}" >/dev/null 2>&1; then
        echo >&2 "❌ RUSTFS_PYTHON='${RUSTFS_PYTHON}' is not an executable command."
        exit 1
    fi
    if ! version_ok "${RUSTFS_PYTHON}"; then
        echo >&2 "❌ RUSTFS_PYTHON='${RUSTFS_PYTHON}' is older than Python ${MIN_MAJOR}.${MIN_MINOR}."
        echo >&2 "   The repository's checkers import tomllib (Python ${MIN_MAJOR}.${MIN_MINOR}+)."
        exit 1
    fi
    if [ "${print_only}" = "1" ]; then
        command -v "${RUSTFS_PYTHON}"
        exit 0
    fi
    exec "${RUSTFS_PYTHON}" "$@"
fi

for candidate in python3.14 python3.13 python3.12 python3.11 python3 python; do
    if command -v "${candidate}" >/dev/null 2>&1 && version_ok "${candidate}"; then
        if [ "${print_only}" = "1" ]; then
            command -v "${candidate}"
            exit 0
        fi
        exec "${candidate}" "$@"
    fi
done

if command -v uv >/dev/null 2>&1; then
    echo >&2 "ℹ️  No Python ${MIN_MAJOR}.${MIN_MINOR}+ on PATH; using 'uv run --python ${UV_PYTHON_SPEC}'."
    if [ "${print_only}" = "1" ]; then
        echo "uv run --python ${UV_PYTHON_SPEC} --no-project python"
        exit 0
    fi
    exec uv run --python "${UV_PYTHON_SPEC}" --no-project python "$@"
fi

echo >&2 "❌ No Python ${MIN_MAJOR}.${MIN_MINOR}+ interpreter found."
echo >&2 "   The repository's checkers import tomllib, added in Python ${MIN_MAJOR}.${MIN_MINOR}."
echo >&2 "   Fix it with any of:"
echo >&2 "     brew install python@3.12          # macOS: /usr/bin/python3 is 3.9"
echo >&2 "     curl -LsSf https://astral.sh/uv/install.sh | sh   # then re-run"
echo >&2 "     make ... RUSTFS_PYTHON=/path/to/python3.12"
exit 1
