#!/bin/sh
# Tests for scripts/python_bin.sh: the interpreter resolver that keeps
# `make pre-commit` working on machines whose `python3` is older than 3.11
# (notably macOS, which ships /usr/bin/python3 at 3.9).

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RESOLVER="$REPO_ROOT/scripts/python_bin.sh"
TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT HUP INT TERM

fail() {
    echo "❌ $1" >&2
    exit 1
}

# A resolved interpreter must be able to import tomllib, which is the module
# the repository's checkers need and the reason the resolver exists.
"$RESOLVER" -c 'import tomllib, sys; assert sys.version_info >= (3, 11)' \
    || fail "resolver produced an interpreter without tomllib"

"$RESOLVER" --print-interpreter >/dev/null \
    || fail "--print-interpreter failed"

# An explicit RUSTFS_PYTHON override wins over PATH discovery.
selected=$("$RESOLVER" --print-interpreter)
RUSTFS_PYTHON="$selected" "$RESOLVER" -c 'import tomllib' \
    || fail "RUSTFS_PYTHON override rejected a valid interpreter"

# A too-old RUSTFS_PYTHON must fail loudly instead of silently falling back to
# a newer interpreter, so the operator learns their override is unusable.
mkdir -p "$TMP_ROOT/bin"
cat > "$TMP_ROOT/bin/fake-old-python" <<'STUB'
#!/bin/sh
# Pretends to be Python 3.9: the version probe exits non-zero.
exit 1
STUB
chmod +x "$TMP_ROOT/bin/fake-old-python"

if RUSTFS_PYTHON="$TMP_ROOT/bin/fake-old-python" "$RESOLVER" -c 'pass' \
    >"$TMP_ROOT/old.out" 2>"$TMP_ROOT/old.err"; then
    fail "resolver accepted a too-old RUSTFS_PYTHON"
fi
grep -q 'older than Python' "$TMP_ROOT/old.err" \
    || fail "too-old RUSTFS_PYTHON did not explain the version requirement"

# A missing RUSTFS_PYTHON must be reported as such.
if RUSTFS_PYTHON="$TMP_ROOT/bin/definitely-absent" "$RESOLVER" -c 'pass' \
    >"$TMP_ROOT/absent.out" 2>"$TMP_ROOT/absent.err"; then
    fail "resolver accepted a nonexistent RUSTFS_PYTHON"
fi
grep -q 'is not an executable command' "$TMP_ROOT/absent.err" \
    || fail "nonexistent RUSTFS_PYTHON did not explain what was wrong"

# With no usable interpreter and no uv on PATH, the failure must name the fix
# rather than surfacing a bare ModuleNotFoundError from a 3.9 interpreter.
cat > "$TMP_ROOT/bin/python3" <<'STUB'
#!/bin/sh
exit 1
STUB
chmod +x "$TMP_ROOT/bin/python3"

SANDBOX_PATH="$TMP_ROOT/bin:/usr/bin:/bin"
if PATH="$SANDBOX_PATH" command -v uv >/dev/null 2>&1; then
    # uv is reachable even from the sandbox PATH, so the resolver would
    # legitimately fall back to it instead of failing. Skip this case.
    echo "ℹ️  uv is on the sandbox PATH; skipping the no-interpreter case"
else
    if PATH="$SANDBOX_PATH" "$RESOLVER" -c 'pass' \
        >"$TMP_ROOT/none.out" 2>"$TMP_ROOT/none.err"; then
        fail "resolver succeeded with no usable interpreter on PATH"
    fi
    grep -q 'No Python 3.11+ interpreter found' "$TMP_ROOT/none.err" \
        || fail "missing-interpreter failure did not name the requirement"
    grep -q 'RUSTFS_PYTHON=' "$TMP_ROOT/none.err" \
        || fail "missing-interpreter failure did not point at the override"
fi

echo "✅ scripts/python_bin.sh resolver checks passed"
