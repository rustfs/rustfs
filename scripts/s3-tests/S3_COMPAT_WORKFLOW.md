# S3 Compatibility Fix Workflow

Step-by-step guide for identifying and fixing S3 API compatibility issues in RustFS.

## Prerequisites

- Rust toolchain installed (`cargo`, `rustc`)
- Python 3 for running ceph s3-tests
- Access to upstream remote (`git remote add upstream https://github.com/rustfs/rustfs.git`)
- Familiarity with the [run.sh](./run.sh) test runner

## Workflow

### Step 1: Sync with upstream

```bash
git checkout main
git fetch upstream
git merge upstream/main
```

### Step 2: Select candidate tests

Pick ~20 tests from `unimplemented_tests.txt` and write them to `selected_tests.txt`:

```bash
TESTEXPR=$(scripts/s3-tests/build_testexpr.sh selected_tests.txt) \
  DEPLOY_MODE=build MAXFAIL=0 ./scripts/s3-tests/run.sh
```

Or run them directly:

```bash
TESTEXPR="test_foo or test_bar" DEPLOY_MODE=build MAXFAIL=0 ./scripts/s3-tests/run.sh
```

Review `artifacts/s3tests-single/pytest.log` for results.

### Step 3: Triage failures

From the test report, classify each failure:

| Category | Action |
|----------|--------|
| Feature not implemented (e.g., returns `501 Not Implemented`) | Skip — do not attempt |
| Incorrect HTTP status code or response body | Good candidate for a fix |
| Teardown/cleanup issue (test passes but cleanup fails) | Good candidate — usually simple |
| Complex multi-feature dependency | Defer to a later iteration |

Pick the **simplest** failure to fix. "Simplest" means: fewest code paths affected, clearest expected behavior, closest to existing implementation.

### Step 4: Deep analysis

Before writing any code:

1. **Read the test source** in `s3-tests/s3tests/functional/test_s3.py` to understand exactly what the test expects.
2. **Read the S3 API specification** for the operation being tested.
3. **Search the RustFS codebase** for the handler that serves this operation.
4. **Compare with MinIO** (`github.com/minio/minio`) — find the equivalent handler and see how it handles the same edge case. This is critical because RustFS was ported from MinIO's Go code to Rust.
5. **Identify the root cause** — is it a missing header, wrong status code, incorrect XML response, logic bug, etc.?
6. **Document your findings** before making changes.

### Step 5: Create a fix branch

```bash
git checkout main
git checkout -b fix/s3-compat-<short-description>
```

One branch per fix. Never combine unrelated fixes in a single branch.

### Step 6: Write tests first (when applicable)

If the fix involves logic changes, add unit tests before modifying the production code:

- Co-locate tests with their module (`#[cfg(test)] mod tests { ... }`)
- Use descriptive test names: `test_<operation>_<scenario>_<expected_outcome>`
- Cover both the happy path and the edge case being fixed

### Step 7: Implement the fix

Guidelines:

- **Reuse existing abstractions** — do not duplicate logic that already exists in helper functions or shared modules.
- **Follow s3s conventions** — RustFS's S3 layer is built on the `s3s` crate. Respect its traits, error types, and request/response patterns.
- **Name things clearly** — variable names, function names, and types should be self-explanatory.
- **Add comments only where intent is non-obvious** — explain *why*, not *what*.
- **Do not introduce security holes** — validate inputs, check permissions, handle errors properly.
- **Do not use `unwrap()` or `expect()` in production code** — use proper error handling with `Result` and `?`.

### Step 8: Verify the fix

Run the specific test(s) you fixed:

```bash
TESTEXPR="test_the_fixed_test" DEPLOY_MODE=build ./scripts/s3-tests/run.sh
```

Then run the full implemented test suite to confirm no regressions:

```bash
./scripts/s3-tests/run.sh
```

### Step 9: Run quality checks

```bash
make pre-commit
```

This runs:

- `cargo fmt --all --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --workspace --exclude e2e_test`

All three must pass before committing.

### Step 10: Commit and prepare PR

```bash
git add -A
git commit -m "fix(s3): <concise description of the fix>"
```

Write a PR description following `.github/pull_request_template.md`. The description must:

- Be written in English
- Use plain, natural language (no emoji, no marketing speak)
- Explain what was wrong, why, and how it was fixed
- Reference the specific s3-tests that now pass

### Step 11: Update test lists

Move the now-passing test(s) from `unimplemented_tests.txt` to `implemented_tests.txt`. Update the test count comment in `implemented_tests.txt`.

## Important Rules

1. **One branch, one fix** — never mix unrelated changes.
2. **Analyze before coding** — understand the root cause thoroughly before writing a fix.
3. **No shotgun debugging** — do not blindly try different return codes or response shapes hoping to pass the test.
4. **Every change must be justified** — if you cannot explain why a line changed, do not change it.
5. **Compare with MinIO** — when in doubt about the correct behavior, check MinIO's source code for the equivalent logic.
6. **Security first** — never skip input validation, permission checks, or error handling for the sake of passing a test.
