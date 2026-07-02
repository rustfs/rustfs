# S3 Compatibility Tests Configuration

This directory contains the configuration template for running
[Ceph S3 compatibility tests](https://github.com/ceph/s3-tests) against RustFS.

The test runner itself lives in [`scripts/s3-tests/`](../../scripts/s3-tests/)
— see its README for how tests are selected, executed, and reported. This
directory only holds the shared config template.

## Configuration File

`s3tests.conf` is based on the official `s3tests.conf.SAMPLE` from the
ceph/s3-tests repository. It is a template: `scripts/s3-tests/run.sh` renders
it with `envsubst` before every run.

Required environment variables (all have defaults in `run.sh`):

| Variable | Meaning |
|----------|---------|
| `S3_HOST` | RustFS endpoint host |
| `S3_PORT` | RustFS endpoint port |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | main user credentials |
| `S3_ALT_ACCESS_KEY` / `S3_ALT_SECRET_KEY` | alt user credentials (must differ from main) |

TLS is disabled (`is_secure = False`).

## Test Selection Strategy

Tests are selected by exact pytest node ids from classification list files in
`scripts/s3-tests/` — **not** by pytest markers:

- `implemented_tests.txt` — expected to pass; run as the per-PR gate
  (`ci.yml`, job `s3-implemented-tests`)
- `unimplemented_tests.txt` — standard S3 features not yet implemented
- `excluded_tests.txt` — intentionally excluded (vendor-specific behavior or
  product decisions)

The upstream s3-tests repository is pinned to a fixed commit (`S3TESTS_REV` in
`run.sh`) so results are reproducible; bump it deliberately and reclassify the
lists when upstream changes.

## Where Tests Run

- **Per PR**: `.github/workflows/ci.yml` job `s3-implemented-tests` runs the
  implemented whitelist against a single-node debug binary. Any failure blocks
  the PR.
- **Weekly + manual**: `.github/workflows/e2e-s3tests.yml` runs the full
  upstream suite (`TEST_SCOPE=all`) against a Docker deployment (single node
  or a 4-node distributed cluster behind HAProxy). It fails only on
  regressions in the implemented whitelist and publishes a classification
  report (`compat-report.md`, also shown in the job summary) listing promotion
  candidates and unclassified tests.

## Running Tests Locally

```bash
# Whitelist run against a locally built binary (the same thing the PR gate does)
./scripts/s3-tests/run.sh

# Full sweep, never stop on failures, 4 pytest workers
TEST_SCOPE=all MAXFAIL=0 XDIST=4 ./scripts/s3-tests/run.sh

# A specific test against an already-running server
DEPLOY_MODE=existing TESTEXPR="test_bucket_list_empty" ./scripts/s3-tests/run.sh
```

Results land in `artifacts/s3tests-single/` (`junit.xml`, `pytest.log`,
`compat-report.md`).

## Adding New Feature Support

Follow [`scripts/s3-tests/S3_COMPAT_WORKFLOW.md`](../../scripts/s3-tests/S3_COMPAT_WORKFLOW.md).
In short: fix the behavior, verify with `TESTEXPR=...`, then move the test
from `unimplemented_tests.txt` to `implemented_tests.txt` so the PR gate locks
it in.

## References

- [Ceph S3 Tests Repository](https://github.com/ceph/s3-tests)
- [S3 API Compatibility](https://docs.aws.amazon.com/AmazonS3/latest/API/)
- [RustFS test runner](../../scripts/s3-tests/README.md)
