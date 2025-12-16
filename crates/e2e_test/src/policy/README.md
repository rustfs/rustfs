# RustFS Policy Variables Tests

This directory contains comprehensive end-to-end tests for AWS IAM policy variables in RustFS.

## Test Overview

The tests cover the following AWS policy variable scenarios:

1. **Single-value variables** - Basic variable resolution like `${aws:username}`
2. **Multi-value variables** - Variables that can have multiple values
3. **Variable concatenation** - Combining variables with static text like `prefix-${aws:username}-suffix`
4. **Nested variables** - Complex nested variable patterns like `${${aws:username}-test}`
5. **Deny scenarios** - Testing deny policies with variables

## Prerequisites

- RustFS server binary
- `awscurl` utility for admin API calls
- AWS SDK for Rust (included in the project)

## Running Tests

### Run All Policy Tests Using Unified Test Runner

```bash
# Run all policy tests with comprehensive reporting
# Note: Requires a RustFS server running on localhost:9000
cargo test -p e2e_test policy::test_runner::test_policy_full_suite -- --nocapture --ignored  --test-threads=1

# Run only critical policy tests
cargo test -p e2e_test policy::test_runner::test_policy_critical_suite -- --nocapture --ignored --test-threads=1
```

### Run All Policy Tests

```bash
# From the project root directory
cargo test -p e2e_test policy:: -- --nocapture --ignored --test-threads=1
```