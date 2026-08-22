# RustFS Policy Variables Tests

This directory contains comprehensive end-to-end tests for AWS IAM policy variables in RustFS.

## Test Overview

The tests cover the following AWS policy variable scenarios:

1. **Single-value variables** - Basic variable resolution like `${aws:username}`
2. **Multi-value variables** - Variables that can have multiple values
3. **Variable concatenation** - Combining variables with static text like `prefix-${aws:username}-suffix`
4. **Nested variables** - Complex nested variable patterns like `${${aws:username}-test}`
5. **Deny scenarios** - Testing deny policies with variables
6. **STS credentials** - Variable resolution inherited by temporary credentials

## Prerequisites

- `awscurl` utility for admin API calls
- AWS SDK for Rust (included in the project)

## Running Tests

### Run All Policy Tests

```bash
# From the project root directory
cargo test -p e2e_test policy:: -- --nocapture
```

Each test starts an isolated RustFS server on a dynamically allocated local port and cleans it up afterward.
