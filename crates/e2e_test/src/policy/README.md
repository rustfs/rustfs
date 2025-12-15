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
cargo test -p e2e_test policy::test_runner::test_policy_full_suite -- --ignored --nocapture --test-threads=1

# Run only critical policy tests
cargo test -p e2e_test policy::test_runner::test_policy_critical_suite -- --ignored --nocapture --test-threads=1
```

### Run All Policy Tests

```bash
# From the project root directory
cargo test -p e2e_test policy:: --ignored --nocapture --test-threads=1
```

### Run Individual Tests

```bash
# Single-value variables test
cargo test -p e2e_test policy::policy_variables_test::test_aws_policy_variables_single_value -- --ignored --nocapture --test-threads=1

# Multi-value variables test
cargo test -p e2e_test policy::policy_variables_test::test_aws_policy_variables_multi_value -- --ignored --nocapture --test-threads=1

# Variable concatenation test
cargo test -p e2e_test policy::policy_variables_test::test_aws_policy_variables_concatenation -- --ignored --nocapture --test-threads=1

# Nested variables test
cargo test -p e2e_test policy::policy_variables_test::test_aws_policy_variables_nested -- --ignored --nocapture --test-threads=1

# Deny scenarios test
cargo test -p e2e_test policy::policy_variables_test::test_aws_policy_variables_deny -- --ignored --nocapture --test-threads=1
```

## Test Details

### Single-Value Variables Test

Tests basic AWS policy variables:
- `${aws:username}` - Resolves to the authenticated user's name
- `${aws:userid}` - Resolves to the authenticated user's ID
- Policy allows actions on resources matching the pattern `arn:aws:s3:::${aws:username}-*`

### Multi-Value Variables Test

Tests policies with multiple resource patterns:
- User can create/list buckets matching any of the specified patterns
- User cannot create buckets that don't match any pattern

### Variable Concatenation Test

Tests complex variable patterns:
- Pattern: `arn:aws:s3:::prefix-${aws:username}-suffix`
- Verifies variable resolution within concatenated strings

### Nested Variables Test

Tests complex nested variable patterns:
- Pattern: `arn:aws:s3:::${${aws:username}-test}`
- This is a complex scenario that tests the variable resolver's ability to handle nested expressions

### Deny Scenarios Test

Tests deny policies with variables:
- Allow general access with variables
- Deny specific actions based on resource patterns
- Verifies that deny policies take precedence over allow policies

## Manual Testing with awscurl

You can also manually test policy variables using `awscurl`:

### 1. Start RustFS Server

```bash
./target/release/rustfs --address 127.0.0.1:9000 /tmp/rustfs-test-data
```

### 2. Create a Test User

```bash
awscurl -X PUT --service s3 --region us-east-1 \
  --access_key minioadmin --secret_key minioadmin \
  -d '{"secretKey":"testpassword123","status":"enabled"}' \
  -H "Content-Type: application/json" \
  "http://127.0.0.1:9000/rustfs/admin/v3/add-user?accessKey=testuser"
```

### 3. Create a Policy with Variables

```bash
awscurl -X PUT --service s3 --region us-east-1 \
  --access_key minioadmin --secret_key minioadmin \
  -d '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListAllMyBuckets"],"Resource":["arn:aws:s3:::*"]},{"Effect":"Allow","Action":["s3:CreateBucket"],"Resource":["arn:aws:s3:::${aws:username}-*"]}]}' \
  -H "Content-Type: application/json" \
  "http://127.0.0.1:9000/rustfs/admin/v3/add-canned-policy?name=test-policy"
```

### 4. Attach Policy to User

```bash
awscurl -X PUT --service s3 --region us-east-1 \
  --access_key minioadmin --secret_key minioadmin \
  "http://127.0.0.1:9000/rustfs/admin/v3/set-user-or-group-policy?policyName=test-policy&userOrGroup=testuser&isGroup=false"
```

### 5. Test with AWS SDK

Use the AWS SDK with the test user credentials to verify policy behavior:

```bash
# List buckets (should work)
aws s3 --endpoint-url http://127.0.0.1:9000 ls

# Create bucket matching pattern (should work)
aws s3 --endpoint-url http://127.0.0.1:9000 mb s3://testuser-mybucket

# Create bucket not matching pattern (should fail)
aws s3 --endpoint-url http://127.0.0.1:9000 mb s3://otheruser-mybucket
```

### 6. Cleanup

```bash
# Remove policy
awscurl -X DELETE --service s3 --region us-east-1 \
  --access_key minioadmin --secret_key minioadmin \
  "http://127.0.0.1:9000/rustfs/admin/v3/remove-canned-policy?name=test-policy"

# Remove user
awscurl -X DELETE --service s3 --region us-east-1 \
  --access_key minioadmin --secret_key minioadmin \
  "http://127.0.0.1:9000/rustfs/admin/v3/remove-user?accessKey=testuser"
```

## Test Output

The tests will output detailed logs showing:
- Test execution steps
- Policy evaluation results
- Variable resolution details
- Success/failure status for each test case

## Troubleshooting

If tests fail, check:
1. Ensure RustFS server is properly built
2. Verify `awscurl` is installed and in PATH
3. Check that no other RustFS instances are running on the same port
4. Review test logs for specific error messages
5. Ensure sufficient system resources (tests can be resource-intensive)
6. Try running with `--test-threads=1` to avoid port conflicts