# Protocol E2E Tests

FTPS and SFTP protocol end-to-end tests for RustFS.

## Prerequisites

### Required Tools

```bash
# Ubuntu/Debian
sudo apt-get install sshpass ssh-keygen

# RHEL/CentOS
sudo yum install sshpass openssh-clients

# macOS
brew install sshpass openssh
```

## Running Tests

Run all protocol tests:
```bash
cargo test --package e2e_test test_protocol_core_suite -- --test-threads=1 --nocapture
```

Run only FTPS tests:
```bash
cargo test --package e2e_test test_ftps_core_operations -- --test-threads=1 --nocapture
```

## Test Coverage

### FTPS Tests
- mkdir bucket
- cd to bucket
- put file
- ls list objects
- cd . (stay in current directory)
- cd / (return to root)
- cd nonexistent bucket (should fail)
- delete object
- cdup
- rmdir delete bucket