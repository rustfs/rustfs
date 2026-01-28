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
RUSTFS_BUILD_FEATURES=ftps,sftp cargo test --package e2e_test test_protocol_core_suite -- --test-threads=1 --nocapture
```

Run only FTPS tests:
```bash
RUSTFS_BUILD_FEATURES=ftps cargo test --package e2e_test test_ftps_core_operations -- --test-threads=1 --nocapture
```

Run only SFTP tests:
```bash
RUSTFS_BUILD_FEATURES=sftp cargo test --package e2e_test test_sftp_core_operations -- --test-threads=1 --nocapture
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

### SFTP Tests
- mkdir bucket
- cd to bucket
- ls / (list root directory)
- ls /. (list root directory with explicit path)
- cd . (stay in current directory)
- cd / (return to root)
- cd nonexistent bucket (handled gracefully)
- cdup
- rmdir delete bucket