# RustFS Environment Variables

This document describes the environment variables that can be used to configure RustFS behavior.

## Background Services Control

### RUSTFS_ENABLE_SCANNER

Controls whether the data scanner service should be started.

- **Default**: `true`
- **Valid values**: `true`, `false`
- **Description**: When enabled, the data scanner will run background scans to detect inconsistencies and corruption in stored data.

**Examples**:
```bash
# Disable scanner
export RUSTFS_ENABLE_SCANNER=false

# Enable scanner (default behavior)
export RUSTFS_ENABLE_SCANNER=true
```

### RUSTFS_ENABLE_HEAL

Controls whether the auto-heal service should be started.

- **Default**: `true`
- **Valid values**: `true`, `false` 
- **Description**: When enabled, the heal manager will automatically repair detected data inconsistencies and corruption.

**Examples**:
```bash
# Disable auto-heal
export RUSTFS_ENABLE_HEAL=false

# Enable auto-heal (default behavior)
export RUSTFS_ENABLE_HEAL=true
```

### RUSTFS_ENABLE_LOCKS

Controls whether the distributed lock system should be enabled.

- **Default**: `true`
- **Valid values**: `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`, `enabled`, `disabled` (case insensitive)
- **Description**: When enabled, provides distributed locking for concurrent object operations. When disabled, all lock operations immediately return success without actual locking.

**Examples**:
```bash
# Disable lock system
export RUSTFS_ENABLE_LOCKS=false

# Enable lock system (default behavior)
export RUSTFS_ENABLE_LOCKS=true
```

## Service Combinations

The scanner and heal services can be independently controlled:

| RUSTFS_ENABLE_SCANNER | RUSTFS_ENABLE_HEAL | Result |
|----------------------|-------------------|--------|
| `true` (default)     | `true` (default)  | Both scanner and heal are active |
| `true`               | `false`           | Scanner runs without heal capabilities |
| `false`              | `true`            | Heal manager is available but no scanning |
| `false`              | `false`           | No background maintenance services |

## Use Cases

### Development Environment
For development or testing environments where you don't need background maintenance:
```bash
export RUSTFS_ENABLE_SCANNER=false
export RUSTFS_ENABLE_HEAL=false
./rustfs --address 127.0.0.1:9000 ...
```

### Scan-Only Mode
For environments where you want to detect issues but not automatically fix them:
```bash
export RUSTFS_ENABLE_SCANNER=true
export RUSTFS_ENABLE_HEAL=false
./rustfs --address 127.0.0.1:9000 ...
```

### Heal-Only Mode  
For environments where external tools trigger healing but no automatic scanning:
```bash
export RUSTFS_ENABLE_SCANNER=false
export RUSTFS_ENABLE_HEAL=true
./rustfs --address 127.0.0.1:9000 ...
```

### Production Environment (Default)
For production environments where both services should be active:
```bash
# These are the defaults, so no need to set explicitly
# export RUSTFS_ENABLE_SCANNER=true
# export RUSTFS_ENABLE_HEAL=true
./rustfs --address 127.0.0.1:9000 ...
```

### No-Lock Development
For single-node development where locking is not needed:
```bash
export RUSTFS_ENABLE_LOCKS=false
./rustfs --address 127.0.0.1:9000 ...
```

## Protocol Servers

### RUSTFS_FTPS_ENABLE

Controls whether the FTPS (FTP over TLS) server should be started.

- **Default**: `false`
- **Valid values**: `true`, `false`
- **Description**: When enabled, starts an FTPS server for secure file transfers over TLS.

### RUSTFS_FTPS_ADDRESS

FTPS server bind address.

- **Default**: `0.0.0.0:21`
- **Valid values**: Valid IP:PORT combination
- **Description**: The address and port where the FTPS server will listen for connections.

### RUSTFS_FTPS_CERTS_FILE

Path to FTPS server TLS certificate file.

- **Default**: None (required when FTPS is enabled)
- **Valid values**: Path to a PEM-encoded certificate file
- **Description**: TLS certificate used for securing FTPS connections.

### RUSTFS_FTPS_KEY_FILE

Path to FTPS server TLS private key file.

- **Default**: None (required when FTPS is enabled)
- **Valid values**: Path to a PEM-encoded private key file
- **Description**: TLS private key corresponding to the certificate.

### RUSTFS_FTPS_PASSIVE_PORTS

Passive port range for FTPS data connections.

- **Default**: None (system-assigned ports)
- **Valid values**: Port range in format "START-END" (e.g., "40000-50000")
- **Description**: Range of ports for FTPS passive mode data connections.

### RUSTFS_FTPS_EXTERNAL_IP

External IP address for FTPS passive mode.

- **Default**: None (auto-detected)
- **Valid values**: Valid IP address
- **Description**: External IP address advertised to FTPS clients for passive mode, useful for NAT setups.

### RUSTFS_SFTP_ENABLE

Controls whether the SFTP (SSH File Transfer Protocol) server should be started.

- **Default**: `false`
- **Valid values**: `true`, `false`
- **Description**: When enabled, starts an SFTP server for secure file transfers over SSH.

### RUSTFS_SFTP_ADDRESS

SFTP server bind address.

- **Default**: `0.0.0.0:22`
- **Valid values**: Valid IP:PORT combination
- **Description**: The address and port where the SFTP server will listen for connections.

### RUSTFS_SFTP_HOST_KEY

Path to SFTP server SSH host key file.

- **Default**: None (required when SFTP is enabled)
- **Valid values**: Path to an SSH host key file
- **Description**: SSH host key used for server identification.

### RUSTFS_SFTP_AUTHORIZED_KEYS

Path to SFTP authorized keys file.

- **Default**: None (required when SFTP is enabled)
- **Valid values**: Path to a file containing OpenSSH public keys
- **Description**: File containing authorized SSH public keys for client authentication.

## Performance Impact

- **Scanner**: Light to moderate CPU/IO impact during scans
- **Heal**: Moderate to high CPU/IO impact during healing operations
- **Locks**: Minimal CPU/memory overhead for coordination; disabling can improve throughput in single-client scenarios
- **Memory**: Each service uses additional memory for processing queues and metadata
- **FTPS**: Moderate CPU/memory overhead for TLS operations and connection management
- **SFTP**: Moderate CPU/memory overhead for SSH operations and key management

Disabling these services in resource-constrained environments can improve performance for primary storage operations.