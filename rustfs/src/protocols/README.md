# RustFS Protocols

RustFS provides multiple protocol interfaces for accessing object storage, including:

- **FTPS** - File Transfer Protocol over TLS for traditional file transfers
- **SFTP** - SSH File Transfer Protocol for secure file transfers with key-based authentication

## Quick Start

### Enable Protocols on Startup

```bash
# Start RustFS with FTPS enabled
export RUSTFS_FTPS_ENABLE=true
export RUSTFS_FTPS_CERTS_FILE=/path/to/cert.pem
export RUSTFS_FTPS_KEY_FILE=/path/to/key.pem
rustfs --address 0.0.0.0:9000 --access-key rustfsadmin --secret-key rustfsadmin /data

# Start RustFS with SFTP enabled
export RUSTFS_SFTP_ENABLE=true
export RUSTFS_SFTP_HOST_KEY=/path/to/host_key
export RUSTFS_SFTP_AUTHORIZED_KEYS=/path/to/authorized_keys
rustfs --address 0.0.0.0:9000 --access-key rustfsadmin --secret-key rustfsadmin /data
```

## Protocol Details

### FTPS
- **Port**: 8021 (default)
- **Protocol**: FTP over TLS (FTPS)
- **Authentication**: Access Key / Secret Key (same as S3)
- **Features**:
  - File upload/download
  - Directory listing
  - File deletion
  - Passive mode data connections
- **Limitations**:
  - Cannot create/delete buckets (use S3 API)
  - No file rename/copy operations
  - No multipart upload

### SFTP
- **Port**: 8022 (default)
- **Protocol**: SSH File Transfer Protocol
- **Authentication**:
  - Password (Access Key / Secret Key)
  - SSH Public Key (recommended)
  - SSH Certificate (optional)
- **Features**:
  - File upload/download
  - Directory listing and manipulation
  - File deletion
  - Bucket creation/deletion via mkdir/rmdir
- **Limitations**:
  - No file rename/copy operations
  - No multipart upload
  - No symlinks or file attributes modification

- **Documentation: [SFTP README](./sftp/README.md)**

## Architecture

```
┌─────────────────────────────────────────┐
│           RustFS Client                  │
│   (FTPS Client, SFTP Client)  │
└─────────────────────────────────────────┘
                  │
┌─────────────────────────────────────────┐
│      Protocol Gateway Layer             │
│  - Action Mapping                       │
│  - Authorization (IAM Policy)            │
│  - Operation Support Check              │
└─────────────────────────────────────────┘
                  │
┌─────────────────────────────────────────┐
│      Internal S3 Client                 │
│      (ProtocolS3Client)                 │
└─────────────────────────────────────────┘
                  │
┌─────────────────────────────────────────┐
│      Storage Layer (ECStore)            │
│      - Object Storage                   │
│      - Erasure Coding                   │
│      - Metadata Management              │
└─────────────────────────────────────────┘
```

## Security

### Encryption
- **FTPS**: TLS 1.2/1.3 for all control and data connections
- **SFTP**: SSH2 protocol with modern cipher suites (Ed25519, RSA, ECDSA)

### Authentication
All protocols share the same IAM-based authentication system:
- **Access Key**: Username identifier
- **Secret Key**: Password/API key
- **SSH Public Keys**: For SFTP key-based authentication
- **IAM Policies**: Fine-grained access control

### Authorization
Unified authorization based on IAM policies:
- Supports `s3:*` action namespace
- Condition-based policies (IP, time, etc.)
- Bucket-level and object-level permissions

## Troubleshooting

### FTPS Connection Issues
```bash
# Check TLS certificate
openssl s_client -connect localhost:21 -starttls ftp

# Test with lftp
lftp -u username,password -e "set ssl:verify-certificate no; ls; bye" ftps://localhost
```

### SFTP Connection Issues
```bash
# Verbose SFTP connection
sftp -vvv -o StrictHostKeyChecking=no -o LogLevel=DEBUG3 user@localhost

# Check SSH host key
ssh-keygen -l -f /path/to/host_key
```

## Configuration Reference

### FTPS Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `RUSTFS_FTPS_ENABLE` | Enable FTPS server | `false` |
| `RUSTFS_FTPS_ADDRESS` | FTPS bind address | `0.0.0.0:8021` |
| `RUSTFS_FTPS_CERTS_FILE` | TLS certificate file | - |
| `RUSTFS_FTPS_KEY_FILE` | TLS private key file | - |
| `RUSTFS_FTPS_PASSIVE_PORTS` | Passive port range | - |
| `RUSTFS_FTPS_EXTERNAL_IP` | External IP for NAT | - |

### SFTP Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `RUSTFS_SFTP_ENABLE` | Enable SFTP server | `false` |
| `RUSTFS_SFTP_ADDRESS` | SFTP bind address | `0.0.0.0:8022` |
| `RUSTFS_SFTP_HOST_KEY` | SSH host key file | - |
| `RUSTFS_SFTP_AUTHORIZED_KEYS` | Authorized keys file | - |

## See Also

- [FTPS README](./ftps/README.md) - Detailed FTPS usage
- [SFTP README](./sftp/README.md) - Detailed SFTP usage
- [RustFS Documentation](https://rustfs.com/docs/)
- [IAM Policy Reference](https://rustfs.com/docs/iam-policies)