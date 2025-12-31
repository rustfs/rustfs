# RustFS Protocols

RustFS provides multiple protocol interfaces for accessing object storage, including:

- **FTPS** - File Transfer Protocol over TLS for traditional file transfers
- **SFTP** - SSH File Transfer Protocol for secure file transfers with key-based authentication

## Quick Start

### Enable Protocols on Startup

```bash
# Start RustFS with all protocols enabled
rustfs \
  --address 0.0.0.0:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --ftps-enable \
  --ftps-address 0.0.0.0:21 \
  --ftps-certs-file /path/to/cert.pem \
  --ftps-key-file /path/to/key.pem \
  --ftps-passive-ports "40000-41000" \
  --sftp-enable \
  --sftp-address 0.0.0.0:22 \
  --sftp-host-key /path/to/host_key \
  --sftp-authorized-keys /path/to/authorized_keys \
  /data
```

## Protocol Details

### FTPS
- **Port**: 21
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
- **Port**: 22
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

| Option | Environment Variable | Description | Default |
|--------|---------------------|-------------|---------|
| `--ftps-enable` | `RUSTFS_FTPS_ENABLE` | Enable FTPS server | `false` |
| `--ftps-address` | `RUSTFS_FTPS_ADDRESS` | FTPS bind address | `0.0.0.0:21` |
| `--ftps-certs-file` | `RUSTFS_FTPS_CERTS_FILE` | TLS certificate file | - |
| `--ftps-key-file` | `RUSTFS_FTPS_KEY_FILE` | TLS private key file | - |
| `--ftps-passive-ports` | `RUSTFS_FTPS_PASSIVE_PORTS` | Passive port range | - |
| `--ftps-external-ip` | `RUSTFS_FTPS_EXTERNAL_IP` | External IP for NAT | - |

### SFTP Configuration

| Option | Environment Variable | Description | Default |
|--------|---------------------|-------------|---------|
| `--sftp-enable` | `RUSTFS_SFTP_ENABLE` | Enable SFTP server | `false` |
| `--sftp-address` | `RUSTFS_SFTP_ADDRESS` | SFTP bind address | `0.0.0.0:22` |
| `--sftp-host-key` | `RUSTFS_SFTP_HOST_KEY` | SSH host key file | - |
| `--sftp-authorized-keys` | `RUSTFS_SFTP_AUTHORIZED_KEYS` | Authorized keys file | - |

## See Also

- [FTPS README](./ftps/README.md) - Detailed FTPS usage
- [SFTP README](./sftp/README.md) - Detailed SFTP usage
- [RustFS Documentation](https://rustfs.com/docs/)
- [IAM Policy Reference](https://rustfs.com/docs/iam-policies)