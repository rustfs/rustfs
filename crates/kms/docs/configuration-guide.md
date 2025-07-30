# RustFS Object Encryption Configuration Guide

## Overview

This guide covers how to configure and manage object encryption in RustFS, including server-side encryption options, key management, and security best practices.

## Prerequisites

- RustFS cluster deployed and running
- Access to RustFS admin APIs
- Understanding of your encryption requirements
- KMS service configured (for SSE-KMS)

## Server-Side Encryption Options

RustFS supports three server-side encryption methods:

1. **SSE-S3**: AES256 encryption with keys managed by RustFS
2. **SSE-KMS**: Encryption using customer-managed keys via KMS
3. **SSE-C**: Encryption using customer-provided keys

## Configuration Methods

### 1. Bucket-Level Encryption (Recommended)

Configure default encryption for all objects in a bucket:

#### SSE-S3 Configuration

```bash
# Using AWS CLI
aws s3api put-bucket-encryption \
  --bucket mybucket \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        }
      }
    ]
  }' \
  --endpoint-url http://localhost:9000

# Using curl
curl -X PUT "http://localhost:9000/mybucket?encryption" \
  -H "Content-Type: application/json" \
  -d '{
    "ServerSideEncryptionConfiguration": {
      "Rules": [
        {
          "ApplyServerSideEncryptionByDefault": {
            "SSEAlgorithm": "AES256"
          }
        }
      ]
    }
  }'
```

#### SSE-KMS Configuration

```bash
# Using AWS CLI
aws s3api put-bucket-encryption \
  --bucket mybucket \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "aws:kms",
          "KMSMasterKeyID": "my-kms-key-id"
        }
      }
    ]
  }' \
  --endpoint-url http://localhost:9000

# Using curl with custom KMS key
curl -X PUT "http://localhost:9000/mybucket?encryption" \
  -H "Content-Type: application/json" \
  -d '{
    "ServerSideEncryptionConfiguration": {
      "Rules": [
        {
          "ApplyServerSideEncryptionByDefault": {
            "SSEAlgorithm": "aws:kms",
            "KMSMasterKeyID": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
          }
        }
      ]
    }
  }'
```

### 2. Object-Level Encryption

Configure encryption for individual objects:

#### SSE-S3

```bash
# Using AWS CLI
aws s3api put-object \
  --bucket mybucket \
  --key myobject \
  --body myfile.txt \
  --server-side-encryption AES256 \
  --endpoint-url http://localhost:9000

# Using curl
curl -X PUT "http://localhost:9000/mybucket/myobject" \
  -H "x-amz-server-side-encryption: AES256" \
  --data-binary @myfile.txt
```

#### SSE-KMS

```bash
# Using AWS CLI
aws s3api put-object \
  --bucket mybucket \
  --key myobject \
  --body myfile.txt \
  --server-side-encryption aws:kms \
  --ssekms-key-id my-kms-key-id \
  --endpoint-url http://localhost:9000

# Using curl
curl -X PUT "http://localhost:9000/mybucket/myobject" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: my-kms-key-id" \
  --data-binary @myfile.txt
```

#### SSE-C (Customer-Provided Keys)

```bash
# Generate a 256-bit key
KEY=$(openssl rand -base64 32)
KEY_MD5=$(echo $KEY | base64 -d | md5sum | cut -d' ' -f1 | xxd -r -p | base64)

# Upload with SSE-C
curl -X PUT "http://localhost:9000/mybucket/myobject" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-md5: $KEY_MD5" \
  --data-binary @myfile.txt

# Download with same key
curl -X GET "http://localhost:9000/mybucket/myobject" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-md5: $KEY_MD5"
```

## KMS Configuration

### Setting Up KMS Keys

1. **Create KMS Key**

```bash
# Using RustFS KMS API
curl -X POST "http://localhost:9000/api/v1/kms/keys" \
  -H "Content-Type: application/json" \
  -d '{
    "key_id": "my-encryption-key",
    "description": "Encryption key for S3 objects",
    "key_usage": ["ENCRYPT_DECRYPT"],
    "key_spec": "AES_256"
  }'
```

2. **List KMS Keys**

```bash
curl "http://localhost:9000/api/v1/kms/keys"
```

3. **Describe KMS Key**

```bash
curl "http://localhost:9000/api/v1/kms/keys/my-encryption-key"
```

### KMS Key Policies

Configure key access policies:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowBucketEncryption",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:GenerateDataKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/my-encryption-key",
      "Condition": {
        "StringEquals": {
          "kms:ViaService": "s3.us-east-1.amazonaws.com"
        }
      }
    }
  ]
}
```

## Configuration Files

### RustFS Server Configuration

Edit `config.toml` to configure encryption settings:

```toml
[encryption]
# Enable encryption features
enabled = true

# Default encryption algorithm (AES256, aws:kms)
default_algorithm = "AES256"

# KMS configuration
[kms]
endpoint = "http://localhost:9000"
region = "us-east-1"

# Encryption key rotation
[key_rotation]
enabled = true
rotation_period_days = 365

# Audit logging
[audit]
enabled = true
log_file = "/var/log/rustfs/encryption.log"
log_level = "info"
```

### Environment Variables

Configure encryption via environment variables:

```bash
# Enable encryption
export RUSTFS_ENCRYPTION_ENABLED=true

# Default KMS endpoint
export RUSTFS_KMS_ENDPOINT=http://localhost:9000

# Default region
export RUSTFS_REGION=us-east-1

# Audit logging
export RUSTFS_AUDIT_ENABLED=true
export RUSTFS_AUDIT_LOG_FILE=/var/log/rustfs/encryption.log
```

## Advanced Configuration

### Multi-Region KMS

Configure multiple KMS regions for disaster recovery:

```json
{
  "ServerSideEncryptionConfiguration": {
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "aws:kms",
          "KMSMasterKeyID": "arn:aws:kms:us-east-1:123456789012:key/key-1"
        }
      }
    ]
  },
  "BucketKeyEnabled": true,
  "ReplicationConfiguration": {
    "Role": "arn:aws:iam::123456789012:role/rustfs-replication",
    "Rules": [
      {
        "ID": "replication-rule",
        "Status": "Enabled",
        "Priority": 1,
        "DeleteMarkerReplication": {
          "Status": "Enabled"
        },
        "Destination": {
          "Bucket": "arn:aws:s3:::backup-bucket",
          "EncryptionConfiguration": {
            "ReplicaKmsKeyID": "arn:aws:kms:us-west-2:123456789012:key/key-2"
          }
        }
      }
    ]
  }
}
```

### Bucket Policies for Encryption

Enforce encryption via bucket policies:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyUnencryptedObjectUploads",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::mybucket/*",
      "Condition": {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": ["AES256", "aws:kms"]
        }
      }
    }
  ]
}
```

### IAM Policies for Encryption

Create IAM policies for encryption access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::mybucket/*",
      "Condition": {
        "StringEquals": {
          "s3:x-amz-server-side-encryption": "aws:kms"
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:GenerateDataKey"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/my-encryption-key"
    }
  ]
}
```

## Monitoring and Alerting

### Prometheus Metrics

Enable metrics collection:

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'rustfs-encryption'
    static_configs:
      - targets: ['localhost:9000']
    metrics_path: /metrics
```

### Grafana Dashboard

Import the provided Grafana dashboard:

```json
{
  "dashboard": {
    "title": "RustFS Encryption Metrics",
    "panels": [
      {
        "title": "Encryption Operations",
        "targets": [
          {
            "expr": "rate(rustfs_encryption_operations_total[5m])"
          }
        ]
      },
      {
        "title": "Encryption Failures",
        "targets": [
          {
            "expr": "rate(rustfs_encryption_failures_total[5m])"
          }
        ]
      }
    ]
  }
}
```

### Alerting Rules

Configure alerting for encryption issues:

```yaml
# alerts.yml
groups:
  - name: rustfs-encryption
    rules:
      - alert: HighEncryptionFailureRate
        expr: rate(rustfs_encryption_failures_total[5m]) / rate(rustfs_encryption_operations_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High encryption failure rate"
          description: "Encryption failure rate is {{ $value | humanizePercentage }}"
      
      - alert: KMSUnavailable
        expr: up{job="rustfs-kms"} == 0
        for: 1m
        annotations:
          summary: "KMS service unavailable"
          description: "KMS service has been down for more than 1 minute"
```

## Troubleshooting

### Common Issues

#### 1. KMS Key Not Found

**Symptoms**: `InvalidArgument: Invalid KMS key ID`

**Solution**:
```bash
# Verify key exists
curl "http://localhost:9000/api/v1/kms/keys"

# Create key if missing
curl -X POST "http://localhost:9000/api/v1/kms/keys" \
  -H "Content-Type: application/json" \
  -d '{"key_id": "my-key", "description": "My encryption key"}'
```

#### 2. Access Denied

**Symptoms**: `AccessDenied: Access Denied`

**Solution**:
```bash
# Check IAM policies
aws iam list-policies --endpoint-url http://localhost:9000

# Verify bucket policy
aws s3api get-bucket-policy --bucket mybucket --endpoint-url http://localhost:9000
```

#### 3. Encryption Algorithm Not Supported

**Symptoms**: `InvalidArgument: Invalid server-side encryption algorithm`

**Solution**:
```bash
# Use supported algorithms: AES256, aws:kms
curl -X PUT "http://localhost:9000/mybucket/myobject" \
  -H "x-amz-server-side-encryption: AES256" \
  --data-binary @myfile.txt
```

### Debug Mode

Enable debug logging:

```bash
export RUST_LOG=debug
export RUSTFS_DEBUG=true

# Start RustFS with debug output
./rustfs server --config config.toml
```

### Performance Tuning

#### KMS Connection Pooling

```toml
[kms]
connection_pool_size = 100
connection_timeout = "30s"
request_timeout = "10s"
```

#### Encryption Thread Pool

```toml
[encryption]
thread_pool_size = 8
max_concurrent_operations = 100
```

#### Cache Configuration

```toml
[cache]
encryption_key_cache_size = 1000
encryption_key_cache_ttl = "1h"
```

## Migration Guide

### Migrating from Unencrypted to Encrypted

1. **Enable bucket encryption**:
```bash
aws s3api put-bucket-encryption \
  --bucket mybucket \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        }
      }
    ]
  }'
```

2. **Re-encrypt existing objects**:
```bash
# Copy objects to re-encrypt them
aws s3 cp s3://mybucket/old-object s3://mybucket/new-object \
  --metadata-directive COPY \
  --server-side-encryption AES256
```

3. **Verify encryption**:
```bash
aws s3api head-object --bucket mybucket --key new-object
```

### Migrating Between Encryption Methods

1. **SSE-S3 to SSE-KMS**:
```bash
aws s3api copy-object \
  --bucket mybucket \
  --copy-source mybucket/myobject \
  --key myobject \
  --server-side-encryption aws:kms \
  --ssekms-key-id my-kms-key-id
```

2. **SSE-KMS to SSE-S3**:
```bash
aws s3api copy-object \
  --bucket mybucket \
  --copy-source mybucket/myobject \
  --key myobject \
  --server-side-encryption AES256
```