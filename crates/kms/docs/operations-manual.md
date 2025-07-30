# RustFS Object Encryption Operations Manual

## Overview

This manual provides step-by-step procedures for operating and maintaining object encryption in RustFS production environments.

## Daily Operations

### 1. Health Checks

#### Check Encryption Service Status
```bash
# Check service health
curl -f http://localhost:9000/minio/health/live

# Check encryption-specific endpoints
curl -f http://localhost:9000/api/v1/encryption/health
```

#### Verify KMS Connectivity
```bash
# Test KMS connection
curl -f http://localhost:9000/api/v1/kms/health

# List available keys
curl http://localhost:9000/api/v1/kms/keys | jq '.keys[]'
```

#### Monitor Encryption Metrics
```bash
# Check Prometheus metrics
curl http://localhost:9000/metrics | grep "rustfs_encryption"

# Key metrics to monitor:
# - rustfs_encryption_operations_total
# - rustfs_encryption_failures_total
# - rustfs_kms_operations_total
# - rustfs_encryption_duration_seconds
```

### 2. Key Management Operations

#### Daily Key Rotation Check
```bash
#!/bin/bash
# check_key_rotation.sh

KEY_ROTATION_LOG="/var/log/rustfs/key_rotation.log"
TODAY=$(date +%Y-%m-%d)

# Check for key rotation activities
grep "$TODAY.*rotation" "$KEY_ROTATION_LOG" || echo "No key rotation activities today"

# Verify key rotation schedule
NEXT_ROTATION=$(grep "next_rotation" /var/lib/rustfs/encryption/state.json | jq -r '.next_rotation')
echo "Next scheduled rotation: $NEXT_ROTATION"
```

#### Key Usage Monitoring
```bash
#!/bin/bash
# monitor_key_usage.sh

# Get key usage statistics
curl -s http://localhost:9000/api/v1/kms/keys | jq -r '.keys[] | "\(.key_id): \(.usage_count) uses"'

# Check for keys approaching usage limits
LIMIT=1000000
curl -s http://localhost:9000/api/v1/kms/keys | jq -r --argjson limit "$LIMIT" '.keys[] | select(.usage_count > ($limit * 0.8)) | "WARNING: \(.key_id) has \(.usage_count) uses (limit: $limit)"'
```

### 3. Encryption Status Verification

#### Verify Bucket Encryption
```bash
#!/bin/bash
# check_bucket_encryption.sh

BUCKETS=$(aws s3api list-buckets --endpoint-url http://localhost:9000 | jq -r '.Buckets[].Name')

for bucket in $BUCKETS; do
    echo "Checking bucket: $bucket"
    
    # Check encryption configuration
    encryption=$(aws s3api get-bucket-encryption --bucket "$bucket" --endpoint-url http://localhost:9000 2>/dev/null)
    
    if [ $? -eq 0 ]; then
        echo "  ✓ Encryption configured: $(echo "$encryption" | jq -r '.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm')"
    else
        echo "  ⚠ No encryption configuration found"
    fi
    
    # Check bucket policy for encryption enforcement
    policy=$(aws s3api get-bucket-policy --bucket "$bucket" --endpoint-url http://localhost:9000 2>/dev/null)
    if [ $? -eq 0 ]; then
        encrypted=$(echo "$policy" | jq -r '.Policy | fromjson | .Statement[] | select(.Effect=="Deny" and .Condition.StringNotEquals."s3:x-amz-server-side-encryption") | .Effect')
        if [ "$encrypted" = "Deny" ]; then
            echo "  ✓ Encryption enforced by policy"
        fi
    fi
done
```

## Weekly Operations

### 1. Performance Review

#### Encryption Performance Analysis
```bash
#!/bin/bash
# weekly_performance_check.sh

PROMETHEUS_URL="http://localhost:9090"
WEEK_START=$(date -d "7 days ago" +%s)
WEEK_END=$(date +%s)

# Get encryption operation rates
echo "=== Encryption Operations Rate ==="
curl -s "$PROMETHEUS_URL/api/v1/query_range?query=rate(rustfs_encryption_operations_total[1h])&start=$WEEK_START&end=$WEEK_END&step=1h" | jq -r '.data.result[] | "\(.metric.instance): \(.values[-1][1]) ops/sec"'

# Get average encryption duration
echo "=== Average Encryption Duration ==="
curl -s "$PROMETHEUS_URL/api/v1/query_range?query=rate(rustfs_encryption_duration_seconds_sum[1h])/rate(rustfs_encryption_duration_seconds_count[1h])&start=$WEEK_START&end=$WEEK_END&step=1h" | jq -r '.data.result[] | "\(.metric.instance): \(.values[-1][1]) seconds"'

# Check for performance degradation
echo "=== Performance Alerts ==="
curl -s "$PROMETHEUS_URL/api/v1/alerts" | jq -r '.data.alerts[] | select(.labels.alertname | contains("encryption")) | "\(.labels.alertname): \(.state)"'
```

#### Capacity Planning
```bash
#!/bin/bash
# capacity_planning.sh

# Calculate encryption throughput
echo "=== Weekly Encryption Throughput ==="
TOTAL_BYTES=$(curl -s http://localhost:9000/metrics | grep "rustfs_encrypted_bytes_total" | awk '{print $2}' | paste -sd+ | bc)
echo "Total encrypted bytes this week: $TOTAL_BYTES"

# Estimate storage growth
ENCRYPTED_RATIO=$(curl -s http://localhost:9000/metrics | grep "rustfs_encrypted_objects_ratio" | awk '{print $2}')
echo "Encryption ratio: $ENCRYPTED_RATIO"

# Project next week's needs
CURRENT_STORAGE=$(df -h /var/lib/rustfs | awk 'NR==2 {print $3}')
echo "Current storage usage: $CURRENT_STORAGE"
```

### 2. Security Review

#### Access Control Audit
```bash
#!/bin/bash
# security_audit.sh

echo "=== Encryption Access Control Audit ==="

# Check IAM policies for encryption permissions
aws iam list-policies --endpoint-url http://localhost:9000 | jq -r '.Policies[] | select(.PolicyName | contains("encryption")) | "\(.PolicyName): \(.AttachmentCount) attachments"'

# Verify key rotation compliance
KEY_AGE_LIMIT=90
CURRENT_DATE=$(date +%s)

curl -s http://localhost:9000/api/v1/kms/keys | jq -r --argjson limit "$KEY_AGE_LIMIT" --argjson current "$CURRENT_DATE" '.keys[] | select((($current - .creation_date) / 86400) > $limit) | "WARNING: \(.key_id) is \((($current - .creation_date) / 86400)) days old"'

# Check for unauthorized access attempts
grep "Unauthorized" /var/log/rustfs/encryption.log | tail -10
```

### 3. Backup Verification

#### Encryption Key Backup
```bash
#!/bin/bash
# backup_verification.sh

BACKUP_DIR="/backup/rustfs/encryption"
KEY_BACKUP_FILE="$BACKUP_DIR/keys-$(date +%Y%m%d).json"

# Create key backup
curl -s http://localhost:9000/api/v1/kms/keys | jq '.' > "$KEY_BACKUP_FILE"

# Verify backup integrity
if [ -s "$KEY_BACKUP_FILE" ]; then
    echo "✓ Key backup created successfully"
    
    # Test restore capability
    TEMP_DIR=$(mktemp -d)
    cp "$KEY_BACKUP_FILE" "$TEMP_DIR/"
    
    # Verify JSON structure
    if jq empty "$TEMP_DIR/$(basename "$KEY_BACKUP_FILE")"; then
        echo "✓ Backup file is valid JSON"
    else
        echo "✗ Backup file is corrupted"
    fi
    
    rm -rf "$TEMP_DIR"
else
    echo "✗ Failed to create key backup"
fi

# Clean old backups (keep 30 days)
find "$BACKUP_DIR" -name "keys-*.json" -mtime +30 -delete
```

## Monthly Operations

### 1. Key Rotation

#### Automated Key Rotation
```bash
#!/bin/bash
# monthly_key_rotation.sh

ROTATION_SCRIPT="/opt/rustfs/scripts/rotate_keys.sh"
LOG_FILE="/var/log/rustfs/key_rotation.log"

# Check if rotation is needed
OLD_KEYS=$(curl -s http://localhost:9000/api/v1/kms/keys | jq -r '.keys[] | select(.rotation_needed == true) | .key_id')

if [ -n "$OLD_KEYS" ]; then
    echo "$(date): Starting key rotation for: $OLD_KEYS" >> "$LOG_FILE"
    
    for key_id in $OLD_KEYS; do
        echo "Rotating key: $key_id"
        
        # Create new key version
        curl -X POST "http://localhost:9000/api/v1/kms/keys/$key_id/rotate" \
          -H "Content-Type: application/json" \
          -d '{"retain_old_key_days": 30}'
        
        # Verify rotation
        if curl -s "http://localhost:9000/api/v1/kms/keys/$key_id" | jq -r '.rotation_status' | grep -q "completed"; then
            echo "✓ Key $key_id rotated successfully"
        else
            echo "✗ Failed to rotate key $key_id"
        fi
    done
else
    echo "$(date): No keys require rotation" >> "$LOG_FILE"
fi
```

### 2. Performance Optimization

#### Encryption Performance Tuning
```bash
#!/bin/bash
# performance_optimization.sh

# Check current encryption settings
CURRENT_SETTINGS=$(curl -s http://localhost:9000/api/v1/encryption/settings)

# Analyze performance bottlenecks
BENCHMARK_FILE="/tmp/encryption_benchmark_$(date +%Y%m%d).log"

# Run performance benchmark
for size in 1M 10M 100M 1G; do
    echo "Testing with $size file..."
    
    # Create test file
    dd if=/dev/zero of="/tmp/test_$size" bs="$size" count=1
    
    # Measure upload time
    START_TIME=$(date +%s.%N)
    aws s3 cp "/tmp/test_$size" "s3://test-bucket/test_$size" \
      --server-side-encryption AES256 \
      --endpoint-url http://localhost:9000
    END_TIME=$(date +%s.%N)
    
    DURATION=$(echo "$END_TIME - $START_TIME" | bc)
    echo "$size: ${DURATION}s" >> "$BENCHMARK_FILE"
    
    # Clean up
    rm "/tmp/test_$size"
    aws s3 rm "s3://test-bucket/test_$size" --endpoint-url http://localhost:9000
done

echo "Benchmark results saved to: $BENCHMARK_FILE"
```

### 3. Security Compliance

#### Compliance Report Generation
```bash
#!/bin/bash
# compliance_report.sh

REPORT_DIR="/reports/monthly/$(date +%Y-%m)"
mkdir -p "$REPORT_DIR"

# Generate encryption compliance report
cat > "$REPORT_DIR/encryption_compliance.md" << EOF
# Encryption Compliance Report - $(date +%Y-%m)

## Summary
- Report Period: $(date +%Y-%m-01) to $(date +%Y-%m-%d)
- Generated: $(date)

## Encryption Coverage
EOF

# Calculate encryption coverage
TOTAL_BUCKETS=$(aws s3api list-buckets --endpoint-url http://localhost:9000 | jq -r '.Buckets | length')
ENCRYPTED_BUCKETS=$(aws s3api list-buckets --endpoint-url http://localhost:9000 | jq -r '.Buckets[].Name' | while read bucket; do aws s3api get-bucket-encryption --bucket "$bucket" --endpoint-url http://localhost:9000 >/dev/null 2>&1 && echo "$bucket"; done | wc -l)

echo "- Total Buckets: $TOTAL_BUCKETS" >> "$REPORT_DIR/encryption_compliance.md"
echo "- Encrypted Buckets: $ENCRYPTED_BUCKETS" >> "$REPORT_DIR/encryption_compliance.md"
echo "- Encryption Coverage: $(echo "scale=2; $ENCRYPTED_BUCKETS * 100 / $TOTAL_BUCKETS" | bc)%" >> "$REPORT_DIR/encryption_compliance.md"

# Key rotation compliance
TOTAL_KEYS=$(curl -s http://localhost:9000/api/v1/kms/keys | jq -r '.keys | length')
ROTATED_KEYS=$(curl -s http://localhost:9000/api/v1/kms/keys | jq -r '.keys[] | select(.last_rotation >= (now - 2592000))' | wc -l)

echo "- Total Keys: $TOTAL_KEYS" >> "$REPORT_DIR/encryption_compliance.md"
echo "- Recently Rotated Keys: $ROTATED_KEYS" >> "$REPORT_DIR/encryption_compliance.md"
echo "- Rotation Compliance: $(echo "scale=2; $ROTATED_KEYS * 100 / $TOTAL_KEYS" | bc)%" >> "$REPORT_DIR/encryption_compliance.md"
```

## Incident Response

### 1. Encryption Service Down

#### Immediate Response
```bash
#!/bin/bash
# encryption_service_down.sh

# Check service status
if ! curl -f http://localhost:9000/minio/health/live; then
    echo "Encryption service is down"
    
    # Check system resources
    echo "System resources:"
    df -h
    free -h
    top -bn1 | head -20
    
    # Check logs for errors
    echo "Recent errors:"
    tail -100 /var/log/rustfs/error.log | grep -i "encryption\|kms"
    
    # Attempt restart
    systemctl restart rustfs
    
    # Verify service recovery
    sleep 30
    if curl -f http://localhost:9000/minio/health/live; then
        echo "Service recovered"
    else
        echo "Service still down - escalate to on-call"
    fi
fi
```

### 2. Key Compromise

#### Emergency Key Rotation
```bash
#!/bin/bash
# emergency_key_rotation.sh

COMPROMISED_KEY="$1"
if [ -z "$COMPROMISED_KEY" ]; then
    echo "Usage: $0 <compromised-key-id>"
    exit 1
fi

echo "EMERGENCY: Rotating compromised key $COMPROMISED_KEY"

# 1. Disable compromised key immediately
curl -X POST "http://localhost:9000/api/v1/kms/keys/$COMPROMISED_KEY/disable"

# 2. Create new key version
curl -X POST "http://localhost:9000/api/v1/kms/keys/$COMPROMISED_KEY/rotate" \
  -H "Content-Type: application/json" \
  -d '{"retain_old_key_days": 0}'

# 3. Re-encrypt all objects using the compromised key
aws s3api list-buckets --endpoint-url http://localhost:9000 | jq -r '.Buckets[].Name' | while read bucket; do
    echo "Re-encrypting objects in bucket: $bucket"
    aws s3 ls "s3://$bucket" --recursive --endpoint-url http://localhost:9000 | while read -r line; do
        object=$(echo "$line" | awk '{print $4}')
        if [ -n "$object" ]; then
            echo "Re-encrypting: $object"
            aws s3api copy-object \
              --bucket "$bucket" \
              --copy-source "$bucket/$object" \
              --key "$object" \
              --server-side-encryption AES256 \
              --metadata-directive REPLACE \
              --endpoint-url http://localhost:9000
        fi
    done
done

echo "Emergency key rotation completed"
```

### 3. Performance Degradation

#### Performance Investigation
```bash
#!/bin/bash
# performance_investigation.sh

# Check current load
LOAD=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | sed 's/,//')
echo "Current load: $LOAD"

# Check encryption operation rates
OPERATIONS=$(curl -s http://localhost:9000/metrics | grep "rustfs_encryption_operations_total" | awk '{print $2}')
echo "Total encryption operations: $OPERATIONS"

# Check for slow operations
SLOW_OPS=$(grep "duration.*>" /var/log/rustfs/encryption.log | wc -l)
echo "Slow operations detected: $SLOW_OPS"

# Check KMS response times
KMS_LATENCY=$(curl -s http://localhost:9000/metrics | grep "rustfs_kms_request_duration_seconds" | awk '{print $2}')
echo "Average KMS latency: ${KMS_LATENCY}s"

# Generate performance report
REPORT_FILE="/tmp/performance_report_$(date +%Y%m%d_%H%M%S).txt"
cat > "$REPORT_FILE" << EOF
Performance Investigation Report - $(date)

System Metrics:
- Load Average: $LOAD
- Encryption Operations: $OPERATIONS
- Slow Operations: $SLOW_OPS
- KMS Latency: ${KMS_LATENCY}s

Recommendations:
$(if [ "$LOAD" > "2.0" ]; then echo "- High system load detected - investigate resource usage"; fi)
$(if [ "$SLOW_OPS" -gt 100 ]; then echo "- Multiple slow operations detected - check KMS connectivity"; fi)
$(if [ "$(echo "$KMS_LATENCY > 0.5" | bc)" -eq 1 ]; then echo "- High KMS latency - consider KMS optimization"; fi)
EOF

echo "Performance report saved to: $REPORT_FILE"
```

## Maintenance Windows

### Scheduled Maintenance Checklist

#### Pre-Maintenance
```bash
#!/bin/bash
# pre_maintenance.sh

MAINTENANCE_DATE="$(date +%Y-%m-%d)"
MAINTENANCE_LOG="/var/log/rustfs/maintenance_$MAINTENANCE_DATE.log"

echo "$(date): Starting pre-maintenance checks" >> "$MAINTENANCE_LOG"

# 1. Verify all services are healthy
services=("rustfs" "kms" "monitoring")
for service in "${services[@]}"; do
    if systemctl is-active "$service" >/dev/null 2>&1; then
        echo "✓ $service is running" >> "$MAINTENANCE_LOG"
    else
        echo "✗ $service is not running" >> "$MAINTENANCE_LOG"
    fi
done

# 2. Create backup
echo "$(date): Creating encryption backup" >> "$MAINTENANCE_LOG"
BACKUP_DIR="/backup/pre-maintenance/$MAINTENANCE_DATE"
mkdir -p "$BACKUP_DIR"

# Backup encryption keys
curl -s http://localhost:9000/api/v1/kms/keys | jq '.' > "$BACKUP_DIR/keys.json"

# Backup bucket configurations
aws s3api list-buckets --endpoint-url http://localhost:9000 | jq -r '.Buckets[].Name' | while read bucket; do
    aws s3api get-bucket-encryption --bucket "$bucket" --endpoint-url http://localhost:9000 > "$BACKUP_DIR/${bucket}_encryption.json" 2>/dev/null || true
done

echo "$(date): Pre-maintenance checks completed" >> "$MAINTENANCE_LOG"
```

#### Post-Maintenance
```bash
#!/bin/bash
# post_maintenance.sh

MAINTENANCE_DATE="$(date +%Y-%m-%d)"
MAINTENANCE_LOG="/var/log/rustfs/maintenance_$MAINTENANCE_DATE.log"

echo "$(date): Starting post-maintenance verification" >> "$MAINTENANCE_LOG"

# 1. Verify service health
for service in "${services[@]}"; do
    if systemctl is-active "$service" >/dev/null 2>&1; then
        echo "✓ $service is running" >> "$MAINTENANCE_LOG"
    else
        echo "✗ $service failed to start" >> "$MAINTENANCE_LOG"
    fi
done

# 2. Verify encryption functionality
TEST_BUCKET="maintenance-test-$(date +%s)"
aws s3 mb "s3://$TEST_BUCKET" --endpoint-url http://localhost:9000

# Test encryption
echo "test data" > /tmp/test_file.txt
aws s3 cp /tmp/test_file.txt "s3://$TEST_BUCKET/test.txt" \
  --server-side-encryption AES256 \
  --endpoint-url http://localhost:9000

# Verify encryption
aws s3api head-object --bucket "$TEST_BUCKET" --key test.txt --endpoint-url http://localhost:9000 | grep -q "ServerSideEncryption"
if [ $? -eq 0 ]; then
    echo "✓ Encryption working correctly" >> "$MAINTENANCE_LOG"
else
    echo "✗ Encryption verification failed" >> "$MAINTENANCE_LOG"
fi

# Cleanup
aws s3 rb "s3://$TEST_BUCKET" --force --endpoint-url http://localhost:9000
rm /tmp/test_file.txt

echo "$(date): Post-maintenance verification completed" >> "$MAINTENANCE_LOG"
```

## Documentation Updates

### Version Control

Maintain version-controlled documentation:

```bash
# Initialize git repository for documentation
cd /opt/rustfs/docs
git init
git add .
git commit -m "Initial encryption operations documentation"

# Regular updates
git add -A
git commit -m "Update operations manual - $(date +%Y-%m-%d)"
```

### Change Management

Document all configuration changes:

```bash
#!/bin/bash
# log_config_change.sh

CHANGE_TYPE="$1"
DESCRIPTION="$2"
AUTHOR="$3"

CHANGELOG_FILE="/var/log/rustfs/config_changes.log"
echo "$(date '+%Y-%m-%d %H:%M:%S') | $CHANGE_TYPE | $DESCRIPTION | $AUTHOR" >> "$CHANGELOG_FILE"
```

Usage:
```bash
./log_config_change.sh "bucket_encryption" "Updated mybucket to use SSE-KMS" "admin@company.com"
```