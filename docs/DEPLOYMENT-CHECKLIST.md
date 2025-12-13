# RustFS Cluster Power-Off Resilience - Production Deployment Checklist

## Overview

This checklist guides the deployment of the comprehensive power-off resilience solution (P0-P3 + EC Quorum Management) to production RustFS clusters. Follow this step-by-step guide to ensure safe, validated deployment with zero downtime.

---

## Pre-Deployment Preparation

### 1. Environment Assessment

- [ ] **Cluster Configuration Documented**
  - [ ] Number of nodes: ___
  - [ ] Disks per node: ___
  - [ ] EC configuration (data+parity shards): ___+___
  - [ ] Current RustFS version: ___
  - [ ] Network topology documented
  
- [ ] **Monitoring Infrastructure Ready**
  - [ ] Prometheus server accessible
  - [ ] Grafana dashboards deployed (optional)
  - [ ] Alertmanager configured with notification channels
  - [ ] Log aggregation system available (ELK, Loki, etc.)

- [ ] **Backup & Rollback Plan**
  - [ ] Current cluster state backed up
  - [ ] Previous binary versions archived
  - [ ] Configuration files backed up
  - [ ] Rollback procedure documented and tested

### 2. Code Review & Testing

- [ ] **Code Quality Verification**
  - [ ] `cargo fmt --all --check` passes
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes
  - [ ] `cargo check --all-targets` passes
  - [ ] All unit tests pass: `cargo test --workspace --exclude e2e_test`
  
- [ ] **Staging Environment Testing**
  - [ ] Deployed to staging cluster
  - [ ] Single node power-off test passed
  - [ ] Two node power-off test passed (quorum loss validation)
  - [ ] Node recovery test passed
  - [ ] Upload during power-off test passed
  - [ ] Console responsiveness verified

### 3. Documentation Review

- [ ] **Architecture Documents Read**
  - [ ] `cluster-power-off-resilience-comprehensive-solution.md`
  - [ ] `cluster-power-off-resilience-additional-analysis.md`
  - [ ] `cluster-power-off-resilience-complete-solution.md`
  - [ ] `cluster-power-off-resilience-p3-implementation.md`
  - [ ] `cluster-power-off-resilience-ec-quorum-management.md`
  - [ ] This deployment checklist

- [ ] **Operations Team Trained**
  - [ ] Troubleshooting runbook reviewed
  - [ ] Alert response procedures understood
  - [ ] Monitoring dashboards accessible
  - [ ] Rollback procedure practiced

---

## Deployment Phase 1: Prometheus Alerts (Zero Impact)

**Duration**: 15 minutes  
**Risk Level**: Low (monitoring only)  
**Rollback**: Simple (remove alert rules)

### Deploy Alert Rules

- [ ] **Backup Existing Alerts**
  ```bash
  cp /etc/prometheus/alerts.yml /etc/prometheus/alerts.yml.backup
  ```

- [ ] **Deploy New Alert Rules**
  ```bash
  # Copy new alert rules
  sudo cp deploy/prometheus/alerts.yml /etc/prometheus/rustfs_alerts.yml
  
  # Update prometheus.yml to include new rules
  # Add under rule_files:
  #   - /etc/prometheus/rustfs_alerts.yml
  ```

- [ ] **Validate Alert Syntax**
  ```bash
  promtool check rules /etc/prometheus/rustfs_alerts.yml
  ```

- [ ] **Reload Prometheus**
  ```bash
  sudo systemctl reload prometheus
  # OR
  curl -X POST http://localhost:9090/-/reload
  ```

- [ ] **Verify Alerts Loaded**
  - [ ] Navigate to Prometheus UI: http://prometheus-server:9090/alerts
  - [ ] Confirm 15 new alerts visible
  - [ ] Verify all alerts in "Inactive" state (no firing)

### Configure Alertmanager

- [ ] **Update Alertmanager Configuration**
  ```yaml
  # /etc/alertmanager/alertmanager.yml
  route:
    receiver: 'default'
    routes:
      - match:
          severity: critical
        receiver: 'critical-team'
      - match:
          severity: warning
        receiver: 'warning-team'
      - match:
          severity: emergency
        receiver: 'emergency-oncall'
  
  receivers:
    - name: 'critical-team'
      slack_configs:
        - api_url: 'YOUR_SLACK_WEBHOOK'
          channel: '#rustfs-critical'
    - name: 'warning-team'
      email_configs:
        - to: 'team@example.com'
    - name: 'emergency-oncall'
      pagerduty_configs:
        - service_key: 'YOUR_PAGERDUTY_KEY'
  ```

- [ ] **Reload Alertmanager**
  ```bash
  sudo systemctl reload alertmanager
  ```

- [ ] **Test Alert Notifications**
  - [ ] Trigger test alert
  - [ ] Verify notification received

---

## Deployment Phase 2: Binary Update (Node-by-Node Rolling)

**Duration**: 2-4 hours (depends on cluster size)  
**Risk Level**: Medium  
**Rollback**: Per-node binary replacement

### Pre-Deployment Validation

- [ ] **Cluster Health Check**
  ```bash
  # Verify all nodes online
  rustfs admin info
  
  # Check cluster health
  rustfs admin health
  
  # Verify no ongoing operations
  rustfs admin status
  ```

- [ ] **Build New Binary**
  ```bash
  cd /path/to/rustfs
  git checkout copilot/fix-cluster-power-off-issue
  make build
  # OR
  ./build-rustfs.sh --release
  ```

- [ ] **Binary Version Verification**
  ```bash
  ./target/release/rustfs --version
  # Confirm git commit hash matches expected
  ```

### Node 1 Deployment

- [ ] **Select Node for First Update**
  - [ ] Choose node with lowest load
  - [ ] Verify node is not hosting critical operations

- [ ] **Pre-Update Snapshot**
  ```bash
  # SSH to Node 1
  ssh user@node1
  
  # Backup current binary
  sudo cp /usr/local/bin/rustfs /usr/local/bin/rustfs.backup
  
  # Backup configuration
  sudo cp -r /etc/rustfs /etc/rustfs.backup
  ```

- [ ] **Stop RustFS Service**
  ```bash
  sudo systemctl stop rustfs
  # Verify stopped
  sudo systemctl status rustfs
  ```

- [ ] **Replace Binary**
  ```bash
  # Copy new binary
  sudo cp /path/to/new/rustfs /usr/local/bin/rustfs
  sudo chmod +x /usr/local/bin/rustfs
  
  # Verify binary
  /usr/local/bin/rustfs --version
  ```

- [ ] **Start RustFS Service**
  ```bash
  sudo systemctl start rustfs
  ```

- [ ] **Verify Service Health**
  ```bash
  # Check service status
  sudo systemctl status rustfs
  
  # Verify logs (no errors)
  sudo journalctl -u rustfs -n 100 -f
  
  # Wait 2-3 minutes for initialization
  sleep 180
  ```

- [ ] **Validate Node Functionality**
  ```bash
  # Test local health endpoint
  curl http://localhost:9000/health
  
  # Verify node rejoined cluster
  rustfs admin info | grep node1
  
  # Check metrics endpoint
  curl http://localhost:9000/metrics | grep rustfs
  ```

- [ ] **Monitor for 10 Minutes**
  - [ ] Watch Prometheus metrics
  - [ ] Check no new alerts firing
  - [ ] Verify CPU/memory stable
  - [ ] Confirm no error logs

### Repeat for Remaining Nodes

- [ ] **Node 2 Deployment**
  - [ ] Follow Node 1 procedure
  - [ ] Wait 10 minutes before proceeding

- [ ] **Node 3 Deployment**
  - [ ] Follow Node 1 procedure
  - [ ] Wait 10 minutes before proceeding

- [ ] **Node 4 Deployment**
  - [ ] Follow Node 1 procedure
  - [ ] Complete cluster validation below

---

## Deployment Phase 3: Post-Deployment Validation

**Duration**: 30-60 minutes  
**Risk Level**: Low (observation only)

### Cluster-Wide Health Verification

- [ ] **All Nodes Online**
  ```bash
  rustfs admin info
  # Verify all 4 nodes present and "online"
  ```

- [ ] **Cluster Metrics Baseline**
  ```bash
  # Collect baseline metrics
  curl http://node1:9000/metrics > /tmp/metrics_baseline.txt
  
  # Verify new metrics present:
  grep "rustfs_disk_state_check_duration_seconds" /tmp/metrics_baseline.txt
  grep "rustfs_config_read_duration_seconds" /tmp/metrics_baseline.txt
  grep "rustfs_lock_health_status" /tmp/metrics_baseline.txt
  ```

- [ ] **Circuit Breaker Status**
  ```bash
  # Check circuit breaker metrics
  curl http://node1:9000/metrics | grep circuit_breaker
  # All should show "closed" state
  ```

### Functional Testing

- [ ] **Upload Test (No Failures)**
  ```bash
  # Upload 100MB file
  dd if=/dev/urandom of=/tmp/test100mb.bin bs=1M count=100
  rustfs cp /tmp/test100mb.bin s3://test-bucket/test100mb.bin
  
  # Verify upload success
  rustfs stat s3://test-bucket/test100mb.bin
  ```

- [ ] **Console Access Test**
  - [ ] Navigate to Console UI: http://node1:9000
  - [ ] Verify dashboard loads within 5s
  - [ ] Check storage info displays correctly
  - [ ] Verify no hanging requests

- [ ] **IAM Operations Test**
  ```bash
  # Create test user
  rustfs admin user add testuser testpassword
  
  # Verify user created
  rustfs admin user list | grep testuser
  
  # Clean up
  rustfs admin user remove testuser
  ```

### Resilience Testing

- [ ] **Single Node Power-Off Simulation**
  ```bash
  # SSH to Node 4 (non-primary)
  ssh user@node4
  
  # Simulate abrupt power-off
  sudo systemctl kill -s KILL rustfs
  # OR
  sudo kill -9 $(pgrep rustfs)
  ```

- [ ] **Verify Cluster Response (Within 3-8s)**
  - [ ] Monitor Prometheus for circuit breaker opening
  - [ ] Check alert fires: `NodeUnreachableExtended`
  - [ ] Verify remaining nodes detect failure
  - [ ] Console remains responsive
  
- [ ] **Upload During Degraded Mode**
  ```bash
  # Wait 10 seconds after Node 4 failure
  sleep 10
  
  # Attempt upload (should succeed with 3 nodes)
  rustfs cp /tmp/test100mb.bin s3://test-bucket/degraded-test.bin
  
  # Verify success
  echo $?  # Should be 0
  ```

- [ ] **Node Recovery Test**
  ```bash
  # Restart Node 4
  ssh user@node4
  sudo systemctl start rustfs
  
  # Wait 30 seconds for recovery
  sleep 30
  ```

- [ ] **Verify Automatic Reintegration**
  ```bash
  # Check Node 4 rejoined
  rustfs admin info | grep node4
  
  # Verify circuit breaker closed
  curl http://node1:9000/metrics | grep 'circuit_breaker_state{peer="node4"}' 
  # Should show 0 (closed)
  
  # Check alert cleared
  # Prometheus UI should show NodeUnreachableExtended resolved
  ```

---

## Deployment Phase 4: Monitoring Setup

**Duration**: 1-2 hours  
**Risk Level**: Low (observation only)

### Grafana Dashboard Creation (Optional)

- [ ] **Import Dashboard JSON**
  - [ ] Navigate to Grafana: http://grafana-server:3000
  - [ ] Create new dashboard "RustFS Cluster Health"
  - [ ] Add panels per documentation

- [ ] **Panel 1: Cluster Quorum Status**
  ```promql
  rustfs_available_disks_total / rustfs_write_quorum_required
  ```

- [ ] **Panel 2: Circuit Breaker States**
  ```promql
  sum by (peer) (rustfs_circuit_breaker_state)
  ```

- [ ] **Panel 3: Disk State Check Duration**
  ```promql
  histogram_quantile(0.95, rate(rustfs_disk_state_check_duration_seconds_bucket[5m]))
  ```

- [ ] **Panel 4: IAM Operation Duration**
  ```promql
  histogram_quantile(0.95, rate(rustfs_config_read_duration_seconds_bucket[5m]))
  ```

- [ ] **Panel 5: Lock Health Status**
  ```promql
  rustfs_lock_health_status
  ```

- [ ] **Panel 6: Cascading Failures**
  ```promql
  rate(rustfs_cascading_failure_events_total[5m])
  ```

### Alert Validation

- [ ] **Review Active Alerts**
  - [ ] Prometheus UI: Verify 15 alerts configured
  - [ ] All alerts in "Inactive" state
  - [ ] No false positives firing

- [ ] **Test Alert Escalation**
  - [ ] Trigger test critical alert
  - [ ] Verify PagerDuty/Slack notification
  - [ ] Acknowledge and resolve test alert

---

## Post-Deployment Operations

### Documentation

- [ ] **Update Operations Wiki**
  - [ ] New alert descriptions added
  - [ ] Troubleshooting runbook linked
  - [ ] Monitoring dashboard URLs documented

- [ ] **Deployment Record**
  - [ ] Record deployment date/time
  - [ ] Document any issues encountered
  - [ ] Note baseline metrics captured
  - [ ] Update cluster configuration inventory

### Team Handoff

- [ ] **Operations Team Briefing**
  - [ ] New alert notifications reviewed
  - [ ] Troubleshooting procedures trained
  - [ ] Monitoring dashboards demoed
  - [ ] On-call escalation paths confirmed

- [ ] **Stakeholder Communication**
  - [ ] Deployment success notification sent
  - [ ] New capabilities documented (faster recovery)
  - [ ] Performance impact summary shared (~2-3% CPU, ~30-40MB memory)

---

## Monitoring & Observation Period

### First 24 Hours

- [ ] **Hour 1-4: Active Monitoring**
  - [ ] Watch Prometheus metrics continuously
  - [ ] Check logs for errors every 30 minutes
  - [ ] Verify CPU/memory stable

- [ ] **Hour 4-24: Passive Monitoring**
  - [ ] Check dashboards every 2 hours
  - [ ] Review any alerts that fired
  - [ ] Verify no performance degradation

### First Week

- [ ] **Daily Health Checks**
  - [ ] Review metrics baseline drift
  - [ ] Check for any recurring alerts
  - [ ] Verify cluster stability
  - [ ] Collect performance data

- [ ] **Weekly Review Meeting**
  - [ ] Discuss any issues encountered
  - [ ] Review alert false positive rate
  - [ ] Adjust alert thresholds if needed
  - [ ] Plan for any necessary tuning

### First Month

- [ ] **Monthly Performance Review**
  - [ ] Compare metrics to pre-deployment baseline
  - [ ] Analyze any power-off incidents
  - [ ] Measure recovery times
  - [ ] Document lessons learned

---

## Rollback Procedure

**Use if critical issues discovered**

### Quick Rollback (Per Node)

```bash
# SSH to problem node
ssh user@nodeX

# Stop service
sudo systemctl stop rustfs

# Restore old binary
sudo cp /usr/local/bin/rustfs.backup /usr/local/bin/rustfs

# Restore configuration
sudo cp -r /etc/rustfs.backup/* /etc/rustfs/

# Restart service
sudo systemctl start rustfs

# Verify rollback
rustfs --version
sudo systemctl status rustfs
```

### Full Cluster Rollback

- [ ] Stop all RustFS services cluster-wide
- [ ] Replace binaries on all nodes
- [ ] Restore Prometheus alert rules backup
- [ ] Restart all services
- [ ] Verify cluster health
- [ ] Report rollback reason to team

---

## Troubleshooting Common Issues

### Issue: Node Not Rejoining Cluster

**Symptoms**: Node shows offline after restart

**Resolution**:
```bash
# Check network connectivity
ping other-nodes

# Verify service running
sudo systemctl status rustfs

# Check logs for errors
sudo journalctl -u rustfs -n 100

# Restart if needed
sudo systemctl restart rustfs
```

### Issue: Alerts Firing Constantly

**Symptoms**: High false positive rate

**Resolution**:
1. Check actual cluster health (may be legitimate)
2. Review alert thresholds in `alerts.yml`
3. Adjust thresholds if too sensitive
4. Reload Prometheus: `systemctl reload prometheus`

### Issue: Performance Degradation

**Symptoms**: Higher CPU/memory usage

**Resolution**:
1. Check baseline metrics comparison
2. Verify expected ~2-3% CPU overhead
3. If excessive, check for memory leaks: `ps aux | grep rustfs`
4. Review logs for unexpected errors
5. Consider tuning if necessary

### Issue: Upload Failures After Deployment

**Symptoms**: Writes fail with quorum errors

**Resolution**:
1. Verify all nodes actually online: `rustfs admin info`
2. Check disk count meets quorum: Compare available vs required
3. Review circuit breaker states: Check metrics
4. Investigate node connectivity issues
5. Manually recover offline nodes if needed

---

## Success Criteria

### Deployment Considered Successful If:

- [x] All 4 nodes running new binary
- [x] All 15 Prometheus alerts loaded
- [x] Zero service downtime during rollout
- [x] Cluster health status: "healthy"
- [x] All unit tests passing
- [x] Upload test during simulated power-off succeeds
- [x] Node recovery test completes within 30s
- [x] Console remains responsive during failures
- [x] Performance overhead within acceptable range (<5% CPU)
- [x] No critical alerts firing (false positives)

### Deployment Considered Failed If:

- [ ] Any node fails to start after update
- [ ] Cluster enters split-brain state
- [ ] Persistent error logs after deployment
- [ ] Performance degradation >10%
- [ ] Critical alerts firing constantly
- [ ] Upload failures with all nodes online
- [ ] Console becomes unresponsive
- [ ] **→ Initiate rollback procedure immediately**

---

## Support Contacts

- **Development Team**: @houseme, @loverustfs
- **Operations Lead**: [Fill in]
- **On-Call Engineer**: [Fill in]
- **Escalation Path**: [Fill in]

---

## Appendix: Configuration Reference

### Environment Variables (Optional Tuning)

```bash
# /etc/rustfs/rustfs.env

# P0: HTTP keepalive tuning (default values shown)
RUSTFS_HTTP2_KEEPALIVE_INTERVAL_SECS=3
RUSTFS_HTTP2_KEEPALIVE_TIMEOUT_SECS=2
RUSTFS_TCP_KEEPALIVE_SECS=10

# P1: Operation timeouts
RUSTFS_DISK_STATE_CHECK_TIMEOUT_SECS=3
RUSTFS_IAM_CONFIG_READ_TIMEOUT_SECS=10

# P2: Lock retry configuration
RUSTFS_LOCK_RETRY_MAX_ATTEMPTS=3
RUSTFS_LOCK_RETRY_BASE_DELAY_MS=100

# P3: Circuit breaker tuning
RUSTFS_CIRCUIT_BREAKER_FAILURE_THRESHOLD=3
RUSTFS_CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS=30
```

### Prometheus Query Examples

```promql
# Circuit breaker open count
sum(rustfs_circuit_breaker_state{state="open"})

# Average disk check duration
rate(rustfs_disk_state_check_duration_seconds_sum[5m]) / 
rate(rustfs_disk_state_check_duration_seconds_count[5m])

# Lock retry rate
rate(rustfs_config_lock_retries_total[5m])

# Cascading failure detection
rustfs_cascading_failure_patterns_total
```

---

## Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2025-12-09 | Initial deployment checklist | @copilot |

---

**END OF DEPLOYMENT CHECKLIST**

*For technical questions, refer to the comprehensive solution documents in `/docs`.*
