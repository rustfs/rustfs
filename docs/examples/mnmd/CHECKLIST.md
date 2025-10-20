# MNMD Deployment Checklist

This checklist provides step-by-step verification for deploying RustFS in MNMD (Multi-Node Multi-Drive) mode using
Docker.

## Pre-Deployment Checks

### 1. System Requirements

- [ ] Docker Engine 20.10+ installed
- [ ] Docker Compose 2.0+ installed
- [ ] At least 8GB RAM available
- [ ] At least 40GB disk space available (for 4 nodes × 4 volumes)

Verify with:

```bash
docker --version
docker-compose --version
free -h
df -h
```

### 2. File System Checks

- [ ] Using XFS, ext4, or another suitable filesystem (not NFS for production)
- [ ] File system supports extended attributes

Verify with:

```bash
df -T | grep -E '(xfs|ext4)'
```

### 3. Permissions and SELinux

- [ ] Current user is in `docker` group or can run `sudo docker`
- [ ] SELinux is properly configured (if enabled)

Verify with:

```bash
groups | grep docker
getenforce  # If enabled, should show "Permissive" or "Enforcing" with proper policies
```

### 4. Network Configuration

- [ ] Ports 9000-9031 are available
- [ ] No firewall blocking Docker bridge network

Verify with:

```bash
# Check if ports are free
netstat -tuln | grep -E ':(9000|9001|9010|9011|9020|9021|9030|9031)'
# Should return nothing if ports are free
```

### 5. Files Present

- [ ] `docker-compose.yml` exists in current directory

Verify with:

```bash
cd docs/examples/mnmd
ls -la
chmod +x wait-and-start.sh  # If needed
```

## Deployment Steps

### 1. Start the Cluster

- [ ] Navigate to the example directory
- [ ] Pull the latest RustFS image
- [ ] Start the cluster

```bash
cd docs/examples/mnmd
docker-compose pull
docker-compose up -d
```

### 2. Monitor Startup

- [ ] Watch container logs during startup
- [ ] Verify no VolumeNotFound errors
- [ ] Check that peer discovery completes

```bash
# Watch all logs
docker-compose logs -f

# Watch specific node
docker-compose logs -f rustfs-node1

# Look for successful startup messages
docker-compose logs | grep -i "ready\|listening\|started"
```

### 3. Verify Container Status

- [ ] All 4 containers are running
- [ ] All 4 containers show as healthy

```bash
docker-compose ps

# Expected output: 4 containers in "Up" state with "healthy" status
```

### 4. Check Health Endpoints

- [ ] API health endpoints respond on all nodes
- [ ] Console health endpoints respond on all nodes

```bash
# Test API endpoints
curl http://localhost:9000/health
curl http://localhost:9010/health
curl http://localhost:9020/health
curl http://localhost:9030/health

# Test Console endpoints
curl http://localhost:9001/health
curl http://localhost:9011/health
curl http://localhost:9021/health
curl http://localhost:9031/health

# All should return successful health status
```

## Post-Deployment Verification

### 1. In-Container Checks

- [ ] Data directories exist
- [ ] Directories have correct permissions
- [ ] RustFS process is running

```bash
# Check node1
docker exec rustfs-node1 ls -la /data/
docker exec rustfs-node1 ps aux | grep rustfs

# Verify all 4 data directories exist
docker exec rustfs-node1 ls -d /data/rustfs{1..4}
```

### 2. DNS and Network Validation

- [ ] Service names resolve correctly
- [ ] Inter-node connectivity works

```bash
# DNS resolution test
docker exec rustfs-node1 nslookup rustfs-node2
docker exec rustfs-node1 nslookup rustfs-node3
docker exec rustfs-node1 nslookup rustfs-node4

# Connectivity test (using nc if available)
docker exec rustfs-node1 nc -zv rustfs-node2 9000
docker exec rustfs-node1 nc -zv rustfs-node3 9000
docker exec rustfs-node1 nc -zv rustfs-node4 9000

# Or using telnet/curl
docker exec rustfs-node1 curl -v http://rustfs-node2:9000/health
```

### 3. Volume Configuration Validation

- [ ] RUSTFS_VOLUMES environment variable is correct
- [ ] All 16 endpoints are configured (4 nodes × 4 drives)

```bash
# Check environment variable
docker exec rustfs-node1 env | grep RUSTFS_VOLUMES

# Expected output:
# RUSTFS_VOLUMES=http://rustfs-node{1...4}:9000/data/rustfs{1...4}
```

### 4. Cluster Functionality

- [ ] Can list buckets via API
- [ ] Can create a bucket
- [ ] Can upload an object
- [ ] Can download an object

```bash
# Configure AWS CLI or s3cmd
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin

# Using AWS CLI (if installed)
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
aws --endpoint-url http://localhost:9000 s3 ls
echo "test content" > test.txt
aws --endpoint-url http://localhost:9000 s3 cp test.txt s3://test-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/test.txt downloaded.txt
cat downloaded.txt

# Or using curl
curl -X PUT http://localhost:9000/test-bucket \
  -H "Host: localhost:9000" \
  --user rustfsadmin:rustfsadmin
```

### 5. Healthcheck Verification

- [ ] Docker reports all services as healthy
- [ ] Healthcheck scripts work in containers

```bash
# Check Docker health status
docker inspect rustfs-node1 --format='{{.State.Health.Status}}'
docker inspect rustfs-node2 --format='{{.State.Health.Status}}'
docker inspect rustfs-node3 --format='{{.State.Health.Status}}'
docker inspect rustfs-node4 --format='{{.State.Health.Status}}'

# All should return "healthy"

# Test healthcheck command manually
docker exec rustfs-node1 nc -z localhost 9000
echo $?  # Should be 0
```

## Troubleshooting Checks

### If VolumeNotFound Error Occurs

- [ ] Verify volume indexing starts at 1, not 0
- [ ] Check that RUSTFS_VOLUMES matches mounted paths
- [ ] Ensure all /data/rustfs{1..4} directories exist

```bash
# Check mounted volumes
docker inspect rustfs-node1 | jq '.[].Mounts'

# Verify directories in container
docker exec rustfs-node1 ls -la /data/
```

### If Healthcheck Fails

- [ ] Check if `nc` is available in the image
- [ ] Try alternative healthcheck (curl/wget)
- [ ] Increase `start_period` in docker-compose.yml

```bash
# Check if nc is available
docker exec rustfs-node1 which nc

# Test healthcheck manually
docker exec rustfs-node1 nc -z localhost 9000

# Check logs for errors
docker-compose logs rustfs-node1 | grep -i error
```

### If Startup Takes Too Long

- [ ] Check peer discovery timeout in logs
- [ ] Verify network connectivity between nodes
- [ ] Consider increasing timeout in wait-and-start.sh

```bash
# Check startup logs
docker-compose logs rustfs-node1 | grep -i "waiting\|peer\|timeout"

# Check network
docker network inspect mnmd_rustfs-mnmd
```

### If Containers Crash or Restart

- [ ] Review container logs
- [ ] Check resource usage (CPU/Memory)
- [ ] Verify no port conflicts

```bash
# View last crash logs
docker-compose logs --tail=100 rustfs-node1

# Check resource usage
docker stats --no-stream

# Check restart count
docker-compose ps
```

## Cleanup Checklist

When done testing:

- [ ] Stop the cluster: `docker-compose down`
- [ ] Remove volumes (optional, destroys data): `docker-compose down -v`
- [ ] Clean up dangling images: `docker image prune`
- [ ] Verify ports are released: `netstat -tuln | grep -E ':(9000|9001|9010|9011|9020|9021|9030|9031)'`

## Production Deployment Additional Checks

Before deploying to production:

- [ ] Change default credentials (RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY)
- [ ] Configure TLS certificates
- [ ] Set up proper logging and monitoring
- [ ] Configure backups for volumes
- [ ] Review and adjust resource limits
- [ ] Set up external load balancer (if needed)
- [ ] Document disaster recovery procedures
- [ ] Test failover scenarios
- [ ] Verify data persistence after container restart

## Summary

This checklist ensures:

- ✓ Correct disk indexing (1..4 instead of 0..3)
- ✓ Proper startup coordination via wait-and-start.sh
- ✓ Service discovery via Docker service names
- ✓ Health checks function correctly
- ✓ All 16 endpoints (4 nodes × 4 drives) are operational
- ✓ No VolumeNotFound errors occur

For more details, see [README.md](./README.md) in this directory.
