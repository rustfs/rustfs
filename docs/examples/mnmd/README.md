# RustFS MNMD (Multi-Node Multi-Drive) Docker Example

This directory contains a complete, ready-to-use MNMD deployment example for RustFS with 4 nodes and 4 drives per node (4x4 configuration).

## Overview

This example addresses common deployment issues including:
- **VolumeNotFound errors** - Fixed by using correct disk indexing (`/data/rustfs{1...4}` instead of `/data/rustfs{0...3}`)
- **Startup race conditions** - Solved with startup coordination via `wait-and-start.sh`
- **Service discovery** - Uses Docker service names (`rustfs-node{1..4}`) instead of hard-coded IPs
- **Health checks** - Implements proper health monitoring with `nc` (with alternatives documented)

## Quick Start

From this directory (`docs/examples/mnmd`), run:

```bash
# Start the cluster
docker-compose up -d

# Check the status
docker-compose ps

# View logs
docker-compose logs -f

# Test the deployment
curl http://localhost:9000/health
curl http://localhost:9001/health

# Run comprehensive tests
./test-deployment.sh

# Stop the cluster
docker-compose down

# Clean up volumes (WARNING: deletes all data)
docker-compose down -v
```

## Configuration Details

### Volume Configuration

The example uses the following volume configuration:

```bash
RUSTFS_VOLUMES=http://rustfs-node{1...4}:9000/data/rustfs{1...4}
```

This expands to 16 endpoints (4 nodes × 4 drives):
- Node 1: `/data/rustfs1`, `/data/rustfs2`, `/data/rustfs3`, `/data/rustfs4`
- Node 2: `/data/rustfs1`, `/data/rustfs2`, `/data/rustfs3`, `/data/rustfs4`
- Node 3: `/data/rustfs1`, `/data/rustfs2`, `/data/rustfs3`, `/data/rustfs4`
- Node 4: `/data/rustfs1`, `/data/rustfs2`, `/data/rustfs3`, `/data/rustfs4`

**Important:** Disk indexing starts at 1 to match the mounted paths (`/data/rustfs1..4`).

### Port Mappings

| Node | API Port | Console Port |
|------|----------|--------------|
| node1 | 9000 | 9001 |
| node2 | 9010 | 9011 |
| node3 | 9020 | 9021 |
| node4 | 9030 | 9031 |

### Startup Coordination

The `wait-and-start.sh` script ensures:
1. All required data directories (`/data/rustfs1..4`) are created
2. Peer nodes are reachable before starting (best-effort, with 120s timeout)
3. No circular deadlocks (continues after timeout)
4. Proper command execution via `CMD` or `RUSTFS_CMD` fallback

### Health Checks

Default health check using `nc` (netcat):

```yaml
healthcheck:
  test: ["CMD-SHELL", "nc -z localhost 9000 || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 30s
```

#### Alternative Health Checks

If your base image lacks `nc`, use one of these alternatives:

**Using curl:**
```yaml
healthcheck:
  test: ["CMD-SHELL", "curl -f http://localhost:9000/health || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 30s
```

**Using wget:**
```yaml
healthcheck:
  test: ["CMD-SHELL", "wget --no-verbose --tries=1 --spider http://localhost:9000/health || exit 1"]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 30s
```

### Brace Expansion Alternatives

If your Docker Compose runtime doesn't support brace expansion (`{1...4}`), replace with explicit endpoints:

```yaml
environment:
  - RUSTFS_VOLUMES=http://rustfs-node1:9000/data/rustfs1,http://rustfs-node1:9000/data/rustfs2,http://rustfs-node1:9000/data/rustfs3,http://rustfs-node1:9000/data/rustfs4,http://rustfs-node2:9000/data/rustfs1,http://rustfs-node2:9000/data/rustfs2,http://rustfs-node2:9000/data/rustfs3,http://rustfs-node2:9000/data/rustfs4,http://rustfs-node3:9000/data/rustfs1,http://rustfs-node3:9000/data/rustfs2,http://rustfs-node3:9000/data/rustfs3,http://rustfs-node3:9000/data/rustfs4,http://rustfs-node4:9000/data/rustfs1,http://rustfs-node4:9000/data/rustfs2,http://rustfs-node4:9000/data/rustfs3,http://rustfs-node4:9000/data/rustfs4
```

## Using RUSTFS_CMD

The `RUSTFS_CMD` environment variable provides a fallback when no command is specified:

```yaml
environment:
  - RUSTFS_CMD=rustfs  # Default fallback command
```

This allows the entrypoint to execute the correct command when Docker doesn't provide one.

## Testing the Deployment

After starting the cluster, verify it's working:

### Automated Testing

Use the provided test script for comprehensive validation:

```bash
./test-deployment.sh
```

This script tests:
- Container status (4/4 running)
- Health checks (4/4 healthy)
- API endpoints (4 ports)
- Console endpoints (4 ports)
- Inter-node connectivity
- Data directory existence

### Manual Testing

For manual verification:

```bash
# 1. Check all containers are healthy
docker-compose ps

# 2. Test API endpoints
for port in 9000 9010 9020 9030; do
  echo "Testing port $port..."
  curl -s http://localhost:${port}/health | jq '.'
done

# 3. Test console endpoints
for port in 9001 9011 9021 9031; do
  echo "Testing console port $port..."
  curl -s http://localhost:${port}/health | jq '.'
done

# 4. Check inter-node connectivity
docker exec rustfs-node1 nc -zv rustfs-node2 9000
docker exec rustfs-node1 nc -zv rustfs-node3 9000
docker exec rustfs-node1 nc -zv rustfs-node4 9000
```

## Troubleshooting

### VolumeNotFound Error

**Symptom:** Error message about `/data/rustfs0` not found.

**Solution:** This example uses `/data/rustfs{1...4}` indexing to match the mounted Docker volumes. Ensure your `RUSTFS_VOLUMES` configuration starts at index 1, not 0.

### Health Check Failures

**Symptom:** Containers show as unhealthy.

**Solutions:**
1. Check if `nc` is available: `docker exec rustfs-node1 which nc`
2. Use alternative health checks (curl/wget) as documented above
3. Increase `start_period` if nodes need more time to initialize

### Startup Timeouts

**Symptom:** Services timeout waiting for peers.

**Solutions:**
1. Check logs: `docker-compose logs rustfs-node1`
2. Increase timeout in `wait-and-start.sh` (default: 120s)
3. Verify network connectivity: `docker-compose exec rustfs-node1 ping rustfs-node2`

### Permission Issues

**Symptom:** Cannot create directories or write data.

**Solution:** Ensure volumes have correct permissions or set `RUSTFS_UID` and `RUSTFS_GID` environment variables.

## Advanced Configuration

### Custom Credentials

Replace default credentials in production:

```yaml
environment:
  - RUSTFS_ACCESS_KEY=your_access_key
  - RUSTFS_SECRET_KEY=your_secret_key
```

### TLS Configuration

Add TLS certificates:

```yaml
volumes:
  - ./certs:/opt/tls:ro
environment:
  - RUSTFS_TLS_PATH=/opt/tls
```

### Resource Limits

Add resource constraints:

```yaml
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 4G
    reservations:
      cpus: '1'
      memory: 2G
```

## See Also

- [CHECKLIST.md](./CHECKLIST.md) - Step-by-step verification guide
- [../../console-separation.md](../../console-separation.md) - Console & endpoint service separation guide
- [../../../examples/docker-comprehensive.yml](../../../examples/docker-comprehensive.yml) - More deployment examples
- [Issue #618](https://github.com/rustfs/rustfs/issues/618) - Original VolumeNotFound issue

## References

- RustFS Documentation: https://rustfs.io
- Docker Compose Documentation: https://docs.docker.com/compose/
- MinIO Distributed Setup (similar architecture): https://min.io/docs/minio/linux/operations/install-deploy-manage/deploy-minio-multi-node-multi-drive.html
