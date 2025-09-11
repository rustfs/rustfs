# Console Service Separation Documentation

This document provides comprehensive guidance on the RustFS console and endpoint service separation feature, including configuration, deployment, and troubleshooting.

## Overview

RustFS implements a complete separation between the console web interface and the S3 API endpoint service, allowing them to run on independent ports with enhanced security, flexibility, and deployment options.

## Architecture

- **S3 API Endpoint**: Handles all S3-compatible API requests (default port: 9000)
- **Console Interface**: Provides web-based management interface (default port: 9001)
- **Independent Services**: Each service has its own HTTP server, CORS configuration, and lifecycle management
- **Cross-Service Communication**: Console communicates with the endpoint via configurable external address

## Configuration Parameters

### Core Parameters

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `address` | `RUSTFS_ADDRESS` | `:9000` | S3 API endpoint bind address |
| `console_address` | `RUSTFS_CONSOLE_ADDRESS` | `:9001` | Console service bind address |
| `console_enable` | `RUSTFS_CONSOLE_ENABLE` | `true` | Enable/disable console service |
| `external_address` | `RUSTFS_EXTERNAL_ADDRESS` | - | External endpoint address for console access |

### CORS Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `cors_allowed_origins` | `RUSTFS_CORS_ALLOWED_ORIGINS` | `*` | Allowed origins for endpoint CORS |
| `console_cors_allowed_origins` | `RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS` | `*` | Allowed origins for console CORS |

### Security Parameters

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `tls_path` | `RUSTFS_TLS_PATH` | - | TLS certificate path for both services |
| `console_tls_enable` | `RUSTFS_CONSOLE_TLS_ENABLE` | `false` | Enable TLS for console server |
| `console_tls_cert` | `RUSTFS_CONSOLE_TLS_CERT` | - | TLS certificate path for console |
| `console_tls_key` | `RUSTFS_CONSOLE_TLS_KEY` | - | TLS private key path for console |
| `console_rate_limit_enable` | `RUSTFS_CONSOLE_RATE_LIMIT_ENABLE` | `false` | Enable rate limiting for console |
| `console_rate_limit_rpm` | `RUSTFS_CONSOLE_RATE_LIMIT_RPM` | `100` | Console rate limit (requests per minute) |
| `console_auth_timeout` | `RUSTFS_CONSOLE_AUTH_TIMEOUT` | `3600` | Console authentication timeout (seconds) |
| `access_key` | `RUSTFS_ACCESS_KEY` | `rustfsadmin` | Authentication access key |
| `secret_key` | `RUSTFS_SECRET_KEY` | `rustfsadmin` | Authentication secret key |

## Health Check Endpoints

Both services provide independent health check endpoints for monitoring and orchestration:

### Console Health Check
- **Endpoint**: `GET /health`
- **Response**:
```json
{
  "status": "ok",
  "service": "rustfs-console",
  "timestamp": "2024-01-15T10:30:00Z",
  "version": "0.0.5",
  "details": {
    "storage": {"status": "connected"},
    "iam": {"status": "connected"}
  },
  "uptime": 1800
}
```

### Endpoint Health Check
- **Endpoint**: `GET /health`
- **Response**:
```json
{
  "status": "ok",
  "service": "rustfs-endpoint",
  "timestamp": "2024-01-15T10:30:00Z",
  "version": "0.0.5"
}
```

## Deployment Scenarios

### 1. Standard Local Deployment

```bash
# Basic setup with default ports
rustfs /data/volume

# Access points:
# API: http://localhost:9000
# Console: http://localhost:9001/rustfs/console/
```

### 2. Custom Port Configuration

```bash
# Custom ports for both services
RUSTFS_ADDRESS=":8000" RUSTFS_CONSOLE_ADDRESS=":8080" rustfs /data/volume

# Access points:
# API: http://localhost:8000
# Console: http://localhost:8080/rustfs/console/
```

### 3. Console-Only Deployment

```bash
# Disable console for production API-only deployments
RUSTFS_CONSOLE_ENABLE=false rustfs /data/volume

# Only API available: http://localhost:9000
```

### 4. Docker Deployment with Port Mapping

#### Basic Docker Deployment
```bash
docker run -d \
  --name rustfs \
  -p 9020:9000 \  # Map API port
  -p 9021:9001 \  # Map console port
  -e RUSTFS_EXTERNAL_ADDRESS=":9020" \
  -e RUSTFS_CORS_ALLOWED_ORIGINS="http://localhost:9021" \
  rustfs/rustfs:latest

# Access points:
# API: http://localhost:9020
# Console: http://localhost:9021/rustfs/console/
```

#### Production Docker Deployment with TLS
```bash
docker run -d \
  --name rustfs-production \
  -p 9443:9001 \  # HTTPS console port
  -p 9000:9000 \  # API port
  -v /path/to/certs:/certs:ro \
  -e RUSTFS_CONSOLE_TLS_ENABLE=true \
  -e RUSTFS_CONSOLE_TLS_CERT=/certs/console.crt \
  -e RUSTFS_CONSOLE_TLS_KEY=/certs/console.key \
  -e RUSTFS_CONSOLE_RATE_LIMIT_ENABLE=true \
  -e RUSTFS_CONSOLE_RATE_LIMIT_RPM=60 \
  -e RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://admin.yourdomain.com" \
  -e RUSTFS_EXTERNAL_ADDRESS=":9000" \
  rustfs/rustfs:latest

# Access points:
# API: http://localhost:9000
# Console: https://localhost:9443/rustfs/console/
```

#### Enterprise Docker Deployment with Full Security
```bash
docker run -d \
  --name rustfs-enterprise \
  -p 9443:9001 \  # HTTPS console
  -p 9000:9000 \  # HTTP API (behind load balancer)
  -v /path/to/certs:/certs:ro \
  -v /path/to/data:/data \
  -e RUSTFS_CONSOLE_TLS_ENABLE=true \
  -e RUSTFS_CONSOLE_TLS_CERT=/certs/console.crt \
  -e RUSTFS_CONSOLE_TLS_KEY=/certs/console.key \
  -e RUSTFS_CONSOLE_RATE_LIMIT_ENABLE=true \
  -e RUSTFS_CONSOLE_RATE_LIMIT_RPM=30 \
  -e RUSTFS_CONSOLE_AUTH_TIMEOUT=1800 \
  -e RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://admin.yourdomain.com,https://backup-admin.yourdomain.com" \
  -e RUSTFS_CORS_ALLOWED_ORIGINS="https://api.yourdomain.com" \
  -e RUSTFS_ACCESS_KEY="secure-admin-key" \
  -e RUSTFS_SECRET_KEY="secure-secret-key-12345" \
  rustfs/rustfs:latest /data

# Monitoring endpoints:
# Console health: https://localhost:9443/health
# API health: http://localhost:9000/health
```

## Logging and Auditing

### Separate Logging Targets

Console and endpoint services use separate logging targets for better auditing and troubleshooting:

#### Console Logging Targets
- `rustfs::console::startup` - Server startup and configuration
- `rustfs::console::access` - HTTP access logs with timing
- `rustfs::console::error` - Console-specific errors
- `rustfs::console::shutdown` - Graceful shutdown logs
- `rustfs::console::tls` - TLS-related logs

#### Endpoint Logging Targets
- `rustfs::endpoint::startup` - API server startup
- `rustfs::endpoint::access` - S3 API access logs
- `rustfs::endpoint::error` - API-specific errors
- `rustfs::endpoint::auth` - Authentication and authorization

### Log Format Examples

```bash
# Console access log
2024-01-15T10:30:15.123Z INFO rustfs::console::access: Console access method=GET uri=/health status=200 duration_ms=5

# Console TLS log
2024-01-15T10:30:16.456Z INFO rustfs::console::tls: Console TLS handshake successful remote_addr=192.168.1.100:54321

# Console startup log  
2024-01-15T10:30:00.000Z INFO rustfs::console::startup: Starting console server address=0.0.0.0:9001 tls_enabled=true rate_limit_enabled=true
```

### Centralized Logging Configuration

For production deployments, configure centralized logging:

```bash
# JSON structured logging
RUST_LOG="rustfs::console=info,rustfs::endpoint=info" \
RUSTFS_LOG_FORMAT=json \
docker run -d rustfs/rustfs:latest

# Forward logs to external systems
docker run -d \
  --log-driver=fluentd \
  --log-opt fluentd-address=localhost:24224 \
  --log-opt tag="rustfs.{{.Name}}" \
  rustfs/rustfs:latest
```

## Security Hardening

### TLS Configuration

#### Generate Self-Signed Certificates (Development)
```bash
# Generate console TLS certificates
openssl req -x509 -newkey rsa:4096 -keyout console.key -out console.crt -days 365 -nodes \
  -subj "/C=US/ST=CA/L=SF/O=RustFS/CN=localhost"

# Use in Docker
docker run -d \
  -v $(pwd)/console.crt:/certs/console.crt:ro \
  -v $(pwd)/console.key:/certs/console.key:ro \
  -e RUSTFS_CONSOLE_TLS_ENABLE=true \
  -e RUSTFS_CONSOLE_TLS_CERT=/certs/console.crt \
  -e RUSTFS_CONSOLE_TLS_KEY=/certs/console.key \
  rustfs/rustfs:latest
```

#### Production TLS with Let's Encrypt
```bash
# Use with reverse proxy (recommended)
version: '3.8'
services:
  rustfs:
    image: rustfs/rustfs:latest
    environment:
      - RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9001
      - RUSTFS_ADDRESS=127.0.0.1:9000
    networks:
      - internal

  nginx:
    image: nginx:alpine
    ports:
      - "443:443"
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - /etc/letsencrypt:/etc/letsencrypt:ro
    networks:
      - internal
      - external
```

### Rate Limiting and DDoS Protection

```bash
# Console rate limiting configuration
RUSTFS_CONSOLE_RATE_LIMIT_ENABLE=true \
RUSTFS_CONSOLE_RATE_LIMIT_RPM=60 \  # 60 requests per minute per IP
RUSTFS_CONSOLE_AUTH_TIMEOUT=1800 \  # 30 minute session timeout
rustfs /data
```

### Network Security

#### Firewall Configuration
```bash
# Allow only necessary ports
sudo ufw allow 9000/tcp  # API endpoint
sudo ufw allow 9001/tcp  # Console (internal only)
sudo ufw deny 9001/tcp from any to any port 9001  # Block external console access

# Or use iptables
iptables -A INPUT -p tcp --dport 9000 -j ACCEPT  # API
iptables -A INPUT -p tcp --dport 9001 -s 10.0.0.0/8 -j ACCEPT  # Console internal only
```

#### Docker Network Isolation
```yaml
version: '3.8'
services:
  rustfs:
    image: rustfs/rustfs:latest
    networks:
      - api-network     # External API access
      - console-network # Internal console access
    environment:
      - RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS=https://admin.internal.com

networks:
  api-network:
    driver: bridge
  console-network:
    driver: bridge
    internal: true  # No external access
```

## Error Handling and Fallback Mechanisms

### Configuration Validation

RustFS validates configuration at startup and provides detailed error messages:

```bash
# Invalid TLS configuration
ERROR rustfs::console::startup: Console TLS cert path required when TLS is enabled

# Invalid address format  
ERROR rustfs::console::startup: Failed to parse console address "invalid:format": invalid socket address syntax

# Missing external address for Docker
WARN rustfs::console::startup: External address not configured, console may not access API in containerized environments
```

### Graceful Degradation

#### Storage Backend Failure
```json
{
  "status": "degraded",
  "service": "rustfs-console", 
  "details": {
    "storage": {"status": "disconnected"},
    "iam": {"status": "connected"}
  }
}
```

#### Service Recovery
- Console automatically retries failed connections to backend services
- Health checks report degraded status but maintain availability
- Graceful shutdown ensures no data loss during restarts

### Monitoring Integration

#### Prometheus Metrics
```bash
# Health check metrics endpoint
curl http://localhost:9001/health | jq '.status'

# Integration with monitoring
- alert: ConsoleServiceDown
  expr: up{job="rustfs-console"} == 0
  for: 30s
  labels:
    severity: critical
  annotations:
    summary: "RustFS Console service is down"
```

#### Kubernetes Health Checks
```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: rustfs
    image: rustfs/rustfs:latest
    livenessProbe:
      httpGet:
        path: /health
        port: 9001
      initialDelaySeconds: 30
      periodSeconds: 10
    readinessProbe:
      httpGet:
        path: /health  
        port: 9001
      initialDelaySeconds: 5
      periodSeconds: 5
```
```

### 5. Docker Compose Deployment

```yaml
version: "3.8"
services:
  rustfs:
    image: rustfs/rustfs:latest
    ports:
      - "9020:9000"  # API port mapping
      - "9021:9001"  # Console port mapping
    environment:
      - RUSTFS_ADDRESS=0.0.0.0:9000
      - RUSTFS_CONSOLE_ADDRESS=0.0.0.0:9001
      - RUSTFS_EXTERNAL_ADDRESS=:9020  # Must match mapped API port
      - RUSTFS_CORS_ALLOWED_ORIGINS=http://localhost:9021
      - RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS=*
      - RUSTFS_CONSOLE_ENABLE=true
    volumes:
      - rustfs_data:/data
```

### 6. Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustfs
spec:
  replicas: 1
  selector:
    matchLabels:
      app: rustfs
  template:
    metadata:
      labels:
        app: rustfs
    spec:
      containers:
      - name: rustfs
        image: rustfs/rustfs:latest
        ports:
        - containerPort: 9000
          name: api
        - containerPort: 9001
          name: console
        env:
        - name: RUSTFS_ADDRESS
          value: "0.0.0.0:9000"
        - name: RUSTFS_CONSOLE_ADDRESS
          value: "0.0.0.0:9001"
        - name: RUSTFS_EXTERNAL_ADDRESS
          value: ":9000"  # Use service port
        - name: RUSTFS_CORS_ALLOWED_ORIGINS
          value: "*"
        - name: RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS
          value: "*"
---
apiVersion: v1
kind: Service
metadata:
  name: rustfs-service
spec:
  selector:
    app: rustfs
  ports:
  - name: api
    port: 9000
    targetPort: 9000
  - name: console
    port: 9001
    targetPort: 9001
  type: LoadBalancer
```

## Security Configuration

### 1. CORS Configuration

#### Restrictive CORS for Production
```bash
# Endpoint CORS - restrict to specific domains
RUSTFS_CORS_ALLOWED_ORIGINS="https://app.example.com,https://api.example.com"

# Console CORS - restrict console access
RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://admin.example.com"
```

#### Development CORS
```bash
# Allow all origins for development
RUSTFS_CORS_ALLOWED_ORIGINS="*"
RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="*"
```

### 2. TLS/HTTPS Configuration

```bash
# Enable TLS for both services
RUSTFS_TLS_PATH="/path/to/certs"

# Certificate files required:
# /path/to/certs/cert.pem
# /path/to/certs/key.pem
```

### 3. Network Security

#### Firewall Configuration
```bash
# Allow API access from all networks
sudo ufw allow 9000/tcp

# Restrict console access to internal networks only
sudo ufw allow from 192.168.1.0/24 to any port 9001
sudo ufw allow from 10.0.0.0/8 to any port 9001
```

#### Reverse Proxy Setup (Nginx)
```nginx
# API endpoint - public access
server {
    listen 80;
    server_name api.example.com;
    
    location / {
        proxy_pass http://localhost:9000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}

# Console - restricted access with authentication
server {
    listen 80;
    server_name admin.example.com;
    
    # Basic authentication
    auth_basic "RustFS Admin";
    auth_basic_user_file /etc/nginx/.htpasswd;
    
    location / {
        proxy_pass http://localhost:9001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Health Checks

Both services provide independent health check endpoints:

### API Endpoint Health Check
```bash
curl http://localhost:9000/health
# Response: {"status":"ok","service":"rustfs-api","timestamp":"..."}
```

### Console Health Check
```bash
curl http://localhost:9001/health
# Response: {"status":"ok","service":"rustfs-console","timestamp":"..."}
```

### Docker Health Checks
```dockerfile
# In Dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:9000/health && curl -f http://localhost:9001/health || exit 1
```

## Troubleshooting

### 1. Console Cannot Access API

**Problem**: Console shows connection errors or cannot load data.

**Solution**: Check `RUSTFS_EXTERNAL_ADDRESS` configuration.

```bash
# For Docker with port mapping 9020:9000
RUSTFS_EXTERNAL_ADDRESS=":9020"  # Must match the mapped host port

# For direct access
RUSTFS_EXTERNAL_ADDRESS=":9000"  # Must match the API service port
```

### 2. CORS Errors

**Problem**: Browser console shows CORS policy errors.

**Solutions**:

```bash
# Allow specific origins
RUSTFS_CORS_ALLOWED_ORIGINS="http://localhost:9001,https://admin.example.com"

# For development, allow all origins
RUSTFS_CORS_ALLOWED_ORIGINS="*"

# Check browser network tab for actual request origins
```

### 3. Port Conflicts

**Problem**: "Address already in use" errors.

**Solutions**:

```bash
# Check which process is using the port
sudo lsof -i :9000
sudo lsof -i :9001

# Use different ports
RUSTFS_ADDRESS=":8000" RUSTFS_CONSOLE_ADDRESS=":8001"
```

### 4. Service Not Starting

**Problem**: Console or API service fails to start.

**Debugging**:

```bash
# Enable debug logging
RUST_LOG=debug rustfs /data/volume

# Check service-specific logs
journalctl -f | grep -i "console\|endpoint"

# Verify configuration
rustfs --help
```

### 5. Docker Networking Issues

**Problem**: Services cannot communicate in Docker environment.

**Solutions**:

```bash
# Check container network
docker inspect rustfs-container

# Verify port mapping
docker port rustfs-container

# Check container logs
docker logs rustfs-container

# Test connectivity from host
curl http://localhost:9020/health  # API
curl http://localhost:9021/health  # Console
```

## Migration Guide

### From Single-Port to Separated Services

1. **Update Configuration**:
   ```bash
   # Old single-port setup
   RUSTFS_ADDRESS=":9000"
   
   # New separated setup
   RUSTFS_ADDRESS=":9000"           # API port
   RUSTFS_CONSOLE_ADDRESS=":9001"   # Console port
   RUSTFS_EXTERNAL_ADDRESS=":9000"  # For console->API communication
   ```

2. **Update Firewall Rules**:
   ```bash
   sudo ufw allow 9001/tcp  # Allow console port
   ```

3. **Update Application URLs**:
   - API: `http://localhost:9000` (unchanged)
   - Console: `http://localhost:9001/rustfs/console/` (new)

4. **Update Docker Deployments**:
   ```bash
   # Add console port mapping
   docker run -p 9000:9000 -p 9001:9001 rustfs/rustfs:latest
   ```

## Best Practices

### 1. Production Deployment
- Use specific CORS origins instead of `*`
- Enable TLS for both services
- Restrict console access to internal networks
- Use reverse proxy for additional security
- Implement proper monitoring and health checks

### 2. Development Environment
- Use permissive CORS settings (`*`)
- Enable debug logging
- Use docker-compose for consistent environment

### 3. Security Hardening
- Change default credentials
- Use environment-specific TLS certificates
- Implement network segmentation
- Regular security updates
- Monitor access logs

### 4. Monitoring and Observability
- Set up health check endpoints
- Monitor both services independently
- Use structured logging
- Implement metrics collection
- Set up alerting for service failures

## Example Configurations

### Production-Ready Setup

```bash
#!/bin/bash
# production-deploy.sh

export RUSTFS_ADDRESS="0.0.0.0:9000"
export RUSTFS_CONSOLE_ADDRESS="127.0.0.1:9001"  # Internal only
export RUSTFS_CONSOLE_ENABLE="true"
export RUSTFS_EXTERNAL_ADDRESS=":9000"
export RUSTFS_CORS_ALLOWED_ORIGINS="https://app.example.com"
export RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://admin.example.com"
export RUSTFS_TLS_PATH="/etc/rustfs/certs"
export RUSTFS_ACCESS_KEY="$(openssl rand -hex 32)"
export RUSTFS_SECRET_KEY="$(openssl rand -hex 32)"

rustfs /data/rustfs-production
```

### Development Setup

```bash
#!/bin/bash
# dev-setup.sh

export RUSTFS_ADDRESS=":9000"
export RUSTFS_CONSOLE_ADDRESS=":9001"
export RUSTFS_CONSOLE_ENABLE="true"
export RUSTFS_EXTERNAL_ADDRESS=":9000"
export RUSTFS_CORS_ALLOWED_ORIGINS="*"
export RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="*"
export RUST_LOG="debug"

rustfs /data/rustfs-dev
```

This documentation provides comprehensive coverage of the console separation feature, addressing all the requirements mentioned in the improvement request.

## Configuration

### Environment Variables

- `RUSTFS_CONSOLE_ENABLE`: Enable or disable the console service (default: `true`)
- `RUSTFS_CONSOLE_ADDRESS`: Console server bind address (default: `:9001`)
- `RUSTFS_ADDRESS`: Main endpoint server bind address (default: `:9000`)
- `RUSTFS_EXTERNAL_ADDRESS`: External address for console to access endpoint in Docker deployments

### Command Line Arguments

```bash
rustfs --console-enable true --console-address ":9001" --address ":9000" /data/volume
```

## Usage Examples

### 1. Default Configuration (Separated Services)

```bash
# Start RustFS with console on separate port
rustfs /data/volume

# Access:
# - S3 API: http://localhost:9000
# - WebUI Console: http://localhost:9001/rustfs/console/index.html
```

### 2. Disable Console

```bash
# Run without console
RUSTFS_CONSOLE_ENABLE=false rustfs /data/volume

# Only S3 API available: http://localhost:9000
```

### 3. Custom Console Port

```bash
# Console on port 8080
RUSTFS_CONSOLE_ADDRESS=":8080" rustfs /data/volume

# Access WebUI at: http://localhost:8080/rustfs/console/index.html
```

## Docker Deployment

### Standard Docker Run

```bash
docker run -d \
  --name rustfs \
  -p 9020:9000 \
  -p 9021:9001 \
  -e RUSTFS_CONSOLE_ADDRESS=":9001" \
  -e RUSTFS_ADDRESS=":9000" \
  -e RUSTFS_EXTERNAL_ADDRESS=":9020" \
  -e RUSTFS_VOLUMES="/data" \
  rustfs/rustfs:latest
```

Access:
- S3 API: `http://localhost:9020`
- WebUI Console: `http://localhost:9021/rustfs/console/index.html`

### Docker Compose

```yaml
version: "3.8"
services:
  rustfs:
    image: rustfs/rustfs:latest
    ports:
      - "9000:9000"  # S3 API
      - "9001:9001"  # Console
    environment:
      - RUSTFS_ADDRESS=0.0.0.0:9000
      - RUSTFS_CONSOLE_ADDRESS=0.0.0.0:9001
      - RUSTFS_CONSOLE_ENABLE=true
      - RUSTFS_VOLUMES=/data
    volumes:
      - rustfs_data:/data
```

## Security Considerations

1. **Port Isolation**: Console and API services run on separate ports, allowing for different security policies
2. **Firewall Rules**: You can restrict console port access while keeping API port open
3. **CORS Configuration**: Cross-origin requests between console and API are handled automatically
4. **Disable Console**: For production deployments, consider disabling console entirely

## Migration from Previous Versions

Previous versions served the console from the same port as the S3 API. This change:

- **Maintains backward compatibility** when `console_enable=false`
- **Separates services** for better security and deployment flexibility
- **Requires port mapping updates** in Docker deployments to access console
- **Provides cleaner service boundaries** between management and data interfaces

## Troubleshooting

### Console Can't Access API

1. Check `RUSTFS_EXTERNAL_ADDRESS` is set correctly for Docker deployments
2. Verify CORS is working (should be automatic)
3. Ensure both services are running on expected ports

### Port Conflicts

1. Change console port: `RUSTFS_CONSOLE_ADDRESS=":8080"`
2. Check for other services using default ports 9000/9001

### Console Not Starting

1. Check console is enabled: `RUSTFS_CONSOLE_ENABLE=true`
2. Verify console address binding: check logs for binding errors
3. Ensure console port is not in use by another service