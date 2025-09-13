# RustFS Console & Endpoint Service Separation Guide

This document provides comprehensive guidance on RustFS's console and endpoint service separation architecture, enabling independent deployment of the web management interface and S3 API service with enterprise-grade security, monitoring, and Docker deployment standards.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)  
- [Quick Start](#quick-start)
- [Configuration Reference](#configuration-reference)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Security Hardening](#security-hardening)
- [Health Monitoring](#health-monitoring)
- [Troubleshooting](#troubleshooting)
- [Migration Guide](#migration-guide)
- [Best Practices](#best-practices)

## Overview

RustFS implements complete separation between the console web interface and the S3 API endpoint service, enabling:

- **Independent Port Management**: Console (`:9001`) and API (`:9000`) run on separate ports
- **Enhanced Security**: Different CORS policies, TLS configurations, and access controls
- **Flexible Deployment**: Console can be disabled or restricted to internal networks
- **Docker-Native**: Optimized for containerized deployments with proper port mapping
- **Enterprise Ready**: Rate limiting, authentication timeouts, and comprehensive monitoring

## Architecture

### Service Components

- **S3 API Endpoint** (Port 9000)
  - Handles all S3-compatible API requests
  - Independent CORS configuration via `RUSTFS_CORS_ALLOWED_ORIGINS`
  - Health check endpoint: `GET /health`
  - Production-ready with comprehensive error handling

- **Console Interface** (Port 9001)
  - Web-based management dashboard at `/rustfs/console/`
  - Independent CORS configuration via `RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS` 
  - TLS support using shared certificate infrastructure
  - Rate limiting and authentication timeout controls
  - Health check endpoint: `GET /health`

### Communication Flow

```
Browser → Console (9001) → API Endpoint (9000) → Storage Backend
                ↓
        External Address Configuration
        (RUSTFS_EXTERNAL_ADDRESS)
```

The console communicates with the API endpoint using the `RUSTFS_EXTERNAL_ADDRESS` parameter, which is critical for Docker deployments with port mapping.

## Quick Start

### Local Development

```bash
# Start with default configuration
rustfs /data/volume

# Access points:
# API: http://localhost:9000
# Console: http://localhost:9001/rustfs/console/
```

### Docker Quick Start

```bash
# Basic Docker deployment
docker run -d \
  --name rustfs \
  -p 9020:9000 -p 9021:9001 \
  -e RUSTFS_EXTERNAL_ADDRESS=":9020" \
  rustfs/rustfs:latest

# Access points:
# API: http://localhost:9020  
# Console: http://localhost:9021/rustfs/console/
```

### Production Quick Start

Use our enhanced deployment script for production-ready setup:

```bash
# Use the enhanced security deployment script
./examples/enhanced-security-deployment.sh

# Or customize the enhanced Docker deployment
./examples/enhanced-docker-deployment.sh prod
```

## Configuration Reference

### Core Service Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `address` | `RUSTFS_ADDRESS` | `:9000` | S3 API endpoint bind address |
| `console_address` | `RUSTFS_CONSOLE_ADDRESS` | `:9001` | Console service bind address |
| `console_enable` | `RUSTFS_CONSOLE_ENABLE` | `true` | Enable/disable console service |
| `external_address` | `RUSTFS_EXTERNAL_ADDRESS` | `:9000` | External endpoint address for console→API communication |

### CORS Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `cors_allowed_origins` | `RUSTFS_CORS_ALLOWED_ORIGINS` | `*` | Comma-separated allowed origins for endpoint CORS |
| `console_cors_allowed_origins` | `RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS` | `*` | Comma-separated allowed origins for console CORS |

### Security Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `tls_path` | `RUSTFS_TLS_PATH` | - | TLS certificate directory path (shared by both services) |
| `console_rate_limit_enable` | `RUSTFS_CONSOLE_RATE_LIMIT_ENABLE` | `false` | Enable rate limiting for console access |
| `console_rate_limit_rpm` | `RUSTFS_CONSOLE_RATE_LIMIT_RPM` | `100` | Console rate limit (requests per minute) |
| `console_auth_timeout` | `RUSTFS_CONSOLE_AUTH_TIMEOUT` | `3600` | Console authentication timeout (seconds) |

### Authentication Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `access_key` | `RUSTFS_ACCESS_KEY` | `rustfsadmin` | Administrative access key |
| `secret_key` | `RUSTFS_SECRET_KEY` | `rustfsadmin` | Administrative secret key |

### Important Notes

- **External Address**: Critical for Docker deployments. Must match the host-mapped API port.
- **TLS Configuration**: Console uses shared TLS certificates from `RUSTFS_TLS_PATH` (no separate cert config needed).
- **Environment Priority**: Console security settings are read directly from environment variables.

## Docker Deployment

### Prerequisites

Ensure Docker is installed and the RustFS image is available:

```bash
# Pull the latest RustFS image
docker pull rustfs/rustfs:latest

# Or build from source
docker build -t rustfs/rustfs:latest .
```

### Basic Docker Deployment

Simple deployment with port mapping:

```bash
docker run -d \
  --name rustfs-basic \
  -p 9020:9000 \  # API: host 9020 → container 9000
  -p 9021:9001 \  # Console: host 9021 → container 9001
  -e RUSTFS_EXTERNAL_ADDRESS=":9020" \  # Critical: must match host API port
  -e RUSTFS_CORS_ALLOWED_ORIGINS="http://localhost:9021" \
  -v rustfs-data:/data \
  rustfs/rustfs:latest

# Access:
# API: http://localhost:9020
# Console: http://localhost:9021/rustfs/console/
```

### Docker Compose Deployment

Use the provided `docker-compose.yml` for complete setup:

```bash
# Start the complete stack
docker-compose up -d

# Start with specific profiles
docker-compose --profile dev up -d          # Development environment
docker-compose --profile observability up -d # With monitoring stack
```

The compose configuration provides:

- **Production Service** (`rustfs`): Ports 9000:9000 and 9001:9001
- **Development Service** (`rustfs-dev`): Ports 9010:9000 and 9011:9001  
- **Observability Stack**: Grafana, Prometheus, Jaeger, and OpenTelemetry
- **Reverse Proxy**: Nginx configuration for production deployments

### Enhanced Docker Deployment Scripts

#### Production Deployment with Security

```bash
# Use the enhanced security deployment script
./examples/enhanced-security-deployment.sh

# This will:
# - Generate TLS certificates
# - Create secure credentials  
# - Deploy with rate limiting
# - Configure restricted CORS
# - Enable health monitoring
```

#### Multiple Environment Deployment

```bash
# Deploy different environments simultaneously
./examples/enhanced-docker-deployment.sh all

# Individual deployments:
./examples/enhanced-docker-deployment.sh basic  # Basic setup
./examples/enhanced-docker-deployment.sh dev    # Development environment  
./examples/enhanced-docker-deployment.sh prod   # Production-like setup
```

### Custom Docker Deployment Examples

#### Development Environment

```bash
docker run -d \
  --name rustfs-dev \
  -p 9000:9000 -p 9001:9001 \
  -e RUSTFS_EXTERNAL_ADDRESS=":9000" \
  -e RUSTFS_CORS_ALLOWED_ORIGINS="*" \
  -e RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="*" \
  -e RUSTFS_ACCESS_KEY="dev-admin" \
  -e RUSTFS_SECRET_KEY="dev-secret" \
  -e RUST_LOG="debug" \
  -v rustfs-dev-data:/data \
  rustfs/rustfs:latest
```

#### Production with TLS and Security

```bash
docker run -d \
  --name rustfs-production \
  -p 9443:9001 -p 9000:9000 \
  -v /path/to/certs:/certs:ro \
  -v /path/to/data:/data \
  -e RUSTFS_TLS_PATH="/certs" \
  -e RUSTFS_CONSOLE_RATE_LIMIT_ENABLE="true" \
  -e RUSTFS_CONSOLE_RATE_LIMIT_RPM="60" \
  -e RUSTFS_CONSOLE_AUTH_TIMEOUT="1800" \
  -e RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://admin.yourdomain.com" \
  -e RUSTFS_CORS_ALLOWED_ORIGINS="https://api.yourdomain.com" \
  -e RUSTFS_ACCESS_KEY="$(openssl rand -hex 16)" \
  -e RUSTFS_SECRET_KEY="$(openssl rand -hex 32)" \
  rustfs/rustfs:latest
```

#### Console-Disabled API-Only Deployment

```bash
docker run -d \
  --name rustfs-api-only \
  -p 9000:9000 \
  -e RUSTFS_CONSOLE_ENABLE="false" \
  -e RUSTFS_CORS_ALLOWED_ORIGINS="https://your-app.com" \
  -v rustfs-api-data:/data \
  rustfs/rustfs:latest

# Only API available: http://localhost:9000
```

### Docker Health Checks

The Dockerfile includes health checks for both services:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:9000/health && curl -f http://localhost:9001/health || exit 1
```

Check container health:

```bash
# View health status
docker ps --format "table {{.Names}}\t{{.Status}}"

# View detailed health check logs
docker inspect rustfs --format='{{json .State.Health}}' | jq
```

## Kubernetes Deployment

### Basic Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustfs
  labels:
    app: rustfs
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
          value: ":9000"
        - name: RUSTFS_CORS_ALLOWED_ORIGINS
          value: "*"
        - name: RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS
          value: "*"
        livenessProbe:
          httpGet:
            path: /health
            port: 9000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 9001
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: rustfs-data

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

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: rustfs-data
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

### Production Kubernetes with TLS

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustfs-production
spec:
  replicas: 3
  selector:
    matchLabels:
      app: rustfs-production
  template:
    metadata:
      labels:
        app: rustfs-production
    spec:
      containers:
      - name: rustfs
        image: rustfs/rustfs:latest
        env:
        - name: RUSTFS_TLS_PATH
          value: "/certs"
        - name: RUSTFS_CONSOLE_RATE_LIMIT_ENABLE
          value: "true"
        - name: RUSTFS_CONSOLE_RATE_LIMIT_RPM
          value: "100"
        - name: RUSTFS_CONSOLE_AUTH_TIMEOUT
          value: "1800"
        - name: RUSTFS_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: rustfs-credentials
              key: access-key
        - name: RUSTFS_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: rustfs-credentials
              key: secret-key
        volumeMounts:
        - name: certs
          mountPath: /certs
          readOnly: true
        - name: data
          mountPath: /data
      volumes:
      - name: certs
        secret:
          secretName: rustfs-tls
      - name: data
        persistentVolumeClaim:
          claimName: rustfs-production-data

---
apiVersion: v1
kind: Secret
metadata:
  name: rustfs-credentials
type: Opaque
stringData:
  access-key: "your-secure-access-key"
  secret-key: "your-secure-secret-key"
```

### Ingress Configuration

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: rustfs-ingress
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/cors-allow-origin: "https://admin.yourdomain.com"
spec:
  tls:
  - hosts:
    - api.yourdomain.com
    - admin.yourdomain.com
    secretName: rustfs-tls-ingress
  rules:
  - host: api.yourdomain.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: rustfs-service
            port:
              number: 9000
  - host: admin.yourdomain.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: rustfs-service
            port:
              number: 9001
```

## Security Hardening

### TLS Configuration

RustFS console uses shared TLS certificate infrastructure. Place certificates in a directory and configure via `RUSTFS_TLS_PATH`:

#### Certificate Requirements

```bash
# Certificate directory structure
/path/to/certs/
├── cert.pem      # TLS certificate  
└── key.pem       # Private key
```

#### Generate Self-Signed Certificates (Development)

```bash
# Generate development certificates
mkdir -p ./certs
openssl req -x509 -newkey rsa:4096 \
  -keyout ./certs/key.pem \
  -out ./certs/cert.pem \
  -days 365 -nodes \
  -subj "/C=US/ST=CA/L=SF/O=RustFS/CN=localhost"

# Set proper permissions
chmod 600 ./certs/key.pem
chmod 644 ./certs/cert.pem
```

#### Production TLS with Let's Encrypt

```bash
# Use certbot to generate certificates
certbot certonly --standalone -d yourdomain.com
cp /etc/letsencrypt/live/yourdomain.com/fullchain.pem ./certs/cert.pem
cp /etc/letsencrypt/live/yourdomain.com/privkey.pem ./certs/key.pem
```

### Rate Limiting and Authentication

Configure console security settings via environment variables:

```bash
# Enable rate limiting and configure timeouts
export RUSTFS_CONSOLE_RATE_LIMIT_ENABLE=true
export RUSTFS_CONSOLE_RATE_LIMIT_RPM=60        # 60 requests per minute
export RUSTFS_CONSOLE_AUTH_TIMEOUT=1800       # 30 minutes session timeout

# Start with security settings
docker run -d \
  -e RUSTFS_CONSOLE_RATE_LIMIT_ENABLE=true \
  -e RUSTFS_CONSOLE_RATE_LIMIT_RPM=60 \
  -e RUSTFS_CONSOLE_AUTH_TIMEOUT=1800 \
  rustfs/rustfs:latest
```

### CORS Security

Configure restrictive CORS policies for production:

```bash
# Production CORS configuration
export RUSTFS_CORS_ALLOWED_ORIGINS="https://myapp.com,https://api.myapp.com"
export RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://admin.myapp.com"

# Development CORS (permissive)
export RUSTFS_CORS_ALLOWED_ORIGINS="*"
export RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="*"
```

### Network Security

#### Firewall Configuration

```bash
# Allow API access from all networks
sudo ufw allow 9000/tcp

# Restrict console access to internal networks only  
sudo ufw allow from 192.168.1.0/24 to any port 9001
sudo ufw allow from 10.0.0.0/8 to any port 9001

# Block external console access
sudo ufw deny 9001/tcp
```

#### Docker Network Isolation

```yaml
# docker-compose.yml with network isolation
version: '3.8'
services:
  rustfs:
    image: rustfs/rustfs:latest
    networks:
      - api-network      # Public API access
      - console-network  # Internal console access
    environment:
      - RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS=https://admin.internal.com

networks:
  api-network:
    driver: bridge
  console-network:
    driver: bridge
    internal: true  # No external access
```

#### Reverse Proxy Setup

Use Nginx for additional security layer:

```nginx
# /etc/nginx/sites-available/rustfs
# API endpoint - public access
server {
    listen 80;
    server_name api.example.com;
    
    location / {
        proxy_pass http://localhost:9000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        
        # Rate limiting
        limit_req zone=api burst=20 nodelay;
    }
}

# Console - restricted access with authentication
server {
    listen 443 ssl;
    server_name admin.example.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    # Basic authentication
    auth_basic "RustFS Admin";
    auth_basic_user_file /etc/nginx/.htpasswd;
    
    # IP whitelist
    allow 192.168.1.0/24;
    allow 10.0.0.0/8;
    deny all;
    
    location / {
        proxy_pass http://localhost:9001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## Health Monitoring

### Health Check Endpoints

Both services provide independent health check endpoints:

#### Console Health Check

- **Endpoint**: `GET /health`
- **Response**:

```json
{
  "status": "ok",
  "service": "rustfs-console",
  "timestamp": "2024-01-15T10:30:00Z",
  "version": "0.0.5",
  "details": {
    "storage": {
      "status": "connected"
    },
    "iam": {
      "status": "connected"  
    }
  },
  "uptime": 1800
}
```

#### Endpoint Health Check

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

### Monitoring Integration

#### Prometheus Metrics

```bash
# Health check monitoring
curl http://localhost:9000/health | jq '.status'
curl http://localhost:9001/health | jq '.status'

# Prometheus alert rules
- alert: RustFSConsoleDown
  expr: up{job="rustfs-console"} == 0
  for: 30s
  labels:
    severity: critical
  annotations:
    summary: "RustFS Console service is down"

- alert: RustFSEndpointDown
  expr: up{job="rustfs-endpoint"} == 0
  for: 30s
  labels:
    severity: critical
  annotations:
    summary: "RustFS API Endpoint is down"
```

#### Docker Health Checks

Built-in Docker health checks are configured in the Dockerfile:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:9000/health && curl -f http://localhost:9001/health || exit 1
```

Check health status:

```bash
# View health status
docker ps --format "table {{.Names}}\t{{.Status}}"

# Detailed health information
docker inspect rustfs --format='{{json .State.Health}}' | jq
```

### Logging and Auditing

#### Separate Logging Targets

Console and endpoint services use separate logging targets:

**Console Logging Targets:**
- `rustfs::console::startup` - Server startup and configuration
- `rustfs::console::access` - HTTP access logs with timing
- `rustfs::console::error` - Console-specific errors  
- `rustfs::console::shutdown` - Graceful shutdown logs

**Endpoint Logging Targets:**
- `rustfs::endpoint::startup` - API server startup
- `rustfs::endpoint::access` - S3 API access logs
- `rustfs::endpoint::auth` - Authentication and authorization

#### Centralized Logging

```bash
# JSON structured logging
RUST_LOG="rustfs::console=info,rustfs::endpoint=info" \
docker run -d rustfs/rustfs:latest

# Forward to log aggregation
docker run -d \
  --log-driver=fluentd \
  --log-opt fluentd-address=localhost:24224 \
  --log-opt tag="rustfs.{{.Name}}" \
  rustfs/rustfs:latest
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Console Cannot Access API

**Symptoms**: Console UI shows connection errors, "Failed to load data" messages.

**Cause**: Incorrect `RUSTFS_EXTERNAL_ADDRESS` configuration.

**Solutions**:

```bash
# For Docker with port mapping 9020:9000 (API) and 9021:9001 (Console)
RUSTFS_EXTERNAL_ADDRESS=":9020"  # Must match the mapped host API port

# For direct access without port mapping
RUSTFS_EXTERNAL_ADDRESS=":9000"  # Must match the API service port

# For Kubernetes or complex networking
RUSTFS_EXTERNAL_ADDRESS="http://rustfs-service:9000"  # Use service name
```

**Debug steps**:
```bash
# Test API connectivity from console container
docker exec rustfs-container curl http://localhost:9000/health

# Check CORS configuration
curl -H "Origin: http://localhost:9021" -v http://localhost:9020/health
```

#### 2. CORS Errors

**Symptoms**: Browser console shows "Access to fetch blocked by CORS policy" errors.

**Causes and Solutions**:

```bash
# Allow specific origins (production)
RUSTFS_CORS_ALLOWED_ORIGINS="https://admin.yourdomain.com,https://backup.yourdomain.com"
RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://console.yourdomain.com"

# Allow all origins (development only)
RUSTFS_CORS_ALLOWED_ORIGINS="*"
RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="*"

# Docker deployment with port mapping
RUSTFS_CORS_ALLOWED_ORIGINS="http://localhost:9021,http://127.0.0.1:9021"
```

**Debug CORS issues**:
```bash
# Check actual request origin in browser network tab
# Ensure the origin matches CORS configuration

# Test CORS with curl
curl -H "Origin: http://localhost:9021" \
     -H "Access-Control-Request-Method: GET" \
     -H "Access-Control-Request-Headers: authorization" \
     -X OPTIONS \
     http://localhost:9020/
```

#### 3. Port Conflicts

**Symptoms**: "Address already in use" or "bind: address already in use" errors.

**Solutions**:

```bash
# Check which process is using the port
sudo lsof -i :9000
sudo lsof -i :9001
sudo netstat -tulpn | grep :9000

# Kill conflicting process
sudo kill -9 <PID>

# Use different ports
RUSTFS_ADDRESS=":8000" RUSTFS_CONSOLE_ADDRESS=":8001" rustfs /data

# For Docker, change host port mapping
docker run -p 8020:9000 -p 8021:9001 rustfs/rustfs:latest
```

#### 4. TLS Certificate Issues

**Symptoms**: "TLS handshake failed", "certificate verify failed" errors.

**Solutions**:

```bash
# Verify certificate files exist and are readable
ls -la /path/to/certs/
# Should show cert.pem and key.pem with proper permissions

# Test certificate validity
openssl x509 -in /path/to/certs/cert.pem -text -noout

# Generate new certificates
openssl req -x509 -newkey rsa:4096 \
  -keyout /path/to/certs/key.pem \
  -out /path/to/certs/cert.pem \
  -days 365 -nodes \
  -subj "/C=US/O=RustFS/CN=localhost"

# For Docker, ensure certificate volume mount is correct
docker run -v /host/path/to/certs:/certs:ro rustfs/rustfs:latest
```

#### 5. Service Not Starting

**Symptoms**: Container exits immediately, "failed to start console server" errors.

**Debug steps**:

```bash
# Check container logs
docker logs rustfs-container

# Enable debug logging
docker run -e RUST_LOG=debug rustfs/rustfs:latest

# Check configuration
docker exec rustfs-container env | grep RUSTFS

# Test configuration outside Docker
RUST_LOG=debug rustfs --help
```

#### 6. Health Check Failures

**Symptoms**: Docker health checks fail, Kubernetes pods not ready.

**Solutions**:

```bash
# Test health endpoints manually
curl http://localhost:9000/health
curl http://localhost:9001/health

# Check if services are listening
docker exec rustfs-container netstat -tulpn

# Increase health check timeouts
# For Docker
HEALTHCHECK --interval=30s --timeout=30s --retries=5

# For Kubernetes  
livenessProbe:
  initialDelaySeconds: 60
  timeoutSeconds: 30
```

#### 7. Docker Network Issues

**Symptoms**: Services cannot communicate within Docker network.

**Solutions**:

```bash
# Check Docker network
docker network ls
docker inspect <network-name>

# Test connectivity between containers
docker exec container1 ping container2
docker exec container1 curl http://container2:9000/health

# Use Docker network aliases
docker run --network=my-network --network-alias=rustfs rustfs/rustfs:latest
```

### Debugging Commands

#### Service Status

```bash
# Check running containers
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Check service logs
docker logs rustfs-container --tail=100 -f

# Check resource usage
docker stats rustfs-container

# Inspect container configuration
docker inspect rustfs-container | jq '.Config.Env'
```

#### Network Debugging

```bash
# Test connectivity from host
curl -v http://localhost:9020/health
curl -v http://localhost:9021/health

# Test from inside container
docker exec rustfs-container curl http://localhost:9000/health
docker exec rustfs-container curl http://localhost:9001/health

# Check port listening
docker exec rustfs-container netstat -tulpn | grep -E ':(9000|9001)'
```

#### Configuration Debugging

```bash
# Show effective configuration
docker exec rustfs-container env | grep RUSTFS | sort

# Test configuration parsing
docker exec rustfs-container rustfs --help

# Check file permissions
docker exec rustfs-container ls -la /certs/
docker exec rustfs-container ls -la /data/
```

### Getting Help

#### Log Collection

```bash
# Collect comprehensive logs
mkdir -p ./debug-logs
docker logs rustfs-container > ./debug-logs/container.log 2>&1
docker inspect rustfs-container > ./debug-logs/inspect.json
docker exec rustfs-container env > ./debug-logs/environment.txt
docker exec rustfs-container ps aux > ./debug-logs/processes.txt
docker exec rustfs-container netstat -tulpn > ./debug-logs/network.txt
```

#### Community Support

- **GitHub Issues**: [rustfs/rustfs/issues](https://github.com/rustfs/rustfs/issues)
- **Discussions**: [rustfs/rustfs/discussions](https://github.com/rustfs/rustfs/discussions)
- **Documentation**: Check the `docs/` directory for additional guides

## Migration Guide

### From Previous Versions

Previous versions served the console from the same port as the S3 API. This section helps migrate to the separated architecture.

#### Pre-Migration Checklist

1. **Backup Configuration**: Save current environment variables and configuration files
2. **Document Current Setup**: Note current port usage, firewall rules, and proxy configurations
3. **Plan Downtime**: Brief service restart required for migration
4. **Update Clients**: Prepare to update console access URLs

#### Step-by-Step Migration

##### 1. Update Configuration

```bash
# Old single-port configuration
RUSTFS_ADDRESS=":9000"

# New separated configuration
RUSTFS_ADDRESS=":9000"           # API port (unchanged)
RUSTFS_CONSOLE_ADDRESS=":9001"   # Console port (new)
RUSTFS_EXTERNAL_ADDRESS=":9000"  # For console→API communication
```

##### 2. Update Firewall Rules

```bash
# Allow new console port
sudo ufw allow 9001/tcp

# Optional: restrict console to internal networks
sudo ufw delete allow 9001/tcp
sudo ufw allow from 192.168.1.0/24 to any port 9001
```

##### 3. Update Docker Deployments

```bash
# Old deployment
docker run -p 9000:9000 rustfs/rustfs:legacy

# New deployment with both ports
docker run \
  -p 9000:9000 \    # API port
  -p 9001:9001 \    # Console port  
  -e RUSTFS_EXTERNAL_ADDRESS=":9000" \
  rustfs/rustfs:latest
```

##### 4. Update Application URLs

- **API Endpoint**: `http://localhost:9000` (unchanged)
- **Console UI**: `http://localhost:9001/rustfs/console/` (new URL)

##### 5. Update Monitoring and Health Checks

```bash
# Add console health check
curl http://localhost:9001/health

# Update monitoring configuration to check both endpoints
```

#### Docker Migration Example

```bash
#!/bin/bash
# migrate-docker.sh

# Stop old container
docker stop rustfs-old
docker rm rustfs-old

# Start new separated services
docker run -d \
  --name rustfs-new \
  -p 9000:9000 \
  -p 9001:9001 \
  -e RUSTFS_EXTERNAL_ADDRESS=":9000" \
  -e RUSTFS_CORS_ALLOWED_ORIGINS="http://localhost:9001" \
  -v rustfs-data:/data \
  rustfs/rustfs:latest

echo "Migration completed!"
echo "API: http://localhost:9000"
echo "Console: http://localhost:9001/rustfs/console/"
```

#### Kubernetes Migration

```yaml
# Update deployment to expose both ports
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustfs
spec:
  template:
    spec:
      containers:
      - name: rustfs
        ports:
        - containerPort: 9000
          name: api
        - containerPort: 9001  # Add console port
          name: console

---
# Update service to include console port
apiVersion: v1
kind: Service
metadata:
  name: rustfs-service
spec:
  ports:
  - name: api
    port: 9000
  - name: console     # Add console service
    port: 9001
```

#### Rollback Plan

If issues occur, you can disable the console to return to single-service mode:

```bash
# Disable console service
RUSTFS_CONSOLE_ENABLE=false rustfs /data

# Or use older image version temporarily
docker run rustfs/rustfs:legacy-tag
```

### Configuration Migration

#### Environment Variable Changes

```bash
# New variables (add these)
export RUSTFS_CONSOLE_ADDRESS=":9001"
export RUSTFS_EXTERNAL_ADDRESS=":9000"
export RUSTFS_CORS_ALLOWED_ORIGINS="*"
export RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="*"

# Optional security variables
export RUSTFS_CONSOLE_RATE_LIMIT_ENABLE="true"
export RUSTFS_CONSOLE_RATE_LIMIT_RPM="100"
export RUSTFS_CONSOLE_AUTH_TIMEOUT="3600"
```

#### Validation

After migration, validate the setup:

```bash
# Check both services are running
curl http://localhost:9000/health  # Should return API health
curl http://localhost:9001/health  # Should return console health

# Test console functionality
open http://localhost:9001/rustfs/console/

# Verify API still works
aws s3 ls --endpoint-url http://localhost:9000
```

## Best Practices

### Production Deployment

#### Security Best Practices

1. **Restrict Console Access**
   ```bash
   # Bind console to internal interface only
   RUSTFS_CONSOLE_ADDRESS="127.0.0.1:9001"
   
   # Use restrictive CORS
   RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://admin.yourdomain.com"
   ```

2. **Enable TLS**
   ```bash
   # Use TLS for console
   RUSTFS_TLS_PATH="/path/to/certs"
   ```

3. **Configure Rate Limiting**
   ```bash
   # Prevent brute force attacks
   RUSTFS_CONSOLE_RATE_LIMIT_ENABLE="true"
   RUSTFS_CONSOLE_RATE_LIMIT_RPM="60"
   ```

4. **Use Strong Credentials**
   ```bash
   # Generate secure credentials
   RUSTFS_ACCESS_KEY="$(openssl rand -hex 16)"
   RUSTFS_SECRET_KEY="$(openssl rand -hex 32)"
   ```

#### Operational Best Practices

1. **Independent Monitoring**
   - Set up health checks for both API and console services
   - Monitor resource usage separately
   - Configure separate alerting rules

2. **Network Segmentation**
   - Use different networks for public API and internal console
   - Implement proper firewall rules
   - Consider using a reverse proxy for additional security

3. **Logging Strategy**
   - Configure separate log targets for console and API
   - Use structured logging for better analysis
   - Implement centralized log collection

#### Docker Best Practices

1. **Resource Limits**
   ```yaml
   services:
     rustfs:
       deploy:
         resources:
           limits:
             memory: 1G
             cpus: "0.5"
   ```

2. **Health Checks**
   ```yaml
   healthcheck:
     test: ["CMD", "curl", "-f", "http://localhost:9000/health", "&&", "curl", "-f", "http://localhost:9001/health"]
     interval: 30s
     timeout: 10s
     retries: 3
   ```

3. **Volume Management**
   ```yaml
   volumes:
     - rustfs-data:/data
     - rustfs-certs:/certs:ro
     - rustfs-logs:/logs
   ```

### Development Environment

#### Development Best Practices

1. **Permissive Configuration**
   ```bash
   # Allow all origins for development
   RUSTFS_CORS_ALLOWED_ORIGINS="*"
   RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="*"
   
   # Enable debug logging
   RUST_LOG="debug"
   ```

2. **Hot Reload Support**
   ```bash
   # Mount source code for development
   docker run -v $(pwd):/app rustfs/rustfs:dev
   ```

3. **Use Development Scripts**
   ```bash
   # Use provided development deployment
   ./examples/enhanced-docker-deployment.sh dev
   ```

### Monitoring and Observability

#### Metrics Collection

1. **Health Check Monitoring**
   ```bash
   # Regular health checks
   */1 * * * * curl -f http://localhost:9000/health >/dev/null || echo "API down"
   */1 * * * * curl -f http://localhost:9001/health >/dev/null || echo "Console down"
   ```

2. **Performance Monitoring**
   - Monitor response times for both services
   - Track error rates separately
   - Set up resource usage alerts

3. **Business Metrics**
   - Track console usage patterns
   - Monitor API request patterns
   - Measure service availability

#### Alerting Strategy

```yaml
# Example Prometheus alerting rules
groups:
- name: rustfs
  rules:
  - alert: RustFSAPIDown
    expr: up{job="rustfs-api"} == 0
    for: 30s
    labels:
      severity: critical
    annotations:
      summary: RustFS API is down
      
  - alert: RustFSConsoleDown
    expr: up{job="rustfs-console"} == 0  
    for: 30s
    labels:
      severity: warning
    annotations:
      summary: RustFS Console is down
      
  - alert: HighResponseTime
    expr: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 1
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: High response time detected
```

### Troubleshooting Workflows

#### Systematic Debugging Approach

1. **Service Status Check**
   ```bash
   # Check if services are running
   curl -f http://localhost:9000/health
   curl -f http://localhost:9001/health
   ```

2. **Network Connectivity**
   ```bash
   # Test from different network contexts
   docker exec container curl http://localhost:9000/health
   curl -H "Origin: http://localhost:9001" http://localhost:9000/health
   ```

3. **Configuration Validation**
   ```bash
   # Verify environment variables
   docker exec container env | grep RUSTFS | sort
   ```

4. **Log Analysis**
   ```bash
   # Check service-specific logs
   docker logs container 2>&1 | grep -E "(console|endpoint)"
   ```

This comprehensive guide covers all aspects of RustFS console and endpoint service separation, from basic deployment to enterprise-grade production configurations. For additional support, refer to the example scripts in the `examples/` directory and the community resources listed in the troubleshooting section.