# RustFS Docker Deployment Examples

This directory contains various deployment scripts and configuration files for RustFS with console and endpoint service separation.

## Quick Start Scripts

### `docker-quickstart.sh`
The fastest way to get RustFS running with different configurations.

```bash
# Basic deployment (ports 9000-9001)
./docker-quickstart.sh basic

# Development environment (ports 9010-9011) 
./docker-quickstart.sh dev

# Production-like deployment (ports 9020-9021)
./docker-quickstart.sh prod

# Check status of all deployments
./docker-quickstart.sh status

# Test health of all running services
./docker-quickstart.sh test

# Clean up all containers
./docker-quickstart.sh cleanup
```

### `enhanced-docker-deployment.sh`
Comprehensive deployment script with multiple scenarios and detailed logging.

```bash
# Deploy individual scenarios
./enhanced-docker-deployment.sh basic    # Basic setup with port mapping
./enhanced-docker-deployment.sh dev      # Development environment  
./enhanced-docker-deployment.sh prod     # Production-like with security

# Deploy all scenarios at once
./enhanced-docker-deployment.sh all

# Check status and test services
./enhanced-docker-deployment.sh status
./enhanced-docker-deployment.sh test

# View logs for specific container
./enhanced-docker-deployment.sh logs rustfs-dev

# Complete cleanup
./enhanced-docker-deployment.sh cleanup
```

### `enhanced-security-deployment.sh`
Production-ready deployment with enhanced security features including TLS, rate limiting, and secure credential generation.

```bash
# Deploy with security hardening
./enhanced-security-deployment.sh

# Features:
# - Automatic TLS certificate generation
# - Secure credential generation
# - Rate limiting configuration
# - Console access restrictions
# - Health check validation
```

## Docker Compose Examples

### `docker-comprehensive.yml`
Complete Docker Compose configuration with multiple deployment profiles.

```bash
# Deploy specific profiles
docker-compose -f docker-comprehensive.yml --profile basic up -d
docker-compose -f docker-comprehensive.yml --profile dev up -d
docker-compose -f docker-comprehensive.yml --profile production up -d
docker-compose -f docker-comprehensive.yml --profile enterprise up -d
docker-compose -f docker-comprehensive.yml --profile api-only up -d

# Deploy with reverse proxy
docker-compose -f docker-comprehensive.yml --profile production --profile nginx up -d
```

#### Available Profiles:

- **basic**: Simple deployment for testing (ports 9000-9001)
- **dev**: Development environment with debug logging (ports 9010-9011)
- **production**: Production deployment with security (ports 9020-9021)
- **enterprise**: Full enterprise setup with TLS (ports 9030-9443)
- **api-only**: API endpoint without console (port 9040)

## Usage Examples by Scenario

### Development Setup

```bash
# Quick development start
./docker-quickstart.sh dev

# Or use enhanced deployment for more features
./enhanced-docker-deployment.sh dev

# Or use Docker Compose
docker-compose -f docker-comprehensive.yml --profile dev up -d
```

**Access Points:**
- API: http://localhost:9010 (or 9030 for enhanced)
- Console: http://localhost:9011/rustfs/console/ (or 9031 for enhanced)
- Credentials: dev-admin / dev-secret

### Production Deployment

```bash
# Security-hardened deployment
./enhanced-security-deployment.sh

# Or production profile
./enhanced-docker-deployment.sh prod
```

**Features:**
- TLS encryption for console
- Rate limiting enabled
- Restricted CORS policies
- Secure credential generation
- Console bound to localhost only

### Testing and CI/CD

```bash
# API-only deployment for testing
docker-compose -f docker-comprehensive.yml --profile api-only up -d

# Quick basic setup for integration tests
./docker-quickstart.sh basic
```

## Configuration Examples

### Environment Variables

All deployment scripts support customization via environment variables:

```bash
# Custom image and ports
export RUSTFS_IMAGE="rustfs/rustfs:custom-tag"
export CONSOLE_PORT="8001"
export API_PORT="8000"

# Custom data directories
export DATA_DIR="/custom/data/path"
export CERTS_DIR="/custom/certs/path"

# Run with custom configuration
./enhanced-security-deployment.sh
```

### Common Configurations

```bash
# Development - permissive CORS
RUSTFS_CORS_ALLOWED_ORIGINS="*"
RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="*"

# Production - restrictive CORS  
RUSTFS_CORS_ALLOWED_ORIGINS="https://myapp.com,https://api.myapp.com"
RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS="https://admin.myapp.com"

# Security hardening
RUSTFS_CONSOLE_RATE_LIMIT_ENABLE="true"
RUSTFS_CONSOLE_RATE_LIMIT_RPM="60"
RUSTFS_CONSOLE_AUTH_TIMEOUT="1800"
```

## Monitoring and Health Checks

All deployments include health check endpoints:

```bash
# Test API health
curl http://localhost:9000/health

# Test console health  
curl http://localhost:9001/health

# Test all deployments
./docker-quickstart.sh test
./enhanced-docker-deployment.sh test
```

## Network Architecture

### Port Mappings

| Deployment | API Port | Console Port | Description |
|-----------|----------|--------------|-------------|
| Basic | 9000 | 9001 | Simple deployment |
| Dev | 9010 | 9011 | Development environment |
| Prod | 9020 | 9021 | Production-like setup |
| Enterprise | 9030 | 9443 | Enterprise with TLS |
| API-Only | 9040 | - | API endpoint only |

### Network Isolation

Production deployments use network isolation:

- **Public API Network**: Exposes API endpoints to external clients
- **Internal Console Network**: Restricts console access to internal networks
- **Secure Network**: Isolated network for enterprise deployments

## Security Considerations

### Development
- Permissive CORS policies for easy testing
- Debug logging enabled
- Default credentials for simplicity

### Production  
- Restrictive CORS policies
- TLS encryption for console
- Rate limiting enabled
- Secure credential generation
- Console bound to localhost
- Network isolation

### Enterprise
- Complete TLS encryption
- Advanced rate limiting
- Authentication timeouts
- Secret management
- Network segregation

## Troubleshooting

### Common Issues

1. **Port Conflicts**: Use different ports via environment variables
2. **CORS Errors**: Check origin configuration and browser network tab
3. **Health Check Failures**: Verify services are running and ports are accessible
4. **Permission Issues**: Check volume mount permissions and certificate file permissions

### Debug Commands

```bash
# Check container logs
docker logs rustfs-container

# Check container environment
docker exec rustfs-container env | grep RUSTFS

# Test connectivity
docker exec rustfs-container curl http://localhost:9000/health
docker exec rustfs-container curl http://localhost:9001/health

# Check listening ports
docker exec rustfs-container netstat -tulpn | grep -E ':(9000|9001)'
```

## Migration from Previous Versions

See [docs/console-separation.md](../docs/console-separation.md) for detailed migration instructions from single-port deployments to the separated architecture.

## Additional Resources

- [Console Separation Documentation](../docs/console-separation.md)
- [Docker Compose Configuration](../docker-compose.yml)
- [Main Dockerfile](../Dockerfile)
- [Security Best Practices](../docs/console-separation.md#security-hardening)