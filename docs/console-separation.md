# Console and Endpoint Service Separation

As of this implementation, RustFS supports running the console web interface on a separate port from the main S3 API endpoint service, providing better security isolation and deployment flexibility.

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