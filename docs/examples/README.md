# RustFS Deployment Examples

This directory contains practical deployment examples and configurations for RustFS.

## Available Examples

### [MNMD (Multi-Node Multi-Drive)](./mnmd/)

Complete Docker Compose example for deploying RustFS in a 4-node, 4-drive-per-node configuration.

**Features:**
- Proper disk indexing (1..4) to avoid VolumeNotFound errors
- Startup coordination via `wait-and-start.sh` script
- Service discovery using Docker service names
- Health checks with alternatives for different base images
- Comprehensive documentation and verification checklist

**Use Case:** Production-ready multi-node deployment for high availability and performance.

**Quick Start:**
```bash
cd docs/examples/mnmd
docker-compose up -d
```

**See also:**
- [MNMD README](./mnmd/README.md) - Detailed usage guide
- [MNMD CHECKLIST](./mnmd/CHECKLIST.md) - Step-by-step verification

## Other Deployment Examples

For additional deployment examples, see:
- [`examples/`](/examples/) - Root-level examples directory with:
  - `docker-quickstart.sh` - Quick start script for basic deployments
  - `enhanced-docker-deployment.sh` - Advanced deployment scenarios
  - `docker-comprehensive.yml` - Docker Compose with multiple profiles
- [`.docker/compose/`](/.docker/compose/) - Docker Compose configurations:
  - `docker-compose.cluster.yaml` - Basic cluster setup
  - `docker-compose.observability.yaml` - Observability stack integration

## Related Documentation

- [Console & Endpoint Service Separation](../console-separation.md)
- [Environment Variables](../ENVIRONMENT_VARIABLES.md)
- [Performance Testing](../PERFORMANCE_TESTING.md)

## Contributing

When adding new examples:
1. Create a dedicated subdirectory under `docs/examples/`
2. Include a comprehensive README.md
3. Provide working configuration files
4. Add verification steps or checklists
5. Document common issues and troubleshooting

## Support

For issues or questions:
- GitHub Issues: https://github.com/rustfs/rustfs/issues
- Documentation: https://rustfs.io
