# MQTT Broker (EMQX)

This directory contains the configuration for running an EMQX MQTT broker, which can be used for testing RustFS's MQTT integration.

## 🚀 Quick Start

To start the EMQX broker:

```bash
docker compose up -d
```

## 📊 Access

- **Dashboard**: [http://localhost:18083](http://localhost:18083)
- **Default Credentials**: `admin` / `public`
- **MQTT Port**: `1883`
- **WebSocket Port**: `8083`

## 🛠️ Configuration

The `docker-compose.yml` file sets up a single-node EMQX instance.

- **Persistence**: Data is not persisted by default (for testing).
- **Network**: Uses the default bridge network.

## 📝 Notes

- This setup is intended for development and testing purposes.
- For production deployments, please refer to the official [EMQX Documentation](https://www.emqx.io/docs/en/latest/).
