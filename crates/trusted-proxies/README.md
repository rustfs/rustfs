# RustFS Trusted Proxies

The `rustfs-trusted-proxies` module provides secure and efficient management of trusted proxy servers within the RustFS ecosystem. It is designed to handle multi-layer proxy architectures, ensuring accurate client IP identification while maintaining a zero-trust security model.

## Features

- **Multi-Layer Proxy Validation**: Supports `Strict`, `Lenient`, and `HopByHop` validation modes to accurately identify the real client IP address.
- **Zero-Trust Security**: Verifies every hop in the proxy chain against a configurable list of trusted networks.
- **Cloud Integration**: Automatic discovery of trusted IP ranges for major cloud providers including AWS, Azure, and GCP.
- **High Performance**: Utilizes the `moka` cache for fast lookup of validation results and `axum` for a high-performance web interface.
- **Observability**: Built-in support for Prometheus metrics and structured JSON logging via `tracing`.
- **RFC 7239 Support**: Full support for the modern `Forwarded` header alongside legacy `X-Forwarded-For` headers.

## Configuration

The module is configured primarily through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TRUSTED_PROXY_VALIDATION_MODE` | `hop_by_hop` | Validation strategy (`strict`, `lenient`, `hop_by_hop`) |
| `TRUSTED_PROXY_NETWORKS` | `127.0.0.1,::1,...` | Comma-separated list of trusted CIDR ranges |
| `TRUSTED_PROXY_MAX_HOPS` | `10` | Maximum allowed proxy hops |
| `TRUSTED_PROXY_CACHE_CAPACITY` | `10000` | Max entries in the validation cache |
| `TRUSTED_PROXY_METRICS_ENABLED` | `true` | Enable Prometheus metrics collection |
| `TRUSTED_PROXY_CLOUD_METADATA_ENABLED` | `false` | Enable auto-discovery of cloud IP ranges |

## Usage

### As a Middleware

Integrate the trusted proxy validation into your Axum application:

```rust
use rustfs_trusted_proxies::{TrustedProxyLayer, TrustedProxyConfig};

let config = TrustedProxyConfig::default();
let layer = TrustedProxyLayer::enabled(config, None);

let app = Router::new()
    .route("/", get(handler))
    .layer(layer);
```

### Accessing Client Info

Retrieve the verified client information in your handlers:

```rust
use rustfs_trusted_proxies::ClientInfo;

async fn handler(Extension(client_info): Extension<ClientInfo>) -> impl IntoResponse {
    println!("Real Client IP: {}", client_info.real_ip);
}
```

## Development

### Pre-Commit Checklist
Before committing, ensure all checks pass:
```bash
make pre-commit
```

### Testing
Run the test suite:
```bash
cargo test --workspace --exclude e2e_test
```

## License
Licensed under the Apache License, Version 2.0.
