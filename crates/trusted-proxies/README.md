# RustFS Trusted Proxies

The `rustfs-trusted-proxies` module provides secure and efficient management of trusted proxy servers within the RustFS
ecosystem. It is designed to handle multi-layer proxy architectures, ensuring accurate client IP identification while
maintaining a zero-trust security model.

## Features

- **Multi-Layer Proxy Validation**: Supports `Strict`, `Lenient`, and `HopByHop` validation modes to accurately identify
  the real client IP address.
- **Zero-Trust Security**: Verifies every hop in the proxy chain against a configurable list of trusted networks.
- **Cloud Integration**: Automatic discovery of trusted IP ranges for major cloud providers including AWS, Azure, and
  GCP.
- **High Performance**: Utilizes the `moka` cache for fast lookup of validation results and `axum` for a
  high-performance web interface.
- **Observability**: Built-in support for Prometheus metrics and structured JSON logging via `tracing`.
- **RFC 7239 Support**: Full support for the modern `Forwarded` header alongside legacy `X-Forwarded-For` headers.

## Configuration

The module is configured primarily through environment variables:

| Variable                                      | Default             | Description                                             |
|-----------------------------------------------|---------------------|---------------------------------------------------------|
| `RUSTFS_TRUSTED_PROXY_ENABLED`                | `true`              | Enable the trusted proxy middleware                     |
| `RUSTFS_TRUSTED_PROXY_VALIDATION_MODE`        | `hop_by_hop`        | Validation strategy (`strict`, `lenient`, `hop_by_hop`) |
| `RUSTFS_TRUSTED_PROXY_NETWORKS`               | `127.0.0.1,::1,...` | Comma-separated list of trusted CIDR ranges             |
| `RUSTFS_TRUSTED_PROXY_MAX_HOPS`               | `10`                | Maximum allowed proxy hops                              |
| `RUSTFS_TRUSTED_PROXY_CACHE_CAPACITY`         | `10000`             | Max entries in the validation cache                     |
| `RUSTFS_TRUSTED_PROXY_METRICS_ENABLED`        | `true`              | Enable Prometheus metrics collection                    |
| `RUSTFS_TRUSTED_PROXY_CLOUD_METADATA_ENABLED` | `false`             | Enable auto-discovery of cloud IP ranges                |

## Usage

### Initialization

Initialize the global trusted proxy system at the start of your application (e.g., in `main.rs`):

```rust
// Initialize trusted proxies system
rustfs_trusted_proxies::init();
```

### As a Middleware

Integrate the trusted proxy validation into your Axum application or HTTP service stack:

```rust
use rustfs_trusted_proxies;

let app = Router::new()
    .route("/", get(handler))
    // Add the trusted proxy layer if enabled
    .option_layer(if rustfs_trusted_proxies::is_enabled() {
        Some(rustfs_trusted_proxies::layer().clone())
    } else {
        None
    });
```

### Accessing Client Info

Retrieve the verified client information in your handlers or other middleware:

```rust
use rustfs_trusted_proxies::ClientInfo;

async fn handler(req: Request) -> impl IntoResponse {
    if let Some(client_info) = req.extensions().get::<ClientInfo>() {
        println!("Real Client IP: {}", client_info.real_ip);
        println!("Is Trusted: {}", client_info.is_from_trusted_proxy);
    }
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
