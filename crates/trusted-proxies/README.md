# RustFS Trusted Proxies

The `rustfs-trusted-proxies` module provides secure and efficient management of trusted proxy servers within the RustFS
ecosystem. It is designed to handle multi-layer proxy architectures, ensuring accurate client IP identification while
maintaining a zero-trust security model.

## Modes

- **Simple default**: trusts one internal proxy and uses the rightmost `X-Forwarded-For` address, including repeated header lines.
- **Legacy full mode**: keeps the original proxy-chain validation, available
  via `legacy_*` helpers.

## Features

- **Multi-Layer Proxy Validation**: Supports `Strict`, `Lenient`, and `HopByHop` validation modes to accurately identify
  the real client IP address.
- **Zero-Trust Security**: Verifies every hop in the proxy chain against a configurable list of trusted networks.
- **Cloud Integration**: Automatic discovery of trusted IP ranges for major cloud providers including AWS, Azure, and
  GCP.
- **High Performance**: Utilizes the `moka` cache for fast lookup of validation results and `axum` for a
  high-performance web interface.
- **Observability**: Built-in support for Prometheus metrics and structured JSON logging via `tracing`.
- **RFC 7239 Support**: Legacy mode can explicitly select `Forwarded` instead of `X-Forwarded-For`.

## Configuration

The module is configured primarily through environment variables:

| Variable                                      | Default             | Description                                             |
|-----------------------------------------------|---------------------|---------------------------------------------------------|
| `RUSTFS_TRUSTED_PROXY_ENABLED`                | `true`              | Enable the trusted proxy middleware                     |
| `RUSTFS_TRUSTED_PROXY_IMPLEMENTATION`         | `simple`            | Select `simple` or `legacy` implementation              |
| `RUSTFS_TRUSTED_PROXY_ENABLE_RFC7239`         | `false`             | In legacy mode, select `Forwarded` exclusively instead of `X-Forwarded-For` |
| `RUSTFS_TRUSTED_PROXY_VALIDATION_MODE`        | `hop_by_hop`        | Validation strategy (`strict`, `lenient`, `hop_by_hop`) |
| `RUSTFS_TRUSTED_PROXY_NETWORKS`               | `127.0.0.1,::1`     | Comma-separated list of trusted CIDR ranges (loopback-only by default) |
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

### Simple default mode

The default mode trusts one proxy whose direct socket address is internal. It uses the last address that proxy appended to `X-Forwarded-For`, even if that address is a private LAN client or another proxy. Client-written earlier addresses do not influence policy conditions or rate-limit identity. The proxy must append or overwrite `X-Forwarded-For` and overwrite `X-Forwarded-Proto` and `X-Forwarded-Host`; these last two headers use their rightmost values. `Forwarded`, `X-Real-IP`, and `X-Forwarded-Scheme` are not fallback sources.

For multiple proxies, use legacy mode with `RUSTFS_TRUSTED_PROXY_NETWORKS` restricted to the actual proxy addresses. Simple mode reports the immediately preceding proxy in such deployments. Use the same explicit network configuration when untrusted clients can connect directly from an internal network.

```bash
RUSTFS_TRUSTED_PROXY_IMPLEMENTATION=simple
```

### Legacy mode

The original implementation is still available:

```rust
rustfs_trusted_proxies::legacy_init();
let layer = rustfs_trusted_proxies::LegacyTrustedProxyLayer::enabled(config, None);
```

Or switch the global default path:

```bash
RUSTFS_TRUSTED_PROXY_IMPLEMENTATION=legacy
```

Legacy mode uses `X-Forwarded-For` by default. Deployments whose proxy writes RFC 7239 headers must set `RUSTFS_TRUSTED_PROXY_ENABLE_RFC7239=true`; that selects `Forwarded` exclusively, including its last element's `proto` and `host`. There is no automatic fallback between families. Every intermediary must append or overwrite the selected header and strip or overwrite any protocol/host headers it does not populate. Hop-by-hop and lenient modes derive the client from the trusted suffix, not the client-written prefix. Strict mode continues to require every listed address to be trusted.

Malformed forwarding headers, a missing selected family when another family is supplied, and rejected proxy chains now return HTTP 400 before the request reaches application handlers. They are never attributed to the proxy's own address. IPv4-mapped IPv6 peers and forwarding values are canonicalized to IPv4; policies still recognize explicitly mapped IPv6 networks. Upgrading a multi-proxy or `Forwarded`-only deployment requires the mode settings above before serving traffic. Rolling back restores the previous permissive header selection and validation fallback.

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
