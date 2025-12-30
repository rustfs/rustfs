# TLS / mTLS configuration

RustFS supports TLS for serving HTTPS and for outbound gRPC connections (MNMD).
It also supports optional client certificate authentication (mTLS) for outbound gRPC:
if a client identity is configured, RustFS will present it; otherwise it will use
server-authenticated TLS only.

## Recommended `tls/` directory layout

Place these files in a directory (default: `./tls`, configurable via `RUSTFS_TLS_DIR`).

```
TLS_DIR/
  ca.crt              # PEM bundle of CA/root certificates to trust (recommended)
  public.crt          # optional extra root bundle (PEM)
  rustfs_cert.pem     # server leaf certificate (PEM) used by the RustFS server
  rustfs_key.pem      # server private key (PEM) used by the RustFS server

  # Optional: outbound mTLS client identity for MNMD
  client_cert.pem     # client certificate chain (PEM)
  client_key.pem      # client private key (PEM)
```

## Environment variables

### Root trust

- `RUSTFS_TLS_DIR` (default: `tls`): TLS directory.
- `RUSTFS_TRUST_SYSTEM_CA` (default: `true`): When `true`, include the platform/system
  trust store as additional roots. When `false`, system roots are not used.
- `RUSTFS_TRUST_LEAF_CERT_AS_CA` (default: `false`): Compatibility switch. If `true`,
  RustFS will also load `rustfs_cert.pem` into the root store (treating leaf certificates
  as trusted roots). Prefer providing `ca.crt` instead.

### Outbound mTLS identity

- `RUSTFS_MTLS_CLIENT_CERT` (default: `${RUSTFS_TLS_DIR}/client_cert.pem`): path to PEM client cert/chain.
- `RUSTFS_MTLS_CLIENT_KEY` (default: `${RUSTFS_TLS_DIR}/client_key.pem`): path to PEM private key.

If both files exist, RustFS enables outbound mTLS. If either is missing, RustFS proceeds
with server-only TLS.

## Failure mode for HTTPS without roots

When connecting to an `https://` MNMD address, RustFS requires at least one configured
trusted root. If none are loaded (no `ca.crt`/`public.crt` and system roots disabled),
RustFS fails fast with a clear error message.
