# TLS Bundle Generator

Generate a local TLS/mTLS certificate bundle for RustFS tests with:

```bash
cargo run -p e2e_test --bin tls_gen -- --out-dir target/tls
```

Overwrite an existing bundle with:

```bash
cargo run -p e2e_test --bin tls_gen -- --out-dir target/tls --force
```

Change the validity window with:

```bash
cargo run -p e2e_test --bin tls_gen -- --out-dir target/tls --days 30
```

Generated files:

- `rustfs_cert.pem`
- `rustfs_key.pem`
- `ca.crt`
- `public.crt`
- `client_ca.crt`
- `client_cert.pem`
- `client_key.pem`

Notes:

- The command refuses to overwrite existing bundle files unless `--force` is set.
- `--days` must be a positive integer.
- The default output directory is `target/tls`.
