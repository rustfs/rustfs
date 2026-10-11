# External certificate issuance and TLS rotation

Use an independent certificate controller to obtain and renew certificates.
RustFS consumes certificate material; it does not register ACME accounts or
change DNS records. The certificate-maintenance protocol in `plugin-runner`
is a separate integration boundary. Its admission, scheduler, CA/DNS runner
capabilities and deployment broker are tracked in
[backlog #2806](https://github.com/rustfs/backlog/issues/2806).

## Deployment boundaries

For a distributed Kubernetes deployment, prefer public TLS at an
Ingress/Gateway and private TLS/mTLS between the ingress and RustFS. Public
CA certificates cannot cover internal names such as `*.svc.cluster.local`;
DNS-01 can cover names under a controlled public domain even when the service
is private. A listener
that requires mTLS also requires client certificates from browsers and S3
clients; selecting a different SNI certificate does not change that policy.

Native server TLS can consume an externally managed `kubernetes.io/tls`
Secret with only `tls.crt` and `tls.key`:

```yaml
mode:
  distributed:
    enabled: false
  standalone:
    enabled: true
ingress:
  enabled: false
tls:
  enabled: true
  existingSecret: rustfs-serving-tls
  reloadInterval: 30
```

Create and renew that Secret using cert-manager or an independently managed
publisher. Configure the issuing resource with the actual server DNS/IP SANs
and explicit private-key rotation. The chart does not install cert-manager,
create an ACME account or grant DNS permissions. For distributed native TLS,
the certificate must also cover the configured peer hostnames; explicitly
configure independent outbound trust when a private server CA is used.

`tls.trustSecret` optionally names a separate Secret with an explicitly chosen
`ca.crt`, mounted as outbound `public.crt`. It does not enable mTLS or add a
public issuing CA to client authentication trust. Do not write a fabricated
`ca.crt` into the Secret managed by an ACME issuer.

`tls.enabled` and `mtls.enabled` are mutually exclusive. Existing mTLS mode
keeps its separate server/client certificate Secrets and projects their
contents into one read-only directory. The chart enables certificate reload
for either mode and disables TLS key logging. Explicit container environment
variables can override these settings.

## Material layout and publication

`RUSTFS_TLS_PATH` contains `rustfs_cert.pem` and `rustfs_key.pem`. SNI
identities can reside in visible domain subdirectories with the same two
filenames. Outbound trust uses `public.crt` and `ca.crt`; inbound mTLS client
trust prefers `client_ca.crt`, falling back to `ca.crt` only at initial load.
The default outbound client identity is `client_cert.pem`/`client_key.pem`.

Runtime loading validates existing PEM/DER, key pairing, leaf validity and
explicit authentication EKUs. Each named SNI identity must match its domain.
Chain structure validation does not replace issuer/trust-policy verification
by the issuing controller and client PKI verifier.

Kubernetes mounts must project the entire directory; single-file `subPath`
mounts do not receive Secret updates. RustFS pins one physical `..data`
generation before loading its material. Separate controllers may update the
serving identity and trust independently, so private CA migration still needs
an old-plus-new trust overlap.

For VMs, publish immutable version directories and atomically change the
directory pointer after syncing the complete candidate and its containing
directories. Keep current/accepted/in-use versions during cleanup. Certbot
file symlinks remain compatible, but independent file replacements do not
provide an atomic directory generation; key mismatch is rejected, and a
publisher should use a complete immutable bundle for coherent publication.

`RUSTFS_MTLS_CLIENT_CERT`/`RUSTFS_MTLS_CLIENT_KEY` overrides are validated as
one identity. Explicit missing, malformed, mismatched, expired or unsuitable
override material rejects the update; it does not silently select another
identity. Keep default identity files valid when they are present as well.

## Activation and failure handling

Native `RUSTFS_TLS_RELOAD_ENABLE` defaults to false. Set it to `1` outside the
chart. `RUSTFS_TLS_RELOAD_INTERVAL` defaults to 30 seconds and is clamped to a
minimum of 5 seconds.

Listeners sharing the same configured TLS directory use one reload group.
The group prepares TCP and enabled HTTP/3 configurations from the same
snapshot before swapping them. A late listener first joins the accepted
configuration, even if the current files have become invalid. TCP, QUIC and
separate processes still have a short activation window; there is no global
atomic certificate switch.

Unchanged material is skipped. A serving certificate renewal does not advance
the outbound generation unless outbound trust or identity actually changes.
Outbound state is published as one immutable trust/identity/generation
snapshot. Control and bulk RPC cache acquisition checks that generation;
existing in-flight calls retain their channels.

Reload rejects the disappearance of accepted CA/identity source components,
SNI entries or accepted DNS/IP SAN coverage. Removing or renaming an identity
or trust source requires an explicit configuration change and restart;
ordinary renewal must preserve those requirements. Rejected updates retain
the last accepted in-memory configuration. New handshakes reject certificates
outside their validity interval. Normal renewal does not terminate existing
uploads, downloads or sessions.

The chart uses TCP listener liveness for TLS deployments so certificate expiry
does not trigger a restart loop. Readiness continues to check the TLS service.
This liveness probe detects listener availability, not complete application
health. Keep ingress traffic admission separate from internal peer routing
and storage quorum. Do not restart every node because a CA is unavailable.

There is no automatic persisted private-key fallback in Kubernetes. On
restart, invalid explicitly configured TLS material prevents startup. Key
compromise, explicit certificate revocation, session invalidation, per-purpose
profile coordination and deployment-boundary fencing require the separate
operator/control-plane workflow; ordinary reload does not implement them.

## Verification

Check a fresh TLS connection to each endpoint and compare the leaf serial,
public certificate fingerprint, SANs and expiry. Include HTTP/2, enabled
HTTP/3, Console and internal RPC endpoints. Do not infer served certificates
from Secret contents or an outbound generation gauge alone.

Focused local checks:

```sh
RUSTUP_TOOLCHAIN=stable cargo test -p rustfs-tls-runtime
RUSTUP_TOOLCHAIN=stable cargo test -p rustfs-protos --lib
bash scripts/test_helm_tls_reload.sh
```

Production acceptance also requires real Kubernetes projection, CA/DNS
issuance and renewal, outage/expiry, long-transfer continuity, private CA
overlap and distributed convergence tests. These are separate from protocol
and local loader tests.
