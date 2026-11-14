# OpenBao as the Vault KMS backend

**Use this when:** deciding whether to run RustFS server-side encryption against OpenBao instead of HashiCorp Vault, planning an OpenBao high-availability deployment for RustFS, or diagnosing `Key not found` on key creation against an OpenBao cluster.
**Source of truth:** `crates/kms/src/backends/vault.rs` (KV2 backend, `create_key` and `store_key_metadata`), `crates/kms/src/backends/vault_transit.rs` (Transit backend and its KV2 metadata record), `crates/kms/src/config.rs` (`RUSTFS_KMS_VAULT_*`). 中文: [kms-openbao-compatibility.zh-CN.md](kms-openbao-compatibility.zh-CN.md).

Every behavioural statement on this page was measured against OpenBao 2.7.2 on 2026-11-13. Other OpenBao versions were not tested. Statements that are expectations rather than measurements are marked as such.

## Summary

RustFS has no OpenBao-specific backend. OpenBao is used through the existing `vault-kv2` and `vault-transit` backends with the same `RUSTFS_KMS_VAULT_*` configuration described in the [Vault KMS authentication runbook](vault-kms-authentication.md) and [KMS backend security properties](kms-backend-security.md).

- **Single node, or RustFS pointed at the active node:** OpenBao behaves the same as Vault for everything RustFS does. No configuration change is needed.
- **High availability with RustFS reaching a standby node:** a standby answers reads from its own copy, which lags writes by a short interval. RustFS 1.0.5 and earlier fail key creation and other key management calls against such a node. Later releases tolerate it on the node that made the change; a different RustFS node can still observe the earlier state for that interval. See [High availability](#high-availability-standby-nodes-serve-stale-reads).

## What was verified

| Area | Result on OpenBao 2.7.2 |
| --- | --- |
| `rustfs-kms` test suite with the live KV2 and Transit backends enabled | 712 of 712 passed |
| Live-server contract, backend, and fault-injection tests | 17 of 17 passed |
| AppRole authentication (KV2, Transit, custom mount paths) | 3 of 3 passed |
| End-to-end SSE through a real RustFS server (SSE-S3, SSE-KMS, multipart, large objects, key operations, rekey sweep) | 8 of 8 passed |
| Black-box KMS and SSE lifecycle suite, KV2 and Transit backends | Same result as HashiCorp Vault 1.21 with the same RustFS binary |
| Namespaces through `RUSTFS_KMS_VAULT_NAMESPACE`, KV2 and Transit backends | Passed; keys were confirmed to land inside the namespace |
| Three-node Raft cluster with `disable_standby_reads = true`, leader killed during decrypt traffic | Passed |
| Three-node Raft cluster, default configuration, key lifecycle and SSE through a standby node | See [High availability](#high-availability-standby-nodes-serve-stale-reads) |

Transit ciphertext keeps the `vault:v1:` prefix on OpenBao, associated data on Transit encrypt and decrypt is honoured, and Transit key deletion still requires `deletion_allowed` first.

Not covered by this verification: Kubernetes authentication, TLS and mutual TLS connections, and OpenBao versions other than 2.7.2.

## High availability: standby nodes serve stale reads

In an OpenBao HA cluster a standby node handles a request from its own copy of the data and forwards it to the active node only when it has to write. Two consequences were measured on a local three-node Raft cluster:

- A read through a standby does not return a value written through the same standby until the standby has applied the change. The gap was 60 to 100 ms.
- A write that the standby can refuse from its own copy is refused without being forwarded. Deleting a Transit key immediately after setting `deletion_allowed` returned `deletion is not allowed for this key` for the same interval.

This is the ordinary behaviour of a store that answers reads from replicas, not a defect of OpenBao. Not measured: HashiCorp documents Vault Enterprise performance standbys as serving reads locally as well, so the same behaviour is expected there. Vault Community Edition standbys forward every request and do not show it.

### RustFS 1.0.5 and earlier

These releases read back their own writes during key management, so a RustFS server whose `RUSTFS_KMS_VAULT_ADDRESS` resolves to a standby, directly or through a load balancer, fails as follows. Pointing the same server at the active node makes every case pass.

- **KV2 backend:** every key creation fails with HTTP 404 `failed to create key: Key not found: <key-id>`. The key record has in fact been written and is usable, but without the requested description and tags. Operations issued right after a create or an update fail with `Key not found` or `Concurrent modification of key <key-id> detected, retry the operation`.
- **Transit backend:** a disable followed quickly by an enable fails with `Concurrent modification of transit key <key-id> metadata detected; retry the operation`.
- **Object traffic** with keys that already exist (PUT, GET, multipart, copy) was not affected in testing.

### Later releases

RustFS no longer depends on reading back its own writes:

- Key creation on the KV2 backend is a single create-only write that carries the description and tags.
- Each RustFS node remembers, for five seconds, the version it last wrote of each key record and re-reads an answer that is older, including a record it deleted that is still being served. The re-read pauses 25 to 400 ms per attempt, up to five attempts, and then returns whatever the backend answered.
- A check-and-set write that loses pauses the same way before re-reading, with five attempts instead of three.
- A Transit key delete refused because the deletion switch is not visible yet is retried the same way.

Measured through a standby node with the default OpenBao configuration: the key lifecycle, encryption and rotation behaviour suite passed on both backends in three consecutive runs; the SSE data-plane suite (34 cases) passed on both backends; the key management suite passed on the Transit backend, and on the KV2 backend it passed except for the listing case below, which failed in two of three runs.

What still applies, because only the node that made a change knows about it:

- **Another RustFS node, or any other client, reading the same standby** observes the earlier state until the standby catches up. A key created through one node and used through another within that interval can be reported as not found, and a disable becomes effective on other nodes after that interval in addition to their own cache lifetime.
- **Listing keys on the KV2 backend immediately after creating one** can omit the new key.
- **Reads keep succeeding through a leader failure.** A standby that serves reads does not need the active node for them, so KV2 decrypts continued without a retry while the leader was killed. `scripts/test/vault_ha_kms_live.sh` asserts the opposite, as observed with Vault, and fails on that assertion against a default OpenBao cluster.

### Recommended configuration

For strict consistency across RustFS nodes, use one of the following. Both were verified.

1. Turn off standby reads on every OpenBao node, so that standbys forward reads to the active node:

   ```hcl
   disable_standby_reads = true
   ```

2. Route RustFS only to the active node. `GET /v1/sys/health` returns 200 on the active node and 429 on an unsealed standby, which a load balancer health check can use.

RustFS does not send the `X-Vault-Index` request header. Sending `X-Vault-Forward: active-node` or `X-Vault-Inconsistent: forward-active-node` did not change the standby's behaviour in testing.

## Deployment differences from Vault

These affect how OpenBao itself is deployed, not how RustFS is configured.

- **Storage:** OpenBao 2.7.2 rejects `storage "file"` with `unknown storage type file`. Use `storage "raft"`, including for a single node.
- **Container image:** the data directory is `/openbao/file`, the configuration directory is `/openbao/config`, and inline configuration is passed in `BAO_LOCAL_CONFIG`. `VAULT_LOCAL_CONFIG` is not read. The image keeps a `vault` command as a link to `bao`.
- **Audit devices:** `sys/audit` refuses to enable an audit device through the API by default and asks for configuration-file audit devices. `unsafe_allow_api_audit_creation = true` restores the API path and is suitable for test clusters only.
- **Raft membership:** a node that has just joined is listed as a non-voter and was promoted to voter after roughly ten seconds in testing. Wait for all nodes to be voters before relying on failover.
- **CLI:** the `bao` binary accepts the same `server -dev` flags the RustFS test scripts pass to `vault`, so `RUSTFS_TEST_VAULT_BIN=bao` runs `scripts/test/vault_approle_kms_live.sh` and the end-to-end Vault KMS tests unchanged.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| `failed to create key: Key not found: <key-id>` from `POST /rustfs/admin/v3/kms/keys` on the KV2 backend, with OpenBao in HA | RustFS 1.0.5 or earlier reached a standby that had not yet applied the create | Upgrade RustFS or apply one of the [recommended configurations](#recommended-configuration), then repeat the create request to attach the description and tags |
| `Concurrent modification of ... detected` on enable, disable, rotation, or tagging | A standby kept serving the version a write was based on for longer than the retry budget | Retry the operation; if it recurs, apply one of the recommended configurations |
| A key just created through one RustFS node is reported as not found by another | The other node read a standby that had not applied the create | Retry, or apply one of the recommended configurations |
| OpenBao fails to start with `unknown storage type file` | The `file` storage backend is not available | Switch the configuration to `storage "raft"` |
| `cannot enable audit device via API` | API audit creation is disabled by default | Declare the audit device in the configuration file |
