# KMS admin API contract

**Use this when:** you are wiring a client (CLI, console, automation) to the KMS admin endpoints and need the IAM action, risk class, per-key scope, and key-listing paging rules for each route.
**Source of truth:** `rustfs/src/admin/route_policy.rs` (action and risk per route, asserted by `rustfs/src/admin/route_registration_test.rs`); `crates/kms/src/backends/mod.rs` (`DEFAULT_LIST_KEYS_PAGE_SIZE`, `MAX_LIST_KEYS_PAGE_SIZE`, `list_keys_page_size`); response shapes pinned by snapshots under `crates/kms/src/snapshots/` and `rustfs/src/admin/handlers/snapshots/`.

The wire prefix is `/rustfs/admin/v3`. `GET /kms/status` and `GET /kms/service-status` return different response types; `capabilities` on `/kms/status` is additive and optional. The **Per-key** column says whether the route authorizes against the key it names (see [Per-key KMS authorization](kms-per-key-authorization.md)); `no` means the route matches any KMS resource in the caller's policy.

## Endpoint matrix

| Method and endpoint | IAM action | Risk | Per-key | Notes |
| --- | --- | --- | --- | --- |
| `POST /kms/configure` | `kms:Configure` | high | no | Persists to cluster storage, switches the local node, broadcasts a best-effort peer reload |
| `POST /kms/reconfigure` | `kms:Configure` | high | no | Same contract as configure |
| `POST /kms/start` | `kms:ServiceControl` | high | no | |
| `POST /kms/stop` | `kms:ServiceControl` | high | no | |
| `POST /kms/reload` | `kms:ServiceControl` | high | no | Re-reads the persisted configuration without resubmitting secrets; reuses the configure response shape |
| `GET /kms/status` | `kms:ServiceControl` | sensitive | no | Backend type plus capability matrix |
| `POST /kms/status` | `kms:ServiceControl` | high | no | Compatibility route; not a client command |
| `GET /kms/service-status` | `kms:ServiceControl` | sensitive | no | Carries `cluster_config` fingerprints and the `consistent` flag |
| `GET /kms/config` | `kms:Configure` | sensitive | no | Contains operational paths; redact before display |
| `POST /kms/clear-cache` | `kms:ClearCache` | high | no | `KmsClearCacheResponse` (`{status,message}`) |
| `POST /kms/keys` | `kms:Configure` | high | no | Key creation shares the configure action |
| `GET /kms/keys` | `kms:ListKeys` | sensitive | no | See the key listing contract below |
| `GET /kms/keys/{key_id}` | `kms:DescribeKey` | sensitive | yes | `?impact=true` opts into the configuration-reference report |
| `DELETE /kms/keys/delete` | `kms:DeleteKey` | critical | yes | JSON body; `force_immediate` also requires `confirm_key_id` and the server-side `RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION` gate |
| `POST /kms/keys/cancel-deletion` | `kms:DeleteKey` | high | yes | |
| `POST /kms/keys/enable` | `kms:EnableKey` | high | yes | |
| `POST /kms/keys/disable` | `kms:DisableKey` | high | yes | |
| `POST /kms/keys/rotate` | `kms:RotateKey` | high | yes | Subject to the rotation constraints in [KMS backend security properties](kms-backend-security.md#master-key-rotation-retention-destruction-and-upgrade-ordering) |
| `POST /kms/keys/rekey` | `kms:Rekey` | high | no | Bulk DEK rekey sweep; cluster-scoped, see [`kms-bulk-rekey-contract.md`](../architecture/kms-bulk-rekey-contract.md) |
| `GET /kms/keys/rekey/status` | `kms:Rekey` | sensitive | no | |
| `POST /kms/keys/rekey/cancel` | `kms:Rekey` | high | no | |
| `POST /kms/keys/update-description` | `kms:UpdateKeyDescription` | high | yes | |
| `POST /kms/keys/tag` | `kms:TagResource` | high | yes | |
| `POST /kms/keys/untag` | `kms:UntagResource` | high | yes | |
| `POST /kms/generate-data-key` | `kms:GenerateDataKey` | high | yes | Response carries a base64 plaintext data key; never surface it in a UI or CLI |
| `GET /kms/backup` | `kms:Backup` | sensitive | no | Status and readiness only; no KEK material |
| `POST /kms/backup` | `kms:Backup` | high | no | Returns `backup_id` and metadata only |
| `POST /kms/restore/dry-run` | `kms:Restore` | sensitive | no | Preflight; writes nothing |
| `POST /kms/restore` | `kms:Restore` | high | no | Requires `confirm_backup_id` and `confirm_conflict_policy` |
| `POST /kms/restore/abort` | `kms:Restore` | high | no | Requires `confirm_target_key_dir` |
| `POST /kms/create-key`, `POST /kms/key/create` | `kms:Configure` | high | no | Legacy `mc` aliases of `POST /kms/keys` |
| `GET /kms/describe-key`, `GET /kms/key/status` | `kms:DescribeKey` | sensitive | yes | Legacy aliases of `GET /kms/keys/{key_id}` |
| `GET /kms/list-keys` | `kms:ListKeys` | sensitive | no | Legacy alias of `GET /kms/keys`; same listing contract |

## Key listing contract

Both listing routes (`GET /kms/keys` and the legacy `GET /kms/list-keys`) share one contract.

`limit` is optional. When it is absent the server applies `DEFAULT_LIST_KEYS_PAGE_SIZE` (100). When it is present it must parse as a non-negative integer: `limit=abc`, `limit=-1` and a value-less `limit` are refused with `400`, not silently read as "use the default". `limit=0` is a well-formed request for an empty page. Any page size above `MAX_LIST_KEYS_PAGE_SIZE` (1000) is served as 1000 — the response is `truncated` with a usable `next_marker`, so a client that pages until `truncated` is false still reaches every key. Clients must not assume a page is the size they asked for.

`marker` is opaque to the client: treat it as a cursor to hand back unchanged, never as a value to construct. On the Local, Vault KV2, Vault Transit and Static backends it happens to be an exclusive lower bound on the key identifier, which is what makes paging survive keys being created or destroyed mid-listing; on the AWS backend it is AWS's own pagination token, and sending a key id there is rejected. An empty `marker` means the same thing as no marker at all. Filters are applied after the page is cut, so a filtered page can be short — even empty — while more keys remain. Page until `truncated` is false, never until a page comes back short.

`unreadable_key_ids` is present only when the server listed a key whose record it could not describe — a record written by a newer build, or damaged material. The identifiers are reported rather than omitted, so a listing never quietly understates the key set; a client displaying an inventory should surface them as damaged rather than dropping them, and paging always advances past a damaged key. A failure that says nothing about a specific key (timeout, `5xx`, permission denied) still fails the whole listing instead of appearing here.

One case is deliberately an error rather than a report: a listing that covered the entire key set — no `marker`, and not `truncated` — in which nothing was readable. An empty `keys` array there would be indistinguishable, to any client written before this field existed, from a deployment that has no keys, and the usual response to that is to provision a new one. Such a listing returns `500` instead, naming the first failure; the individual identifiers are in the server log. A truncated page, or one resumed from a marker, always reports rather than failing, so a damaged key can never strand the keys behind it.
