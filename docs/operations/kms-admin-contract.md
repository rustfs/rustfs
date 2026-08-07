# KMS admin API contract and client handoff

This page is the server-side handoff for rustfs/backlog#1639. Response-shape snapshots for the key and metadata handlers live beside the producers under `rustfs/src/admin/handlers/snapshots/` (PR #5626). This matrix records the remaining route-level handoff contract without duplicating those snapshots. The management `KmsStatusResponse` shape remains an identified gap and will be pinned after the status-handler changes in #1636 land.

The wire prefix is `/rustfs/admin/v3`. Request and response field names for the producer snapshots are pinned in #5626; fields in this matrix are the client handoff reference. `GET /kms/status` and `GET /kms/service-status` intentionally use different response types; `capabilities` on `/kms/status` is additive and optional.

| Method and endpoint | Action / risk | Per-key | rc | console | Handoff |
| --- | --- | ---: | --- | --- | --- |
| `POST /kms/configure` | `kms:Configure` / high | no | supported | supported | none |
| `POST /kms/reconfigure` | `kms:Configure` / high | no | supported | supported | none |
| `POST /kms/start` | `kms:ServiceControl` / high | no | supported | supported | none |
| `POST /kms/stop` | `kms:ServiceControl` / high | no | supported | supported | none |
| `GET /kms/config` | `kms:Configure` / sensitive | no | no | supported | Redact operational paths before display. |
| `POST /kms/clear-cache` | `kms:ClearCache` / high | no | no | supported | Keep the current `{status,message}` response stable. |
| `POST /kms/keys` | `kms:Configure` / high | no | supported | supported | none |
| `GET /kms/keys` | `kms:ListKeys` / sensitive | no | supported | supported | none |
| `GET /kms/keys/{key_id}` | `kms:DescribeKey` / sensitive | yes | supported | supported | none |
| `DELETE /kms/keys/delete` | `kms:DeleteKey` / critical | yes | supported | supported | Preserve immediate-delete confirmations. |
| `POST /kms/keys/cancel-deletion` | `kms:DeleteKey` / high | yes | supported | supported | none |
| `POST /kms/create-key` | `kms:Configure` / high | no | no | no | Legacy `mc` alias; do not add a second client command. |
| `POST /kms/key/create` | `kms:Configure` / high | no | no | no | Legacy `mc` alias; do not add a second client command. |
| `GET /kms/describe-key` | `kms:DescribeKey` / sensitive | yes | no | no | Legacy `mc` alias. |
| `GET /kms/key/status` | `kms:DescribeKey` / sensitive | yes | supported | no | `rc key status` uses this legacy-compatible shape. |
| `GET /kms/list-keys` | `kms:ListKeys` / sensitive | no | supported | no | `rc key list` uses this legacy-compatible shape. |
| `POST /kms/generate-data-key` | `kms:GenerateDataKey` / high | yes | do not expose | do not expose | Programmatic primitive; never print plaintext key material. |
| `GET /kms/status` | `kms:ServiceControl` / sensitive | no | supported | supported | Keep `capabilities` optional for old servers. |
| `POST /kms/status` | `kms:ServiceControl` / high | no | no | no | Internal compatibility route; not a client command. |
| `GET /kms/service-status` | `kms:ServiceControl` / sensitive | no | no | supported | Do not conflate this type with `/kms/status`. |
| `POST /kms/keys/enable` | `kms:EnableKey` / high | yes | pending | pending | Add an explicit lifecycle command/UI action. |
| `POST /kms/keys/disable` | `kms:DisableKey` / high | yes | pending | pending | Add an explicit lifecycle command/UI action. |
| `POST /kms/keys/rotate` | `kms:RotateKey` / high | yes | pending | pending | Add an explicit lifecycle command/UI action. |
| `POST /kms/keys/update-description` | `kms:UpdateKeyDescription` / high | yes | pending | pending | Add a metadata mutation command/UI action. |
| `POST /kms/keys/tag` | `kms:TagResource` / high | yes | pending | pending | Add a metadata mutation command/UI action. |
| `POST /kms/keys/untag` | `kms:UntagResource` / high | yes | pending | pending | Add a metadata mutation command/UI action. |
| `GET /kms/backup` | `kms:Backup` / sensitive | no | pending | pending | Status/readiness only; never expose KEK material. |
| `POST /kms/backup` | `kms:Backup` / high | no | pending | pending | Preserve `backup_id` and metadata-only response. |
| `POST /kms/restore/dry-run` | `kms:Restore` / sensitive | no | pending | pending | Dry-run must be the default and show differences. |
| `POST /kms/restore` | `kms:Restore` / high | no | pending | pending | Require `confirm_backup_id` and `confirm_conflict_policy`; no blanket `--yes`. |
| `POST /kms/restore/abort` | `kms:Restore` / high | no | pending | pending | Require `confirm_target_key_dir`. |

## Key listing contract

Both listing routes (`GET /kms/keys` and the legacy `GET /kms/list-keys`) share one contract.

`limit` is optional. When it is absent the server applies its own default page size of 100. When it is present it must parse as a non-negative integer: `limit=abc`, `limit=-1` and a value-less `limit` are refused with `400`, not silently read as "use the default". `limit=0` is a well-formed request for an empty page. Any page size above 1000 is served as 1000 — the response is `truncated` with a usable `next_marker`, so a client that pages until `truncated` is false still reaches every key. Clients must not assume a page is the size they asked for.

`marker` is opaque to the client: treat it as a cursor to hand back unchanged, never as a value to construct. On the Local, Vault KV2, Vault Transit and Static backends it happens to be an exclusive lower bound on the key identifier, which is what makes paging survive keys being created or destroyed mid-listing; on the AWS backend it is AWS's own pagination token, and sending a key id there is rejected. An empty `marker` means the same thing as no marker at all. Filters are applied after the page is cut, so a filtered page can be short — even empty — while more keys remain. Page until `truncated` is false, never until a page comes back short.

`unreadable_key_ids` is present only when the server listed a key whose record it could not describe — a record written by a newer build, or damaged material. The identifiers are reported rather than omitted, so a listing never quietly understates the key set; a client displaying an inventory should surface them as damaged rather than dropping them, and paging always advances past a damaged key. A failure that says nothing about a specific key (timeout, `5xx`, permission denied) still fails the whole listing instead of appearing here.

One case is deliberately an error rather than a report: a listing that covered the entire key set — no `marker`, and not `truncated` — in which nothing was readable. An empty `keys` array there would be indistinguishable, to any client written before this field existed, from a deployment that has no keys, and the usual response to that is to provision a new one. Such a listing returns `500` instead, naming the first failure; the individual identifiers are in the server log. A truncated page, or one resumed from a marker, always reports rather than failing, so a damaged key can never strand the keys behind it.

## Server-side snapshot coverage

The merged #5626 producer snapshots cover the nine modern/legacy key response types and the metadata response type served by `kms_keys.rs` and `kms_key_metadata.rs`: create, describe, list, generate-data-key, delete, cancel-deletion, update-description, tag, and untag. The four dynamic responses served verbatim by `kms_dynamic.rs` are covered in `crates/kms/src/snapshots/`: configure, start, stop, and the `service-status` response.

`POST /kms/clear-cache` now has a named `KmsClearCacheResponse` and a producer snapshot beside the others; its serialized bytes are unchanged from the inline JSON it replaced.

The remaining wire-shape gaps are intentionally documented rather than duplicated here: the management `KmsStatusResponse` (`GET|POST /kms/status`, pending #1636), `KmsConfigResponse`, all three lifecycle responses, and the backup/restore response family. Adding producer snapshots for those gaps is a separate server test task; it must not be inferred from the client matrix.

## Client handoff gaps

The `rc` client currently has status, key list/status/create/delete/cancel-deletion, configure/reconfigure/start/restart/stop, and diagnostic/roundtrip entry points. It has no lifecycle enable/disable/rotate, key metadata, or backup/restore commands. The console currently calls service-status, configure/reconfigure/start/stop/config, clear-cache, status, and the modern key CRUD routes. It has no lifecycle, metadata, or backup/restore UI. These pending cells are delivery items for `rustfs/cli` and `rustfs/console`; they are not implemented in this repository. A read-only issue search on 2026-08-02 found no matching KMS issue in either client repository, so the client handoff still needs issue creation there.

`POST /kms/generate-data-key` is deliberately marked “do not expose” for both clients: its response contains a base64 plaintext data key. `GET /kms/config` and backup status/restore responses contain operational paths and identifiers, not key material, but still require UI/CLI redaction and confirmation handling.

The producer response snapshots in #5626 and this matrix do not imply that rustfs/backlog#1639 is complete. The pending client cells must be closed in their respective repositories before the parent delivery item can be marked complete.
