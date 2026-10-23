**Use this when:** a migrated server configuration reports ignored settings, repeated config reads fail, or an admin update must retain fields from another version.

# Persisted server configuration compatibility

RustFS reads its native subsystem/KVS format and supported external object forms at the persistence boundary in `crates/ecstore/src/config/com.rs`. A successful compatibility decode applies supported settings and retains unsupported fields in the raw seed used by subsequent config saves. Retention does not imply runtime support.

MinIO heal keys `bitrotscan`, `drive_workers`, `max_io`, and `max_sleep` are retained but are not applied. They do not have a one-to-one mapping to RustFS heal controls. In particular, do not copy `bitrotscan` into `bitrot_cycle` without checking the RustFS setting's semantics and accepted values. Export the current config through the admin API and configure supported RustFS settings deliberately.

Legacy scalar or null scanner/heal sections contain no decodable settings. Recognized external configurations ignore these remnants, and the next save removes them. Malformed values inside supported object/KVS structures still fail decoding. An unrecognized document does not become valid merely because it contains an ignorable section.

## Diagnostic behavior

Successful compatibility decoding emits `server_config_compatibility_ignored` as one warning summary for the ignored scanner/heal settings. It reports counts, JSON shape kinds, and only the four fixed MinIO heal key names above. It does not log arbitrary key names, values, raw sections, or a payload fingerprint.

Each process retains the 64 most recently used diagnostic signatures. Re-reading the same signature, including after an intervening signature, suppresses another warning while the entry remains cached. Unknown-key count does not consume cache slots per key. Eviction or process restart can cause a warning to appear again. Changing only an ignored value does not require a new warning because the unsupported setting is still not applied.

Final decode/decrypt failures remain errors on every read. Their diagnostic events, `server_config_decode_failed` and `server_config_decrypt_failed`, use a separate 64-entry history. A distinct failure reports immediately; an unchanged failure reports again on the first read at least five minutes after its previous report, including the number of suppressed attempts. There is no timer that retries or repairs configuration, and rate limiting does not convert a failed read into success. Existing startup recovery and admin error handling continue to determine the caller's result.

## Saving and resetting aliases

The encoder retains unknown fields and extra KVS attributes at their original paths, including `storage_class` alongside `storageclass`, `identity_openid` alongside `openid`, and `audit` alongside `logger`. Notification/audit subsystem aliases and `_`/`default` branches are also retained where they contain data. The encoder synchronizes supported settings from the effective configuration and clears stale shadow overrides, so resetting the preferred branch cannot expose an older value.

Unknown fields with the same name in different alias branches remain independent. They are not merged and do not trigger an alias-conflict rejection. A known KVS entry carrying extra attributes may remain with its effective default value after reset. A deleted target with retained KVS attributes stays disabled rather than inheriting an enabled default target.

Config updates continue to use the existing snapshot lock and conditional object write. Canonical saves use the semantic no-op shortcut; alias-bearing seeds are encoded and compared by bytes so stale shadow values are synchronized before treating an update as unchanged. A subsequent unchanged save produces no further write.

Before replacing or manually editing a persisted config, retain a protected backup and use the supported admin read/modify/write path where possible. Do not delete unknown fields solely to silence a compatibility warning. To roll back, restore the prior binary and, if a deployment-specific format requires it, the protected pre-change config through the normal operational recovery procedure.

## Interoperability evidence

The regression corpus in `crates/ecstore/tests/fixtures/server-config/` contains a complete plaintext config object produced by the pinned MinIO release documented in its README. It includes native KVS arrays and null subsystem defaults. Tests exercise complete-payload decode, seeded save/reload, and preservation of unsupported heal controls. A separate test wraps those plaintext bytes with RustFS encryption to exercise the existing decrypt callback entry; it does not establish interoperability with a MinIO-produced encrypted envelope.
