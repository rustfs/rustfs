---
name: plugin-contract-guard
description: Invariants and change procedure for the target-plugin / extension system — plugin manifests, admin plugin/extension catalog and instance APIs, secret redaction, external-plugin install policy. Use when editing crates/targets (manifest, plugin, control_plane, catalog, runtime), crates/extension-schema, or rustfs/src/admin plugin_contract.rs / plugins_*.rs / extensions.rs / target_descriptor.rs.
---

# Plugin & Extension Contract Guard

The "plugin system" spans four surfaces that must stay consistent:

| Surface | Location |
|---|---|
| Manifests & registry | `crates/targets/src/{manifest,plugin}.rs` |
| Install/enable planning (control plane) | `crates/targets/src/control_plane.rs` |
| Extension schemas | `crates/extension-schema/src/lib.rs`, `crates/targets/src/catalog/extension.rs` |
| Admin API contract | `rustfs/src/admin/plugin_contract.rs`, `handlers/{plugins_catalog,plugins_instances,extensions,target_descriptor}.rs` |

## Hard invariants (verify before merging)

1. **Secrets have one source of truth.** Secret config keys are declared only
   in the plugin manifest (`TargetPluginManifest.secret_fields`,
   `crates/targets/src/manifest.rs`) and flow to admin via
   `AdminTargetSpec.secret_fields`. Never add a hand-maintained per-service
   secret table in a handler; if redaction misses a field, fix the manifest.

2. **Redaction must round-trip.** Instance GET responses replace secret values
   with `***redacted***` (`REDACTED_SECRET_VALUE` in `plugins_instances.rs`).
   Instance PUT restores the stored secret when it receives that placeholder
   back (`restore_redacted_secret_values`). Any new read or write path for
   target config must keep both halves: redact on the way out, restore the
   placeholder on the way in. The placeholder literal must never be persisted.

3. **Fixtures never reach production responses.**
   `example_external_webhook_plugin()` (`crates/targets/src/catalog/mod.rs`)
   is a test/demo fixture for control-plane planning tests. Production
   catalog/extension handlers must not include it; regression tests
   (`plugin_catalog_never_exposes_example_or_external_fixtures`,
   `extension_catalog_never_exposes_example_or_external_fixtures`) enforce it.

4. **External plugin flow is planning-only and deny-by-default.**
   `plan_external_target_plugin_action` returns decisions, it executes
   nothing. `TargetPluginExternalFlowGate::default()` is fully closed and
   `TargetPluginInstallPolicy::default().allowed_download_hosts` is empty —
   keep it that way; tests opt in via explicit policies. Install validation
   requires https, an allowlisted host, a full 64-hex-char sha256 digest,
   signature and provenance URIs, and an artifact matching the host
   `target_triple`.

5. **Custom target types must not collide.** Unknown target types get an
   interned unique `custom:<type>` plugin id (`custom_plugin_id` in
   `manifest.rs`). Custom plugins with secrets must register via
   `TargetPluginDescriptor::with_manifest` and declare `secret_fields`;
   `::new` derives a manifest with no secrets.

## Changing the admin JSON contract

- Shapes are locked twice in `plugin_contract.rs` tests: insta snapshots
  (`rustfs/src/admin/snapshots/`) plus literal `json!` assertions. Update
  both deliberately; a shape change is a console-facing API change.
- Field naming is `snake_case`, except discovery blocks
  (`runtimeCapabilities`, `clusterSnapshot`, `extensionsCatalog`) which are
  camelCase **by cross-endpoint convention** (same shape in `system.rs`,
  `console.rs`, `pools.rs`). Do not "fix" that inconsistency locally.
- Contract types deliberately duplicate `rustfs_targets` types
  (anti-corruption layer). Add a `From` impl; do not serialize internal
  types directly.

## Handler conventions

- Every new admin plugin/extension route needs authorization at the top of
  `call` and an `include_str!` guard test asserting it (repo-wide pattern —
  see `plugin_instance_handlers_require_admin_authorization_contract`).
- Reads use `GetBucketTargetAction` (instances) or `ServerInfoAdminAction`
  (catalogs); writes use `SetBucketTargetAction`.
- Refresh persisted module switches once per request
  (`refresh_persisted_module_switches`), then evaluate the sync
  `module_disabled_block_reason` per domain — do not re-read the store per
  domain or per instance.

## Generic bounds

Event-payload generics use the `PluginEvent` blanket trait
(`crates/targets/src/plugin.rs`). Do not respell
`Send + Sync + 'static + Clone + Serialize + DeserializeOwned`.
