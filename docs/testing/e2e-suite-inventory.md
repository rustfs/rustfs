# e2e_test suite inventory

> Authoritative per-module test counts for the `e2e_test` crate (backlog#1149
> ci-4), generated from `cargo nextest list -p e2e_test`. Regenerate with:
> ```bash
> cargo nextest list -p e2e_test --message-format json | jq -r '.["rust-suites"][]?.testcases | to_entries[] | select(.value.ignored == false) | .key | split("::")[0]' | sort | uniq -c
> ```
> Modules marked ✅ are in the PR smoke profile `e2e-smoke`
> (`.config/nextest.toml`); admission criteria: `crates/e2e_test/README.md`.
> 🌙 marks tests in the scheduled `e2e-repl-nightly` profile (backlog#1147
> repl-1): `replication_extension_test` splits 20 fast tests into the PR smoke
> lane and 27 slow / `_real_dual_node` / `_real_three_node` / `_real_single_node` tests into the
> nightly lane (`.github/workflows/e2e-replication-nightly.yml`).
> Note: counts exclude `#[ignore]`d tests (nextest lists them separately).
> The SSE-S3 replication contract is ignored under backlog#1291 until its
> plaintext downgrade is fixed.

| module | tests | PR smoke |
|---|---|---|
| admin_auth_test | 3 | ✅ |
| admin_iam_crud_test | 2 | ✅ |
| admin_pools_test | 1 | ✅ |
| admin_timeout_regression_test | 1 |  |
| anonymous_access_test | 3 | ✅ |
| api_rate_limit_test | 3 |  |
| archive_download_integrity_test | 13 |  |
| bucket_logging_test | 3 |  |
| bucket_policy_check_test | 1 | ✅ |
| checksum_upload_test | 7 |  |
| cluster_concurrency_test | 2 |  |
| cluster_multidrive_pool_test | 2 |  |
| common | 10 |  |
| compression_test | 1 |  |
| connection_cap_test | 2 |  |
| console_smoke_test | 1 | ✅ |
| content_encoding_test | 3 | ✅ |
| copy_object_checksum_test | 3 |  |
| copy_object_metadata_test | 4 | ✅ |
| copy_object_tagging_test | 2 | ✅ |
| copy_object_version_restore_test | 2 |  |
| copy_source_invalid_date_test | 1 | ✅ |
| create_bucket_region_test | 2 | ✅ |
| degraded_read_eof_regression_test | 3 |  |
| delete_marker_migration_semantics_test | 2 | ✅ |
| delete_object_no_content_length_test | 1 |  |
| delete_objects_versioning_test | 2 | ✅ |
| existing_object_tag_policy_test | 4 |  |
| fake_s3_target | 4 | ✅ |
| fault_proxy | 7 |  |
| get_codec_streaming_compat_test | 1 |  |
| head_object_consistency_test | 1 | ✅ |
| head_object_range_test | 1 | ✅ |
| heal_erasure_disk_rebuild_test | 3 |  |
| kms | 40 |  |
| leading_slash_key_test | 2 | ✅ |
| list_buckets_double_slash_test | 3 | ✅ |
| list_object_versions_metadata_extension_test | 1 |  |
| list_object_versions_regression_test | 2 | ✅ |
| list_objects_duplicates_test | 3 | ✅ |
| list_objects_v2_metadata_extension_test | 1 |  |
| list_objects_v2_pagination_test | 12 | ✅ |
| mc_mirror_small_bucket_test | 1 |  |
| multipart_auth_test | 109 |  |
| multipart_storage_class_test | 3 | ✅ |
| namespace_lock_quorum_test | 2 |  |
| negative_sigv4_test | 6 | ✅ |
| notification_webhook_test | 2 | ✅ |
| object_lambda_test | 16 |  |
| object_lock | 33 |  |
| overwrite_cleanup_regression_test | 1 |  |
| presigned_negative_test | 7 | ✅ |
| protocols | 16 |  |
| quota_test | 14 |  |
| reliability_disk_fault_test | 3 |  |
| reliant | 10 | 4 ✅ |
| replication_extension_test | 47 | 20 ✅ +27 🌙 |
| security_boundary_test | 4 |  |
| server_startup_failfast_test | 1 |  |
| snowball_auto_extract_test | 6 |  |
| special_chars_test | 14 | ✅ |
| stale_multipart_cleanup_cluster_test | 1 |  |
| storage_class_capability_test | 4 | ✅ |
| tls_gen | 3 |  |
| tls_hot_reload_test | 1 | ✅ |
| version_id_regression_test | 10 | ✅ |

`notification_webhook_test` also has 1 ignored store-and-forward regression tracked by rustfs#4852; ignored tests are excluded from the active counts above.

**Total listed: 479 tests across 66 modules · PR smoke subset: 126 tests / 31 modules** (29 full modules + 4 `reliant` tests + 20 of `replication_extension_test`) **· nightly `e2e-repl-nightly`: 27 tests** · generated 2026-07-24.
