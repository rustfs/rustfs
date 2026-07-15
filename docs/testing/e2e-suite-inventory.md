# e2e_test suite inventory

> Authoritative per-module test counts for the `e2e_test` crate (backlog#1149
> ci-4), generated from `cargo nextest list -p e2e_test`. Regenerate with:
> ```bash
> cargo nextest list -p e2e_test --message-format oneline | awk '{split($2,a,"::"); print a[1]}' | sort | uniq -c
> ```
> Modules marked ✅ are in the PR smoke profile `e2e-smoke`
> (`.config/nextest.toml`); admission criteria: `crates/e2e_test/README.md`.
> 🌙 marks tests in the scheduled `e2e-repl-nightly` profile (backlog#1147
> repl-1): `replication_extension_test` splits 20 fast tests into the PR smoke
> lane and 24 slow / `_real_dual_node` / `_real_three_node` / `_real_single_node` tests into the
> nightly lane (`.github/workflows/e2e-replication-nightly.yml`).
> Note: counts exclude `#[ignore]`d tests (nextest lists them separately).

| module | tests | PR smoke |
|---|---|---|
| admin_auth_test | 3 |  |
| admin_timeout_regression_test | 1 |  |
| anonymous_access_test | 3 | ✅ |
| archive_download_integrity_test | 13 |  |
| bucket_logging_test | 3 |  |
| bucket_policy_check_test | 1 | ✅ |
| checksum_upload_test | 6 |  |
| cluster_concurrency_test | 2 |  |
| common | 3 |  |
| compression_test | 1 |  |
| content_encoding_test | 3 | ✅ |
| copy_object_metadata_test | 1 | ✅ |
| copy_object_version_restore_test | 1 |  |
| copy_source_invalid_date_test | 1 | ✅ |
| create_bucket_region_test | 2 | ✅ |
| degraded_read_eof_regression_test | 3 |  |
| delete_marker_migration_semantics_test | 2 | ✅ |
| delete_object_no_content_length_test | 1 |  |
| delete_objects_versioning_test | 2 | ✅ |
| existing_object_tag_policy_test | 4 |  |
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
| namespace_lock_quorum_test | 2 |  |
| negative_sigv4_test | 6 |  |
| object_lambda_test | 16 |  |
| object_lock | 33 |  |
| overwrite_cleanup_regression_test | 1 |  |
| presigned_negative_test | 7 | ✅ |
| protocols | 16 |  |
| quota_test | 13 |  |
| reliability_disk_fault_test | 3 |  |
| reliant | 9 | 3 ✅ |
| replication_extension_test | 44 | 20 ✅ +24 🌙 |
| security_boundary_test | 4 |  |
| server_startup_failfast_test | 1 |  |
| snowball_auto_extract_test | 6 |  |
| special_chars_test | 14 | ✅ |
| stale_multipart_cleanup_cluster_test | 1 |  |
| tls_gen | 3 |  |
| version_id_regression_test | 10 | ✅ |

**Total listed: 425 tests across 52 modules · PR smoke subset: 93 tests / 20 modules** (18 full modules + 3 `reliant::lifecycle` tests + 20 of `replication_extension_test`) **· nightly `e2e-repl-nightly`: 24 tests** · generated 2026-07-15.
