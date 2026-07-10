# e2e_test suite inventory

> Authoritative per-module test counts for the `e2e_test` crate (backlog#1149
> ci-4), generated from `cargo nextest list -p e2e_test`. Regenerate with:
> ```bash
> cargo nextest list -p e2e_test | grep "^ " | sed "s/^ *//;s/::.*//" | sort | uniq -c
> ```
> Modules marked ✅ are in the PR smoke profile `e2e-smoke`
> (`.config/nextest.toml`); admission criteria: `crates/e2e_test/README.md`.
> Note: counts exclude `#[ignore]`d tests (nextest lists them separately).

| module | tests | PR smoke |
|---|---|---|
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
| delete_marker_migration_semantics_test | 2 | ✅ |
| delete_object_no_content_length_test | 1 |  |
| delete_objects_versioning_test | 2 | ✅ |
| existing_object_tag_policy_test | 4 |  |
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
| object_lambda_test | 16 |  |
| object_lock | 33 |  |
| overwrite_cleanup_regression_test | 1 |  |
| protocols | 16 |  |
| quota_test | 13 |  |
| reliability_disk_fault_test | 3 |  |
| reliant | 6 |  |
| replication_extension_test | 36 |  |
| security_boundary_test | 4 |  |
| server_startup_failfast_test | 1 |  |
| snowball_auto_extract_test | 6 |  |
| special_chars_test | 14 | ✅ |
| stale_multipart_cleanup_cluster_test | 1 |  |
| tls_gen | 3 |  |
| version_id_regression_test | 10 | ✅ |

**Total listed: 394 tests across 47 modules · PR smoke subset: 63 tests / 17 modules** · generated 2026-07-10.
