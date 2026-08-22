# e2e_test suite inventory

> Authoritative per-module test counts for the `e2e_test` crate (backlog#1149
> ci-4), generated from `cargo nextest list -p e2e_test`. Regenerate with:
> ```bash
> cargo nextest list -p e2e_test --message-format json | jq -r '.["rust-suites"][]?.testcases | to_entries[] | select(.value.ignored == false) | .key | split("::")[0]' | sort | uniq -c
> ```
> Modules marked ✅ are in the PR smoke profile `e2e-smoke`; 🌙 marks the
> cluster, protocol, and replication subsets in the consolidated nightly
> workflow. The `e2e-full` merge/main profile covers the remaining default
> single-node tests. Committed test-ID digests are enforced before each run.
> Note: counts exclude `#[ignore]`d tests (nextest lists them separately).
> Managed-SSE (SSE-S3/SSE-KMS) replication contracts assert successful
> re-encryption on the target (backlog#1783); SSE-C replication still pins a
> fail-closed FAILED contract until ciphertext passthrough lands.

| module | tests | PR smoke |
|---|---|---|
| admin_auth_test | 4 | ✅ |
| admin_iam_crud_test | 3 | ✅ |
| admin_pools_test | 1 | ✅ |
| admin_timeout_regression_test | 1 | 🌙 |
| anonymous_access_test | 4 | ✅ |
| api_rate_limit_test | 3 |  |
| archive_download_integrity_test | 13 |  |
| bucket_logging_test | 3 |  |
| bucket_policy_check_test | 1 | ✅ |
| bucket_stats_regression_test | 3 |  |
| chaos | 2 |  |
| checksum_upload_test | 7 |  |
| cluster_concurrency_test | 2 | 🌙 |
| cluster_multidrive_pool_test | 2 | 🌙 |
| common | 14 |  |
| compression_test | 6 | ✅ |
| connection_cap_test | 2 |  |
| console_smoke_test | 1 | ✅ |
| content_encoding_test | 3 | ✅ |
| copy_object_checksum_test | 7 |  |
| copy_object_metadata_test | 4 | ✅ |
| copy_object_tagging_test | 2 | ✅ |
| copy_object_version_restore_test | 2 |  |
| copy_source_invalid_date_test | 1 | ✅ |
| create_bucket_region_test | 2 | ✅ |
| degraded_read_eof_regression_test | 3 |  |
| delete_marker_migration_semantics_test | 2 | ✅ |
| delete_object_no_content_length_test | 1 |  |
| delete_objects_versioning_test | 2 | ✅ |
| delete_regression_test | 5 |  |
| distributed_startup_regression_test | 3 |  |
| existing_object_tag_policy_test | 4 |  |
| fake_s3_target | 6 | ✅ |
| fault_proxy | 7 |  |
| get_codec_streaming_compat_test | 1 |  |
| get_stream_failure_observability_test | 1 |  |
| group_delete_test | 1 |  |
| head_object_consistency_test | 1 | ✅ |
| head_object_range_test | 1 | ✅ |
| heal_erasure_disk_rebuild_test | 4 | 🌙 |
| inline_fast_path_cluster_test | 16 |  |
| internode_rpc_signature_e2e_test | 5 |  |
| kms | 46 |  |
| leading_slash_key_test | 2 | ✅ |
| lifecycle_regression_test | 4 |  |
| list_buckets_auth_test | 1 | ✅ |
| list_buckets_double_slash_test | 3 | ✅ |
| list_buckets_iam_filter_test | 1 | ✅ |
| list_object_versions_metadata_extension_test | 1 |  |
| list_object_versions_regression_test | 2 | ✅ |
| list_objects_duplicates_test | 3 | ✅ |
| list_objects_v2_metadata_extension_test | 1 |  |
| list_objects_v2_pagination_test | 12 | ✅ |
| listing_regression_test | 4 |  |
| mc_mirror_small_bucket_test | 1 |  |
| multipart_auth_test | 75 |  |
| multipart_storage_class_test | 3 | ✅ |
| namespace_lock_quorum_test | 2 | 🌙 |
| negative_sigv4_test | 6 | ✅ |
| notification_startup_regression_test | 2 |  |
| notification_webhook_test | 3 | ✅ |
| object_lambda_test | 16 | 🌙 |
| object_lock | 34 |  |
| overwrite_cleanup_regression_test | 1 |  |
| presigned_negative_test | 7 | ✅ |
| protocols | 16 | 🌙 |
| quota_test | 14 |  |
| reliability_disk_fault_test | 4 |  |
| reliant | 25 | 19 ✅ |
| replication_extension_test | 75 | 20 ✅ +55 🌙 |
| security_boundary_test | 4 |  |
| server_startup_failfast_test | 1 |  |
| snowball_auto_extract_test | 6 |  |
| special_chars_test | 14 | ✅ |
| ssec_copy_test | 2 | ✅ |
| stale_multipart_cleanup_cluster_test | 1 | 🌙 |
| storage_class_capability_test | 4 | ✅ |
| sts_query_compat_test | 6 | ✅ |
| tier_transition_regression_test | 3 |  |
| tls_gen | 3 |  |
| tls_hot_reload_test | 1 | ✅ |
| version_id_regression_test | 10 | ✅ |

**Total listed: 575 tests across 82 modules · PR smoke: 163 tests / 36 modules · merge/main full: 453 tests / 73 modules · nightly replication: 55 tests · nightly cluster faults: 28 tests / 7 modules · nightly protocols: 16 tests** · updated 2026-08-23.
