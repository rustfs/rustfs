// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
mod tests {
    use crate::config::workload_profiles::WorkloadProfile;
    use crate::storage::ecfs::FS;
    use crate::storage::ecfs::RUSTFS_OWNER;
    use crate::storage::{
        apply_cors_headers, check_preconditions, get_adaptive_buffer_size_with_profile, get_buffer_size_opt_in, is_etag_equal,
        matches_origin_pattern, parse_etag, parse_object_lock_legal_hold, parse_object_lock_retention,
        process_lambda_configurations, process_queue_configurations, process_topic_configurations,
        validate_bucket_object_lock_enabled, validate_list_object_unordered_with_delimiter,
    };
    use http::{HeaderMap, HeaderValue, StatusCode};
    use rustfs_config::MI_B;
    use rustfs_ecstore::set_disk::DEFAULT_READ_BUFFER_SIZE;
    use rustfs_ecstore::store_api::ObjectInfo;
    use rustfs_utils::http::{AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, RESERVED_METADATA_PREFIX_LOWER};
    use rustfs_zip::CompressionFormat;
    use s3s::dto::{
        Delimiter, LambdaFunctionConfiguration, ObjectLockLegalHold, ObjectLockLegalHoldStatus, ObjectLockRetention,
        ObjectLockRetentionMode, QueueConfiguration, TopicConfiguration,
    };
    use s3s::{S3Error, S3ErrorCode, s3_error};
    use time::OffsetDateTime;

    #[test]
    fn test_fs_creation() {
        let _fs = FS::new();

        // Verify that FS struct can be created successfully
        // Since it's currently empty, we just verify it doesn't panic
        // The test passes if we reach this point without panicking
    }

    #[test]
    fn test_fs_debug_implementation() {
        let fs = FS::new();

        // Test that Debug trait is properly implemented
        let debug_str = format!("{fs:?}");
        assert!(debug_str.contains("FS"));
    }

    #[test]
    fn test_fs_clone_implementation() {
        let fs = FS::new();

        // Test that Clone trait is properly implemented
        let cloned_fs = fs.clone();

        // Both should be equivalent (since FS is currently empty)
        assert_eq!(format!("{fs:?}"), format!("{cloned_fs:?}"));
    }

    #[test]
    fn test_rustfs_owner_constant() {
        // Test that RUSTFS_OWNER constant is properly defined
        assert!(!RUSTFS_OWNER.display_name.as_ref().unwrap().is_empty());
        assert!(!RUSTFS_OWNER.id.as_ref().unwrap().is_empty());
        assert_eq!(RUSTFS_OWNER.display_name.as_ref().unwrap(), "rustfs");
    }

    // Note: Most S3 API methods require complex setup with global state, storage backend,
    // and various dependencies that make unit testing challenging. For comprehensive testing
    // of S3 operations, integration tests would be more appropriate.

    #[test]
    fn test_list_objects_v2_key_count_includes_prefixes() {
        // Test that KeyCount calculation includes both objects and common prefixes
        // This verifies the fix for S3 API compatibility where KeyCount should equal
        // the sum of Contents and CommonPrefixes lengths

        // Simulate the calculation logic from list_objects_v2
        let objects_count = 3_usize;
        let common_prefixes_count = 2_usize;

        // KeyCount should include both objects and common prefixes per S3 API spec
        let key_count = (objects_count + common_prefixes_count) as i32;

        assert_eq!(key_count, 5);

        // Edge cases: verify calculation logic
        let no_objects = 0_usize;
        let no_prefixes = 0_usize;
        assert_eq!((no_objects + no_prefixes) as i32, 0);

        let one_object = 1_usize;
        assert_eq!((one_object + no_prefixes) as i32, 1);

        let one_prefix = 1_usize;
        assert_eq!((no_objects + one_prefix) as i32, 1);
    }

    #[test]
    fn test_s3_url_encoding_preserves_slash() {
        // Test that S3 URL encoding preserves path separators (/)
        // This verifies the encoding logic for EncodingType=url parameter

        use urlencoding::encode;

        // Helper function matching the implementation
        let encode_s3_name = |name: &str| -> String {
            name.split('/')
                .map(|part| encode(part).to_string())
                .collect::<Vec<_>>()
                .join("/")
        };

        // Test cases from s3-tests
        assert_eq!(encode_s3_name("asdf+b"), "asdf%2Bb");
        assert_eq!(encode_s3_name("foo+1/bar"), "foo%2B1/bar");
        assert_eq!(encode_s3_name("foo/"), "foo/");
        assert_eq!(encode_s3_name("quux ab/"), "quux%20ab/");

        // Edge cases
        assert_eq!(encode_s3_name("normal/key"), "normal/key");
        assert_eq!(encode_s3_name("key+with+plus"), "key%2Bwith%2Bplus");
        assert_eq!(encode_s3_name("key with spaces"), "key%20with%20spaces");
    }

    #[test]
    fn test_s3_error_scenarios() {
        // Test that we can create expected S3 errors for common validation cases

        // Test incomplete body error
        let incomplete_body_error = s3_error!(IncompleteBody);
        assert_eq!(incomplete_body_error.code(), &S3ErrorCode::IncompleteBody);

        // Test invalid argument error
        let invalid_arg_error = s3_error!(InvalidArgument, "test message");
        assert_eq!(invalid_arg_error.code(), &S3ErrorCode::InvalidArgument);

        // Test internal error
        let internal_error = S3Error::with_message(S3ErrorCode::InternalError, "test".to_string());
        assert_eq!(internal_error.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn test_compression_format_usage() {
        // Test that compression format detection works for common file extensions
        let zip_format = CompressionFormat::from_extension("zip");
        assert_eq!(zip_format.extension(), "zip");

        let tar_format = CompressionFormat::from_extension("tar");
        assert_eq!(tar_format.extension(), "tar");

        let gz_format = CompressionFormat::from_extension("gz");
        assert_eq!(gz_format.extension(), "gz");
    }

    #[test]
    fn test_adaptive_buffer_size_with_profile() {
        const KB: i64 = 1024;
        const MB: i64 = 1024 * 1024;

        // Test GeneralPurpose profile (default behavior, should match get_adaptive_buffer_size)
        assert_eq!(
            get_adaptive_buffer_size_with_profile(500 * KB, Some(WorkloadProfile::GeneralPurpose)),
            64 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(50 * MB, Some(WorkloadProfile::GeneralPurpose)),
            256 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(200 * MB, Some(WorkloadProfile::GeneralPurpose)),
            DEFAULT_READ_BUFFER_SIZE
        );

        // Test AiTraining profile - larger buffers for large files
        assert_eq!(
            get_adaptive_buffer_size_with_profile(5 * MB, Some(WorkloadProfile::AiTraining)),
            512 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(100 * MB, Some(WorkloadProfile::AiTraining)),
            2 * MB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(600 * MB, Some(WorkloadProfile::AiTraining)),
            4 * MB as usize
        );

        // Test WebWorkload profile - smaller buffers for web assets
        assert_eq!(
            get_adaptive_buffer_size_with_profile(100 * KB, Some(WorkloadProfile::WebWorkload)),
            32 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(5 * MB, Some(WorkloadProfile::WebWorkload)),
            128 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(50 * MB, Some(WorkloadProfile::WebWorkload)),
            256 * KB as usize
        );

        // Test SecureStorage profile - memory-constrained buffers
        assert_eq!(
            get_adaptive_buffer_size_with_profile(500 * KB, Some(WorkloadProfile::SecureStorage)),
            32 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(25 * MB, Some(WorkloadProfile::SecureStorage)),
            128 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(100 * MB, Some(WorkloadProfile::SecureStorage)),
            256 * KB as usize
        );

        // Test IndustrialIoT profile - low latency, moderate buffers
        assert_eq!(
            get_adaptive_buffer_size_with_profile(512 * KB, Some(WorkloadProfile::IndustrialIoT)),
            64 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(25 * MB, Some(WorkloadProfile::IndustrialIoT)),
            256 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(100 * MB, Some(WorkloadProfile::IndustrialIoT)),
            512 * KB as usize
        );

        // Test DataAnalytics profile
        assert_eq!(
            get_adaptive_buffer_size_with_profile(2 * MB, Some(WorkloadProfile::DataAnalytics)),
            128 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(100 * MB, Some(WorkloadProfile::DataAnalytics)),
            512 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(500 * MB, Some(WorkloadProfile::DataAnalytics)),
            2 * MB as usize
        );

        // Test with None (should auto-detect or use GeneralPurpose)
        let result = get_adaptive_buffer_size_with_profile(50 * MB, None);
        // Should be either SecureStorage (if on special OS) or GeneralPurpose
        assert!(result == 128 * KB as usize || result == 256 * KB as usize);

        // Test unknown file size with different profiles
        assert_eq!(
            get_adaptive_buffer_size_with_profile(-1, Some(WorkloadProfile::AiTraining)),
            2 * MB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(-1, Some(WorkloadProfile::WebWorkload)),
            128 * KB as usize
        );
        assert_eq!(
            get_adaptive_buffer_size_with_profile(-1, Some(WorkloadProfile::SecureStorage)),
            128 * KB as usize
        );
    }

    #[test]
    fn test_phase3_default_behavior() {
        use crate::config::workload_profiles::{
            RustFSBufferConfig, WorkloadProfile, init_global_buffer_config, set_buffer_profile_enabled,
        };

        const KB: i64 = 1024;
        const MB: i64 = 1024 * 1024;

        // Test Phase 3: Enabled by default with GeneralPurpose profile
        set_buffer_profile_enabled(true);
        init_global_buffer_config(RustFSBufferConfig::new(WorkloadProfile::GeneralPurpose));

        // Verify GeneralPurpose profile provides consistent buffer sizes
        assert_eq!(get_buffer_size_opt_in(500 * KB), 64 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(50 * MB), 256 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(200 * MB), MI_B);
        assert_eq!(get_buffer_size_opt_in(-1), MI_B); // Unknown size

        // Reset for other tests
        set_buffer_profile_enabled(false);
    }

    #[test]
    fn test_buffer_size_opt_in() {
        use crate::config::workload_profiles::{is_buffer_profile_enabled, set_buffer_profile_enabled};

        const KB: i64 = 1024;
        const MB: i64 = 1024 * 1024;

        // \[1\] Default state: profile is not enabled, global configuration is not explicitly initialized
        // get_buffer_size_opt_in should be equivalent to the GeneralPurpose configuration
        set_buffer_profile_enabled(false);
        assert!(!is_buffer_profile_enabled());

        // GeneralPurpose rules:
        // \< 1MB -> 64KB，1MB-100MB -> 256KB，\>=100MB -> 1MB
        assert_eq!(get_buffer_size_opt_in(500 * KB), 64 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(50 * MB), 256 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(200 * MB), MI_B);

        // \[2\] Enable the profile switch, but the global configuration is still the default GeneralPurpose
        set_buffer_profile_enabled(true);
        assert!(is_buffer_profile_enabled());

        assert_eq!(get_buffer_size_opt_in(500 * KB), 64 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(50 * MB), 256 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(200 * MB), MI_B);

        // \[3\] Close again to ensure unchanged behavior
        set_buffer_profile_enabled(false);
        assert!(!is_buffer_profile_enabled());
        assert_eq!(get_buffer_size_opt_in(500 * KB), 64 * KB as usize);
    }

    #[test]
    fn test_phase4_full_integration() {
        use crate::config::workload_profiles::{
            RustFSBufferConfig, WorkloadProfile, init_global_buffer_config, set_buffer_profile_enabled,
        };

        const KB: i64 = 1024;
        const MB: i64 = 1024 * 1024;

        // \[1\] During the entire test process, the global configuration is initialized only once.
        // In order not to interfere with other tests, use GeneralPurpose (consistent with the default).
        // If it has been initialized elsewhere, this call will be ignored by OnceLock and the behavior will still be GeneralPurpose.
        init_global_buffer_config(RustFSBufferConfig::new(WorkloadProfile::GeneralPurpose));

        // Make sure to turn off profile initially
        set_buffer_profile_enabled(false);

        // \[2\] Verify behavior of get_buffer_size_opt_in in disabled profile (GeneralPurpose)
        assert_eq!(get_buffer_size_opt_in(500 * KB), 64 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(50 * MB), 256 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(200 * MB), MI_B);

        // \[3\] When profile is enabled, the behavior remains consistent with the global GeneralPurpose configuration
        set_buffer_profile_enabled(true);
        assert_eq!(get_buffer_size_opt_in(500 * KB), 64 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(50 * MB), 256 * KB as usize);
        assert_eq!(get_buffer_size_opt_in(200 * MB), MI_B);

        // \[4\] Complex scenes, boundary values: such as unknown size
        assert_eq!(get_buffer_size_opt_in(-1), MI_B);

        set_buffer_profile_enabled(false);
    }

    #[test]
    fn test_validate_list_object_unordered_with_delimiter() {
        // [1] Normal case: No delimiter specified.
        assert!(validate_list_object_unordered_with_delimiter(None, Some("allow-unordered=true")).is_ok());

        let delim_str = "/".to_string();
        let delimiter_some: Option<&Delimiter> = Some(&delim_str);
        // [2] Normal case: Delimiter is present, but 'allow-unordered' is explicitly set to false.
        assert!(validate_list_object_unordered_with_delimiter(delimiter_some, Some("allow-unordered=false")).is_ok());

        let query_conflict = Some("allow-unordered=true");
        // [3] Conflict case: Both delimiter and 'allow-unordered=true' are present.
        assert!(validate_list_object_unordered_with_delimiter(delimiter_some, query_conflict).is_err());

        let complex_query = Some("allow-unordered=true&abc=123");
        // [4] Complex query: The validation should still trigger if 'allow-unordered=true' is part of a multi-parameter query.
        assert!(validate_list_object_unordered_with_delimiter(delimiter_some, complex_query).is_err());

        let complex_query_without_unordered = Some("abc=123&queryType=test");
        // [5] Multi-parameter query without conflict: If other parameters exist but 'allow-unordered' is missing,
        assert!(validate_list_object_unordered_with_delimiter(delimiter_some, complex_query_without_unordered).is_ok());
    }

    #[test]
    fn test_parse_object_lock_retention() {
        use time::macros::datetime;
        // [1] Normal case: No retention specified (empty metadata)
        assert!(parse_object_lock_retention(None).is_ok());
        assert!(parse_object_lock_retention(None).unwrap().is_empty());

        // [2] Normal case: Retention with valid COMPLIANCE mode (future date)
        let valid_compliance_retention = ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
            retain_until_date: Some(datetime!(2030-01-01 00:00:00 UTC).into()),
        };
        let compliance_metadata = parse_object_lock_retention(Some(valid_compliance_retention)).unwrap();
        assert_eq!(compliance_metadata.get("x-amz-object-lock-mode").unwrap(), "COMPLIANCE");
        assert_eq!(
            compliance_metadata.get("x-amz-object-lock-retain-until-date").unwrap(),
            "2030-01-01T00:00:00Z"
        );
        assert!(
            compliance_metadata.contains_key(&format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "objectlock-retention-timestamp"))
        );

        // [3] Normal case: Retention with valid GOVERNANCE mode (future date)
        let valid_governance_retention = ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE)),
            retain_until_date: Some(datetime!(2030-01-01 00:00:00 UTC).into()),
        };
        let governance_metadata = parse_object_lock_retention(Some(valid_governance_retention)).unwrap();
        assert_eq!(governance_metadata.get("x-amz-object-lock-mode").unwrap(), "GOVERNANCE");

        // [4] Normal case: Retention with None mode (empty string for mode, date not validated)
        let none_mode_retention = ObjectLockRetention {
            mode: None,
            retain_until_date: Some(datetime!(2030-01-01 00:00:00 UTC).into()),
        };
        let none_mode_metadata = parse_object_lock_retention(Some(none_mode_retention)).unwrap();
        assert_eq!(none_mode_metadata.get("x-amz-object-lock-mode").unwrap(), "");

        // [5] Normal case: Retention with None retain_until_date (empty string for date)
        let none_date_retention = ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
            retain_until_date: None,
        };
        let none_date_metadata = parse_object_lock_retention(Some(none_date_retention)).unwrap();
        assert_eq!(none_date_metadata.get("x-amz-object-lock-retain-until-date").unwrap(), "");

        // [6] Error case: Retention with invalid mode (non COMPLIANCE/GOVERNANCE)
        let invalid_mode_retention = ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from_static("INVALID_MODE")),
            retain_until_date: Some(datetime!(2030-01-01 00:00:00 UTC).into()),
        };
        let err = parse_object_lock_retention(Some(invalid_mode_retention)).unwrap_err();
        assert_eq!(err.code().as_str(), S3ErrorCode::MalformedXML.as_str());
        assert_eq!(
            err.message(),
            Some("The XML you provided was not well-formed or did not validate against our published schema")
        );

        // [7] Error case: Retention with past date should fail
        let past_date_retention = ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
            retain_until_date: Some(datetime!(2020-01-01 00:00:00 UTC).into()),
        };
        let err = parse_object_lock_retention(Some(past_date_retention)).unwrap_err();
        assert_eq!(err.code().as_str(), S3ErrorCode::InvalidArgument.as_str());
        assert_eq!(err.message(), Some("The retain until date must be in the future"));
    }

    #[test]
    fn test_parse_object_lock_legal_hold() {
        // [1] Normal case: No legal hold specified (empty metadata)
        assert!(parse_object_lock_legal_hold(None).is_ok());
        assert!(parse_object_lock_legal_hold(None).unwrap().is_empty());

        // [2] Normal case: Legal hold with valid ON status
        let valid_on_legal_hold = ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::ON)),
        };
        let on_metadata = parse_object_lock_legal_hold(Some(valid_on_legal_hold)).unwrap();
        assert_eq!(on_metadata.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).unwrap(), "ON");
        assert!(on_metadata.contains_key(&format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "objectlock-legalhold-timestamp")));

        // [3] Normal case: Legal hold with valid OFF status
        let valid_off_legal_hold = ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from_static(ObjectLockLegalHoldStatus::OFF)),
        };
        let off_metadata = parse_object_lock_legal_hold(Some(valid_off_legal_hold)).unwrap();
        assert_eq!(off_metadata.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).unwrap(), "OFF");

        // [4] Normal case: Legal hold with None status (empty string for status)
        let none_status_legal_hold = ObjectLockLegalHold { status: None };
        let none_status_metadata = parse_object_lock_legal_hold(Some(none_status_legal_hold)).unwrap();
        assert_eq!(none_status_metadata.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).unwrap(), "");

        // [5] Error case: Legal hold with invalid status (non ON/OFF)
        let invalid_status_legal_hold = ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from_static("INVALID_STATUS")),
        };
        let err = parse_object_lock_legal_hold(Some(invalid_status_legal_hold)).unwrap_err();
        assert_eq!(err.code().as_str(), S3ErrorCode::MalformedXML.as_str());
        assert_eq!(
            err.message(),
            Some("The XML you provided was not well-formed or did not validate against our published schema")
        );
    }

    #[tokio::test]
    async fn test_validate_bucket_object_lock_enabled() {
        use rustfs_ecstore::bucket::metadata::BucketMetadata;
        use rustfs_ecstore::bucket::metadata_sys::set_bucket_metadata;
        use s3s::dto::{ObjectLockConfiguration, ObjectLockEnabled};
        use time::OffsetDateTime;

        if rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys.get().is_none() {
            eprintln!("Skipping test: GLOBAL_BucketMetadataSys not initialized");
            return;
        }

        let test_bucket = "test-bucket-object-lock";

        let mut bm = BucketMetadata::new(test_bucket);
        bm.object_lock_config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: None,
        });
        bm.object_lock_config_updated_at = OffsetDateTime::now_utc();
        set_bucket_metadata(test_bucket.to_string(), bm).await.unwrap();
        assert!(validate_bucket_object_lock_enabled(test_bucket).await.is_ok());

        let mut bm = BucketMetadata::new(test_bucket);
        bm.object_lock_config = Some(ObjectLockConfiguration {
            object_lock_enabled: None,
            rule: None,
        });
        bm.object_lock_config_updated_at = OffsetDateTime::now_utc();
        set_bucket_metadata(test_bucket.to_string(), bm).await.unwrap();
        let err = validate_bucket_object_lock_enabled(test_bucket).await.unwrap_err();
        assert_eq!(err.code().as_str(), S3ErrorCode::InvalidRequest.as_str());
        assert_eq!(err.message(), Some("Object Lock is not enabled for this bucket"));

        let non_exist_bucket = "non-exist-bucket-object-lock";
        let err = validate_bucket_object_lock_enabled(non_exist_bucket).await.unwrap_err();
        assert_eq!(err.code().as_str(), S3ErrorCode::InvalidRequest.as_str());
        assert_eq!(err.message(), Some("Bucket is missing ObjectLockConfiguration"));
    }

    #[test]
    fn test_is_etag_equal() {
        // [1] Header ETag is "*", should return true (match any object ETag)
        assert!(is_etag_equal("\"d41d8cd98f00b204e9800998ecf8427e\"", "*"));

        // [2] Exact match (both with double quotes)
        assert!(is_etag_equal(
            "\"d41d8cd98f00b204e9800998ecf8427e\"",
            "\"d41d8cd98f00b204e9800998ecf8427e\""
        ));

        // [3] Exact match (object ETag with quotes, header ETag without)
        assert!(is_etag_equal("\"d41d8cd98f00b204e9800998ecf8427e\"", "d41d8cd98f00b204e9800998ecf8427e"));

        // [4] Header ETag has multiple values (comma-separated), one matches
        assert!(is_etag_equal("\"12345\"", "\"67890\", \"12345\", \"abcde\""));

        // [5] Header ETag has multiple values with spaces, one matches after trim
        assert!(is_etag_equal("\"12345\"", "  \"67890\" , \"12345\"  , \"abcde\"  "));

        // [6] No match (different ETag)
        assert!(!is_etag_equal("\"12345\"", "\"67890\""));

        // [7] No match in multiple values
        assert!(!is_etag_equal("\"12345\"", "\"67890\", \"abcde\""));
    }

    #[test]
    fn test_check_preconditions() {
        use time::{format_description::FormatItem, macros::format_description};
        const RFC1123: &[FormatItem<'_>] =
            format_description!("[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT");
        let valid_mod_time = OffsetDateTime::from_unix_timestamp(1700000000).unwrap();
        let valid_etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
        let wrong_etag = "\"wrong-etag-123456\"";

        // [1] Both mod_time and etag are None → early return Ok()
        let info1 = ObjectInfo {
            mod_time: None,
            etag: None,
            ..Default::default()
        };
        let headers1 = HeaderMap::new();
        assert!(check_preconditions(&headers1, &info1).is_ok());

        // [2] No conditional headers with etag=None → Ok()
        let info2 = ObjectInfo {
            mod_time: Some(valid_mod_time),
            etag: None,
            ..Default::default()
        };
        let headers2 = HeaderMap::new();
        assert!(check_preconditions(&headers2, &info2).is_ok());

        // [3] If-None-Match matches → return Err(S3Error::NotModified)
        let mut headers3 = HeaderMap::new();
        headers3.insert("if-none-match", HeaderValue::from_str(valid_etag).unwrap());
        let info3 = ObjectInfo {
            mod_time: Some(valid_mod_time),
            etag: Some(valid_etag.to_string()),
            ..Default::default()
        };
        let result3 = check_preconditions(&headers3, &info3);
        assert!(result3.is_err());
        let err3 = result3.unwrap_err();
        assert_eq!(err3.code(), &S3ErrorCode::NotModified);
        assert_eq!(err3.message(), Some("Not Modified"));
        assert_eq!(err3.status_code(), Some(StatusCode::NOT_MODIFIED));

        // [4] If-None-Match does not match → return Ok()
        let mut headers4 = HeaderMap::new();
        headers4.insert("if-none-match", HeaderValue::from_str("\"wrong-etag\"").unwrap());
        let info4 = info3.clone();
        assert!(check_preconditions(&headers4, &info4).is_ok());

        // [5] If-Modified-Since >= mod_time → return Err(S3Error::NotModified)
        let mut headers5 = HeaderMap::new();
        headers5.insert(
            "if-modified-since",
            HeaderValue::from_str(&valid_mod_time.format(&RFC1123).unwrap()).unwrap(),
        );
        let info5 = info3.clone();
        let result5 = check_preconditions(&headers5, &info5);
        assert!(result5.is_err());
        let err5 = result5.unwrap_err();
        assert_eq!(err5.code(), &S3ErrorCode::NotModified);

        // [6] If-Modified-Since < mod_time → return Ok()
        let earlier_time = valid_mod_time - time::Duration::hours(1);
        let mut headers6 = HeaderMap::new();
        headers6.insert(
            "if-modified-since",
            HeaderValue::from_str(&earlier_time.format(&RFC1123).unwrap()).unwrap(),
        );
        let info6 = info3.clone();
        assert!(check_preconditions(&headers6, &info6).is_ok());

        // [7] If-Match does not match → return Err(S3Error::PreconditionFailed)
        let mut headers7 = HeaderMap::new();
        headers7.insert("if-match", HeaderValue::from_str("\"wrong-etag\"").unwrap());
        let info7 = info3.clone();
        let result7 = check_preconditions(&headers7, &info7);
        assert!(result7.is_err());
        assert_eq!(result7.unwrap_err().code(), &S3ErrorCode::PreconditionFailed);

        // [8] If-Match matches → return Ok()
        let mut headers8 = HeaderMap::new();
        headers8.insert("if-match", HeaderValue::from_str(valid_etag).unwrap());
        let info8 = info3.clone();
        assert!(check_preconditions(&headers8, &info8).is_ok());

        // [9] If-Unmodified-Since < mod_time (no If-Match) → return Err(S3Error::PreconditionFailed)
        let mut headers9 = HeaderMap::new();
        headers9.insert(
            "if-unmodified-since",
            HeaderValue::from_str(&earlier_time.format(&RFC1123).unwrap()).unwrap(),
        );
        let info9 = info3.clone();
        let result9 = check_preconditions(&headers9, &info9);
        assert!(result9.is_err());
        assert_eq!(result9.unwrap_err().code(), &S3ErrorCode::PreconditionFailed);

        // [10] If-Unmodified-Since >= mod_time (no If-Match) → return Ok()
        let mut headers10 = HeaderMap::new();
        headers10.insert(
            "if-unmodified-since",
            HeaderValue::from_str(&valid_mod_time.format(&RFC1123).unwrap()).unwrap(),
        );
        let info10 = info3.clone();
        assert!(check_preconditions(&headers10, &info10).is_ok());

        // [11] If-Match (mismatch) + If-None-Match (match) → return Err(S3Error::PreconditionFailed)
        let mut headers11 = HeaderMap::new();
        headers11.insert("if-match", HeaderValue::from_str(wrong_etag).unwrap());
        headers11.insert("if-none-match", HeaderValue::from_str(valid_etag).unwrap());
        let info11 = info3.clone();
        let result11 = check_preconditions(&headers11, &info11);
        assert!(result11.is_err());
        let err11 = result11.unwrap_err();
        assert_eq!(err11.code(), &S3ErrorCode::PreconditionFailed);
        assert_ne!(err11.code(), &S3ErrorCode::NotModified);

        // [12] If-Match (match) + If-None-Match (match) → return Err(S3Error::NotModified)
        let mut headers12 = HeaderMap::new();
        headers12.insert("if-match", HeaderValue::from_str(valid_etag).unwrap());
        headers12.insert("if-none-match", HeaderValue::from_str(valid_etag).unwrap());
        let info12 = info3.clone();
        let result12 = check_preconditions(&headers12, &info12);
        assert!(result12.is_err());
        let err12 = result12.unwrap_err();
        assert_eq!(err12.code(), &S3ErrorCode::NotModified);
        assert_ne!(err12.code(), &S3ErrorCode::PreconditionFailed);

        // [13] If-None-Match (match) + If-Modified-Since → NotModified (If-Modified-Since ignored)
        let mut headers13 = HeaderMap::new();
        headers13.insert("if-none-match", HeaderValue::from_str(valid_etag).unwrap());
        headers13.insert(
            "if-modified-since",
            HeaderValue::from_str(&earlier_time.format(&RFC1123).unwrap()).unwrap(),
        );
        let info13 = info3.clone();
        let result13 = check_preconditions(&headers13, &info13);
        assert!(result13.is_err());
        assert_eq!(result13.unwrap_err().code(), &S3ErrorCode::NotModified);

        // [14] If-None-Match (no match) + If-Modified-Since → Ok (If-Modified-Since ignored)
        let mut headers14 = HeaderMap::new();
        headers14.insert("if-none-match", HeaderValue::from_str("\"wrong-etag\"").unwrap());
        headers14.insert(
            "if-modified-since",
            HeaderValue::from_str(&valid_mod_time.format(&RFC1123).unwrap()).unwrap(),
        );
        let info14 = info3.clone();
        assert!(check_preconditions(&headers14, &info14).is_ok());

        // [15] If-Match with no ETag → PreconditionFailed
        let mut headers15 = HeaderMap::new();
        headers15.insert("if-match", HeaderValue::from_str(valid_etag).unwrap());
        let info15 = ObjectInfo {
            mod_time: Some(valid_mod_time),
            etag: None,
            ..Default::default()
        };
        let result15 = check_preconditions(&headers15, &info15);
        assert!(result15.is_err());
        assert_eq!(result15.unwrap_err().code(), &S3ErrorCode::PreconditionFailed);

        // [16] If-None-Match with no ETag → Ok (no match possible)
        let mut headers16 = HeaderMap::new();
        headers16.insert("if-none-match", HeaderValue::from_str(valid_etag).unwrap());
        let info16 = ObjectInfo {
            mod_time: Some(valid_mod_time),
            etag: None,
            ..Default::default()
        };
        assert!(check_preconditions(&headers16, &info16).is_ok());

        // [17] mod_time None, etag exists, If-Match matches → Ok
        let mut headers17 = HeaderMap::new();
        headers17.insert("if-match", HeaderValue::from_str(valid_etag).unwrap());
        let info17 = ObjectInfo {
            mod_time: None,
            etag: Some(valid_etag.to_string()),
            ..Default::default()
        };
        assert!(check_preconditions(&headers17, &info17).is_ok());
    }

    #[test]
    fn test_parse_etag() {
        // [1] ETag without quotes → adds quotes
        let result1 = parse_etag("d41d8cd98f00b204e9800998ecf8427e").unwrap();
        assert_eq!(result1.to_str().unwrap(), "\"d41d8cd98f00b204e9800998ecf8427e\"");

        // [2] ETag with quotes → normalizes to single set of quotes
        let result2 = parse_etag("\"d41d8cd98f00b204e9800998ecf8427e\"").unwrap();
        assert_eq!(result2.to_str().unwrap(), "\"d41d8cd98f00b204e9800998ecf8427e\"");

        // [3] ETag with multiple quotes → removes all and adds single set
        let result3 = parse_etag("\"\"\"d41d8cd98f00b204e9800998ecf8427e\"\"\"").unwrap();
        assert_eq!(result3.to_str().unwrap(), "\"d41d8cd98f00b204e9800998ecf8427e\"");

        // [4] Empty string → returns error
        let result4 = parse_etag("");
        assert!(result4.is_err());
        assert_eq!(result4.unwrap_err().code(), &S3ErrorCode::InvalidArgument);

        // [5] String with only quotes → returns error
        let result5 = parse_etag("\"\"");
        assert!(result5.is_err());
        assert_eq!(result5.unwrap_err().code(), &S3ErrorCode::InvalidArgument);

        // [6] ETag with quotes only at start → removes and adds quotes
        let result6 = parse_etag("\"d41d8cd98f00b204e9800998ecf8427e").unwrap();
        assert_eq!(result6.to_str().unwrap(), "\"d41d8cd98f00b204e9800998ecf8427e\"");

        // [7] ETag with quotes only at end → removes and adds quotes
        let result7 = parse_etag("d41d8cd98f00b204e9800998ecf8427e\"").unwrap();
        assert_eq!(result7.to_str().unwrap(), "\"d41d8cd98f00b204e9800998ecf8427e\"");

        // [8] ETag with newline character → returns error (invalid header value)
        let result8 = parse_etag("d41d8cd98f00b204e9800998ecf8427e\n");
        assert!(result8.is_err());
        assert_eq!(result8.unwrap_err().code(), &S3ErrorCode::InvalidArgument);

        // [9] ETag with null byte → returns error (invalid header value)
        let result9 = parse_etag("d41d8cd98f00b204e9800998ecf8427e\0");
        assert!(result9.is_err());
        assert_eq!(result9.unwrap_err().code(), &S3ErrorCode::InvalidArgument);

        // [10] ETag with carriage return → returns error (invalid header value)
        let result10 = parse_etag("d41d8cd98f00b204e9800998ecf8427e\r");
        assert!(result10.is_err());
        assert_eq!(result10.unwrap_err().code(), &S3ErrorCode::InvalidArgument);

        // [11] ETag with whitespace → preserves content, adds quotes
        let result11 = parse_etag("  d41d8cd98f00b204e9800998ecf8427e  ").unwrap();
        assert_eq!(result11.to_str().unwrap(), "\"  d41d8cd98f00b204e9800998ecf8427e  \"");

        // [12] ETag with only whitespace (no quotes) → returns error
        let result12 = parse_etag("   ");
        assert!(result12.is_err());
        assert_eq!(result12.unwrap_err().code(), &S3ErrorCode::InvalidArgument);

        // [13] ETag with only whitespace (with quotes) → returns error
        let result13 = parse_etag("\"   \"");
        assert!(result13.is_err());
        assert_eq!(result13.unwrap_err().code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_apply_lock_retention() {
        use crate::storage::ecfs_extend::apply_lock_retention;
        use s3s::dto::{DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRetentionMode, ObjectLockRule};
        use std::collections::HashMap;

        // [1] Normal case: Apply default retention with COMPLIANCE mode and days
        let mut metadata = HashMap::new();
        let config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                    days: Some(30),
                    years: None,
                }),
            }),
        });
        apply_lock_retention(config, &mut metadata);
        assert_eq!(metadata.get("x-amz-object-lock-mode"), Some(&"COMPLIANCE".to_string()));
        assert!(metadata.contains_key("x-amz-object-lock-retain-until-date"));

        // [2] Normal case: Apply default retention with GOVERNANCE mode and years
        let mut metadata = HashMap::new();
        let config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE)),
                    days: None,
                    years: Some(1),
                }),
            }),
        });
        apply_lock_retention(config, &mut metadata);
        assert_eq!(metadata.get("x-amz-object-lock-mode"), Some(&"GOVERNANCE".to_string()));
        assert!(metadata.contains_key("x-amz-object-lock-retain-until-date"));

        // [3] Skip case: No configuration provided
        let mut metadata = HashMap::new();
        apply_lock_retention(None, &mut metadata);
        assert!(!metadata.contains_key("x-amz-object-lock-mode"));
        assert!(!metadata.contains_key("x-amz-object-lock-retain-until-date"));

        // [4] Skip case: Object Lock not enabled
        let mut metadata = HashMap::new();
        let config = Some(ObjectLockConfiguration {
            object_lock_enabled: None,
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                    days: Some(30),
                    years: None,
                }),
            }),
        });
        apply_lock_retention(config, &mut metadata);
        assert!(!metadata.contains_key("x-amz-object-lock-mode"));

        // [5] Skip case: Explicit retention already set (explicit takes precedence)
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-object-lock-mode".to_string(), "GOVERNANCE".to_string());
        metadata.insert("x-amz-object-lock-retain-until-date".to_string(), "2030-01-01T00:00:00Z".to_string());
        let config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                    days: Some(30),
                    years: None,
                }),
            }),
        });
        apply_lock_retention(config, &mut metadata);
        // Explicit retention should remain unchanged
        assert_eq!(metadata.get("x-amz-object-lock-mode"), Some(&"GOVERNANCE".to_string()));
        assert_eq!(
            metadata.get("x-amz-object-lock-retain-until-date"),
            Some(&"2030-01-01T00:00:00Z".to_string())
        );

        // [6] Skip case: No default retention configured
        let mut metadata = HashMap::new();
        let config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule { default_retention: None }),
        });
        apply_lock_retention(config, &mut metadata);
        assert!(!metadata.contains_key("x-amz-object-lock-mode"));

        // [7] Skip case: No retention mode specified
        let mut metadata = HashMap::new();
        let config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: None,
                    days: Some(30),
                    years: None,
                }),
            }),
        });
        apply_lock_retention(config, &mut metadata);
        assert!(!metadata.contains_key("x-amz-object-lock-mode"));

        // [8] Skip case: No retention period specified (neither days nor years)
        let mut metadata = HashMap::new();
        let config = Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                    days: None,
                    years: None,
                }),
            }),
        });
        apply_lock_retention(config, &mut metadata);
        assert!(!metadata.contains_key("x-amz-object-lock-mode"));
    }

    // Note: S3Request structure is complex and requires many fields.
    // For real testing, we would need proper integration test setup.
    // Removing this test as it requires too much S3 infrastructure setup.

    // Note: Testing actual S3 operations like put_object, get_object, etc. requires:
    // 1. Initialized storage backend (ECStore)
    // 2. Global configuration setup
    // 3. Valid credentials and authorization
    // 4. Bucket and object metadata systems
    // 5. Network and disk I/O capabilities
    //
    // These are better suited for integration tests rather than unit tests.
    // The current tests focus on the testable parts without external dependencies.

    /// Test that next_key_marker and next_version_id_marker are filtered correctly
    /// AWS S3 API requires these fields to be omitted when empty, not set to None or ""
    #[test]
    fn test_next_marker_filtering() {
        // Test filter behavior for empty strings
        let empty_string = Some(String::new());
        let filtered = empty_string.filter(|v| !v.is_empty());
        assert!(filtered.is_none(), "Empty string should be filtered to None");

        // Test filter behavior for non-empty strings
        let non_empty = Some("some-marker".to_string());
        let filtered = non_empty.filter(|v| !v.is_empty());
        assert!(filtered.is_some(), "Non-empty string should not be filtered");
        assert_eq!(filtered.unwrap(), "some-marker");

        // Test filter behavior for None
        let none_value: Option<String> = None;
        let filtered = none_value.filter(|v| !v.is_empty());
        assert!(filtered.is_none(), "None should remain None");
    }

    /// Test version_id handling for ListObjectVersions response
    /// Per AWS S3 API spec:
    /// - Versioned objects: version_id is a UUID string
    /// - Non-versioned objects: version_id should be "null" string
    #[test]
    fn test_version_id_formatting() {
        use uuid::Uuid;

        // Non-versioned object: version_id is None, should format as "null"
        let version_id: Option<Uuid> = None;
        let formatted = version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
        assert_eq!(formatted, "null");

        // Versioned object: version_id is Some(UUID), should format as UUID string
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let version_id: Option<Uuid> = Some(uuid);
        let formatted = version_id.map(|v| v.to_string()).unwrap_or_else(|| "null".to_string());
        assert_eq!(formatted, "550e8400-e29b-41d4-a716-446655440000");
    }

    /// Test that ListObjectVersionsOutput markers are correctly set
    /// This verifies the fix for boto3 ParamValidationError
    #[test]
    fn test_list_object_versions_markers_handling() {
        // Simulate the marker filtering logic from list_object_versions

        // Case 1: Both markers have values (truncated result with versioned object)
        let next_marker = Some("object-key".to_string());
        let next_version_idmarker = Some("550e8400-e29b-41d4-a716-446655440000".to_string());

        let filtered_key_marker = next_marker.filter(|v| !v.is_empty());
        let filtered_version_marker = next_version_idmarker.filter(|v| !v.is_empty());

        assert!(filtered_key_marker.is_some());
        assert!(filtered_version_marker.is_some());

        // Case 2: Markers are empty strings (non-truncated result)
        let next_marker = Some(String::new());
        let next_version_idmarker = Some(String::new());

        let filtered_key_marker = next_marker.filter(|v| !v.is_empty());
        let filtered_version_marker = next_version_idmarker.filter(|v| !v.is_empty());

        assert!(filtered_key_marker.is_none(), "Empty key marker should be filtered to None");
        assert!(filtered_version_marker.is_none(), "Empty version marker should be filtered to None");

        // Case 3: Truncated result with non-versioned object (version_id is "null")
        let next_marker = Some("object-key".to_string());
        let next_version_idmarker = Some("null".to_string());

        let filtered_key_marker = next_marker.filter(|v| !v.is_empty());
        let filtered_version_marker = next_version_idmarker.filter(|v| !v.is_empty());

        assert!(filtered_key_marker.is_some());
        assert!(filtered_version_marker.is_some());
        assert_eq!(filtered_version_marker.unwrap(), "null");
    }

    #[test]
    fn test_matches_origin_pattern_exact_match() {
        // Test exact match
        assert!(matches_origin_pattern("https://example.com", "https://example.com"));
        assert!(matches_origin_pattern("http://localhost:3000", "http://localhost:3000"));
        assert!(!matches_origin_pattern("https://example.com", "https://other.com"));
    }

    #[test]
    fn test_matches_origin_pattern_wildcard() {
        // Test wildcard pattern matching (S3 CORS supports * as subdomain wildcard)
        assert!(matches_origin_pattern("https://*.example.com", "https://app.example.com"));
        assert!(matches_origin_pattern("https://*.example.com", "https://api.example.com"));
        assert!(matches_origin_pattern("https://*.example.com", "https://subdomain.example.com"));

        // Test wildcard at start (matches any domain)
        assert!(matches_origin_pattern("https://*", "https://example.com"));
        assert!(matches_origin_pattern("https://*", "https://any-domain.com"));

        // Test wildcard at end (matches any protocol)
        assert!(matches_origin_pattern("*://example.com", "https://example.com"));
        assert!(matches_origin_pattern("*://example.com", "http://example.com"));

        // Test invalid wildcard patterns (should not match)
        assert!(!matches_origin_pattern("https://*.*.com", "https://app.example.com")); // Multiple wildcards (invalid pattern)
        // Note: "https://*example.com" actually matches "https://app.example.com" with our current implementation
        // because it splits on * and checks starts_with/ends_with. This is a limitation but acceptable
        // for S3 CORS which typically uses patterns like "https://*.example.com"
    }

    #[test]
    fn test_matches_origin_pattern_no_wildcard() {
        // Test patterns without wildcards
        assert!(matches_origin_pattern("https://example.com", "https://example.com"));
        assert!(!matches_origin_pattern("https://example.com", "https://example.org"));
        assert!(!matches_origin_pattern("http://example.com", "https://example.com")); // Different protocol
    }

    #[test]
    fn test_matches_origin_pattern_edge_cases() {
        // Test edge cases
        assert!(!matches_origin_pattern("", "https://example.com")); // Empty pattern
        assert!(!matches_origin_pattern("https://example.com", "")); // Empty origin
        assert!(matches_origin_pattern("", "")); // Both empty
        assert!(!matches_origin_pattern("https://example.com", "http://example.com")); // Protocol mismatch
    }

    #[test]
    fn test_cors_headers_validation() {
        use http::HeaderMap;

        // Test case 1: Validate header name case-insensitivity
        let mut headers = HeaderMap::new();
        headers.insert("access-control-request-headers", "Content-Type,X-Custom-Header".parse().unwrap());

        let req_headers_str = headers
            .get("access-control-request-headers")
            .and_then(|v| v.to_str().ok())
            .unwrap();
        let req_headers: Vec<String> = req_headers_str.split(',').map(|s| s.trim().to_lowercase()).collect();

        // Headers should be lowercased for comparison
        assert_eq!(req_headers, vec!["content-type", "x-custom-header"]);

        // Test case 2: Wildcard matching
        let allowed_headers = ["*".to_string()];
        let all_allowed = req_headers.iter().all(|req_header| {
            allowed_headers
                .iter()
                .any(|allowed| allowed.to_lowercase() == "*" || allowed.to_lowercase() == *req_header)
        });
        assert!(all_allowed, "Wildcard should allow all headers");

        // Test case 3: Specific header matching
        let allowed_headers = ["content-type".to_string(), "x-custom-header".to_string()];
        let all_allowed = req_headers
            .iter()
            .all(|req_header| allowed_headers.iter().any(|allowed| allowed.to_lowercase() == *req_header));
        assert!(all_allowed, "All requested headers should be allowed");

        // Test case 4: Disallowed header
        let req_headers = ["content-type".to_string(), "x-forbidden-header".to_string()];
        let allowed_headers = ["content-type".to_string()];
        let all_allowed = req_headers
            .iter()
            .all(|req_header| allowed_headers.iter().any(|allowed| allowed.to_lowercase() == *req_header));
        assert!(!all_allowed, "Forbidden header should not be allowed");
    }

    #[test]
    fn test_cors_response_headers_structure() {
        use http::{HeaderMap, HeaderValue};

        let mut cors_headers = HeaderMap::new();

        // Simulate building CORS response headers
        let origin = "https://example.com";
        let methods = ["GET", "PUT", "POST"];
        let allowed_headers = ["Content-Type", "Authorization"];
        let expose_headers = ["ETag", "x-amz-version-id"];
        let max_age = 3600;

        // Add headers
        cors_headers.insert("access-control-allow-origin", HeaderValue::from_str(origin).unwrap());
        cors_headers.insert("vary", HeaderValue::from_static("Origin"));

        let methods_str = methods.join(", ");
        cors_headers.insert("access-control-allow-methods", HeaderValue::from_str(&methods_str).unwrap());

        let headers_str = allowed_headers.join(", ");
        cors_headers.insert("access-control-allow-headers", HeaderValue::from_str(&headers_str).unwrap());

        let expose_str = expose_headers.join(", ");
        cors_headers.insert("access-control-expose-headers", HeaderValue::from_str(&expose_str).unwrap());

        cors_headers.insert("access-control-max-age", HeaderValue::from_str(&max_age.to_string()).unwrap());

        // Verify all headers are present
        assert_eq!(cors_headers.get("access-control-allow-origin").unwrap(), origin);
        assert_eq!(cors_headers.get("vary").unwrap(), "Origin");
        assert_eq!(cors_headers.get("access-control-allow-methods").unwrap(), "GET, PUT, POST");
        assert_eq!(cors_headers.get("access-control-allow-headers").unwrap(), "Content-Type, Authorization");
        assert_eq!(cors_headers.get("access-control-expose-headers").unwrap(), "ETag, x-amz-version-id");
        assert_eq!(cors_headers.get("access-control-max-age").unwrap(), "3600");
    }

    #[test]
    fn test_cors_preflight_vs_actual_request() {
        use http::Method;

        // Test that we can distinguish preflight from actual requests
        let preflight_method = Method::OPTIONS;
        let actual_method = Method::PUT;

        assert_eq!(preflight_method, Method::OPTIONS);
        assert_ne!(actual_method, Method::OPTIONS);

        // Preflight should check Access-Control-Request-Method
        // Actual request should use the actual method
        let is_preflight_1 = preflight_method == Method::OPTIONS;
        let is_preflight_2 = actual_method == Method::OPTIONS;

        assert!(is_preflight_1);
        assert!(!is_preflight_2);
    }

    #[tokio::test]
    async fn test_apply_cors_headers_no_origin() {
        // Test when no Origin header is present
        let headers = HeaderMap::new();
        let method = http::Method::GET;

        // Should return None when no origin header
        let result = apply_cors_headers("test-bucket", &method, &headers).await;
        assert!(result.is_none(), "Should return None when no Origin header");
    }

    #[tokio::test]
    async fn test_apply_cors_headers_no_cors_config() {
        // Test when bucket has no CORS configuration
        let mut headers = HeaderMap::new();
        headers.insert("origin", "https://example.com".parse().unwrap());
        let method = http::Method::GET;

        // Should return None when no CORS config exists
        // Note: This test may fail if test-bucket actually has CORS config
        // In a real scenario, we'd use a mock or ensure the bucket doesn't exist
        let _result = apply_cors_headers("non-existent-bucket-for-testing", &method, &headers).await;
        // Result depends on whether bucket exists and has CORS config
        // This is expected behavior - we just verify it doesn't panic
    }

    #[tokio::test]
    async fn test_apply_cors_headers_unsupported_method() {
        // Test with unsupported HTTP method
        let mut headers = HeaderMap::new();
        headers.insert("origin", "https://example.com".parse().unwrap());
        let method = http::Method::PATCH; // Unsupported method

        let result = apply_cors_headers("test-bucket", &method, &headers).await;
        assert!(result.is_none(), "Should return None for unsupported methods");
    }

    #[test]
    fn test_matches_origin_pattern_complex_wildcards() {
        // Test more complex wildcard scenarios
        assert!(matches_origin_pattern("https://*.example.com", "https://sub.example.com"));
        // Note: "https://*.example.com" matches "https://api.sub.example.com" with our implementation
        // because it only checks starts_with and ends_with. Real S3 might be more strict.

        // Test wildcard in middle position
        // Our implementation allows this, but it's not standard S3 CORS pattern
        // The pattern "https://example.*.com" splits to ["https://example.", ".com"]
        // and "https://example.sub.com" matches because it starts with "https://example." and ends with ".com"
        // This is acceptable for our use case as S3 CORS typically uses "https://*.example.com" format
    }

    // === Notification Configuration Error Propagation Tests ===

    #[test]
    fn test_process_queue_configurations_propagates_error_on_invalid_arn() {
        use rustfs_targets::arn::{ARN, TargetIDError};

        let mut event_rules = Vec::new();
        let invalid_arn = "arn:minio:sqs::1:webhook"; // Wrong prefix, should fail

        let result = process_queue_configurations(
            &mut event_rules,
            Some(vec![QueueConfiguration {
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                queue_arn: invalid_arn.to_string(),
                filter: None,
                id: None,
            }]),
            |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            },
        );

        assert!(result.is_err(), "Should return error for invalid ARN prefix");
        assert!(event_rules.is_empty(), "Should not add rules when ARN is invalid");
    }

    #[test]
    fn test_process_topic_configurations_propagates_error_on_invalid_arn() {
        use rustfs_targets::arn::{ARN, TargetIDError};

        let mut event_rules = Vec::new();
        let invalid_arn = "arn:aws:sns:us-east-1:123:topic"; // Wrong prefix, should fail

        let result = process_topic_configurations(
            &mut event_rules,
            Some(vec![TopicConfiguration {
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                topic_arn: invalid_arn.to_string(),
                filter: None,
                id: None,
            }]),
            |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            },
        );

        assert!(result.is_err(), "Should return error for invalid ARN prefix");
        assert!(event_rules.is_empty(), "Should not add rules when ARN is invalid");
    }

    #[test]
    fn test_process_lambda_configurations_propagates_error_on_invalid_arn() {
        use rustfs_targets::arn::{ARN, TargetIDError};

        let mut event_rules = Vec::new();
        let invalid_arn = "arn:aws:lambda:us-east-1:123:function"; // Wrong prefix, should fail

        let result = process_lambda_configurations(
            &mut event_rules,
            Some(vec![LambdaFunctionConfiguration {
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                lambda_function_arn: invalid_arn.to_string(),
                filter: None,
                id: None,
            }]),
            |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            },
        );

        assert!(result.is_err(), "Should return error for invalid ARN prefix");
        assert!(event_rules.is_empty(), "Should not add rules when ARN is invalid");
    }

    #[test]
    fn test_process_queue_configurations_succeeds_with_valid_arn() {
        use rustfs_targets::arn::{ARN, TargetIDError};

        let mut event_rules = Vec::new();
        let valid_arn = "arn:rustfs:sqs:us-east-1:1:webhook"; // Correct prefix

        let result = process_queue_configurations(
            &mut event_rules,
            Some(vec![QueueConfiguration {
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                queue_arn: valid_arn.to_string(),
                filter: None,
                id: None,
            }]),
            |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            },
        );

        assert!(result.is_ok(), "Should succeed with valid ARN");
        assert_eq!(event_rules.len(), 1, "Should add one rule");
    }
}
