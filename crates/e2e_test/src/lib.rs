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

mod reliant;

// Common utilities for all E2E tests
#[cfg(test)]
pub mod common;

#[cfg(test)]
mod version_id_regression_test;

// Data usage regression tests
#[cfg(test)]
mod data_usage_test;

// KMS-specific test modules
#[cfg(test)]
mod kms;

// Regression test for issue #1797
#[cfg(test)]
mod list_objects_duplicates_test;

// Quota tests
#[cfg(test)]
mod quota_test;

#[cfg(test)]
mod bucket_policy_check_test;

/// IAM / bucket / STS session policy with `s3:ExistingObjectTag` conditions (E2E).
#[cfg(test)]
mod existing_object_tag_policy_test;

// Regression tests for Issue #2036: anonymous access with PublicAccessBlock
#[cfg(test)]
mod anonymous_access_test;

// Special characters in path test modules
#[cfg(test)]
mod special_chars_test;

// Content-Encoding header preservation test
#[cfg(test)]
mod content_encoding_test;

// ListObjectsV2 pagination test (Issue #1596)
#[cfg(test)]
mod list_objects_v2_pagination_test;

// Policy variables tests
#[cfg(test)]
mod policy;

#[cfg(test)]
mod compression_test;

// Regression test for Issue #1878: DeleteMarkers not visible immediately after delete_objects
#[cfg(test)]
mod delete_objects_versioning_test;

// Regression test for Issue #2252: ListObjectVersions misses newest version after put -> delete -> put
#[cfg(test)]
mod list_object_versions_regression_test;

// versions&metadata=true extension regression test
#[cfg(test)]
mod list_object_versions_metadata_extension_test;

// list-type=2&metadata=true extension regression test
#[cfg(test)]
mod list_objects_v2_metadata_extension_test;

#[cfg(test)]
mod protocols;

// Object Lock tests
#[cfg(test)]
mod object_lock;

#[cfg(test)]
mod cluster_concurrency_test;

// PutObject / MultipartUpload with checksum (Content-MD5, x-amz-checksum-*)
#[cfg(test)]
mod checksum_upload_test;

// Group deletion tests
#[cfg(test)]
mod group_delete_test;

// S3 dummy-compat bucket API tests
#[cfg(test)]
mod bucket_logging_test;

// Multipart control API auth regression tests
#[cfg(test)]
mod multipart_auth_test;

// Object lambda end-to-end regression tests
#[cfg(test)]
mod object_lambda_test;

// Replication extension end-to-end regression tests
#[cfg(test)]
mod replication_extension_test;

#[cfg(test)]
mod snowball_auto_extract_test;
