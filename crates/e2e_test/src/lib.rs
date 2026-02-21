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
