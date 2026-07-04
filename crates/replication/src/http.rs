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

use std::collections::HashMap;

const RUSTFS_INTERNAL_PREFIX: &str = "x-rustfs-internal-";
const MINIO_INTERNAL_PREFIX: &str = "x-minio-internal-";
const RUSTFS_HEADER_PREFIX: &str = "x-rustfs-";
const MINIO_HEADER_PREFIX: &str = "x-minio-";

pub(crate) const AMZ_BUCKET_REPLICATION_STATUS: &str = "X-Amz-Replication-Status";
pub(crate) const AMZ_OBJECT_LOCK_LEGAL_HOLD: &str = "X-Amz-Object-Lock-Legal-Hold";
pub(crate) const AMZ_OBJECT_LOCK_MODE: &str = "X-Amz-Object-Lock-Mode";
pub(crate) const AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE: &str = "X-Amz-Object-Lock-Retain-Until-Date";
pub(crate) const AMZ_OBJECT_TAGGING: &str = "X-Amz-Tagging";
pub(crate) const AMZ_WEBSITE_REDIRECT_LOCATION: &str = "x-amz-website-redirect-location";
pub(crate) const CACHE_CONTROL: &str = "Cache-Control";
pub(crate) const CONTENT_DISPOSITION: &str = "Content-Disposition";
pub(crate) const CONTENT_ENCODING: &str = "Content-Encoding";
pub(crate) const CONTENT_LANGUAGE: &str = "Content-Language";
pub(crate) const EXPIRES: &str = "Expires";
pub(crate) const SSEC_ALGORITHM_HEADER: &str = "x-amz-server-side-encryption-customer-algorithm";
pub(crate) const SSEC_KEY_HEADER: &str = "x-amz-server-side-encryption-customer-key";
pub(crate) const SSEC_KEY_MD5_HEADER: &str = "x-amz-server-side-encryption-customer-key-md5";
pub(crate) const SUFFIX_ACTUAL_SIZE: &str = "actual-size";
pub(crate) const SUFFIX_REPLICATION_RESET_STATUS: &str = "replication-reset-status";

fn internal_keys(suffix: &str) -> (String, String) {
    (format!("{RUSTFS_INTERNAL_PREFIX}{suffix}"), format!("{MINIO_INTERNAL_PREFIX}{suffix}"))
}

fn rustfs_header_key(suffix: &str) -> String {
    format!("{RUSTFS_HEADER_PREFIX}{suffix}")
}

fn minio_header_key(suffix: &str) -> String {
    format!("{MINIO_HEADER_PREFIX}{suffix}")
}

pub(crate) fn internal_key_rustfs(suffix: &str) -> String {
    format!("{RUSTFS_INTERNAL_PREFIX}{suffix}")
}

pub(crate) fn get_internal_metadata(map: &HashMap<String, String>, suffix: &str) -> Option<String> {
    let (rustfs_key, minio_key) = internal_keys(suffix);
    map.get(&rustfs_key)
        .cloned()
        .or_else(|| map.get(&minio_key).cloned())
        .or_else(|| {
            map.iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(&rustfs_key) || key.eq_ignore_ascii_case(&minio_key))
                .map(|(_, value)| value.clone())
        })
}

pub(crate) fn get_header_metadata(map: &HashMap<String, String>, suffix: &str) -> Option<String> {
    let rustfs_key = rustfs_header_key(suffix);
    let minio_key = minio_header_key(suffix);
    map.get(&rustfs_key).cloned().or_else(|| map.get(&minio_key).cloned())
}

pub(crate) fn has_prefix_fold(s: &str, prefix: &str) -> bool {
    if s.starts_with(prefix) {
        return true;
    }

    s.get(..prefix.len())
        .is_some_and(|s_prefix| s_prefix.eq_ignore_ascii_case(prefix))
}

pub(crate) fn trim_etag(etag: &str) -> String {
    etag.trim_matches('"').to_string()
}

#[cfg(test)]
pub(crate) fn insert_internal_metadata(map: &mut HashMap<String, String>, suffix: &str, value: String) {
    let (rustfs_key, minio_key) = internal_keys(suffix);
    map.insert(rustfs_key, value.clone());
    map.insert(minio_key, value);
}

#[cfg(test)]
mod tests {
    use super::{
        SUFFIX_ACTUAL_SIZE, SUFFIX_REPLICATION_RESET_STATUS, get_header_metadata, get_internal_metadata, has_prefix_fold,
        insert_internal_metadata, internal_key_rustfs, trim_etag,
    };
    use std::collections::HashMap;

    #[test]
    fn internal_metadata_prefers_rustfs_and_falls_back_to_minio() {
        let mut metadata = HashMap::from([("x-minio-internal-actual-size".to_string(), "10".to_string())]);

        assert_eq!(get_internal_metadata(&metadata, SUFFIX_ACTUAL_SIZE).as_deref(), Some("10"));

        metadata.insert("x-rustfs-internal-actual-size".to_string(), "11".to_string());
        assert_eq!(get_internal_metadata(&metadata, SUFFIX_ACTUAL_SIZE).as_deref(), Some("11"));
    }

    #[test]
    fn internal_metadata_keeps_case_insensitive_lookup_compatibility() {
        let metadata = HashMap::from([("X-RustFS-Internal-Actual-Size".to_string(), "12".to_string())]);

        assert_eq!(get_internal_metadata(&metadata, SUFFIX_ACTUAL_SIZE).as_deref(), Some("12"));
    }

    #[test]
    fn internal_metadata_insert_writes_rustfs_and_minio_keys() {
        let mut metadata = HashMap::new();
        insert_internal_metadata(&mut metadata, SUFFIX_ACTUAL_SIZE, "13".to_string());

        assert_eq!(metadata.get("x-rustfs-internal-actual-size").map(String::as_str), Some("13"));
        assert_eq!(metadata.get("x-minio-internal-actual-size").map(String::as_str), Some("13"));
    }

    #[test]
    fn header_metadata_prefers_rustfs_then_minio() {
        let mut metadata = HashMap::from([("x-minio-replication-reset-status".to_string(), "old".to_string())]);

        assert_eq!(get_header_metadata(&metadata, SUFFIX_REPLICATION_RESET_STATUS).as_deref(), Some("old"));

        metadata.insert("x-rustfs-replication-reset-status".to_string(), "new".to_string());
        assert_eq!(get_header_metadata(&metadata, SUFFIX_REPLICATION_RESET_STATUS).as_deref(), Some("new"));
    }

    #[test]
    fn helper_contracts_match_replication_wire_rules() {
        assert_eq!(
            internal_key_rustfs("replication-reset-arn:target"),
            "x-rustfs-internal-replication-reset-arn:target"
        );
        assert_eq!(trim_etag("\"abc\""), "abc");
        assert!(has_prefix_fold("X-Amz-Meta-Foo", "x-amz-meta-"));
        assert!(!has_prefix_fold("X-Amz-Meta-Foo", "amz-meta"));
    }
}
