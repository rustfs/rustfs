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

use http::HeaderMap;
use rustfs_ecstore::compress::{MIN_COMPRESSIBLE_SIZE, is_compressible};
use rustfs_ecstore::store_api::ObjectOptions;
use rustfs_utils::http::headers::{
    AMZ_DECODED_CONTENT_LENGTH, AMZ_MINIO_SNOWBALL_IGNORE_DIRS, AMZ_MINIO_SNOWBALL_IGNORE_ERRORS, AMZ_MINIO_SNOWBALL_PREFIX,
    AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS, AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, AMZ_RUSTFS_SNOWBALL_PREFIX, AMZ_SERVER_SIDE_ENCRYPTION,
    AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, AMZ_SNOWBALL_EXTRACT, AMZ_SNOWBALL_IGNORE_DIRS, AMZ_SNOWBALL_IGNORE_ERRORS,
    AMZ_SNOWBALL_PREFIX,
};
use s3s::dto::{ChecksumAlgorithm, PutObjectInput, ServerSideEncryption};
use s3s::{S3Error, s3_error};
use std::collections::HashMap;
use tokio::io::AsyncRead;
use tokio_tar::Archive;

pub const AMZ_SNOWBALL_EXTRACT_COMPAT: &str = "X-Amz-Snowball-Auto-Extract";
pub const AMZ_SNOWBALL_PREFIX_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Prefix";
pub const AMZ_SNOWBALL_IGNORE_DIRS_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Ignore-Dirs";
pub const AMZ_SNOWBALL_IGNORE_ERRORS_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Ignore-Errors";

const AMZ_META_PREFIX_LOWER: &str = "x-amz-meta-";
const SNOWBALL_PREFIX_SUFFIX_LOWER: &str = "snowball-prefix";
const SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER: &str = "snowball-ignore-dirs";
const SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER: &str = "snowball-ignore-errors";
const SNOWBALL_PREFIX_HEADER_KEYS: &[&str] = &[AMZ_MINIO_SNOWBALL_PREFIX, AMZ_SNOWBALL_PREFIX, AMZ_RUSTFS_SNOWBALL_PREFIX];
const SNOWBALL_IGNORE_DIRS_HEADER_KEYS: &[&str] = &[
    AMZ_MINIO_SNOWBALL_IGNORE_DIRS,
    AMZ_SNOWBALL_IGNORE_DIRS,
    AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS,
];
const SNOWBALL_IGNORE_ERRORS_HEADER_KEYS: &[&str] = &[
    AMZ_MINIO_SNOWBALL_IGNORE_ERRORS,
    AMZ_SNOWBALL_IGNORE_ERRORS,
    AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS,
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutObjectChecksums {
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
    pub crc64nvme: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutObjectIngressPlan {
    pub buffer_size: usize,
    pub enable_zero_copy: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutObjectBodyPlan {
    pub ingress: PutObjectIngressPlan,
    pub should_compress: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutObjectExtractOptions {
    pub prefix: Option<String>,
    pub ignore_dirs: bool,
    pub ignore_errors: bool,
}

pub fn apply_trailing_checksums(
    algorithm: Option<&str>,
    trailing_headers: &Option<s3s::TrailingHeaders>,
    checksums: &mut PutObjectChecksums,
) {
    let Some(alg) = algorithm else { return };
    let Some(checksum_str) = trailing_headers.as_ref().and_then(|trailer| {
        let key = match alg {
            ChecksumAlgorithm::CRC32 => rustfs_rio::ChecksumType::CRC32.key(),
            ChecksumAlgorithm::CRC32C => rustfs_rio::ChecksumType::CRC32C.key(),
            ChecksumAlgorithm::SHA1 => rustfs_rio::ChecksumType::SHA1.key(),
            ChecksumAlgorithm::SHA256 => rustfs_rio::ChecksumType::SHA256.key(),
            ChecksumAlgorithm::CRC64NVME => rustfs_rio::ChecksumType::CRC64_NVME.key(),
            _ => return None,
        };
        trailer.read(|headers| {
            headers
                .get(key.unwrap_or_default())
                .and_then(|value| value.to_str().ok().map(|s| s.to_string()))
        })
    }) else {
        return;
    };

    match alg {
        ChecksumAlgorithm::CRC32 => checksums.crc32 = checksum_str,
        ChecksumAlgorithm::CRC32C => checksums.crc32c = checksum_str,
        ChecksumAlgorithm::SHA1 => checksums.sha1 = checksum_str,
        ChecksumAlgorithm::SHA256 => checksums.sha256 = checksum_str,
        ChecksumAlgorithm::CRC64NVME => checksums.crc64nvme = checksum_str,
        _ => (),
    }
}

pub fn resolve_put_body_size(content_length: Option<i64>, headers: &HeaderMap) -> s3s::S3Result<i64> {
    let size = match content_length {
        Some(c) => c,
        None => {
            if let Some(val) = headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                match atoi::atoi::<i64>(val.as_bytes()) {
                    Some(x) => x,
                    None => return Err(s3_error!(UnexpectedContent)),
                }
            } else {
                return Err(s3_error!(UnexpectedContent));
            }
        }
    };

    if size == -1 {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(size)
}

pub fn should_use_zero_copy(size: i64, headers: &HeaderMap) -> bool {
    const ZERO_COPY_MIN_SIZE: i64 = 1024 * 1024;

    if size < ZERO_COPY_MIN_SIZE {
        return false;
    }

    if headers.get("x-amz-server-side-encryption").is_some()
        || headers.get("x-amz-server-side-encryption-customer-algorithm").is_some()
        || headers.get("x-amz-server-side-encryption-aws-kms-key-id").is_some()
    {
        return false;
    }

    if let Some(content_type) = headers.get("content-type")
        && let Ok(ct) = content_type.to_str()
    {
        let compressible_types = [
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/javascript",
            "application/json",
            "application/xml",
            "text/xml",
        ];
        for ct_type in compressible_types {
            if ct.contains(ct_type) {
                return false;
            }
        }
    }

    true
}

pub fn plan_put_object_ingress(size: i64, headers: &HeaderMap, buffer_size: usize) -> PutObjectIngressPlan {
    PutObjectIngressPlan {
        buffer_size,
        enable_zero_copy: should_use_zero_copy(size, headers),
    }
}

pub fn plan_put_object_body(size: i64, headers: &HeaderMap, key: &str, buffer_size: usize) -> PutObjectBodyPlan {
    PutObjectBodyPlan {
        ingress: plan_put_object_ingress(size, headers, buffer_size),
        should_compress: size > MIN_COMPRESSIBLE_SIZE as i64 && is_compressible(headers, key),
    }
}

pub fn resolve_put_effective_copy_mode(applied_compression: bool, applied_encryption: bool) -> rustfs_io_metrics::CopyMode {
    if applied_compression || applied_encryption {
        rustfs_io_metrics::CopyMode::Transformed
    } else {
        rustfs_io_metrics::CopyMode::SingleCopy
    }
}

pub fn header_value_is_true(headers: &HeaderMap, key: &str) -> bool {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

pub fn is_put_object_extract_requested(headers: &HeaderMap) -> bool {
    header_value_is_true(headers, AMZ_SNOWBALL_EXTRACT) || header_value_is_true(headers, AMZ_SNOWBALL_EXTRACT_COMPAT)
}

fn trimmed_header_value(headers: &HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
}

fn is_exact_snowball_meta_key(key: &str, exact_keys: &[&str]) -> bool {
    exact_keys.iter().any(|exact_key| key.eq_ignore_ascii_case(exact_key))
}

fn snowball_meta_value_by_suffix(headers: &HeaderMap, suffix_lower: &str, exact_keys: &[&str]) -> Option<String> {
    for (name, value) in headers {
        let key = name.as_str();
        if key.starts_with(AMZ_META_PREFIX_LOWER)
            && key.ends_with(suffix_lower)
            && !is_exact_snowball_meta_key(key, exact_keys)
            && let Ok(parsed) = value.to_str()
        {
            return Some(parsed.trim().to_string());
        }
    }

    None
}

fn snowball_meta_value(headers: &HeaderMap, exact_keys: &[&str], suffix_lower: &str) -> Option<String> {
    for key in exact_keys {
        if let Some(value) = trimmed_header_value(headers, key) {
            return Some(value);
        }
    }

    snowball_meta_value_by_suffix(headers, suffix_lower, exact_keys)
}

fn snowball_meta_flag(headers: &HeaderMap, exact_keys: &[&str], suffix_lower: &str) -> bool {
    snowball_meta_value(headers, exact_keys, suffix_lower).is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub fn normalize_snowball_prefix(prefix: &str) -> Option<String> {
    let normalized = prefix.trim().trim_matches('/');
    if normalized.is_empty() {
        return None;
    }

    Some(normalized.to_string())
}

pub fn normalize_extract_entry_key(path: &str, prefix: Option<&str>, is_dir: bool) -> String {
    let path = path.trim_matches('/');
    let mut key = match prefix {
        Some(prefix) if !path.is_empty() => format!("{prefix}/{path}"),
        Some(prefix) => prefix.to_string(),
        None => path.to_string(),
    };

    if is_dir && !key.ends_with('/') {
        key.push('/');
    }

    key
}

pub fn resolve_put_object_extract_options(headers: &HeaderMap) -> PutObjectExtractOptions {
    let prefix = snowball_meta_value(headers, SNOWBALL_PREFIX_HEADER_KEYS, SNOWBALL_PREFIX_SUFFIX_LOWER)
        .and_then(|value| normalize_snowball_prefix(&value));
    let ignore_dirs = snowball_meta_flag(headers, SNOWBALL_IGNORE_DIRS_HEADER_KEYS, SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER);
    let ignore_errors = snowball_meta_flag(headers, SNOWBALL_IGNORE_ERRORS_HEADER_KEYS, SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER);

    PutObjectExtractOptions {
        prefix,
        ignore_dirs,
        ignore_errors,
    }
}

pub fn map_extract_archive_error(err: impl std::fmt::Display) -> S3Error {
    s3_error!(InvalidArgument, "Failed to process archive entry: {}", err)
}

pub async fn apply_extract_entry_pax_extensions<R>(
    entry: &mut tokio_tar::Entry<Archive<R>>,
    metadata: &mut HashMap<String, String>,
    opts: &mut ObjectOptions,
) -> s3s::S3Result<()>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let Some(extensions) = entry.pax_extensions().await.map_err(map_extract_archive_error)? else {
        return Ok(());
    };

    for ext in extensions {
        let ext = ext.map_err(map_extract_archive_error)?;
        let key = ext.key().map_err(map_extract_archive_error)?;
        let value = ext.value().map_err(map_extract_archive_error)?;

        if let Some(meta_key) = key.strip_prefix("minio.metadata.") {
            let meta_key = meta_key.strip_prefix("x-amz-meta-").unwrap_or(meta_key);
            if !meta_key.is_empty() {
                metadata.insert(meta_key.to_string(), value.to_string());
            }
            continue;
        }

        if key == "minio.versionId" && !value.is_empty() {
            opts.version_id = Some(value.to_string());
        }
    }

    Ok(())
}

pub fn is_sse_kms_requested(input: &PutObjectInput, headers: &HeaderMap) -> bool {
    input
        .server_side_encryption
        .as_ref()
        .is_some_and(|sse| sse.as_str().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        || input.ssekms_key_id.is_some()
        || headers
            .get(AMZ_SERVER_SIDE_ENCRYPTION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID)
}

pub fn is_post_object_sse_kms_requested(input: &PutObjectInput, headers: &HeaderMap) -> bool {
    is_sse_kms_requested(input, headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderName, HeaderValue};

    #[test]
    fn should_use_zero_copy_accepts_large_unencrypted_binary_payload() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        assert!(should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_small_payloads() {
        assert!(!should_use_zero_copy(512 * 1024, &HeaderMap::new()));
    }

    #[test]
    fn resolve_put_body_size_uses_content_length_when_present() {
        assert_eq!(resolve_put_body_size(Some(123), &HeaderMap::new()).unwrap(), 123);
    }

    #[test]
    fn resolve_put_body_size_uses_decoded_content_length_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_DECODED_CONTENT_LENGTH, "456".parse().unwrap());
        assert_eq!(resolve_put_body_size(None, &headers).unwrap(), 456);
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_payloads() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-server-side-encryption", HeaderValue::from_static("AES256"));
        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_compressible_payloads() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn plan_put_object_ingress_preserves_buffer_size_and_fast_path_decision() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));

        let plan = plan_put_object_ingress(2 * 1024 * 1024, &headers, 256 * 1024);

        assert_eq!(plan.buffer_size, 256 * 1024);
        assert!(plan.enable_zero_copy);
    }

    #[test]
    fn plan_put_object_body_disables_compression_for_small_payloads() {
        let plan = plan_put_object_body(1024, &HeaderMap::new(), "small.bin", 64 * 1024);

        assert_eq!(plan.ingress.buffer_size, 64 * 1024);
        assert!(!plan.ingress.enable_zero_copy);
        assert!(!plan.should_compress);
    }

    #[test]
    fn resolve_put_effective_copy_mode_marks_transformed_paths() {
        assert_eq!(resolve_put_effective_copy_mode(false, false), rustfs_io_metrics::CopyMode::SingleCopy);
        assert_eq!(resolve_put_effective_copy_mode(true, false), rustfs_io_metrics::CopyMode::Transformed);
        assert_eq!(resolve_put_effective_copy_mode(false, true), rustfs_io_metrics::CopyMode::Transformed);
    }

    #[test]
    fn is_put_object_extract_requested_accepts_meta_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));
        assert!(is_put_object_extract_requested(&headers));
    }

    #[test]
    fn is_put_object_extract_requested_accepts_compat_header_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_EXTRACT_COMPAT, HeaderValue::from_static(" TRUE "));
        assert!(is_put_object_extract_requested(&headers));
    }

    #[test]
    fn is_put_object_extract_requested_rejects_missing_or_false_value() {
        let mut headers = HeaderMap::new();
        assert!(!is_put_object_extract_requested(&headers));
        headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("false"));
        assert!(!is_put_object_extract_requested(&headers));
    }

    #[test]
    fn normalize_snowball_prefix_trims_slashes_and_whitespace() {
        assert_eq!(normalize_snowball_prefix(" /batch/incoming/ "), Some("batch/incoming".to_string()));
        assert_eq!(normalize_snowball_prefix("///"), None);
    }

    #[test]
    fn normalize_extract_entry_key_applies_prefix_and_directory_suffix() {
        assert_eq!(
            normalize_extract_entry_key("nested/path.txt", Some("imports"), false),
            "imports/nested/path.txt"
        );
        assert_eq!(normalize_extract_entry_key("nested/dir/", Some("imports"), true), "imports/nested/dir/");
        assert_eq!(normalize_extract_entry_key("top-level", None, false), "top-level");
    }

    #[test]
    fn resolve_put_object_extract_options_defaults_when_headers_missing() {
        let headers = HeaderMap::new();
        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(
            options,
            PutObjectExtractOptions {
                prefix: None,
                ignore_dirs: false,
                ignore_errors: false
            }
        );
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_internal_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX_INTERNAL, HeaderValue::from_static("/internal/prefix/"));
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS_INTERNAL, HeaderValue::from_static("true"));
        headers.insert(AMZ_SNOWBALL_IGNORE_ERRORS_INTERNAL, HeaderValue::from_static("TRUE"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("internal/prefix"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_standard_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static(" /standard/prefix/ "));
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static(" true "));
        headers.insert(AMZ_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("TRUE"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("standard/prefix"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_suffix_compatible_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-prefix"),
            HeaderValue::from_static(" /partner/import "),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-ignore-dirs"),
            HeaderValue::from_static(" true "),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-ignore-errors"),
            HeaderValue::from_static("TRUE"),
        );

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("partner/import"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_prefers_exact_headers_over_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-acme-snowball-prefix", HeaderValue::from_static("/fallback/prefix/"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_PREFIX, HeaderValue::from_static("/internal/prefix/"));
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static("/standard/prefix/"));
        headers.insert(AMZ_MINIO_SNOWBALL_PREFIX, HeaderValue::from_static("/minio/prefix/"));

        let options = resolve_put_object_extract_options(&headers);
        assert_eq!(options.prefix.as_deref(), Some("minio/prefix"));
    }

    #[test]
    fn resolve_put_object_extract_options_exact_flags_override_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-dirs", HeaderValue::from_static("true"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-errors", HeaderValue::from_static("true"));

        let options = resolve_put_object_extract_options(&headers);
        assert!(!options.ignore_dirs);
        assert!(!options.ignore_errors);
    }
}
