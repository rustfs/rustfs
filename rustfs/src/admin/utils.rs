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

use crate::server::MINIO_ADMIN_PREFIX;
use rustfs_crypto::{decrypt_data, decrypt_stream_io, encrypt_stream_io};
use s3s::{Body, S3Result, s3_error};

pub(crate) fn has_space_be(s: &str) -> bool {
    s.trim().len() != s.len()
}

pub(crate) fn is_minio_admin_request(path: &str) -> bool {
    path.starts_with(MINIO_ADMIN_PREFIX)
}

pub(crate) async fn read_compatible_admin_body(
    mut input: Body,
    max_len: usize,
    path: &str,
    secret_key: &str,
) -> S3Result<Vec<u8>> {
    let body = input
        .store_all_limited(max_len)
        .await
        .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

    if is_minio_admin_request(path) {
        decrypt_stream_io(secret_key.as_bytes(), body.as_ref())
            .or_else(|_| decrypt_data(secret_key.as_bytes(), body.as_ref()))
            .map_err(|e| s3_error!(InvalidRequest, "failed to decrypt MinIO admin payload: {}", e))
    } else {
        Ok(body.to_vec())
    }
}

pub(crate) fn encode_compatible_admin_payload(path: &str, secret_key: &str, data: Vec<u8>) -> S3Result<(Vec<u8>, &'static str)> {
    if is_minio_admin_request(path) {
        let encrypted = encrypt_stream_io(secret_key.as_bytes(), &data)
            .map_err(|e| s3_error!(InternalError, "failed to encrypt MinIO admin payload: {}", e))?;
        Ok((encrypted, "application/octet-stream"))
    } else {
        Ok((data, "application/json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_crypto::encrypt_data;
    use s3s::Body;

    #[test]
    fn detects_minio_admin_paths_only_for_minio_prefix() {
        assert!(is_minio_admin_request("/minio/admin/v3/list-users"));
        assert!(!is_minio_admin_request("/rustfs/admin/v3/list-users"));
    }

    #[test]
    fn encodes_plain_payload_for_rustfs_admin_paths() {
        let payload = b"{\"ok\":true}".to_vec();
        let (encoded, content_type) =
            encode_compatible_admin_payload("/rustfs/admin/v3/list-users", "secret", payload.clone()).expect("encode payload");

        assert_eq!(encoded, payload);
        assert_eq!(content_type, "application/json");
    }

    #[test]
    fn encodes_minio_payload_with_compatible_encryption() {
        let payload = b"{\"ok\":true}".to_vec();
        let (encoded, content_type) =
            encode_compatible_admin_payload("/minio/admin/v3/list-users", "secret", payload.clone()).expect("encode payload");

        assert_ne!(encoded, payload);
        assert_eq!(content_type, "application/octet-stream");
        assert_eq!(decrypt_stream_io(b"secret", &encoded).expect("decrypt payload"), payload);
    }

    #[tokio::test]
    async fn reads_legacy_minio_payload_as_fallback() {
        let payload = b"{\"ok\":true}".to_vec();
        let encrypted = encrypt_data(b"secret", &payload).expect("encrypt payload");

        let decoded = read_compatible_admin_body(Body::from(encrypted), 1024, "/minio/admin/v3/list-users", "secret")
            .await
            .expect("decode payload");

        assert_eq!(decoded, payload);
    }
}
