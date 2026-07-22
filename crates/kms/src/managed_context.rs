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

use crate::{KmsError, Result};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use rustfs_utils::http::get_consistent_metadata_value;
use std::collections::HashMap;

pub const RUSTFS_ENCRYPTION_CONTEXT_HEADER: &str = "x-rustfs-encryption-context";
pub const MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Context";

pub fn decode_managed_kms_context(metadata: &HashMap<String, String>) -> Result<Option<HashMap<String, String>>> {
    let minio_context = consistent_value(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)?
        .map(|context| {
            let decoded = BASE64_STANDARD
                .decode(context)
                .map_err(|err| KmsError::serialization_error(format!("Failed to decode MinIO KMS context: {err}")))?;
            serde_json::from_slice(&decoded)
                .map_err(|err| KmsError::serialization_error(format!("Failed to parse MinIO KMS context: {err}")))
        })
        .transpose()?;
    let rustfs_context = consistent_value(metadata, RUSTFS_ENCRYPTION_CONTEXT_HEADER)?
        .map(|context| {
            serde_json::from_str(context)
                .map_err(|err| KmsError::serialization_error(format!("Failed to parse RustFS KMS context: {err}")))
        })
        .transpose()?;

    match (minio_context, rustfs_context) {
        (Some(minio), Some(rustfs)) if minio != rustfs => {
            Err(KmsError::context_mismatch("Conflicting RustFS and MinIO KMS contexts"))
        }
        (Some(context), _) | (_, Some(context)) => Ok(Some(context)),
        (None, None) => Ok(None),
    }
}

fn consistent_value<'a>(metadata: &'a HashMap<String, String>, name: &str) -> Result<Option<&'a str>> {
    get_consistent_metadata_value(metadata, name)
        .map_err(|_| KmsError::validation_error(format!("Conflicting managed encryption metadata for {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_context_accepts_compatible_headers_and_rejects_conflicts() {
        let expected = HashMap::from([("tenant".to_string(), "alpha".to_string())]);
        let metadata = HashMap::from([
            (
                RUSTFS_ENCRYPTION_CONTEXT_HEADER.to_string(),
                serde_json::to_string(&expected).expect("RustFS KMS context should serialize"),
            ),
            (
                MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(),
                BASE64_STANDARD.encode(serde_json::to_vec(&expected).expect("MinIO KMS context should serialize")),
            ),
        ]);
        assert_eq!(
            decode_managed_kms_context(&metadata).expect("matching KMS contexts should parse"),
            Some(expected.clone())
        );
        assert_eq!(
            decode_managed_kms_context(&HashMap::from([(
                RUSTFS_ENCRYPTION_CONTEXT_HEADER.to_string(),
                serde_json::to_string(&expected).expect("legacy RustFS KMS context should serialize"),
            )]))
            .expect("legacy RustFS KMS context should parse"),
            Some(expected)
        );

        let conflicting = HashMap::from([
            (
                RUSTFS_ENCRYPTION_CONTEXT_HEADER.to_string(),
                serde_json::to_string(&HashMap::from([("tenant", "alpha")])).expect("RustFS KMS context should serialize"),
            ),
            (
                MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(),
                BASE64_STANDARD.encode(
                    serde_json::to_vec(&HashMap::from([("tenant", "beta")])).expect("MinIO KMS context should serialize"),
                ),
            ),
        ]);
        assert!(decode_managed_kms_context(&conflicting).is_err());

        assert!(
            decode_managed_kms_context(&HashMap::from([(
                MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(),
                "not-base64".to_string(),
            )]))
            .is_err()
        );
        assert!(
            decode_managed_kms_context(&HashMap::from([(RUSTFS_ENCRYPTION_CONTEXT_HEADER.to_string(), "not-json".to_string(),)]))
                .is_err()
        );
    }
}
