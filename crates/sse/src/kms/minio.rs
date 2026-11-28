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

//! Reading data keys sealed by MinIO's builtin KMS.

use crate::core::keying::context::canonical_kms_bucket_path;
use crate::core::keying::object::HmacSha256;
use crate::error::SseError;
use crate::kms::local::LocalSseDekProvider;
use base64_simd::STANDARD as BASE64_STANDARD;
use hmac::Mac;
use rustfs_kms::types::ObjectEncryptionContext;
use serde::Deserialize;
use std::collections::HashMap;

/// Associated data MinIO binds when sealing a data key.
///
/// MinIO passes the object's encryption context as the AEAD's associated data,
/// serialized as canonical JSON with sorted keys — the same canonicalization
/// [`rustfs_kms::context_aad`] performs, which is why the context RustFS
/// already rebuilds for the read can be reused verbatim. For SSE-S3 that
/// context is `{bucket: "bucket/object"}`; for SSE-KMS it is whatever the
/// request supplied, recovered from the stored MinIO context header.
pub(crate) fn minio_kms_associated_data(context: &ObjectEncryptionContext) -> Result<Vec<u8>, SseError> {
    let mut ctx = context.encryption_context.clone();
    ctx.entry(context.bucket.clone())
        .or_insert_with(|| canonical_kms_bucket_path(&context.bucket, &context.object_key));
    rustfs_kms::context_aad(&ctx).map_err(|e| SseError::internal(format!("Failed to canonicalize MinIO KMS context: {e}")))
}

/// MinIO's builtin-KMS ciphertext in its JSON encoding.
///
/// Deliberately its own type rather than a relaxation of
/// [`LocalSseDekEnvelope`]: widening that envelope's `deny_unknown_fields`
/// to admit this shape would also admit malformed RustFS envelopes, which
/// backlog#1567 requires to keep failing closed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MinioKmsCiphertextJson {
    aead: String,
    #[allow(
        dead_code,
        reason = "present in MinIO's encoding; the key is identified by metadata instead"
    )]
    #[serde(default)]
    id: String,
    iv: String,
    nonce: String,
    bytes: String,
}

/// Bytes of trailing randomness every MinIO builtin-KMS ciphertext carries:
/// a 16-byte IV followed by a 12-byte nonce, *after* the sealed bytes.
const MINIO_KMS_RANDOM_LEN: usize = 28;

const MINIO_KMS_IV_LEN: usize = 16;

const MINIO_KMS_AEAD_AES_GCM: &str = "AES-256-GCM-HMAC-SHA-256";

const MINIO_KMS_AEAD_CHACHA20: &str = "ChaCha20Poly1305";

/// Unwrap a data key sealed by MinIO's builtin (static-secret) KMS.
///
/// The wire format is `sealed_bytes || iv[16] || nonce[12]` — the randomness
/// trails the ciphertext rather than leading it, and MinIO's own decoder
/// normalizes its legacy JSON encoding into exactly that byte order before
/// opening it (`internal/kms/secret-key.go`, `parseCiphertext`). A raw
/// (non-JSON) ciphertext is AES-256-GCM by definition there; the JSON form
/// names its algorithm.
///
/// The sealing key is derived per ciphertext rather than being the master key:
/// `HMAC-SHA256(master, iv)` for AES-256-GCM, `HChaCha20(master, iv)` for
/// ChaCha20-Poly1305. The encryption context is bound as associated data.
pub(crate) fn decrypt_minio_kms_data_key(encrypted_dek: &[u8], master_key: &[u8; 32], aad: &[u8]) -> Result<[u8; 32], SseError> {
    let (body, algorithm) = match std::str::from_utf8(encrypted_dek) {
        // MinIO only treats a payload as JSON when it both starts and ends like
        // an object, and falls back to the raw layout when it does not parse —
        // mirrored here so a ciphertext that merely looks like JSON is not
        // rejected outright.
        Ok(text)
            if text.starts_with('{')
                && text.ends_with('}')
                && let Ok(json) = serde_json::from_str::<MinioKmsCiphertextJson>(text) =>
        {
            let decode = |what: &str, value: &str| -> Result<Vec<u8>, SseError> {
                BASE64_STANDARD
                    .decode_to_vec(value)
                    .map_err(|e| SseError::internal(format!("Invalid MinIO KMS {what}: {e}")))
            };
            let mut body = decode("ciphertext", &json.bytes)?;
            body.extend_from_slice(&decode("iv", &json.iv)?);
            body.extend_from_slice(&decode("nonce", &json.nonce)?);
            (body, json.aead)
        }
        _ => (encrypted_dek.to_vec(), MINIO_KMS_AEAD_AES_GCM.to_string()),
    };

    if body.len() <= MINIO_KMS_RANDOM_LEN {
        return Err(SseError::internal("MinIO KMS ciphertext is too short to carry its IV and nonce"));
    }
    let (sealed, random) = body.split_at(body.len() - MINIO_KMS_RANDOM_LEN);
    let (iv, nonce) = random.split_at(MINIO_KMS_IV_LEN);

    let plaintext = match algorithm.as_str() {
        MINIO_KMS_AEAD_AES_GCM => {
            use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
            let mut mac = HmacSha256::new_from_slice(master_key)
                .map_err(|_| SseError::internal("MinIO KMS sealing key derivation failed"))?;
            mac.update(iv);
            let sealing_key: [u8; 32] = mac.finalize().into_bytes().into();
            let cipher = Aes256Gcm::new_from_slice(&sealing_key)
                .map_err(|_| SseError::internal("MinIO KMS sealing key is not a valid AES-256 key"))?;
            let nonce = aes_gcm::Nonce::try_from(nonce).map_err(|_| SseError::internal("MinIO KMS nonce is not 12 bytes"))?;
            cipher.decrypt(&nonce, aes_gcm::aead::Payload { msg: sealed, aad })
        }
        MINIO_KMS_AEAD_CHACHA20 => {
            use chacha20poly1305::{KeyInit, XChaCha20Poly1305, aead::Aead};
            // MinIO derives this branch's key with HChaCha20 over the 16-byte
            // IV, which is exactly XChaCha20-Poly1305's own construction, so the
            // extended-nonce cipher does the derivation rather than hand-rolling it.
            let mut extended = Vec::with_capacity(MINIO_KMS_IV_LEN + nonce.len());
            extended.extend_from_slice(iv);
            extended.extend_from_slice(nonce);
            let cipher = XChaCha20Poly1305::new_from_slice(master_key)
                .map_err(|_| SseError::internal("MinIO KMS master key is not a valid ChaCha20 key"))?;
            let nonce = chacha20poly1305::XNonce::try_from(extended.as_slice())
                .map_err(|_| SseError::internal("MinIO KMS extended nonce is not 24 bytes"))?;
            cipher.decrypt(&nonce, chacha20poly1305::aead::Payload { msg: sealed, aad })
        }
        other => {
            return Err(SseError::internal(format!("Unsupported MinIO KMS AEAD algorithm: {other}")));
        }
    }
    // An AEAD failure here is authentication, not a decode slip: a wrong master
    // key, a tampered ciphertext, and an encryption context that does not match
    // what sealed it all land here and must all fail closed.
    .map_err(|_| SseError::internal("MinIO KMS data key failed authentication"))?;

    plaintext
        .try_into()
        .map_err(|value: Vec<u8>| SseError::internal(format!("MinIO KMS data key must be 32 bytes, got {}", value.len())))
}

/// MinIO's meta bucket: the context key, and the path prefix, a config blob's
/// data key is bound to.
const MINIO_META_BUCKET: &str = ".minio.sys";

/// Format version of a KMS-encrypted config blob (`internal/config/crypto.go`).
const MINIO_CONFIG_BLOB_VERSION: u8 = 1;

/// Version byte plus the little-endian length of the metadata JSON.
const MINIO_CONFIG_BLOB_HEADER_LEN: usize = 5;

/// MinIO's own bound on the metadata JSON.
const MINIO_CONFIG_BLOB_MAX_METADATA_LEN: usize = 1 << 20;

/// Plaintext bytes per fragment of the `sio-go` stream MinIO encrypts config
/// blobs with (`sio.BufSize`).
const SIO_STREAM_FRAGMENT_LEN: usize = 1 << 14;

/// AEAD tag appended to every fragment.
const SIO_STREAM_TAG_LEN: usize = 16;

/// Trailing nonce bytes a `sio-go` stream reserves for the fragment counter.
const SIO_STREAM_COUNTER_LEN: usize = 4;

/// Associated-data flag of every fragment but the last.
const SIO_STREAM_FRAGMENT_FLAG: u8 = 0x00;

/// Associated-data flag of the last fragment; it is what detects truncation.
const SIO_STREAM_FINAL_FLAG: u8 = 0x80;

const SIO_ALGORITHM_AES_256_GCM: &str = "AES-256-GCM";

const SIO_ALGORITHM_CHACHA20_POLY1305: &str = "ChaCha20-Poly1305";

/// Metadata MinIO prepends to a KMS-encrypted config blob.
#[derive(Deserialize)]
struct MinioConfigBlobMetadata {
    /// Base64 of the builtin-KMS ciphertext of the stream key.
    kmskey: String,
    algorithm: String,
    /// Base64 of the stream nonce.
    nonce: String,
}

enum SioStreamCipher {
    Aes256Gcm(Box<aes_gcm::Aes256Gcm>),
    ChaCha20Poly1305(chacha20poly1305::ChaCha20Poly1305),
}

impl SioStreamCipher {
    fn new(algorithm: &str, key: &[u8; 32]) -> Result<Self, SseError> {
        use aes_gcm::KeyInit;
        match algorithm {
            SIO_ALGORITHM_AES_256_GCM => aes_gcm::Aes256Gcm::new_from_slice(key)
                .map(|cipher| Self::Aes256Gcm(Box::new(cipher)))
                .map_err(|_| SseError::internal("MinIO config stream key is not a valid AES-256 key")),
            SIO_ALGORITHM_CHACHA20_POLY1305 => chacha20poly1305::ChaCha20Poly1305::new_from_slice(key)
                .map(Self::ChaCha20Poly1305)
                .map_err(|_| SseError::internal("MinIO config stream key is not a valid ChaCha20 key")),
            other => Err(SseError::internal(format!("Unsupported MinIO config stream algorithm: {other}"))),
        }
    }

    fn seal(&self, nonce: &[u8; 12], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, SseError> {
        use aes_gcm::aead::{Aead, Payload};
        let payload = Payload { msg: plaintext, aad };
        match self {
            Self::Aes256Gcm(cipher) => cipher.encrypt(&aes_gcm::Nonce::from(*nonce), payload),
            Self::ChaCha20Poly1305(cipher) => cipher.encrypt(&chacha20poly1305::Nonce::from(*nonce), payload),
        }
        .map_err(|_| SseError::internal("MinIO config stream associated data could not be derived"))
    }

    fn open(&self, nonce: &[u8; 12], ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>, SseError> {
        use aes_gcm::aead::{Aead, Payload};
        let payload = Payload { msg: ciphertext, aad };
        match self {
            Self::Aes256Gcm(cipher) => cipher.decrypt(&aes_gcm::Nonce::from(*nonce), payload),
            Self::ChaCha20Poly1305(cipher) => cipher.decrypt(&chacha20poly1305::Nonce::from(*nonce), payload),
        }
        .map_err(|_| SseError::internal("MinIO config stream failed authentication"))
    }
}

/// Decrypt a `sio-go` stream: the format MinIO's `sio.Algorithm.Stream` writes.
///
/// The plaintext is cut into `fragment_len` pieces, each sealed under
/// `stream_nonce || counter` with the counter starting at 1. Counter 0 seals an
/// empty message whose tag, behind a one-byte flag, is every fragment's
/// associated data; the flag marks the last fragment, so a stream cut at a
/// fragment boundary fails authentication instead of reading as shorter data.
fn decrypt_sio_stream(
    cipher: &SioStreamCipher,
    stream_nonce: &[u8],
    ciphertext: &[u8],
    fragment_len: usize,
) -> Result<Vec<u8>, SseError> {
    let mut nonce = [0u8; 12];
    let counter_at = nonce.len() - SIO_STREAM_COUNTER_LEN;
    if stream_nonce.len() != counter_at {
        return Err(SseError::internal("MinIO config stream nonce has the wrong length"));
    }
    nonce[..counter_at].copy_from_slice(stream_nonce);
    if ciphertext.len() < SIO_STREAM_TAG_LEN {
        return Err(SseError::internal("MinIO config stream is shorter than one fragment"));
    }

    let mut associated_data = [SIO_STREAM_FRAGMENT_FLAG; 1 + SIO_STREAM_TAG_LEN];
    associated_data[1..].copy_from_slice(&cipher.seal(&nonce, &[], &[])?);

    let fragments = ciphertext.chunks(fragment_len + SIO_STREAM_TAG_LEN);
    let last = fragments.len() - 1;
    let mut plaintext = Vec::with_capacity(ciphertext.len());
    for (index, fragment) in fragments.enumerate() {
        let counter = u32::try_from(index + 1).map_err(|_| SseError::internal("MinIO config stream has too many fragments"))?;
        nonce[counter_at..].copy_from_slice(&counter.to_le_bytes());
        if index == last {
            associated_data[0] = SIO_STREAM_FINAL_FLAG;
        }
        plaintext.extend_from_slice(&cipher.open(&nonce, fragment, &associated_data)?);
    }
    Ok(plaintext)
}

/// A KMS context in the text form MinIO binds as associated data
/// (`internal/kms/context.go`, `Context.MarshalText`).
///
/// It is JSON with sorted keys, but not the JSON a general encoder emits:
/// MinIO escapes `<`, `>` and `&` as `\u003c`, `\u003e` and `\u0026`, U+2028
/// and U+2029 as `\u2028` and `\u2029`, and every control character other
/// than tab, line feed and carriage return as `\u00XX`. A context holding any
/// of those must be spelled the same way or the data key fails authentication.
fn minio_kms_context_text(context: &HashMap<String, String>) -> Vec<u8> {
    fn escape_into(text: &mut String, value: &str) {
        use std::fmt::Write as _;
        for character in value.chars() {
            match character {
                '"' => text.push_str("\\\""),
                '\\' => text.push_str("\\\\"),
                '\n' => text.push_str("\\n"),
                '\r' => text.push_str("\\r"),
                '\t' => text.push_str("\\t"),
                '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                    let _ = write!(text, "\\u{:04x}", u32::from(character));
                }
                control if u32::from(control) < 0x20 => {
                    let _ = write!(text, "\\u{:04x}", u32::from(control));
                }
                other => text.push(other),
            }
        }
    }

    let mut entries: Vec<(&String, &String)> = context.iter().collect();
    entries.sort();
    let mut text = String::from("{");
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index > 0 {
            text.push(',');
        }
        text.push('"');
        escape_into(&mut text, key);
        text.push_str("\":\"");
        escape_into(&mut text, value);
        text.push('"');
    }
    text.push('}');
    text.into_bytes()
}

/// Decrypt a config blob MinIO encrypted under its builtin (static-secret) KMS.
///
/// The blob is `version[1] || metadata_len[4, LE] || metadata JSON || stream`
/// (`internal/config/crypto.go`). The metadata carries the stream key sealed by
/// the builtin KMS, with `context` bound as its associated data — the same
/// ciphertext [`decrypt_minio_kms_data_key`] opens for object data keys.
///
/// The recorded key id is not compared: RustFS holds the secret without
/// MinIO's name for it, and a different secret fails authentication anyway.
fn decrypt_minio_kms_config_blob(
    blob: &[u8],
    master_key: &[u8; 32],
    context: &HashMap<String, String>,
) -> Result<Vec<u8>, SseError> {
    let Some((header, rest)) = blob.split_at_checked(MINIO_CONFIG_BLOB_HEADER_LEN) else {
        return Err(SseError::internal("MinIO config blob is shorter than its header"));
    };
    if header[0] != MINIO_CONFIG_BLOB_VERSION {
        return Err(SseError::internal(format!("Unknown MinIO config blob version {}", header[0])));
    }
    let metadata_len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);
    let metadata_len = usize::try_from(metadata_len)
        .ok()
        .filter(|len| *len <= MINIO_CONFIG_BLOB_MAX_METADATA_LEN)
        .ok_or_else(|| SseError::internal("MinIO config blob metadata is too large"))?;
    let Some((metadata, stream)) = rest.split_at_checked(metadata_len) else {
        return Err(SseError::internal("MinIO config blob is shorter than its metadata"));
    };
    let metadata: MinioConfigBlobMetadata =
        serde_json::from_slice(metadata).map_err(|_| SseError::internal("MinIO config blob metadata is not valid JSON"))?;
    let decode = |what: &str, value: &str| -> Result<Vec<u8>, SseError> {
        BASE64_STANDARD
            .decode_to_vec(value)
            .map_err(|e| SseError::internal(format!("Invalid MinIO config blob {what}: {e}")))
    };
    let sealed_stream_key = decode("stream key", &metadata.kmskey)?;
    let stream_nonce = decode("nonce", &metadata.nonce)?;

    let aad = minio_kms_context_text(context);
    let stream_key = decrypt_minio_kms_data_key(&sealed_stream_key, master_key, &aad)?;
    let cipher = SioStreamCipher::new(&metadata.algorithm, &stream_key)?;
    decrypt_sio_stream(&cipher, &stream_nonce, stream, SIO_STREAM_FRAGMENT_LEN)
}

/// Decrypt a config object MinIO stored KMS-encrypted in its meta bucket.
///
/// With a KMS configured, MinIO encrypts every `config/iam/*` object and binds
/// the data key to the object's location (`cmd/iam-object-store.go`,
/// `saveIAMConfig`). `object_path` is that location relative to the meta
/// bucket, for example `config/iam/format.json`. Like MinIO's `decryptData`,
/// the context is tried with the meta bucket prefixed to the path and, for
/// blobs written before that prefix existed, without it.
///
/// Returns `None` when no local master key is configured, or when `blob` does
/// not decrypt under it: not this format, another key, or a different path.
/// Only the builtin static-secret KMS is covered; a blob whose key a KES or
/// other external KMS sealed stays unreadable.
pub(crate) fn decrypt_minio_kms_config(blob: &[u8], object_path: &str) -> Option<Vec<u8>> {
    let provider = LocalSseDekProvider::new_from_env().ok()?;
    [format!("{MINIO_META_BUCKET}/{object_path}"), object_path.to_owned()]
        .into_iter()
        .find_map(|bound_path| {
            let context = HashMap::from([(MINIO_META_BUCKET.to_owned(), bound_path)]);
            decrypt_minio_kms_config_blob(blob, provider.master_key(), &context).ok()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    /// `config/iam/format.json` exactly as a MinIO server started with
    /// `MINIO_KMS_SECRET_KEY=compat-key:<base64 of the key below>` wrote it,
    /// reassembled from the drives of a 4-drive erasure set.
    const MINIO_IAM_FORMAT_BLOB_HEX: &str = "01c10000007b226b65796964223a22636f6d7061742d6b6579222c226b6d736b6579223a22645a334538415a4d3148566c336148346e33706e74646c6f57366c4d536c66725465546a7070504d786f464e30454e32443373504e4a6c5371623678764c6e4c58384c516f715673723857624654625748537339576a71774a4339643569374157334d3241513d3d222c22616c676f726974686d223a2243686143686132302d506f6c7931333035222c226e6f6e6365223a224d586d54506668593950553d227d88ec07cab8d47f3d2df4055a77f3ed11c6c0e126236afcbfefcd80a0e5";

    const MINIO_IAM_FORMAT_PATH: &str = "config/iam/format.json";

    fn fixture_master_key() -> [u8; 32] {
        Sha256::digest(b"minio-compat-harness-static-kms-key").into()
    }

    fn hex(data: &str) -> Vec<u8> {
        hex_simd::decode_to_vec(data).expect("test vector is valid hex")
    }

    fn iam_context(bound_path: &str) -> HashMap<String, String> {
        HashMap::from([(MINIO_META_BUCKET.to_owned(), bound_path.to_owned())])
    }

    /// `sio-go` reference streams: key `0x10..=0x2f`, nonce `0xa0..=0xa7`,
    /// plaintext byte `i * 7 + 3`, produced by the library with a 16-byte
    /// fragment so a short vector still spans several fragments.
    fn reference_stream_key() -> [u8; 32] {
        std::array::from_fn(|index| 0x10 + u8::try_from(index).expect("an index below 32 fits in u8"))
    }

    fn reference_plaintext(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| u8::try_from((index * 7 + 3) % 256).expect("a remainder below 256 fits in u8"))
            .collect()
    }

    const REFERENCE_STREAM_NONCE_HEX: &str = "a0a1a2a3a4a5a6a7";

    #[test]
    fn config_blob_written_by_minio_decrypts_under_its_location() {
        let blob = hex(MINIO_IAM_FORMAT_BLOB_HEX);
        let context = iam_context(&format!("{MINIO_META_BUCKET}/{MINIO_IAM_FORMAT_PATH}"));

        let plaintext =
            decrypt_minio_kms_config_blob(&blob, &fixture_master_key(), &context).expect("decrypt MinIO's IAM format");

        assert_eq!(plaintext, br#"{"version":1}"#);
    }

    #[test]
    fn config_blob_is_bound_to_its_key_its_location_and_its_bytes() {
        let blob = hex(MINIO_IAM_FORMAT_BLOB_HEX);
        let context = iam_context(&format!("{MINIO_META_BUCKET}/{MINIO_IAM_FORMAT_PATH}"));
        let master_key = fixture_master_key();

        assert!(decrypt_minio_kms_config_blob(&blob, &[0x42; 32], &context).is_err(), "another master key");
        let moved = iam_context(&format!("{MINIO_META_BUCKET}/config/iam/users/alice/identity.json"));
        assert!(
            decrypt_minio_kms_config_blob(&blob, &master_key, &moved).is_err(),
            "another object's context"
        );

        let mut tampered = blob.clone();
        *tampered.last_mut().expect("the blob is not empty") ^= 0x01;
        assert!(
            decrypt_minio_kms_config_blob(&tampered, &master_key, &context).is_err(),
            "a flipped stream bit"
        );

        assert!(
            decrypt_minio_kms_config_blob(&blob[..blob.len() - 1], &master_key, &context).is_err(),
            "a truncated stream"
        );
        assert!(
            decrypt_minio_kms_config_blob(&blob[..4], &master_key, &context).is_err(),
            "a truncated header"
        );
        assert!(
            decrypt_minio_kms_config_blob(br#"{"version":1}"#, &master_key, &context).is_err(),
            "plaintext JSON"
        );

        let mut unknown_version = blob;
        unknown_version[0] = 2;
        assert!(decrypt_minio_kms_config_blob(&unknown_version, &master_key, &context).is_err());
    }

    #[test]
    fn sio_stream_matches_the_reference_library_across_fragments() {
        let aes = SioStreamCipher::new(SIO_ALGORITHM_AES_256_GCM, &reference_stream_key()).expect("AES-256-GCM stream");
        let chacha = SioStreamCipher::new(SIO_ALGORITHM_CHACHA20_POLY1305, &reference_stream_key()).expect("ChaCha20 stream");
        let nonce = hex(REFERENCE_STREAM_NONCE_HEX);

        // Two full fragments and a short final one.
        let three_fragments = hex(
            "968e58c082f6f71d3b1b1b30a2bf62ea84349b52d5b00218fa0f88a5c278605e729c74f0adfc53c8ab93aefd599e9c8eaf7b25bbac75b4cc2d1abfa8d4e3c7ac640730a9673ae193ed1277387b8b76828cc2cccf99990fb8",
        );
        assert_eq!(
            decrypt_sio_stream(&aes, &nonce, &three_fragments, 16).expect("three fragments"),
            reference_plaintext(40)
        );

        // The final fragment is a full one: nothing shorter follows it.
        let two_full_fragments = hex(
            "793565d5fcc714541652eb435cc1998f42b8a7dc9945ce78cc03a79c193390297278282fd557cfa8231466615cb9e3acfc98c39b3dde904eaccdf9d3c2ae8281",
        );
        assert_eq!(
            decrypt_sio_stream(&chacha, &nonce, &two_full_fragments, 16).expect("two full fragments"),
            reference_plaintext(32)
        );

        // An empty plaintext is still one authenticated final fragment.
        let empty = hex("908c14d9c38a43d4a745d3cacb41ec24");
        assert!(
            decrypt_sio_stream(&aes, &nonce, &empty, 16)
                .expect("empty plaintext")
                .is_empty()
        );
    }

    #[test]
    fn sio_stream_rejects_truncation_reordering_and_bad_framing() {
        let aes = SioStreamCipher::new(SIO_ALGORITHM_AES_256_GCM, &reference_stream_key()).expect("AES-256-GCM stream");
        let nonce = hex(REFERENCE_STREAM_NONCE_HEX);
        let stream = hex(
            "968e58c082f6f71d3b1b1b30a2bf62ea84349b52d5b00218fa0f88a5c278605e729c74f0adfc53c8ab93aefd599e9c8eaf7b25bbac75b4cc2d1abfa8d4e3c7ac640730a9673ae193ed1277387b8b76828cc2cccf99990fb8",
        );

        // Dropping the final fragment leaves a whole number of valid fragments:
        // only the final flag tells that apart from a complete stream.
        assert!(decrypt_sio_stream(&aes, &nonce, &stream[..64], 16).is_err(), "cut at a fragment boundary");

        let mut swapped = stream.clone();
        swapped[..64].rotate_left(32);
        assert!(decrypt_sio_stream(&aes, &nonce, &swapped, 16).is_err(), "fragments in another order");

        assert!(decrypt_sio_stream(&aes, &nonce, &[], 16).is_err(), "no fragment at all");
        assert!(decrypt_sio_stream(&aes, &nonce[..7], &stream, 16).is_err(), "a short stream nonce");
        assert!(SioStreamCipher::new("XChaCha20-Poly1305", &reference_stream_key()).is_err());
    }

    /// Build a config blob the way MinIO does, for the shapes no captured
    /// fixture covers: the stream key sealed raw (`sealed || iv || nonce`,
    /// AES-256-GCM under `HMAC-SHA256(master, iv)`) and a single-fragment
    /// AES-256-GCM `sio-go` stream.
    fn seal_config_blob(plaintext: &[u8], master_key: &[u8; 32], context: &HashMap<String, String>) -> Vec<u8> {
        use aes_gcm::aead::{Aead, Payload};
        use aes_gcm::{Aes256Gcm, KeyInit};
        assert!(plaintext.len() <= SIO_STREAM_FRAGMENT_LEN, "the helper writes one fragment");

        let (stream_key, iv, key_nonce) = ([0x61u8; 32], [0x21u8; 16], [0x42u8; 12]);
        let mut mac = HmacSha256::new_from_slice(master_key).expect("hmac key");
        mac.update(&iv);
        let sealing_key: [u8; 32] = mac.finalize().into_bytes().into();
        // Spelled out by hand: the helper must not borrow the reader's own
        // canonicalizer, and every context it is used with has one entry.
        let (context_key, context_value) = context.iter().next().expect("a one-entry context");
        assert_eq!(context.len(), 1);
        let aad = format!(r#"{{"{context_key}":"{context_value}"}}"#).into_bytes();
        let mut sealed_stream_key = Aes256Gcm::new_from_slice(&sealing_key)
            .expect("sealing key")
            .encrypt(
                &aes_gcm::Nonce::from(key_nonce),
                Payload {
                    msg: &stream_key,
                    aad: &aad,
                },
            )
            .expect("seal the stream key");
        sealed_stream_key.extend_from_slice(&iv);
        sealed_stream_key.extend_from_slice(&key_nonce);

        let cipher = SioStreamCipher::new(SIO_ALGORITHM_AES_256_GCM, &stream_key).expect("stream cipher");
        let stream_nonce = [0x7Bu8; 8];
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&stream_nonce);
        let mut associated_data = [SIO_STREAM_FINAL_FLAG; 1 + SIO_STREAM_TAG_LEN];
        associated_data[1..].copy_from_slice(&cipher.seal(&nonce, &[], &[]).expect("derive associated data"));
        nonce[8..].copy_from_slice(&1u32.to_le_bytes());
        let stream = cipher
            .seal(&nonce, plaintext, &associated_data)
            .expect("seal the only fragment");

        let metadata = serde_json::json!({
            "keyid": "compat-key",
            "kmskey": BASE64_STANDARD.encode_to_string(&sealed_stream_key),
            "algorithm": SIO_ALGORITHM_AES_256_GCM,
            "nonce": BASE64_STANDARD.encode_to_string(stream_nonce),
        })
        .to_string();
        let mut blob = vec![MINIO_CONFIG_BLOB_VERSION];
        blob.extend_from_slice(
            &u32::try_from(metadata.len())
                .expect("metadata length fits in u32")
                .to_le_bytes(),
        );
        blob.extend_from_slice(metadata.as_bytes());
        blob.extend_from_slice(&stream);
        blob
    }

    #[test]
    fn config_reader_needs_the_local_master_key_and_the_objects_own_path() {
        let blob = hex(MINIO_IAM_FORMAT_BLOB_HEX);
        let master_key = BASE64_STANDARD.encode_to_string(fixture_master_key());

        temp_env::with_var(crate::kms::local::ENV_SSE_S3_MASTER_KEY, Some(master_key.as_str()), || {
            assert_eq!(
                decrypt_minio_kms_config(&blob, MINIO_IAM_FORMAT_PATH).as_deref(),
                Some(br#"{"version":1}"#.as_slice())
            );
            assert_eq!(decrypt_minio_kms_config(&blob, "config/iam/users/alice/identity.json"), None);
            assert_eq!(decrypt_minio_kms_config(br#"{"version":1}"#, MINIO_IAM_FORMAT_PATH), None);
        });
        temp_env::with_var(crate::kms::local::ENV_SSE_S3_MASTER_KEY, None::<&str>, || {
            assert_eq!(decrypt_minio_kms_config(&blob, MINIO_IAM_FORMAT_PATH), None);
        });
    }

    /// Expected texts are what Go's `kms.Context.MarshalText` produces.
    #[test]
    fn context_text_matches_minio_escaping_and_key_order() {
        let text = |entries: &[(&str, &str)]| {
            let context = entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect();
            String::from_utf8(minio_kms_context_text(&context)).expect("context text is UTF-8")
        };

        assert_eq!(text(&[]), "{}");
        assert_eq!(
            text(&[(".minio.sys", ".minio.sys/config/iam/format.json")]),
            r#"{".minio.sys":".minio.sys/config/iam/format.json"}"#
        );
        assert_eq!(
            text(&[(".minio.sys", ".minio.sys/config/iam/policies/dev&ops<1>/policy.json")]),
            r#"{".minio.sys":".minio.sys/config/iam/policies/dev\u0026ops\u003c1\u003e/policy.json"}"#
        );
        assert_eq!(
            text(&[("b", "say \"hi\"\\"), ("a", "tab\there\nline\u{1}\u{2028}é")]),
            r#"{"a":"tab\there\nline\u0001\u2028é","b":"say \"hi\"\\"}"#
        );
    }

    /// A record whose path holds a character MinIO escapes in the context it
    /// binds: a policy named `dev&ops`.
    #[test]
    fn config_blob_decrypts_under_a_path_minio_escapes() {
        let path = "config/iam/policies/dev&ops/policy.json";
        let policy = br#"{"Version":"2012-10-17","Statement":[]}"#;
        let master_key = fixture_master_key();
        // Sealed under the associated data MinIO would bind, written out literally.
        let minio_aad_context = iam_context(r".minio.sys/config/iam/policies/dev\u0026ops/policy.json");
        let blob = seal_config_blob(policy, &master_key, &minio_aad_context);

        let plaintext = decrypt_minio_kms_config_blob(&blob, &master_key, &iam_context(&format!("{MINIO_META_BUCKET}/{path}")))
            .expect("the reader spells the context the way MinIO does");
        assert_eq!(plaintext, policy);
    }

    #[test]
    fn config_reader_accepts_the_context_older_minio_bound_without_the_meta_bucket() {
        let path = "config/iam/users/alice/identity.json";
        let identity = br#"{"version":1,"credentials":{"accessKey":"alice"}}"#;
        let master_key = fixture_master_key();
        let prefixed = seal_config_blob(identity, &master_key, &iam_context(&format!("{MINIO_META_BUCKET}/{path}")));
        let unprefixed = seal_config_blob(identity, &master_key, &iam_context(path));
        let elsewhere = seal_config_blob(identity, &master_key, &iam_context("config/iam/users/bob/identity.json"));

        let encoded_master_key = BASE64_STANDARD.encode_to_string(master_key);
        temp_env::with_var(crate::kms::local::ENV_SSE_S3_MASTER_KEY, Some(encoded_master_key.as_str()), || {
            assert_eq!(decrypt_minio_kms_config(&prefixed, path).as_deref(), Some(identity.as_slice()));
            assert_eq!(decrypt_minio_kms_config(&unprefixed, path).as_deref(), Some(identity.as_slice()));
            assert_eq!(decrypt_minio_kms_config(&elsewhere, path), None, "a blob copied from another object");
        });
    }
}
