use super::*;
#[cfg(feature = "rio-v2")]
use aes_gcm::aead::Payload;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
#[cfg(feature = "rio-v2")]
use chacha20poly1305::ChaCha20Poly1305;
#[cfg(feature = "rio-v2")]
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use rustfs_kms::types::ObjectEncryptionContext;
use rustfs_utils::http::{SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER};
use rustfs_utils::path::path_join_buf;
#[cfg(feature = "rio-v2")]
use serde::Deserialize;
#[cfg(feature = "rio-v2")]
use sha2::Sha256;
use std::collections::HashMap;
use std::env;

use crate::io_support::rio::Index;

const INTERNAL_ENCRYPTION_KEY_ID_HEADER: &str = "x-rustfs-encryption-key-id";
const INTERNAL_ENCRYPTION_KEY_HEADER: &str = "x-rustfs-encryption-key";
const INTERNAL_ENCRYPTION_CONTEXT_HEADER: &str = "x-rustfs-encryption-context";
const INTERNAL_ENCRYPTION_IV_HEADER: &str = "x-rustfs-encryption-iv";
const INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER: &str = "x-rustfs-encryption-original-size";
const SSEC_ORIGINAL_SIZE_HEADER: &str = "x-amz-server-side-encryption-customer-original-size";
const DEFAULT_SSE_ALGORITHM: &str = "AES256";
#[cfg(feature = "rio-v2")]
const DARE_PAYLOAD_SIZE: i64 = 64 * 1024;
#[cfg(feature = "rio-v2")]
const DARE_PACKAGE_SIZE: i64 = DARE_PAYLOAD_SIZE + 32;
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_IV_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Iv";
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm";
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key";
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Kms-Sealed-Key";
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id";
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key";
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Context";
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER: &str = "X-Minio-Internal-Server-Side-Encryption-Sealed-Key";
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM: &str = "DAREv2-HMAC-SHA256";
#[cfg(feature = "rio-v2")]
const DARE_VERSION_20: u8 = 0x20;
#[cfg(feature = "rio-v2")]
const DARE_CIPHER_AES_256_GCM: u8 = 0x00;
#[cfg(feature = "rio-v2")]
const DARE_CIPHER_CHACHA20_POLY1305: u8 = 0x01;
#[cfg(feature = "rio-v2")]
const DARE_HEADER_SIZE: usize = 16;
#[cfg(feature = "rio-v2")]
const DARE_TAG_SIZE: usize = 16;
#[cfg(feature = "rio-v2")]
const SEALED_KEY_IV_SIZE: usize = 32;
#[cfg(feature = "rio-v2")]
const SEALED_KEY_SIZE: usize = DARE_HEADER_SIZE + 32 + DARE_TAG_SIZE;
#[cfg(feature = "rio-v2")]
const MINIO_SECRET_KEY_RANDOM_SIZE: usize = 28;
#[cfg(feature = "rio-v2")]
const MINIO_SECRET_KEY_IV_SIZE: usize = 16;
#[cfg(feature = "rio-v2")]
const MINIO_SECRET_KEY_NONCE_SIZE: usize = 12;

#[cfg(feature = "rio-v2")]
type HmacSha256 = Hmac<Sha256>;

fn canonical_kms_bucket_path(bucket: &str, object: &str) -> String {
    path_join_buf(&[bucket, object])
}

fn build_object_encryption_context(
    bucket: &str,
    object: &str,
    provided_context: Option<&HashMap<String, String>>,
) -> ObjectEncryptionContext {
    let mut context = provided_context.cloned().unwrap_or_default();
    context
        .entry(bucket.to_string())
        .or_insert_with(|| canonical_kms_bucket_path(bucket, object));

    let mut object_context = ObjectEncryptionContext::new(bucket.to_string(), object.to_string());
    for (ctx_key, ctx_value) in context {
        object_context = object_context.with_encryption_context(ctx_key, ctx_value);
    }
    object_context
}

#[cfg(feature = "rio-v2")]
fn is_legacy_rustfs_managed_metadata(metadata: &HashMap<String, String>) -> bool {
    metadata_get(metadata, INTERNAL_ENCRYPTION_KEY_HEADER).is_some()
        && metadata_get(metadata, INTERNAL_ENCRYPTION_IV_HEADER).is_some()
        && metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER).is_none()
        && metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER).is_none()
}

fn part_plaintext_size(part: &ObjectPartInfo) -> i64 {
    if part.actual_size > 0 {
        part.actual_size
    } else {
        part.size as i64
    }
}

fn http_range_spec_from_object_info(oi: &ObjectInfo, part_number: usize) -> Option<HTTPRangeSpec> {
    HTTPRangeSpec::from_part_sizes(oi.size, part_number, oi.parts.iter().map(part_plaintext_size))
}

fn restore_request_active(opts: &ObjectOptions) -> bool {
    let restore = &opts.transition.restore_request;
    restore.type_.is_some() || restore.days.is_some() || restore.output_location.is_some() || restore.select_parameters.is_some()
}

fn decode_compression_index(index: Option<&bytes::Bytes>) -> Option<Index> {
    crate::io_support::rio::decode_compression_index_bytes(index?)
}

fn get_compressed_offsets(oi: &ObjectInfo, offset: i64) -> (i64, i64, usize, i64, u64) {
    let mut skip_length = 0_i64;
    let mut cumulative_actual_size = 0_i64;
    let mut first_part_idx = 0_usize;
    let mut compressed_offset = 0_i64;

    for (i, part) in oi.parts.iter().enumerate() {
        cumulative_actual_size += part_plaintext_size(part);
        if cumulative_actual_size <= offset {
            compressed_offset += part.size as i64;
        } else {
            first_part_idx = i;
            skip_length = cumulative_actual_size - part_plaintext_size(part);
            break;
        }
    }

    let mut part_skip = offset - skip_length;
    #[cfg(feature = "rio-v2")]
    let (mut decrypt_skip, mut seq_num) = (0_i64, 0_u64);
    #[cfg(not(feature = "rio-v2"))]
    let (decrypt_skip, seq_num) = (0_i64, 0_u64);

    if part_skip > 0
        && let Some(part) = oi.parts.get(first_part_idx)
        && let Some(index) = decode_compression_index(part.index.as_ref())
        && let Ok((comp_off, uncomp_off)) = index.find(part_skip)
        && comp_off > 0
    {
        #[cfg(feature = "rio-v2")]
        if oi.is_encrypted() {
            seq_num = (comp_off / DARE_PAYLOAD_SIZE) as u64;
            decrypt_skip = comp_off % DARE_PAYLOAD_SIZE;
            compressed_offset += (comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE;
        } else {
            compressed_offset += comp_off;
        }

        #[cfg(not(feature = "rio-v2"))]
        {
            compressed_offset += comp_off;
        }

        part_skip -= uncomp_off;
    }

    (compressed_offset, part_skip, first_part_idx, decrypt_skip, seq_num)
}

#[cfg(feature = "rio-v2")]
fn get_encrypted_offsets(oi: &ObjectInfo, offset: i64) -> Result<(i64, usize, usize, u32, i64)> {
    if oi.parts.is_empty() {
        let plaintext_size = oi.decrypted_size()?;
        let start_package_number = offset / DARE_PAYLOAD_SIZE;
        let plaintext_skip = usize::try_from(offset % DARE_PAYLOAD_SIZE)
            .map_err(|_| Error::other(format!("invalid DARE skip offset {offset}")))?;
        let encrypted_offset = start_package_number * DARE_PACKAGE_SIZE;
        let aligned_plaintext_offset = start_package_number * DARE_PAYLOAD_SIZE;
        let remaining_plaintext_size = plaintext_size - aligned_plaintext_offset;
        let sequence_number = u32::try_from(start_package_number)
            .map_err(|_| Error::other(format!("invalid DARE sequence number {start_package_number}")))?;
        return Ok((encrypted_offset, plaintext_skip, 0, sequence_number, remaining_plaintext_size));
    }

    let mut cumulative_plaintext_size = 0_i64;
    let mut cumulative_encrypted_size = 0_i64;

    for (part_index, part) in oi.parts.iter().enumerate() {
        let current_part_plaintext_size = part_plaintext_size(part);
        if offset < cumulative_plaintext_size + current_part_plaintext_size {
            let relative_offset = offset - cumulative_plaintext_size;
            let start_package_number = relative_offset / DARE_PAYLOAD_SIZE;
            let plaintext_skip = usize::try_from(relative_offset % DARE_PAYLOAD_SIZE)
                .map_err(|_| Error::other(format!("invalid DARE skip offset {relative_offset}")))?;
            let encrypted_offset = cumulative_encrypted_size + start_package_number * DARE_PACKAGE_SIZE;
            let aligned_plaintext_offset = cumulative_plaintext_size + start_package_number * DARE_PAYLOAD_SIZE;
            let remaining_plaintext_size = oi.decrypted_size()? - aligned_plaintext_offset;
            let sequence_number = u32::try_from(start_package_number)
                .map_err(|_| Error::other(format!("invalid DARE sequence number {start_package_number}")))?;

            return Ok((encrypted_offset, plaintext_skip, part_index, sequence_number, remaining_plaintext_size));
        }

        cumulative_plaintext_size += current_part_plaintext_size;
        cumulative_encrypted_size += part.size as i64;
    }

    Err(Error::other(format!(
        "invalid encrypted offset {offset} for object with decrypted size {}",
        oi.decrypted_size()?
    )))
}

pub struct PutObjReader {
    pub stream: HashReader,
}

impl Debug for PutObjReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutObjReader").finish()
    }
}

impl PutObjReader {
    pub fn new(stream: HashReader) -> Self {
        PutObjReader { stream }
    }

    pub fn as_hash_reader(&self) -> &HashReader {
        &self.stream
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        use sha2::{Digest, Sha256};
        let content_length = data.len() as i64;
        let sha256hex = if content_length > 0 {
            Some(hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower))
        } else {
            None
        };
        PutObjReader {
            stream: HashReader::from_stream(Cursor::new(data), content_length, content_length, None, sha256hex, false).unwrap(),
        }
    }

    pub fn size(&self) -> i64 {
        self.stream.size()
    }

    pub fn actual_size(&self) -> i64 {
        self.stream.actual_size()
    }
}

pub struct GetObjectReader {
    pub stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    pub object_info: ObjectInfo,
}

#[derive(Debug, Clone, Copy)]
struct EncryptionMaterial {
    key_bytes: [u8; 32],
    base_nonce: [u8; 12],
    key_kind: EncryptionKeyKind,
    reader_backend: crate::io_support::rio::ReadEncryptionBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EncryptionKeyKind {
    Direct,
    Object,
}

#[derive(Debug, Clone)]
enum ReadTransform {
    Plain {
        visible_offset: usize,
        visible_length: i64,
    },
    Compressed {
        algorithm: CompressionAlgorithm,
        backend: crate::io_support::rio::ReadCompressionBackend,
        decompressed_offset: usize,
        decompressed_length: i64,
        total_plaintext_size: usize,
    },
    Encrypted {
        material: EncryptionMaterial,
        is_multipart: bool,
        part_numbers: Vec<usize>,
        sequence_number: u32,
        decrypt_skip: usize,
        plaintext_offset: usize,
        plaintext_length: i64,
        total_plaintext_size: usize,
        compression: Option<(CompressionAlgorithm, crate::io_support::rio::ReadCompressionBackend)>,
    },
}

#[derive(Debug, Clone)]
struct ReadPlan {
    storage_offset: usize,
    storage_length: i64,
    object_size: i64,
    transform: ReadTransform,
}

impl ReadPlan {
    async fn build(rs: Option<HTTPRangeSpec>, oi: &ObjectInfo, opts: &ObjectOptions, h: &HeaderMap<HeaderValue>) -> Result<Self> {
        let mut rs = rs;
        if let Some(part_number) = opts.part_number
            && rs.is_none()
        {
            rs = http_range_spec_from_object_info(oi, part_number);
        }

        if opts.raw_data_movement_read {
            let (visible_offset, visible_length) = if let Some(rs) = rs {
                rs.get_offset_length(oi.size)?
            } else {
                (0, oi.size)
            };

            return Ok(Self {
                storage_offset: visible_offset,
                storage_length: visible_length,
                object_size: oi.size,
                transform: ReadTransform::Plain {
                    visible_offset,
                    visible_length,
                },
            });
        }

        let mut is_encrypted = oi.is_encrypted();
        let (algo, compression_backend, mut is_compressed) = oi.compression_read_plan()?;

        if restore_request_active(opts) {
            is_encrypted = false;
            is_compressed = false;
        }

        if is_compressed && !is_encrypted {
            let actual_size = oi.get_actual_size()?;
            let (storage_offset, storage_length, decompressed_offset, decompressed_length) = if let Some(rs) = rs {
                let (req_off, req_length) = rs.get_offset_length(actual_size)?;
                let (physical_off, decompressed_skip, _, _, _) = get_compressed_offsets(oi, req_off as i64);
                let storage_offset = usize::try_from(physical_off)
                    .map_err(|_| Error::other(format!("invalid compressed offset {physical_off}")))?;
                (storage_offset, oi.size - physical_off, decompressed_skip as usize, req_length)
            } else {
                (0, oi.size, 0, actual_size)
            };

            let total_plaintext_size =
                usize::try_from(actual_size).map_err(|_| Error::other(format!("invalid decompressed size {actual_size}")))?;

            return Ok(Self {
                storage_offset,
                storage_length,
                object_size: decompressed_length,
                transform: ReadTransform::Compressed {
                    algorithm: algo,
                    backend: compression_backend,
                    decompressed_offset,
                    decompressed_length,
                    total_plaintext_size,
                },
            });
        }

        if is_encrypted {
            let material = resolve_encryption_material(oi, h).await?;
            #[cfg(feature = "rio-v2")]
            let encryption_backend = material.reader_backend;
            let is_multipart = is_multipart_encrypted_object(&oi.parts, oi.etag.as_deref());
            let plaintext_size = encrypted_plaintext_size(oi, is_multipart, is_compressed)?;
            let full_plaintext_size =
                usize::try_from(plaintext_size).map_err(|_| Error::other(format!("invalid decrypted size {plaintext_size}")))?;
            let (
                storage_offset,
                storage_length,
                decrypt_skip,
                plaintext_offset,
                plaintext_length,
                total_plaintext_size,
                sequence_number,
                part_numbers,
            ) = if let Some(rs) = rs {
                let (requested_offset, requested_length) = rs.get_offset_length(plaintext_size)?;
                #[cfg(feature = "rio-v2")]
                {
                    if encryption_backend == crate::io_support::rio::ReadEncryptionBackend::Legacy {
                        (
                            0,
                            oi.size,
                            0,
                            requested_offset,
                            requested_length,
                            full_plaintext_size,
                            0,
                            multipart_part_numbers(&oi.parts),
                        )
                    } else if is_compressed {
                        let (physical_off, decompressed_skip, first_part_idx, decrypt_skip, seq_num) =
                            get_compressed_offsets(oi, requested_offset as i64);
                        (
                            usize::try_from(physical_off)
                                .map_err(|_| Error::other(format!("invalid encrypted compressed offset {physical_off}")))?,
                            oi.size - physical_off,
                            usize::try_from(decrypt_skip)
                                .map_err(|_| Error::other(format!("invalid decrypt skip {decrypt_skip}")))?,
                            usize::try_from(decompressed_skip)
                                .map_err(|_| Error::other(format!("invalid decompressed skip {decompressed_skip}")))?,
                            requested_length,
                            full_plaintext_size,
                            u32::try_from(seq_num)
                                .map_err(|_| Error::other(format!("invalid DARE sequence number {seq_num}")))?,
                            multipart_part_numbers(&oi.parts[first_part_idx..]),
                        )
                    } else {
                        let (encrypted_offset, plaintext_skip, part_start_index, sequence_number, remaining_plaintext_size) =
                            get_encrypted_offsets(oi, requested_offset as i64)?;
                        let total_plaintext_size = usize::try_from(remaining_plaintext_size)
                            .map_err(|_| Error::other(format!("invalid remaining decrypted size {remaining_plaintext_size}")))?;
                        (
                            usize::try_from(encrypted_offset)
                                .map_err(|_| Error::other(format!("invalid encrypted offset {encrypted_offset}")))?,
                            oi.size - encrypted_offset,
                            0,
                            plaintext_skip,
                            requested_length,
                            total_plaintext_size,
                            sequence_number,
                            multipart_part_numbers(&oi.parts[part_start_index..]),
                        )
                    }
                }
                #[cfg(not(feature = "rio-v2"))]
                {
                    (
                        0,
                        oi.size,
                        0,
                        requested_offset,
                        requested_length,
                        full_plaintext_size,
                        0,
                        multipart_part_numbers(&oi.parts),
                    )
                }
            } else {
                (
                    0,
                    oi.size,
                    0,
                    0,
                    plaintext_size,
                    full_plaintext_size,
                    0,
                    multipart_part_numbers(&oi.parts),
                )
            };

            return Ok(Self {
                storage_offset,
                storage_length,
                object_size: plaintext_length,
                transform: ReadTransform::Encrypted {
                    material,
                    is_multipart,
                    part_numbers,
                    sequence_number,
                    decrypt_skip,
                    plaintext_offset,
                    plaintext_length,
                    total_plaintext_size,
                    compression: is_compressed.then_some((algo, compression_backend)),
                },
            });
        }

        let (visible_offset, visible_length) = if let Some(rs) = rs {
            rs.get_offset_length(oi.size)?
        } else {
            (0, oi.size)
        };

        Ok(Self {
            storage_offset: visible_offset,
            storage_length: visible_length,
            object_size: oi.size,
            transform: ReadTransform::Plain {
                visible_offset,
                visible_length,
            },
        })
    }

    fn into_reader(
        self,
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        oi: &ObjectInfo,
    ) -> Result<(GetObjectReader, usize, i64)> {
        match self.transform {
            ReadTransform::Plain { .. } => Ok((
                GetObjectReader {
                    stream: reader,
                    object_info: oi.clone(),
                },
                self.storage_offset,
                self.storage_length,
            )),
            ReadTransform::Compressed {
                algorithm,
                backend,
                decompressed_offset,
                decompressed_length,
                total_plaintext_size,
            } => {
                let dec_reader = crate::io_support::rio::decompression_reader(reader, algorithm, backend);
                #[cfg(feature = "rio-v2")]
                let dec_reader = StreamConsumer::new(dec_reader);
                let final_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = if decompressed_offset > 0
                    || decompressed_length != total_plaintext_size as i64
                {
                    #[cfg(feature = "rio-v2")]
                    let ranged_result = RangedDecompressReader::new_draining(
                        dec_reader,
                        decompressed_offset,
                        decompressed_length,
                        total_plaintext_size,
                    );
                    #[cfg(not(feature = "rio-v2"))]
                    let ranged_result =
                        RangedDecompressReader::new(dec_reader, decompressed_offset, decompressed_length, total_plaintext_size);

                    match ranged_result {
                        Ok(ranged_reader) => {
                            tracing::debug!(
                                "Successfully created RangedDecompressReader for offset={}, length={}",
                                decompressed_offset,
                                decompressed_length
                            );
                            Box::new(ranged_reader)
                        }
                        Err(e) => {
                            tracing::error!("RangedDecompressReader failed with invalid range parameters: {}", e);
                            return Err(e);
                        }
                    }
                } else {
                    Box::new(LimitReader::new(dec_reader, total_plaintext_size))
                };

                let mut object_info = oi.clone();
                object_info.size = self.object_size;

                Ok((
                    GetObjectReader {
                        stream: final_reader,
                        object_info,
                    },
                    self.storage_offset,
                    self.storage_length,
                ))
            }
            ReadTransform::Encrypted {
                material,
                is_multipart,
                part_numbers,
                sequence_number,
                decrypt_skip,
                plaintext_offset,
                plaintext_length,
                total_plaintext_size,
                compression,
            } => {
                #[cfg(not(feature = "rio-v2"))]
                let _ = sequence_number;
                let decrypted_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = if is_multipart {
                    match material.key_kind {
                        EncryptionKeyKind::Object => crate::io_support::rio::decrypt_multipart_reader_with_object_key(
                            reader,
                            material.key_bytes,
                            part_numbers,
                            sequence_number,
                        ),
                        EncryptionKeyKind::Direct => crate::io_support::rio::decrypt_multipart_reader(
                            reader,
                            material.key_bytes,
                            material.base_nonce,
                            part_numbers,
                            material.reader_backend,
                            sequence_number,
                        ),
                    }
                } else {
                    match material.key_kind {
                        EncryptionKeyKind::Object => {
                            crate::io_support::rio::decrypt_reader_with_object_key(reader, material.key_bytes, sequence_number)
                        }
                        EncryptionKeyKind::Direct => crate::io_support::rio::decrypt_reader(
                            reader,
                            material.key_bytes,
                            material.base_nonce,
                            material.reader_backend,
                            sequence_number,
                        ),
                    }
                };
                let decrypted_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = if decrypt_skip > 0 {
                    Box::new(SkipReader::new(decrypted_reader, decrypt_skip))
                } else {
                    decrypted_reader
                };
                let total_plaintext_size_i64 = i64::try_from(total_plaintext_size)
                    .map_err(|_| Error::other(format!("invalid plaintext size {total_plaintext_size}")))?;

                let final_reader: Box<dyn AsyncRead + Unpin + Send + Sync> =
                    if let Some((algo, compression_backend)) = compression {
                        let decompressed_reader =
                            crate::io_support::rio::decompression_reader(decrypted_reader, algo, compression_backend);
                        #[cfg(feature = "rio-v2")]
                        let decompressed_reader = StreamConsumer::new(decompressed_reader);
                        if plaintext_offset > 0 || plaintext_length != total_plaintext_size_i64 {
                            #[cfg(feature = "rio-v2")]
                            let ranged_reader = RangedDecompressReader::new_draining(
                                decompressed_reader,
                                plaintext_offset,
                                plaintext_length,
                                total_plaintext_size,
                            )?;
                            #[cfg(not(feature = "rio-v2"))]
                            let ranged_reader = RangedDecompressReader::new(
                                decompressed_reader,
                                plaintext_offset,
                                plaintext_length,
                                total_plaintext_size,
                            )?;
                            Box::new(ranged_reader)
                        } else {
                            Box::new(LimitReader::new(decompressed_reader, total_plaintext_size))
                        }
                    } else if plaintext_offset > 0 || plaintext_length != total_plaintext_size_i64 {
                        Box::new(RangedDecompressReader::new(
                            decrypted_reader,
                            plaintext_offset,
                            plaintext_length,
                            total_plaintext_size,
                        )?)
                    } else {
                        Box::new(LimitReader::new(decrypted_reader, total_plaintext_size))
                    };

                let mut object_info = oi.clone();
                object_info.size = self.object_size;

                Ok((
                    GetObjectReader {
                        stream: final_reader,
                        object_info,
                    },
                    self.storage_offset,
                    self.storage_length,
                ))
            }
        }
    }
}

impl GetObjectReader {
    pub async fn new(
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        h: &HeaderMap<HeaderValue>,
    ) -> Result<(Self, usize, i64)> {
        ReadPlan::build(rs, oi, opts, h).await?.into_reader(reader, oi)
    }
    pub async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        self.stream.read_to_end(&mut data).await?;

        // while let Some(x) = self.stream.next().await {
        //     let buf = match x {
        //         Ok(res) => res,
        //         Err(e) => return Err(Error::other(e.to_string())),
        //     };
        //     data.extend_from_slice(buf.as_ref());
        // }

        Ok(data)
    }
}

impl AsyncRead for GetObjectReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

#[derive(Debug)]
struct SkipReader<R> {
    inner: R,
    bytes_to_skip: usize,
    bytes_skipped: usize,
    scratch: Vec<u8>,
}

impl<R: AsyncRead + Unpin + Send + Sync> SkipReader<R> {
    fn new(inner: R, bytes_to_skip: usize) -> Self {
        Self {
            inner,
            bytes_to_skip,
            bytes_skipped: 0,
            scratch: vec![0u8; 8192],
        }
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for SkipReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.as_mut().get_mut();

        while this.bytes_skipped < this.bytes_to_skip {
            let remaining = this.bytes_to_skip - this.bytes_skipped;
            let scratch_len = remaining.min(this.scratch.len());
            let mut scratch_buf = ReadBuf::new(&mut this.scratch[..scratch_len]);
            match Pin::new(&mut this.inner).poll_read(cx, &mut scratch_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {
                    let n = scratch_buf.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!("unexpected EOF while skipping {} bytes from decrypted stream", this.bytes_to_skip),
                        )));
                    }
                    this.bytes_skipped += n;
                }
            }
        }

        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

/// A streaming decompression reader that supports range requests by skipping data in the decompressed stream.
/// This implementation acknowledges that compressed streams (like LZ4) must be decompressed sequentially
/// from the beginning, so it streams and discards data until reaching the target offset.
#[derive(Debug)]
pub struct RangedDecompressReader<R: AsyncRead + Unpin + Send + Sync + 'static> {
    inner: Option<R>,
    target_offset: usize,
    target_length: usize,
    current_offset: usize,
    bytes_returned: usize,
    scratch: Vec<u8>,
    drain_on_done: bool,
    drain_task: Option<tokio::task::JoinHandle<()>>,
}

impl<R: AsyncRead + Unpin + Send + Sync + 'static> RangedDecompressReader<R> {
    pub fn new(inner: R, offset: usize, length: i64, total_size: usize) -> Result<Self> {
        Self::new_with_drain(inner, offset, length, total_size, false)
    }

    pub fn new_draining(inner: R, offset: usize, length: i64, total_size: usize) -> Result<Self> {
        Self::new_with_drain(inner, offset, length, total_size, true)
    }

    fn new_with_drain(inner: R, offset: usize, length: i64, total_size: usize, drain_on_done: bool) -> Result<Self> {
        // Validate the range request
        if offset >= total_size {
            tracing::debug!("Range offset {} exceeds total size {}", offset, total_size);
            return Err(Error::InvalidRangeSpec("Range offset exceeds file size".to_string()));
        }

        // Adjust length if it extends beyond file end
        let actual_length = std::cmp::min(length as usize, total_size - offset);

        tracing::debug!(
            "Creating RangedDecompressReader: offset={}, length={}, total_size={}, actual_length={}",
            offset,
            length,
            total_size,
            actual_length
        );

        Ok(Self {
            inner: Some(inner),
            target_offset: offset,
            target_length: actual_length,
            current_offset: 0,
            bytes_returned: 0,
            scratch: vec![0u8; 8192],
            drain_on_done,
            drain_task: None,
        })
    }

    fn start_drain(&mut self) {
        if !self.drain_on_done || self.drain_task.is_some() {
            return;
        }

        let Some(mut inner) = self.inner.take() else {
            return;
        };

        self.drain_task = Some(tokio::spawn(async move {
            let mut buf = [0u8; 8192];
            loop {
                match inner.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        }));
    }
}

impl<R: AsyncRead + Unpin + Send + Sync + 'static> AsyncRead for RangedDecompressReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;
        use tokio::io::ReadBuf;

        let this = self.as_mut().get_mut();

        loop {
            // If we've returned all the bytes we need, return EOF
            if this.bytes_returned >= this.target_length {
                this.start_drain();
                return Poll::Ready(Ok(()));
            }

            // Read from the inner stream
            let buf_capacity = buf.remaining();
            if buf_capacity == 0 {
                return Poll::Ready(Ok(()));
            }

            let scratch_len = std::cmp::min(this.scratch.len(), std::cmp::max(buf_capacity, 1));
            let mut temp_read_buf = ReadBuf::new(&mut this.scratch[..scratch_len]);

            let Some(inner) = this.inner.as_mut() else {
                return Poll::Ready(Ok(()));
            };

            match Pin::new(inner).poll_read(cx, &mut temp_read_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = temp_read_buf.filled().len();
                    if n == 0 {
                        // EOF from inner stream
                        if this.current_offset < this.target_offset {
                            // We haven't reached the target offset yet - this is an error
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "Unexpected EOF: only read {} bytes, target offset is {}",
                                    this.current_offset, this.target_offset
                                ),
                            )));
                        }
                        // Normal EOF after reaching target
                        return Poll::Ready(Ok(()));
                    }

                    // Update current position
                    let old_offset = this.current_offset;
                    this.current_offset += n;

                    // Check if we're still in the skip phase
                    if old_offset < this.target_offset {
                        // We're still skipping data
                        let skip_end = std::cmp::min(this.current_offset, this.target_offset);
                        let bytes_to_skip_in_this_read = skip_end - old_offset;

                        if this.current_offset <= this.target_offset {
                            // All data in this read should be skipped
                            tracing::trace!("Skipping {} bytes at offset {}", n, old_offset);
                            // Continue reading in the loop instead of recursive call
                            continue;
                        } else {
                            // Partial skip: some data should be returned
                            let data_start_in_buffer = bytes_to_skip_in_this_read;
                            let available_data = n - data_start_in_buffer;
                            let bytes_to_return = std::cmp::min(
                                available_data,
                                std::cmp::min(buf.remaining(), this.target_length - this.bytes_returned),
                            );

                            if bytes_to_return > 0 {
                                let data_slice = &this.scratch[data_start_in_buffer..data_start_in_buffer + bytes_to_return];
                                buf.put_slice(data_slice);
                                this.bytes_returned += bytes_to_return;

                                tracing::trace!(
                                    "Skipped {} bytes, returned {} bytes at offset {}",
                                    bytes_to_skip_in_this_read,
                                    bytes_to_return,
                                    old_offset
                                );
                            }
                            return Poll::Ready(Ok(()));
                        }
                    } else {
                        // We're in the data return phase
                        let bytes_to_return =
                            std::cmp::min(n, std::cmp::min(buf.remaining(), this.target_length - this.bytes_returned));

                        if bytes_to_return > 0 {
                            buf.put_slice(&this.scratch[..bytes_to_return]);
                            this.bytes_returned += bytes_to_return;

                            tracing::trace!("Returned {} bytes at offset {}", bytes_to_return, old_offset);
                        }
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}

impl<R: AsyncRead + Unpin + Send + Sync + 'static> Drop for RangedDecompressReader<R> {
    fn drop(&mut self) {
        if self.bytes_returned >= self.target_length {
            self.start_drain();
        }
    }
}

/// A wrapper that ensures the inner stream is fully consumed even if the outer reader stops early.
/// This prevents broken pipe errors in erasure coding scenarios where the writer expects
/// the full stream to be consumed.
pub struct StreamConsumer<R: AsyncRead + Unpin + Send + 'static> {
    inner: Option<R>,
    consumer_task: Option<tokio::task::JoinHandle<()>>,
}

impl<R: AsyncRead + Unpin + Send + 'static> StreamConsumer<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: Some(inner),
            consumer_task: None,
        }
    }

    fn ensure_consumer_started(&mut self) {
        if self.consumer_task.is_none() && self.inner.is_some() {
            let mut inner = self.inner.take().unwrap();
            let task = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match inner.read(&mut buf).await {
                        Ok(0) => break,    // EOF
                        Ok(_) => continue, // Keep consuming
                        Err(_) => break,   // Error, stop consuming
                    }
                }
            });
            self.consumer_task = Some(task);
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> AsyncRead for StreamConsumer<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;

        if let Some(ref mut inner) = self.inner {
            Pin::new(inner).poll_read(cx, buf)
        } else {
            Poll::Ready(Ok(())) // EOF
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> Drop for StreamConsumer<R> {
    fn drop(&mut self) {
        if self.consumer_task.is_none() && self.inner.is_some() {
            let mut inner = self.inner.take().unwrap();
            let task = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match inner.read(&mut buf).await {
                        Ok(0) => break,    // EOF
                        Ok(_) => continue, // Keep consuming
                        Err(_) => break,   // Error, stop consuming
                    }
                }
            });
            self.consumer_task = Some(task);
        }
    }
}

fn encrypted_plaintext_size(oi: &ObjectInfo, is_multipart: bool, is_compressed: bool) -> Result<i64> {
    if is_compressed {
        return oi.get_actual_size().map_err(Into::into);
    }

    if is_multipart {
        return Ok(multipart_plaintext_size(&oi.parts, oi.decrypted_size()?));
    }

    oi.decrypted_size().map_err(Into::into)
}

fn is_multipart_encrypted_object(parts: &[rustfs_filemeta::ObjectPartInfo], etag: Option<&str>) -> bool {
    if parts.len() > 1 {
        return true;
    }

    etag.map(|etag| etag.trim_matches('"').len() != 32).unwrap_or(false)
}

fn multipart_plaintext_size(parts: &[rustfs_filemeta::ObjectPartInfo], fallback: i64) -> i64 {
    let total: i64 = parts.iter().map(part_plaintext_size).sum();

    if total > 0 { total } else { fallback }
}

fn multipart_part_numbers(parts: &[rustfs_filemeta::ObjectPartInfo]) -> Vec<usize> {
    parts.iter().map(|part| part.number).collect()
}

fn metadata_get<'a>(metadata: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    metadata.get(key).map(String::as_str).or_else(|| {
        metadata
            .iter()
            .find_map(|(candidate, value)| candidate.eq_ignore_ascii_case(key).then_some(value.as_str()))
    })
}

#[cfg(feature = "rio-v2")]
fn is_supported_sealed_object_key_cipher(cipher: u8) -> bool {
    matches!(cipher, DARE_CIPHER_AES_256_GCM | DARE_CIPHER_CHACHA20_POLY1305)
}

#[cfg(feature = "rio-v2")]
fn decrypt_sealed_object_key_payload(sealing_key: [u8; 32], header: &[u8], sealed_key: &[u8]) -> Result<Vec<u8>> {
    let nonce = &header[4..16];
    let ciphertext = &sealed_key[DARE_HEADER_SIZE..];
    let aad = &header[..4];
    match header[1] {
        DARE_CIPHER_AES_256_GCM => {
            let cipher = Aes256Gcm::new_from_slice(&sealing_key)
                .map_err(|err| Error::other(format!("invalid AES-GCM sealing key: {err}")))?;
            let nonce = Nonce::try_from(nonce).map_err(|_| Error::other("invalid sealed object-key package nonce"))?;
            cipher.decrypt(&nonce, Payload { msg: ciphertext, aad })
        }
        DARE_CIPHER_CHACHA20_POLY1305 => {
            let cipher = ChaCha20Poly1305::new_from_slice(&sealing_key)
                .map_err(|err| Error::other(format!("invalid ChaCha20-Poly1305 sealing key: {err}")))?;
            let nonce =
                chacha20poly1305::Nonce::try_from(nonce).map_err(|_| Error::other("invalid sealed object-key package nonce"))?;
            cipher.decrypt(&nonce, Payload { msg: ciphertext, aad })
        }
        _ => return Err(Error::other("unsupported sealed object-key DARE header")),
    }
    .map_err(|err| Error::other(format!("failed to unseal object key: {err}")))
}

async fn resolve_encryption_material(oi: &ObjectInfo, headers: &HeaderMap<HeaderValue>) -> Result<EncryptionMaterial> {
    if metadata_get(&oi.user_defined, SSEC_ALGORITHM_HEADER).is_some() {
        return resolve_ssec_material(oi, headers);
    }

    if contains_managed_encryption_metadata(&oi.user_defined) {
        return resolve_managed_material(&oi.bucket, &oi.name, &oi.user_defined).await;
    }

    Err(Error::other("encrypted object metadata is incomplete"))
}

fn contains_managed_encryption_metadata(metadata: &HashMap<String, String>) -> bool {
    if metadata_get(metadata, INTERNAL_ENCRYPTION_KEY_HEADER).is_some() {
        return true;
    }

    #[cfg(feature = "rio-v2")]
    {
        metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER).is_some()
            || metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER).is_some()
            || metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER).is_some()
    }

    #[cfg(not(feature = "rio-v2"))]
    {
        false
    }
}

#[cfg(feature = "rio-v2")]
fn canonical_sse_path(bucket: &str, object: &str) -> String {
    let bucket = bucket.trim_matches('/');
    let object = object.trim_matches('/');
    if object.is_empty() {
        bucket.to_string()
    } else if bucket.is_empty() {
        object.to_string()
    } else {
        format!("{bucket}/{object}")
    }
}

#[cfg(feature = "rio-v2")]
fn managed_sse_domain(metadata: &HashMap<String, String>) -> &'static str {
    if metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER).is_some()
        || metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER).is_some()
        || matches!(metadata_get(metadata, "x-amz-server-side-encryption"), Some("aws:kms"))
    {
        "SSE-KMS"
    } else if metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER).is_some() {
        "SSE-C"
    } else {
        "SSE-S3"
    }
}

#[cfg(feature = "rio-v2")]
fn derive_sealing_key(
    external_key: [u8; 32],
    iv: [u8; SEALED_KEY_IV_SIZE],
    domain: &str,
    bucket: &str,
    object: &str,
) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(&external_key).expect("32-byte HMAC key");
    mac.update(&iv);
    mac.update(domain.as_bytes());
    mac.update(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.as_bytes());
    mac.update(canonical_sse_path(bucket, object).as_bytes());

    let mut sealing_key = [0u8; 32];
    sealing_key.copy_from_slice(mac.finalize().into_bytes().as_slice());
    sealing_key
}

#[cfg(feature = "rio-v2")]
fn try_decode_minio_sealed_key(bytes: &str) -> Result<Option<[u8; SEALED_KEY_SIZE]>> {
    let decoded = BASE64_STANDARD
        .decode(bytes)
        .map_err(|e| Error::other(format!("failed to decode sealed object key: {e}")))?;
    match decoded.as_slice().try_into() {
        Ok(sealed_key) => Ok(Some(sealed_key)),
        Err(_) => Ok(None),
    }
}

#[cfg(feature = "rio-v2")]
fn try_decode_minio_sealing_iv(bytes: &str) -> Result<Option<[u8; SEALED_KEY_IV_SIZE]>> {
    let decoded = BASE64_STANDARD
        .decode(bytes)
        .map_err(|e| Error::other(format!("failed to decode sealing IV: {e}")))?;
    match decoded.as_slice().try_into() {
        Ok(iv) => Ok(Some(iv)),
        Err(_) => Ok(None),
    }
}

#[cfg(feature = "rio-v2")]
fn try_unseal_minio_object_key(
    metadata: &HashMap<String, String>,
    bucket: &str,
    object: &str,
    external_key: [u8; 32],
) -> Result<Option<[u8; 32]>> {
    let Some(algorithm) = metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER) else {
        return Ok(None);
    };
    if algorithm != MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM {
        return Ok(None);
    }

    let Some(iv_b64) = metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_IV_HEADER) else {
        return Ok(None);
    };
    let Some(iv) = try_decode_minio_sealing_iv(iv_b64)? else {
        return Ok(None);
    };

    let sealed_key_b64 = metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
        .or_else(|| metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER))
        .or_else(|| metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER));
    let Some(sealed_key_b64) = sealed_key_b64 else {
        return Ok(None);
    };
    let Some(sealed_key) = try_decode_minio_sealed_key(sealed_key_b64)? else {
        return Ok(None);
    };
    let header = &sealed_key[..DARE_HEADER_SIZE];
    if header[0] != DARE_VERSION_20 || !is_supported_sealed_object_key_cipher(header[1]) {
        return Err(Error::other("unsupported sealed object-key DARE header"));
    }
    if u16::from_le_bytes([header[2], header[3]]) != 31 || header[4] & 0x80 == 0 {
        return Err(Error::other("invalid sealed object-key payload header"));
    }

    let sealing_key = derive_sealing_key(external_key, iv, managed_sse_domain(metadata), bucket, object);
    let plaintext = decrypt_sealed_object_key_payload(sealing_key, header, &sealed_key)?;
    let object_key: [u8; 32] = plaintext
        .as_slice()
        .try_into()
        .map_err(|_| Error::other("sealed object key must decrypt to 32 bytes"))?;
    Ok(Some(object_key))
}

fn resolve_ssec_material(oi: &ObjectInfo, headers: &HeaderMap<HeaderValue>) -> Result<EncryptionMaterial> {
    let algorithm = headers
        .get(SSEC_ALGORITHM_HEADER)
        .ok_or_else(|| Error::other("missing SSE-C algorithm header"))?
        .to_str()
        .map_err(|_| Error::other("invalid SSE-C algorithm header"))?;
    if algorithm != DEFAULT_SSE_ALGORITHM {
        return Err(Error::other(format!("unsupported SSE-C algorithm {algorithm}")));
    }

    let key_b64 = headers
        .get(SSEC_KEY_HEADER)
        .ok_or_else(|| Error::other("missing SSE-C key header"))?
        .to_str()
        .map_err(|_| Error::other("invalid SSE-C key header"))?;
    let key_md5 = headers
        .get(SSEC_KEY_MD5_HEADER)
        .ok_or_else(|| Error::other("missing SSE-C key md5 header"))?
        .to_str()
        .map_err(|_| Error::other("invalid SSE-C key md5 header"))?;

    let key_bytes_vec = BASE64_STANDARD
        .decode(key_b64)
        .map_err(|_| Error::other("failed to decode SSE-C key"))?;
    let key_bytes: [u8; 32] = key_bytes_vec
        .try_into()
        .map_err(|_| Error::other("SSE-C key must be 32 bytes"))?;

    let expected_md5 = BASE64_STANDARD.encode(md5_bytes(key_bytes));
    if expected_md5 != key_md5 {
        return Err(Error::other("SSE-C key MD5 mismatch"));
    }

    let stored_md5 =
        metadata_get(&oi.user_defined, SSEC_KEY_MD5_HEADER).ok_or_else(|| Error::other("missing stored SSE-C key md5"))?;
    if stored_md5 != expected_md5 {
        return Err(Error::other("SSE-C key does not match object metadata"));
    }

    #[cfg(feature = "rio-v2")]
    if let Some(object_key) = try_unseal_minio_object_key(&oi.user_defined, &oi.bucket, &oi.name, key_bytes)? {
        return Ok(EncryptionMaterial {
            key_bytes: object_key,
            base_nonce: [0u8; 12],
            key_kind: EncryptionKeyKind::Object,
            reader_backend: crate::io_support::rio::ReadEncryptionBackend::V2,
        });
    }

    Ok(EncryptionMaterial {
        key_bytes,
        base_nonce: generate_ssec_nonce(&oi.bucket, &oi.name),
        key_kind: EncryptionKeyKind::Direct,
        reader_backend: crate::io_support::rio::ReadEncryptionBackend::Legacy,
    })
}

async fn resolve_managed_material(bucket: &str, object: &str, metadata: &HashMap<String, String>) -> Result<EncryptionMaterial> {
    let normalized_metadata = normalize_managed_metadata(metadata);
    let encrypted_dek = metadata_get(&normalized_metadata, INTERNAL_ENCRYPTION_KEY_HEADER)
        .ok_or_else(|| Error::other("missing managed encrypted DEK"))?;
    let encrypted_dek = BASE64_STANDARD
        .decode(encrypted_dek)
        .map_err(|e| Error::other(format!("failed to decode managed encrypted DEK: {e}")))?;

    let kms_key_id = metadata_get(&normalized_metadata, INTERNAL_ENCRYPTION_KEY_ID_HEADER).unwrap_or("default");
    #[cfg(feature = "rio-v2")]
    let kms_context = metadata_get(&normalized_metadata, INTERNAL_ENCRYPTION_CONTEXT_HEADER)
        .map(|value| {
            serde_json::from_str::<HashMap<String, String>>(value)
                .map_err(|e| Error::other(format!("failed to parse managed KMS context: {e}")))
        })
        .transpose()?;
    #[cfg(not(feature = "rio-v2"))]
    let kms_context: Option<HashMap<String, String>> = None;
    let object_context = build_object_encryption_context(bucket, object, kms_context.as_ref());

    let decrypted_key = if let Some(service) = crate::runtime::sources::object_encryption_service().await {
        #[cfg(feature = "rio-v2")]
        let data_key = if is_legacy_rustfs_managed_metadata(&normalized_metadata) {
            service.decrypt_legacy_data_key(&encrypted_dek).await
        } else {
            service.decrypt_data_key(&encrypted_dek, &object_context).await
        };
        #[cfg(not(feature = "rio-v2"))]
        let data_key = service.decrypt_data_key(&encrypted_dek, &object_context).await;

        data_key
            .map_err(|e| Error::other(format!("failed to decrypt managed data key: {e}")))?
            .plaintext_key
    } else {
        decrypt_local_sse_dek(&encrypted_dek, kms_key_id, &object_context)?
    };

    #[cfg(feature = "rio-v2")]
    if let Some(object_key) = try_unseal_minio_object_key(&normalized_metadata, bucket, object, decrypted_key)? {
        return Ok(EncryptionMaterial {
            key_bytes: object_key,
            base_nonce: [0u8; 12],
            key_kind: EncryptionKeyKind::Object,
            reader_backend: crate::io_support::rio::ReadEncryptionBackend::V2,
        });
    }

    let iv_b64 = metadata_get(&normalized_metadata, INTERNAL_ENCRYPTION_IV_HEADER)
        .ok_or_else(|| Error::other("missing managed encryption IV"))?;
    let iv = BASE64_STANDARD
        .decode(iv_b64)
        .map_err(|e| Error::other(format!("failed to decode managed encryption IV: {e}")))?;
    let base_nonce: [u8; 12] = iv
        .as_slice()
        .try_into()
        .map_err(|_| Error::other("managed encryption IV must be 12 bytes"))?;

    Ok(EncryptionMaterial {
        key_bytes: decrypted_key,
        base_nonce,
        key_kind: EncryptionKeyKind::Direct,
        reader_backend: crate::io_support::rio::ReadEncryptionBackend::Legacy,
    })
}

fn normalize_managed_metadata(metadata: &HashMap<String, String>) -> HashMap<String, String> {
    #[cfg(feature = "rio-v2")]
    {
        let mut normalized = metadata.clone();
        if metadata_get(&normalized, INTERNAL_ENCRYPTION_KEY_HEADER).is_none()
            && let Some(value) = metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER)
                .or_else(|| metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER))
                .or_else(|| metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER))
        {
            normalized.insert(INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), value.to_string());
        }

        if metadata_get(&normalized, INTERNAL_ENCRYPTION_IV_HEADER).is_none()
            && let Some(value) = metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_IV_HEADER)
        {
            normalized.insert(INTERNAL_ENCRYPTION_IV_HEADER.to_string(), value.to_string());
        }

        if metadata_get(&normalized, INTERNAL_ENCRYPTION_KEY_ID_HEADER).is_none()
            && let Some(value) = metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER)
        {
            normalized.insert(INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), value.to_string());
        }

        if metadata_get(&normalized, INTERNAL_ENCRYPTION_CONTEXT_HEADER).is_none()
            && let Some(value) = metadata_get(metadata, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)
            && let Ok(decoded) = BASE64_STANDARD.decode(value)
            && let Ok(context) = serde_json::from_slice::<HashMap<String, String>>(&decoded)
            && let Ok(encoded) = serde_json::to_string(&context)
        {
            normalized.insert(INTERNAL_ENCRYPTION_CONTEXT_HEADER.to_string(), encoded);
        }

        normalized
    }

    #[cfg(not(feature = "rio-v2"))]
    {
        metadata.clone()
    }
}

fn decrypt_local_sse_dek(encrypted_dek: &[u8], _kms_key_id: &str, object_context: &ObjectEncryptionContext) -> Result<[u8; 32]> {
    if let Ok(plaintext) = decrypt_rustfs_local_sse_dek(encrypted_dek) {
        return Ok(plaintext);
    }

    #[cfg(feature = "rio-v2")]
    {
        decrypt_minio_secret_key_dek(encrypted_dek, object_context)
    }

    #[cfg(not(feature = "rio-v2"))]
    {
        let _ = object_context;
        Err(Error::other("invalid managed DEK format"))
    }
}

fn decrypt_rustfs_local_sse_dek(encrypted_dek: &[u8]) -> Result<[u8; 32]> {
    let encrypted_dek = std::str::from_utf8(encrypted_dek).map_err(|_| Error::other("managed DEK is not valid UTF-8"))?;
    let parts: Vec<&str> = encrypted_dek.split(':').collect();
    if parts.len() != 2 {
        return Err(Error::other("invalid managed DEK format"));
    }

    let nonce_vec = BASE64_STANDARD
        .decode(parts[0])
        .map_err(|_| Error::other("invalid managed DEK nonce"))?;
    let ciphertext = BASE64_STANDARD
        .decode(parts[1])
        .map_err(|_| Error::other("invalid managed DEK ciphertext"))?;

    let nonce_array: [u8; 12] = nonce_vec
        .as_slice()
        .try_into()
        .map_err(|_| Error::other("invalid managed DEK nonce length"))?;

    let key = Key::<Aes256Gcm>::from(local_sse_master_key()?);
    let cipher = Aes256Gcm::new(&key);
    let plaintext = cipher
        .decrypt(&Nonce::from(nonce_array), ciphertext.as_slice())
        .map_err(|e| Error::other(format!("failed to decrypt managed DEK: {e}")))?;

    plaintext
        .as_slice()
        .try_into()
        .map_err(|_| Error::other("managed DEK has invalid plaintext length"))
}

#[cfg(feature = "rio-v2")]
#[derive(Deserialize)]
struct MinioLegacyCiphertext {
    #[serde(rename = "aead")]
    algorithm: String,
    iv: Vec<u8>,
    nonce: Vec<u8>,
    bytes: Vec<u8>,
}

#[cfg(feature = "rio-v2")]
fn decrypt_minio_secret_key_dek(encrypted_dek: &[u8], object_context: &ObjectEncryptionContext) -> Result<[u8; 32]> {
    let key = local_sse_master_key()?;
    let (ciphertext, iv, nonce) = parse_minio_secret_key_ciphertext(encrypted_dek)?;
    let associated_data = marshal_minio_kms_context(&object_context.encryption_context);

    let mut mac = HmacSha256::new_from_slice(&key).map_err(|err| Error::other(format!("invalid local SSE master key: {err}")))?;
    mac.update(&iv);
    let sealing_key = mac.finalize().into_bytes();
    let cipher = Aes256Gcm::new_from_slice(sealing_key.as_slice())
        .map_err(|err| Error::other(format!("invalid MinIO sealing key: {err}")))?;
    let nonce = Nonce::try_from(&nonce[..]).map_err(|_| Error::other("invalid MinIO managed DEK nonce"))?;
    let plaintext = cipher
        .decrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: &ciphertext,
                aad: &associated_data,
            },
        )
        .map_err(|err| Error::other(format!("failed to decrypt MinIO managed DEK: {err}")))?;

    plaintext
        .as_slice()
        .try_into()
        .map_err(|_| Error::other("MinIO managed DEK has invalid plaintext length"))
}

#[cfg(feature = "rio-v2")]
fn parse_minio_secret_key_ciphertext(
    encrypted_dek: &[u8],
) -> Result<(Vec<u8>, [u8; MINIO_SECRET_KEY_IV_SIZE], [u8; MINIO_SECRET_KEY_NONCE_SIZE])> {
    if encrypted_dek.first() == Some(&b'{') && encrypted_dek.last() == Some(&b'}') {
        let legacy: MinioLegacyCiphertext = serde_json::from_slice(encrypted_dek)
            .map_err(|err| Error::other(format!("failed to parse MinIO legacy managed DEK: {err}")))?;
        if legacy.algorithm != "AES-256-GCM-HMAC-SHA-256" {
            return Err(Error::other(format!(
                "unsupported MinIO legacy managed DEK algorithm {}",
                legacy.algorithm
            )));
        }
        let iv = legacy
            .iv
            .as_slice()
            .try_into()
            .map_err(|_| Error::other("invalid MinIO legacy managed DEK IV length"))?;
        let nonce = legacy
            .nonce
            .as_slice()
            .try_into()
            .map_err(|_| Error::other("invalid MinIO legacy managed DEK nonce length"))?;
        return Ok((legacy.bytes, iv, nonce));
    }

    if encrypted_dek.len() <= MINIO_SECRET_KEY_RANDOM_SIZE {
        return Err(Error::other("invalid MinIO managed DEK length"));
    }

    let split_at = encrypted_dek.len() - MINIO_SECRET_KEY_RANDOM_SIZE;
    let (ciphertext, random) = encrypted_dek.split_at(split_at);
    let iv = random[..MINIO_SECRET_KEY_IV_SIZE]
        .try_into()
        .map_err(|_| Error::other("invalid MinIO managed DEK IV length"))?;
    let nonce = random[MINIO_SECRET_KEY_IV_SIZE..]
        .try_into()
        .map_err(|_| Error::other("invalid MinIO managed DEK nonce length"))?;
    Ok((ciphertext.to_vec(), iv, nonce))
}

#[cfg(feature = "rio-v2")]
fn marshal_minio_kms_context(context: &HashMap<String, String>) -> Vec<u8> {
    let mut entries: Vec<_> = context.iter().collect();
    entries.sort_by_key(|(left, _)| *left);

    let mut json = String::from("{");
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&serde_json::to_string(key).expect("string key serializes"));
        json.push(':');
        json.push_str(&serde_json::to_string(value).expect("string value serializes"));
    }
    json.push('}');
    json.into_bytes()
}

fn local_sse_master_key() -> Result<[u8; 32]> {
    if let Some(key) = decode_master_key_env("__RUSTFS_SSE_SIMPLE_CMK")? {
        return Ok(key);
    }

    if let Some(key) = decode_master_key_env("RUSTFS_SSE_S3_MASTER_KEY")? {
        return Ok(key);
    }

    Ok([0u8; 32])
}

fn decode_master_key_env(name: &str) -> Result<Option<[u8; 32]>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };

    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    let decoded = BASE64_STANDARD
        .decode(value)
        .map_err(|e| Error::other(format!("{name} is not valid base64: {e}")))?;
    let key =
        <[u8; 32]>::try_from(decoded.as_slice()).map_err(|_| Error::other(format!("{name} must decode to exactly 32 bytes")))?;

    Ok(Some(key))
}

fn generate_ssec_nonce(bucket: &str, key: &str) -> [u8; 12] {
    let digest = md5_bytes(format!("{bucket}-{key}").as_bytes());
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&digest[..12]);
    nonce
}

fn md5_bytes(data: impl AsRef<[u8]>) -> [u8; 16] {
    let digest = Md5::digest(data.as_ref());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use md5::{Digest, Md5};
    use std::io::Cursor;
    use temp_env::async_with_vars;
    use tokio::io::AsyncReadExt;

    fn md5_bytes(data: impl AsRef<[u8]>) -> [u8; 16] {
        let digest = Md5::digest(data.as_ref());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest);
        bytes
    }

    fn ssec_headers_from_key(key_bytes: [u8; 32]) -> HeaderMap<HeaderValue> {
        let mut headers = HeaderMap::new();
        headers.insert(rustfs_utils::http::SSEC_ALGORITHM_HEADER, HeaderValue::from_static("AES256"));
        headers.insert(
            rustfs_utils::http::SSEC_KEY_HEADER,
            HeaderValue::from_str(&BASE64_STANDARD.encode(key_bytes)).expect("valid base64 header"),
        );
        headers.insert(
            rustfs_utils::http::SSEC_KEY_MD5_HEADER,
            HeaderValue::from_str(&BASE64_STANDARD.encode(md5_bytes(key_bytes))).expect("valid md5 header"),
        );
        headers
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_legacy_managed_metadata_excludes_sealed_keys() {
        let legacy_metadata = HashMap::from([
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "encrypted-dek".to_string()),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "nonce".to_string()),
        ]);
        assert!(is_legacy_rustfs_managed_metadata(&legacy_metadata));

        let sealed_metadata = HashMap::from([
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "encrypted-dek".to_string()),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "nonce".to_string()),
            (MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(), "sealed-key".to_string()),
        ]);

        assert!(!is_legacy_rustfs_managed_metadata(&sealed_metadata));
    }

    #[cfg(feature = "rio-v2")]
    fn seal_ssec_object_key_for_test(
        bucket: &str,
        object: &str,
        customer_key: [u8; 32],
        object_key: [u8; 32],
    ) -> ([u8; 32], Vec<u8>) {
        let iv = [0x23u8; SEALED_KEY_IV_SIZE];
        let sealing_key = derive_sealing_key(customer_key, iv, "SSE-C", bucket, object);
        let cipher = Aes256Gcm::new_from_slice(&sealing_key).expect("valid sealing key");

        let mut header = [0u8; DARE_HEADER_SIZE];
        header[0] = DARE_VERSION_20;
        header[1] = DARE_CIPHER_AES_256_GCM;
        header[2..4].copy_from_slice(&31u16.to_le_bytes());
        header[4] = 0x80;
        header[5..16].copy_from_slice(&[0x45u8; 11]);

        let nonce = Nonce::try_from(&header[4..16]).expect("valid nonce");
        let mut sealed = header.to_vec();
        sealed.extend_from_slice(
            &cipher
                .encrypt(
                    &nonce,
                    aes_gcm::aead::Payload {
                        msg: &object_key,
                        aad: &header[..4],
                    },
                )
                .expect("seal object key"),
        );
        (iv, sealed)
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader() {
        // Create test data
        let original_data = b"Hello, World! This is a test for range requests on compressed data.";

        // For this test, we'll simulate using the original data directly as "decompressed"
        let cursor = Cursor::new(original_data.to_vec());

        // Test reading a range from the middle
        let mut ranged_reader = RangedDecompressReader::new(cursor, 7, 5, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "World" (5 bytes starting from position 7)
        assert_eq!(result, b"World");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_from_start() {
        let original_data = b"Hello, World! This is a test.";
        let cursor = Cursor::new(original_data.to_vec());

        let mut ranged_reader = RangedDecompressReader::new(cursor, 0, 5, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "Hello" (5 bytes from the start)
        assert_eq!(result, b"Hello");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_to_end() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());

        let mut ranged_reader = RangedDecompressReader::new(cursor, 7, 6, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "World!" (6 bytes starting from position 7)
        assert_eq!(result, b"World!");
    }

    #[tokio::test]
    async fn test_http_range_spec_with_compressed_data() {
        // Test that HTTPRangeSpec::get_offset_length works correctly
        let range_spec = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 14, // inclusive
        };

        let total_size = 100i64;
        let (offset, length) = range_spec.get_offset_length(total_size).unwrap();

        assert_eq!(offset, 5);
        assert_eq!(length, 10); // end - start + 1 = 14 - 5 + 1 = 10
    }

    #[test]
    fn test_http_range_spec_suffix_positive_start() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: 5,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 15);
        assert_eq!(length, 5);
    }

    #[test]
    fn test_http_range_spec_suffix_negative_start() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: -5,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 15);
        assert_eq!(length, 5);
    }

    #[test]
    fn test_http_range_spec_suffix_exceeds_object() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: 50,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 20);
    }

    #[test]
    fn test_http_range_spec_from_object_info_valid_and_invalid_parts() {
        let object_info = ObjectInfo {
            size: 300,
            parts: Arc::new(vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let spec = http_range_spec_from_object_info(&object_info, 2).unwrap();
        assert_eq!(spec.start, 100);
        assert_eq!(spec.end, 199);

        assert!(http_range_spec_from_object_info(&object_info, 0).is_none());
        assert!(http_range_spec_from_object_info(&object_info, 4).is_none());
    }

    #[test]
    fn test_http_range_spec_from_object_info_uses_actual_size() {
        let object_info = ObjectInfo {
            size: 90,
            parts: Arc::new(vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 20,
                    actual_size: 30,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 30,
                    actual_size: 40,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 40,
                    actual_size: 50,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let spec = http_range_spec_from_object_info(&object_info, 2).unwrap();
        assert_eq!(spec.start, 30);
        assert_eq!(spec.end, 69);
    }

    #[test]
    fn test_http_range_spec_from_object_info_falls_back_to_part_size_when_actual_size_missing() {
        let object_info = ObjectInfo {
            size: 90,
            parts: Arc::new(vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 20,
                    actual_size: 0,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 30,
                    actual_size: 40,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 40,
                    actual_size: 0,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let spec = http_range_spec_from_object_info(&object_info, 3).unwrap();
        assert_eq!(spec.start, 60);
        assert_eq!(spec.end, 99);
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_zero_length() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedDecompressReader::new(cursor, 5, 0, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        // Should read nothing
        assert_eq!(result, b"");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_skip_entire_data() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        // Skip to end of data with length 0 - this should read nothing
        let mut ranged_reader = RangedDecompressReader::new(cursor, original_data.len() - 1, 0, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, b"");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_out_of_bounds_offset() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        // Offset beyond EOF should return error in constructor
        let result = RangedDecompressReader::new(cursor, original_data.len() + 10, 5, original_data.len());
        assert!(result.is_err());
        // Use pattern matching to avoid requiring Debug on the error type
        if let Err(e) = result {
            assert!(e.to_string().contains("Range offset exceeds file size"));
        }
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_partial_read() {
        let original_data = b"abcdef";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedDecompressReader::new(cursor, 2, 3, original_data.len()).unwrap();
        let mut buf = [0u8; 2];
        let n = ranged_reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf, b"cd");
        let mut buf2 = [0u8; 2];
        let n2 = ranged_reader.read(&mut buf2).await.unwrap();
        assert_eq!(n2, 1);
        assert_eq!(&buf2[..1], b"e");
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_ranged_decompress_reader_with_rio_v2_s2_stream() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let mut compressed = Vec::new();
        crate::io_support::rio::CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
            .read_to_end(&mut compressed)
            .await
            .expect("compress plaintext into rio_v2 stream");

        let decompress_reader =
            crate::io_support::rio::DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut ranged_reader =
            RangedDecompressReader::new(decompress_reader, 5, 7, plaintext.len()).expect("create ranged reader");

        let mut actual = Vec::new();
        ranged_reader
            .read_to_end(&mut actual)
            .await
            .expect("read ranged decompressed plaintext");

        assert_eq!(actual, b"fghijkl");
    }

    fn encrypt_managed_dek_for_test(dek: [u8; 32], master_key: [u8; 32]) -> String {
        let key = Key::<Aes256Gcm>::from(master_key);
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::from([0u8; 12]);
        let ciphertext = cipher.encrypt(&nonce, dek.as_slice()).expect("encrypt managed dek");
        format!("{}:{}", BASE64_STANDARD.encode(nonce), BASE64_STANDARD.encode(ciphertext))
    }

    #[cfg(feature = "rio-v2")]
    fn seal_managed_s3_object_key_for_test(
        bucket: &str,
        object: &str,
        data_key: [u8; 32],
        object_key: [u8; 32],
    ) -> ([u8; 32], Vec<u8>) {
        seal_managed_s3_object_key_for_test_with_cipher(bucket, object, data_key, object_key, DARE_CIPHER_AES_256_GCM)
    }

    #[cfg(feature = "rio-v2")]
    fn seal_managed_s3_object_key_for_test_with_cipher(
        bucket: &str,
        object: &str,
        data_key: [u8; 32],
        object_key: [u8; 32],
        cipher_id: u8,
    ) -> ([u8; 32], Vec<u8>) {
        let iv = [0x24u8; SEALED_KEY_IV_SIZE];
        let sealing_key = derive_sealing_key(data_key, iv, "SSE-S3", bucket, object);

        let mut header = [0u8; DARE_HEADER_SIZE];
        header[0] = DARE_VERSION_20;
        header[1] = cipher_id;
        header[2..4].copy_from_slice(&31u16.to_le_bytes());
        header[4] = 0x80;
        header[5..16].copy_from_slice(&[0x46u8; 11]);

        let ciphertext = match cipher_id {
            DARE_CIPHER_AES_256_GCM => {
                let cipher = Aes256Gcm::new_from_slice(&sealing_key).expect("valid sealing key");
                let nonce = Nonce::try_from(&header[4..16]).expect("valid nonce");
                cipher
                    .encrypt(
                        &nonce,
                        Payload {
                            msg: &object_key,
                            aad: &header[..4],
                        },
                    )
                    .expect("seal managed object key")
            }
            DARE_CIPHER_CHACHA20_POLY1305 => {
                let cipher = ChaCha20Poly1305::new_from_slice(&sealing_key).expect("valid sealing key");
                let nonce = chacha20poly1305::Nonce::try_from(&header[4..16]).expect("valid nonce");
                cipher
                    .encrypt(
                        &nonce,
                        Payload {
                            msg: &object_key,
                            aad: &header[..4],
                        },
                    )
                    .expect("seal managed object key")
            }
            _ => panic!("unsupported test cipher"),
        };
        let mut sealed = header.to_vec();
        sealed.extend_from_slice(&ciphertext);
        (iv, sealed)
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_supported_sealed_object_key_cipher_accepts_current_minio_fixture_value() {
        assert!(is_supported_sealed_object_key_cipher(DARE_CIPHER_AES_256_GCM));
        assert!(is_supported_sealed_object_key_cipher(DARE_CIPHER_CHACHA20_POLY1305));
        assert!(!is_supported_sealed_object_key_cipher(0x02));
    }

    #[tokio::test]
    async fn resolve_managed_material_accepts_case_insensitive_metadata_keys() {
        async_with_vars([("__RUSTFS_SSE_SIMPLE_CMK", Some(BASE64_STANDARD.encode([0u8; 32])))], async {
            let data_key = [0x24; 32];
            let base_nonce = [0x14; 12];
            let encrypted_dek = encrypt_managed_dek_for_test(data_key, [0u8; 32]);
            let metadata = HashMap::from([
                ("X-Rustfs-Encryption-Key".to_string(), BASE64_STANDARD.encode(encrypted_dek.as_bytes())),
                ("X-Rustfs-Encryption-IV".to_string(), BASE64_STANDARD.encode(base_nonce)),
            ]);

            let material = resolve_managed_material("", "", &metadata)
                .await
                .expect("managed material should resolve mixed-case metadata keys");

            assert_eq!(material.key_bytes, data_key);
            assert_eq!(material.base_nonce, base_nonce);
        })
        .await;
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn resolve_managed_material_accepts_chacha20_poly1305_header_variant() {
        async_with_vars([("__RUSTFS_SSE_SIMPLE_CMK", Some(BASE64_STANDARD.encode([0u8; 32])))], async {
            let data_key = [0x24; 32];
            let object_key = [0x33; 32];
            let (iv, sealed_key) = seal_managed_s3_object_key_for_test_with_cipher(
                "bucket",
                "object",
                data_key,
                object_key,
                DARE_CIPHER_CHACHA20_POLY1305,
            );

            let encrypted_dek = encrypt_managed_dek_for_test(data_key, [0u8; 32]);
            let metadata = HashMap::from([
                (
                    MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode(sealed_key),
                ),
                (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(iv)),
                (
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                ),
                (
                    MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode(encrypted_dek.as_bytes()),
                ),
                (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "default".to_string()),
            ]);

            let material = resolve_managed_material("bucket", "object", &metadata)
                .await
                .expect("managed material should accept current MinIO header variant");
            assert_eq!(material.key_kind, EncryptionKeyKind::Object);
            assert_eq!(material.key_bytes, object_key);
        })
        .await;
    }

    #[tokio::test]
    async fn resolve_encryption_material_accepts_case_insensitive_metadata_keys() {
        async_with_vars([("__RUSTFS_SSE_SIMPLE_CMK", Some(BASE64_STANDARD.encode([0u8; 32])))], async {
            let data_key = [0x24; 32];
            let base_nonce = [0x14; 12];
            let encrypted_dek = encrypt_managed_dek_for_test(data_key, [0u8; 32]);
            let metadata = HashMap::from([
                ("X-Rustfs-Encryption-Key".to_string(), BASE64_STANDARD.encode(encrypted_dek.as_bytes())),
                ("X-Rustfs-Encryption-IV".to_string(), BASE64_STANDARD.encode(base_nonce)),
            ]);
            let object_info = ObjectInfo {
                user_defined: Arc::new(metadata),
                ..Default::default()
            };
            let material = resolve_encryption_material(&object_info, &HeaderMap::new())
                .await
                .expect("resolve_encryption_material should accept mixed-case managed metadata");

            assert_eq!(material.key_bytes, data_key);
            assert_eq!(material.base_nonce, base_nonce);
        })
        .await;
    }

    #[tokio::test]
    async fn test_get_object_reader_rejects_ssec_read_without_headers() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                ("x-amz-server-side-encryption-customer-original-size".to_string(), "20".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 8,
            end: -1,
        };

        let result = GetObjectReader::new(
            Box::new(Cursor::new(b"0123456789".to_vec())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_object_reader_restore_request_bypasses_encryption_range_rewrite() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: Arc::new(HashMap::from([
                ("x-rustfs-encryption-key".to_string(), "encrypted-key".to_string()),
                ("x-rustfs-encryption-original-size".to_string(), "20".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: true,
            start: 4,
            end: -1,
        };

        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);

        let (_, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(b"0123456789".to_vec())),
            Some(range),
            &object_info,
            &opts,
            &HeaderMap::new(),
        )
        .await
        .unwrap();

        assert_eq!(offset, 6);
        assert_eq!(length, 4);
    }

    #[tokio::test]
    async fn test_read_plan_restore_request_uses_plain_range() {
        let object_info = ObjectInfo {
            size: 10,
            user_defined: Arc::new(HashMap::from([
                ("x-rustfs-encryption-key".to_string(), "encrypted-key".to_string()),
                ("x-rustfs-encryption-original-size".to_string(), "20".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: true,
            start: 4,
            end: -1,
        };

        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);

        let plan = ReadPlan::build(Some(range), &object_info, &opts, &HeaderMap::new())
            .await
            .expect("restore requests should bypass rio transforms");

        assert_eq!(plan.storage_offset, 6);
        assert_eq!(plan.storage_length, 4);
        assert_eq!(plan.object_size, 10);
        assert!(matches!(
            plan.transform,
            ReadTransform::Plain {
                visible_offset: 6,
                visible_length: 4
            }
        ));
    }

    #[tokio::test]
    async fn test_raw_data_movement_read_plan_bypasses_compression_transform() {
        let object_info = ObjectInfo {
            size: 3_000_000,
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "klauspost/compress/s2".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };
        let opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };

        let plan = ReadPlan::build(None, &object_info, &opts, &HeaderMap::new())
            .await
            .expect("raw data movement read should bypass compression planning");

        assert_eq!(plan.storage_offset, 0);
        assert_eq!(plan.storage_length, object_info.size);
        assert_eq!(plan.object_size, object_info.size);
        assert!(matches!(
            plan.transform,
            ReadTransform::Plain {
                visible_offset: 0,
                visible_length: 3_000_000
            }
        ));
    }

    #[tokio::test]
    async fn test_raw_data_movement_read_plan_bypasses_encryption_transform() {
        let object_info = ObjectInfo {
            size: 128,
            user_defined: Arc::new(HashMap::from([
                ("X-Amz-Server-Side-Encryption".to_string(), "aws:kms".to_string()),
                ("X-Amz-Server-Side-Encryption-Iv".to_string(), "AAAAAAAAAAAAAAAA".to_string()),
                ("X-Amz-Server-Side-Encryption-Key".to_string(), BASE64_STANDARD.encode([7_u8; 32])),
                ("x-rustfs-encryption-original-size".to_string(), "64".to_string()),
            ])),
            ..Default::default()
        };
        let opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };

        let plan = ReadPlan::build(None, &object_info, &opts, &HeaderMap::new())
            .await
            .expect("raw data movement read should not require decryption material");

        assert_eq!(plan.storage_offset, 0);
        assert_eq!(plan.storage_length, object_info.size);
        assert_eq!(plan.object_size, object_info.size);
        assert!(matches!(
            plan.transform,
            ReadTransform::Plain {
                visible_offset: 0,
                visible_length: 128
            }
        ));
    }

    #[tokio::test]
    async fn test_raw_data_movement_read_plan_bypasses_ssec_header_resolution() {
        let object_info = ObjectInfo {
            size: 256,
            user_defined: Arc::new(HashMap::from([
                (SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
                (SSEC_KEY_MD5_HEADER.to_string(), "stored-key-md5".to_string()),
            ])),
            ..Default::default()
        };
        let opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };

        let plan = ReadPlan::build(None, &object_info, &opts, &HeaderMap::new())
            .await
            .expect("raw data movement read should not require SSE-C request headers");

        assert_eq!(plan.storage_offset, 0);
        assert_eq!(plan.storage_length, object_info.size);
        assert_eq!(plan.object_size, object_info.size);
        assert!(matches!(
            plan.transform,
            ReadTransform::Plain {
                visible_offset: 0,
                visible_length: 256
            }
        ));
    }

    #[tokio::test]
    async fn test_get_object_reader_allows_encrypted_full_object_passthrough() {
        async_with_vars([("__RUSTFS_SSE_SIMPLE_CMK", Some(BASE64_STANDARD.encode([0u8; 32])))], async {
            let plaintext = b"managed-full-object".to_vec();
            let data_key = [0x21; 32];
            let encrypted_dek = encrypt_managed_dek_for_test(data_key, [0u8; 32]);
            let bucket = "bucket";
            let object = "managed-full-object";

            let mut encrypted = Vec::new();
            #[cfg(feature = "rio-v2")]
            let user_defined = {
                let object_key = [0x41; 32];
                let (sealing_iv, sealed_key) = seal_managed_s3_object_key_for_test(bucket, object, data_key, object_key);
                crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
                    .read_to_end(&mut encrypted)
                    .await
                    .expect("encrypt managed object");
                HashMap::from([
                    ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
                    ("x-rustfs-encryption-key".to_string(), BASE64_STANDARD.encode(encrypted_dek.as_bytes())),
                    ("x-rustfs-encryption-original-size".to_string(), plaintext.len().to_string()),
                    (
                        MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                        MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                    ),
                    (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealing_iv)),
                    (
                        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                        BASE64_STANDARD.encode(sealed_key),
                    ),
                ])
            };
            #[cfg(not(feature = "rio-v2"))]
            let user_defined = {
                let base_nonce = [0x11; 12];
                crate::io_support::rio::EncryptReader::new(Cursor::new(plaintext.clone()), data_key, base_nonce)
                    .read_to_end(&mut encrypted)
                    .await
                    .expect("encrypt managed object");
                HashMap::from([
                    ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
                    ("x-rustfs-encryption-key".to_string(), BASE64_STANDARD.encode(encrypted_dek.as_bytes())),
                    ("x-rustfs-encryption-iv".to_string(), BASE64_STANDARD.encode(base_nonce)),
                    ("x-rustfs-encryption-original-size".to_string(), plaintext.len().to_string()),
                ])
            };

            let object_info = ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: encrypted.len() as i64,
                user_defined: Arc::new(user_defined),
                ..Default::default()
            };

            let (mut reader, offset, length) = GetObjectReader::new(
                Box::new(Cursor::new(encrypted.clone())),
                None,
                &object_info,
                &ObjectOptions::default(),
                &HeaderMap::new(),
            )
            .await
            .expect("managed encrypted full-object reads should decrypt inside ecstore");

            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.expect("read managed plaintext");

            assert_eq!(offset, 0);
            assert_eq!(length, object_info.size);
            assert_eq!(reader.object_info.size, plaintext.len() as i64);
            assert_eq!(actual, plaintext);
        })
        .await;
    }

    #[tokio::test]
    async fn test_get_object_reader_decrypts_managed_sse_range_on_plaintext_semantics() {
        async_with_vars([("__RUSTFS_SSE_SIMPLE_CMK", Some(BASE64_STANDARD.encode([0u8; 32])))], async {
            let plaintext = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
            let data_key = [0x23; 32];
            let encrypted_dek = encrypt_managed_dek_for_test(data_key, [0u8; 32]);
            let bucket = "bucket";
            let object = "managed-range-object";

            let mut encrypted = Vec::new();
            #[cfg(feature = "rio-v2")]
            let user_defined = {
                let object_key = [0x43; 32];
                let (sealing_iv, sealed_key) = seal_managed_s3_object_key_for_test(bucket, object, data_key, object_key);
                crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
                    .read_to_end(&mut encrypted)
                    .await
                    .expect("encrypt managed ranged object");
                HashMap::from([
                    ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
                    ("x-rustfs-encryption-key".to_string(), BASE64_STANDARD.encode(encrypted_dek.as_bytes())),
                    ("x-rustfs-encryption-original-size".to_string(), plaintext.len().to_string()),
                    (
                        MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                        MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                    ),
                    (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealing_iv)),
                    (
                        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                        BASE64_STANDARD.encode(sealed_key),
                    ),
                ])
            };
            #[cfg(not(feature = "rio-v2"))]
            let user_defined = {
                let base_nonce = [0x13; 12];
                crate::io_support::rio::EncryptReader::new(Cursor::new(plaintext.clone()), data_key, base_nonce)
                    .read_to_end(&mut encrypted)
                    .await
                    .expect("encrypt managed ranged object");
                HashMap::from([
                    ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
                    ("x-rustfs-encryption-key".to_string(), BASE64_STANDARD.encode(encrypted_dek.as_bytes())),
                    ("x-rustfs-encryption-iv".to_string(), BASE64_STANDARD.encode(base_nonce)),
                    ("x-rustfs-encryption-original-size".to_string(), plaintext.len().to_string()),
                ])
            };

            let object_info = ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: encrypted.len() as i64,
                user_defined: Arc::new(user_defined),
                ..Default::default()
            };
            let range = HTTPRangeSpec {
                is_suffix_length: false,
                start: 5,
                end: 11,
            };

            let (mut reader, offset, length) = GetObjectReader::new(
                Box::new(Cursor::new(encrypted.clone())),
                Some(range),
                &object_info,
                &ObjectOptions::default(),
                &HeaderMap::new(),
            )
            .await
            .expect("managed encrypted range reads should decrypt inside ecstore");

            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.expect("read managed ranged plaintext");

            assert_eq!(offset, 0);
            assert_eq!(length, encrypted.len() as i64);
            assert_eq!(reader.object_info.size, 7);
            assert_eq!(actual, b"56789ab");
        })
        .await;
    }

    #[tokio::test]
    async fn test_get_object_reader_uses_local_managed_fallback_with_explicit_sse_s3_key() {
        async_with_vars(
            [
                ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
                ("RUSTFS_SSE_S3_MASTER_KEY", Some(BASE64_STANDARD.encode([0u8; 32]))),
            ],
            async {
                let plaintext = b"managed-local-fallback".to_vec();
                let data_key = [0x22; 32];
                let encrypted_dek = encrypt_managed_dek_for_test(data_key, [0u8; 32]);
                let bucket = "bucket";
                let object = "managed-local-fallback";

                let mut encrypted = Vec::new();
                #[cfg(feature = "rio-v2")]
                let user_defined = {
                    let object_key = [0x42; 32];
                    let (sealing_iv, sealed_key) = seal_managed_s3_object_key_for_test(bucket, object, data_key, object_key);
                    crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
                        .read_to_end(&mut encrypted)
                        .await
                        .expect("encrypt managed object with local fallback key");
                    HashMap::from([
                        ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
                        ("x-rustfs-encryption-key".to_string(), BASE64_STANDARD.encode(encrypted_dek.as_bytes())),
                        ("x-rustfs-encryption-original-size".to_string(), plaintext.len().to_string()),
                        (
                            MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                            MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                        ),
                        (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealing_iv)),
                        (
                            MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                            BASE64_STANDARD.encode(sealed_key),
                        ),
                    ])
                };
                #[cfg(not(feature = "rio-v2"))]
                let user_defined = {
                    let base_nonce = [0x12; 12];
                    crate::io_support::rio::EncryptReader::new(Cursor::new(plaintext.clone()), data_key, base_nonce)
                        .read_to_end(&mut encrypted)
                        .await
                        .expect("encrypt managed object with local fallback key");
                    HashMap::from([
                        ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
                        ("x-rustfs-encryption-key".to_string(), BASE64_STANDARD.encode(encrypted_dek.as_bytes())),
                        ("x-rustfs-encryption-iv".to_string(), BASE64_STANDARD.encode(base_nonce)),
                        ("x-rustfs-encryption-original-size".to_string(), plaintext.len().to_string()),
                    ])
                };

                let object_info = ObjectInfo {
                    bucket: bucket.to_string(),
                    name: object.to_string(),
                    size: encrypted.len() as i64,
                    user_defined: Arc::new(user_defined),
                    ..Default::default()
                };

                let (mut reader, _, _) = GetObjectReader::new(
                    Box::new(Cursor::new(encrypted)),
                    None,
                    &object_info,
                    &ObjectOptions::default(),
                    &HeaderMap::new(),
                )
                .await
                .expect("managed encrypted reads should use the configured local SSE-S3 key");

                let mut actual = Vec::new();
                reader.read_to_end(&mut actual).await.expect("read managed plaintext");

                assert_eq!(reader.object_info.size, plaintext.len() as i64);
                assert_eq!(actual, plaintext);
            },
        )
        .await;
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_get_object_reader_accepts_minio_only_managed_metadata() {
        async_with_vars([("__RUSTFS_SSE_SIMPLE_CMK", Some(BASE64_STANDARD.encode([0u8; 32])))], async {
            let plaintext = b"managed-minio-metadata".to_vec();
            let data_key = [0x23; 32];
            let encrypted_dek = encrypt_managed_dek_for_test(data_key, [0u8; 32]);
            let bucket = "bucket";
            let object = "managed-minio-metadata";
            let object_key = [0x44; 32];
            let (sealing_iv, sealed_key) = seal_managed_s3_object_key_for_test(bucket, object, data_key, object_key);

            let mut encrypted = Vec::new();
            crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
                .read_to_end(&mut encrypted)
                .await
                .expect("encrypt managed object");

            let object_info = ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: encrypted.len() as i64,
                user_defined: Arc::new(HashMap::from([
                    ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
                    (
                        MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(),
                        BASE64_STANDARD.encode(encrypted_dek.as_bytes()),
                    ),
                    (
                        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                        BASE64_STANDARD.encode(sealed_key),
                    ),
                    (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealing_iv)),
                    (
                        MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                        MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                    ),
                    (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "default".to_string()),
                    ("x-minio-internal-actual-size".to_string(), plaintext.len().to_string()),
                ])),
                ..Default::default()
            };

            let (mut reader, offset, length) = GetObjectReader::new(
                Box::new(Cursor::new(encrypted.clone())),
                None,
                &object_info,
                &ObjectOptions::default(),
                &HeaderMap::new(),
            )
            .await
            .expect("managed encrypted reads should accept MinIO-style metadata");

            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.expect("read managed plaintext");

            assert_eq!(offset, 0);
            assert_eq!(length, object_info.size);
            assert_eq!(reader.object_info.size, plaintext.len() as i64);
            assert_eq!(actual, plaintext);
        })
        .await;
    }

    #[tokio::test]
    async fn test_get_object_reader_compressed_range_returns_physical_offset_from_index() {
        let mut index = crate::io_support::rio::Index::new();
        index.add(0, 0).unwrap();
        index.add(1_048_576, 2_097_152).unwrap();

        let object_info = ObjectInfo {
            size: 3_000_000,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 3_000_000,
                actual_size: 4_194_304,
                index: Some(index.into_vec()),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "gzip".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let decoded = crate::io_support::rio::decode_compression_index_bytes(object_info.parts[0].index.as_ref().unwrap())
            .expect("headerless MinIO-style compression index should decode");
        let (compressed_offset, uncompressed_offset) = decoded.find(2_097_152).expect("seek into decoded index");
        assert!(compressed_offset > 0);
        assert_eq!(uncompressed_offset, 2_097_152);
        let (physical_offset, decompressed_skip, _, _, _) = get_compressed_offsets(&object_info, 2_097_152);
        assert!(physical_offset > 0);
        assert_eq!(decompressed_skip, 0);

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 2_097_152,
            end: 2_097_161,
        };

        let (reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(Vec::<u8>::new())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &HeaderMap::new(),
        )
        .await
        .unwrap();

        assert!(offset > 0);
        assert!(offset < 2_097_152);
        assert_eq!(length, object_info.size - offset as i64);
        assert_eq!(reader.object_info.size, 10);
    }

    #[tokio::test]
    async fn test_read_plan_compressed_range_tracks_storage_and_visible_offsets() {
        let mut index = crate::io_support::rio::Index::new();
        index.add(0, 0).unwrap();
        index.add(1_048_576, 2_097_152).unwrap();

        let object_info = ObjectInfo {
            size: 3_000_000,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 3_000_000,
                actual_size: 4_194_304,
                index: Some(index.into_vec()),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "gzip".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 2_097_152,
            end: 2_097_161,
        };

        let plan = ReadPlan::build(Some(range), &object_info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("compressed range should plan physical offset and visible range");

        assert!(plan.storage_offset > 0);
        assert!(plan.storage_offset < 2_097_152);
        assert_eq!(plan.storage_length, object_info.size - plan.storage_offset as i64);
        assert_eq!(plan.object_size, 10);

        assert!(matches!(
            plan.transform,
            ReadTransform::Compressed {
                decompressed_offset,
                decompressed_length: 10,
                ..
            } if decompressed_offset < 2_097_152
        ));
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_read_plan_accepts_minio_s2_compression_scheme() {
        let object_info = ObjectInfo {
            size: 3_000_000,
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "klauspost/compress/s2".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let plan = ReadPlan::build(None, &object_info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("MinIO S2 compression scheme should be accepted");

        assert!(matches!(
            plan.transform,
            ReadTransform::Compressed {
                decompressed_offset: 0,
                decompressed_length: 4_194_304,
                ..
            }
        ));
        assert_eq!(plan.object_size, 4_194_304);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_read_plan_accepts_minio_headerless_compression_index() {
        let mut index = crate::io_support::rio::Index::new();
        index.add(0, 0).unwrap();
        index.add(1_048_576, 2_097_152).unwrap();
        let headerless_index = crate::io_support::rio::compression_index_storage_bytes(&index);
        assert!(
            !headerless_index.starts_with(&[0x50, 0x2A, 0x4D, 0x18]),
            "rio_v2 should store MinIO-style headerless compression indexes"
        );

        let object_info = ObjectInfo {
            size: 3_000_000,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 3_000_000,
                actual_size: 4_194_304,
                index: Some(headerless_index),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-minio-internal-compression".to_string(), "klauspost/compress/s2".to_string()),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 2_097_152,
            end: 2_097_161,
        };

        let decoded = crate::io_support::rio::decode_compression_index_bytes(object_info.parts[0].index.as_ref().unwrap())
            .expect("headerless MinIO-style compression index should decode");
        let (compressed_offset, uncompressed_offset) = decoded.find(range.start).expect("seek into decoded index");
        assert!(compressed_offset > 0);
        assert_eq!(uncompressed_offset, range.start);

        let (physical_offset, decompressed_skip, _, _, _) = get_compressed_offsets(&object_info, range.start);
        assert!(physical_offset > 0);
        assert_eq!(decompressed_skip, 0);

        let plan = ReadPlan::build(Some(range), &object_info, &ObjectOptions::default(), &HeaderMap::new())
            .await
            .expect("MinIO headerless compression index should be decoded");

        assert_eq!(plan.storage_offset as i64, physical_offset);
        assert_eq!(plan.storage_length, object_info.size - plan.storage_offset as i64);
        assert_eq!(plan.object_size, 10);
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_get_compressed_offsets_aligns_encrypted_ranges_to_dare_packages() {
        let mut index = crate::io_support::rio::Index::new();
        index.add(0, 0).unwrap();
        index.add(200_000, 2_097_152).unwrap();
        let stored_index = crate::io_support::rio::compression_index_storage_bytes(&index);
        let expected_comp_off = crate::io_support::rio::decode_compression_index_bytes(&stored_index)
            .expect("decode stored index")
            .find(2_097_152)
            .expect("find offset in stored index")
            .0;

        let object_info = ObjectInfo {
            size: 400_000,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: 400_000,
                actual_size: 4_194_304,
                index: Some(stored_index),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                ("x-amz-server-side-encryption-customer-original-size".to_string(), "4194304".to_string()),
                (
                    "x-minio-internal-compression".to_string(),
                    crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
                ),
                ("x-minio-internal-actual-size".to_string(), "4194304".to_string()),
            ])),
            ..Default::default()
        };

        let (physical_offset, decompressed_skip, first_part_idx, decrypt_skip, seq_num) =
            get_compressed_offsets(&object_info, 2_097_152);

        assert_eq!(first_part_idx, 0);
        assert_eq!(physical_offset, (expected_comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE);
        assert_eq!(decompressed_skip, 0);
        assert_eq!(decrypt_skip, expected_comp_off % DARE_PAYLOAD_SIZE);
        assert_eq!(seq_num, (expected_comp_off / DARE_PAYLOAD_SIZE) as u64);
    }

    #[tokio::test]
    async fn test_get_object_reader_decrypts_ssec_full_object() {
        let plaintext = b"ecstore-ssec-full-object".to_vec();
        let key_bytes = [0x31; 32];
        let bucket = "bucket";
        let object = "object";
        let nonce = md5_bytes(format!("{bucket}-{object}").as_bytes());
        let mut base_nonce = [0u8; 12];
        base_nonce.copy_from_slice(&nonce[..12]);

        let mut encrypted = Vec::new();
        rustfs_rio::EncryptReader::new(Cursor::new(plaintext.clone()), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt object");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(key_bytes)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
            ])),
            ..Default::default()
        };

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            None,
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(key_bytes),
        )
        .await
        .expect("ssec read should be supported");

        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read decrypted ssec object");

        assert_eq!(offset, 0);
        assert_eq!(length, encrypted.len() as i64);
        assert_eq!(reader.object_info.size, plaintext.len() as i64);
        assert_eq!(actual, plaintext);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_get_object_reader_decrypts_ssec_sealed_object_key_full_object() {
        let plaintext = b"ecstore-rio-v2-ssec-sealed-object-key".repeat(4096);
        let customer_key = [0x31; 32];
        let object_key = [0x67; 32];
        let bucket = "bucket";
        let object = "sealed-object";
        let (sealing_iv, sealed_key) = seal_ssec_object_key_for_test(bucket, object, customer_key, object_key);

        let mut encrypted = Vec::new();
        crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt object with rio-v2 object key");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(customer_key)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
                (
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                ),
                (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealing_iv)),
                (
                    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode(sealed_key),
                ),
            ])),
            ..Default::default()
        };

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            None,
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(customer_key),
        )
        .await
        .expect("rio-v2 ssec sealed-object-key read should be supported");

        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .expect("read decrypted rio-v2 ssec object");

        assert_eq!(offset, 0);
        assert_eq!(length, encrypted.len() as i64);
        assert_eq!(reader.object_info.size, plaintext.len() as i64);
        assert_eq!(actual, plaintext);
    }

    #[tokio::test]
    async fn test_get_object_reader_decrypts_ssec_range_on_plaintext_semantics() {
        let plaintext = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
        let key_bytes = [0x41; 32];
        let bucket = "bucket";
        let object = "range-object";
        let nonce = md5_bytes(format!("{bucket}-{object}").as_bytes());
        let mut base_nonce = [0u8; 12];
        base_nonce.copy_from_slice(&nonce[..12]);

        let mut encrypted = Vec::new();
        rustfs_rio::EncryptReader::new(Cursor::new(plaintext.clone()), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt ranged object");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(key_bytes)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
            ])),
            ..Default::default()
        };
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 11,
        };

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(key_bytes),
        )
        .await
        .expect("ssec range read should be supported");

        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read ranged decrypted object");

        assert_eq!(offset, 0);
        assert_eq!(length, encrypted.len() as i64);
        assert_eq!(reader.object_info.size, 7);
        assert_eq!(actual, b"56789ab");
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_get_object_reader_uses_dare_package_offset_for_large_ssec_ranges() {
        const DARE_PACKAGE_SIZE: usize = 64 * 1024 + 32;

        let plaintext = vec![0x7Bu8; 2 * 64 * 1024 + 97];
        let customer_key = [0x61; 32];
        let object_key = [0x68; 32];
        let bucket = "bucket";
        let object = "large-range-object";
        let (sealing_iv, sealed_key) = seal_ssec_object_key_for_test(bucket, object, customer_key, object_key);

        let mut encrypted = Vec::new();
        crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(plaintext.clone()), object_key)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt large ranged object");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(customer_key)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
                (
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                ),
                (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealing_iv)),
                (
                    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode(sealed_key),
                ),
            ])),
            ..Default::default()
        };
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 70_000,
            end: 70_063,
        };

        let (mut reader, offset, _length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted[DARE_PACKAGE_SIZE..].to_vec())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(customer_key),
        )
        .await
        .expect("large ssec range read should be supported");

        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("read ranged decrypted object");

        assert_eq!(
            offset, DARE_PACKAGE_SIZE,
            "rio_v2 encrypted ranges should start from the second DARE package"
        );
        assert_eq!(reader.object_info.size, 64);
        assert_eq!(actual, plaintext[70_000..70_064]);
    }

    #[tokio::test]
    async fn test_get_object_reader_decrypts_then_decompresses_before_applying_range() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let key_bytes = [0x51; 32];
        let bucket = "bucket";
        let object = "compressed-object";
        let nonce = md5_bytes(format!("{bucket}-{object}").as_bytes());
        let mut base_nonce = [0u8; 12];
        base_nonce.copy_from_slice(&nonce[..12]);

        let mut compressed = Vec::new();
        rustfs_rio::CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
            .read_to_end(&mut compressed)
            .await
            .expect("compress plaintext");

        let mut encrypted = Vec::new();
        rustfs_rio::EncryptReader::new(Cursor::new(compressed.clone()), key_bytes, base_nonce)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt compressed plaintext");

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(key_bytes)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
                ("x-minio-internal-compression".to_string(), CompressionAlgorithm::default().to_string()),
                ("x-minio-internal-actual-size".to_string(), plaintext.len().to_string()),
            ])),
            ..Default::default()
        };
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 11,
        };

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted.clone())),
            Some(range),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(key_bytes),
        )
        .await
        .expect("encrypted+compressed range read should be supported");

        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .expect("read ranged decompressed plaintext");

        assert_eq!(offset, 0);
        assert_eq!(length, encrypted.len() as i64);
        assert_eq!(reader.object_info.size, 7);
        assert_eq!(actual, b"fghijkl");
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_get_object_reader_uses_compression_index_for_encrypted_ranges() {
        use crate::io_support::rio::TryGetIndex;

        let plaintext: Vec<u8> = (0..(10 * 1024 * 1024 + 123_456))
            .map(|i| (((i as u64).wrapping_mul(1_103_515_245).wrapping_add(12_345) >> 16) & 0xFF) as u8)
            .collect();
        let customer_key = [0x73; 32];
        let object_key = [0x74; 32];
        let bucket = "bucket";
        let object = "compressed-large-object";
        let (sealing_iv, sealed_key) = seal_ssec_object_key_for_test(bucket, object, customer_key, object_key);
        let mut compressor = crate::io_support::rio::CompressReader::with_encrypted_padding(
            Cursor::new(plaintext.clone()),
            CompressionAlgorithm::default(),
        );
        let mut compressed = Vec::new();
        compressor
            .read_to_end(&mut compressed)
            .await
            .expect("compress large plaintext");

        let index = compressor
            .try_get_index()
            .cloned()
            .expect("large rio_v2 encrypted+compressed object should expose a compression index");
        let stored_index = crate::io_support::rio::compression_index_storage_bytes(&index);
        let decoded_index = crate::io_support::rio::decode_compression_index_bytes(&stored_index)
            .expect("decode stored encrypted compression index");

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5 * 1024 * 1024,
            end: 5 * 1024 * 1024 + 63,
        };
        let original_offsets = index
            .find(range.start)
            .expect("find large-range compression block in original index");
        let (comp_off, uncomp_off) = decoded_index
            .find(range.start)
            .expect("find large-range compression block in stored index");
        assert_eq!(original_offsets, (comp_off, uncomp_off));
        assert!(comp_off > DARE_PAYLOAD_SIZE);
        assert!(uncomp_off <= range.start);

        let expected_storage_offset = ((comp_off / DARE_PAYLOAD_SIZE) * DARE_PACKAGE_SIZE) as usize;
        let expected_decrypt_skip = comp_off % DARE_PAYLOAD_SIZE;
        assert!(expected_storage_offset > 0);
        assert!(expected_decrypt_skip >= 0);

        let mut encrypted = Vec::new();
        crate::io_support::rio::EncryptReader::new_with_object_key(Cursor::new(compressed.clone()), object_key)
            .read_to_end(&mut encrypted)
            .await
            .expect("encrypt compressed plaintext");

        let expected_sequence_number = u32::try_from(comp_off / DARE_PAYLOAD_SIZE).expect("sequence number fits in u32");
        let chunk_offset = comp_off as usize;
        let chunk_type = compressed[chunk_offset];
        let chunk_len = (compressed[chunk_offset + 1] as usize)
            | ((compressed[chunk_offset + 2] as usize) << 8)
            | ((compressed[chunk_offset + 3] as usize) << 16);
        assert!(matches!(chunk_type, 0x00 | 0x01 | 0xff | 0xfe));
        assert!(chunk_offset + 4 + chunk_len <= compressed.len());

        let mut decrypted_tail = Vec::new();
        crate::io_support::rio::DecryptReader::new_with_object_key_and_sequence(
            Cursor::new(encrypted[expected_storage_offset..].to_vec()),
            object_key,
            expected_sequence_number,
        )
        .read_to_end(&mut decrypted_tail)
        .await
        .expect("decrypt package-aligned ciphertext tail");
        assert_eq!(
            &decrypted_tail[expected_decrypt_skip as usize..],
            &compressed[comp_off as usize..],
            "package-aligned decryption plus decrypt_skip must land on the indexed S2 chunk boundary"
        );

        let mut direct_reader = crate::io_support::rio::DecompressReader::new(
            Cursor::new(decrypted_tail[expected_decrypt_skip as usize..].to_vec()),
            CompressionAlgorithm::default(),
        );
        let mut direct_plaintext = Vec::new();
        direct_reader
            .read_to_end(&mut direct_plaintext)
            .await
            .expect("decompress tail starting at indexed S2 chunk boundary");
        assert_eq!(direct_plaintext, plaintext[uncomp_off as usize..]);

        let object_info = ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            size: encrypted.len() as i64,
            parts: Arc::new(vec![ObjectPartInfo {
                etag: String::new(),
                number: 1,
                size: encrypted.len(),
                actual_size: plaintext.len() as i64,
                index: Some(stored_index),
                ..Default::default()
            }]),
            user_defined: Arc::new(HashMap::from([
                ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
                (
                    "x-amz-server-side-encryption-customer-key-md5".to_string(),
                    BASE64_STANDARD.encode(md5_bytes(customer_key)),
                ),
                (
                    "x-amz-server-side-encryption-customer-original-size".to_string(),
                    plaintext.len().to_string(),
                ),
                (
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                ),
                (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealing_iv)),
                (
                    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode(sealed_key),
                ),
                (
                    "x-minio-internal-compression".to_string(),
                    crate::io_support::rio::compression_metadata_value(CompressionAlgorithm::default()),
                ),
                ("x-minio-internal-actual-size".to_string(), plaintext.len().to_string()),
            ])),
            ..Default::default()
        };

        let plan = ReadPlan::build(
            Some(range.clone()),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(customer_key),
        )
        .await
        .expect("build encrypted+compressed read plan");
        assert_eq!(plan.storage_offset, expected_storage_offset);
        assert_eq!(plan.storage_length, encrypted.len() as i64 - expected_storage_offset as i64);
        assert_eq!(plan.object_size, 64);
        match plan.transform {
            ReadTransform::Encrypted {
                sequence_number,
                decrypt_skip,
                plaintext_offset,
                plaintext_length,
                ..
            } => {
                assert_eq!(sequence_number, expected_sequence_number);
                assert_eq!(decrypt_skip, expected_decrypt_skip as usize);
                assert_eq!(plaintext_offset as i64, range.start - uncomp_off);
                assert_eq!(plaintext_length, 64);
            }
            other => panic!("expected encrypted read plan, got {other:?}"),
        }

        let (mut reader, offset, length) = GetObjectReader::new(
            Box::new(Cursor::new(encrypted[expected_storage_offset..].to_vec())),
            Some(range.clone()),
            &object_info,
            &ObjectOptions::default(),
            &ssec_headers_from_key(customer_key),
        )
        .await
        .expect("encrypted+compressed indexed range read should be supported");

        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .expect("read indexed encrypted+compressed range");

        assert_eq!(offset, expected_storage_offset);
        assert_eq!(length, encrypted.len() as i64 - expected_storage_offset as i64);
        assert_eq!(reader.object_info.size, 64);
        assert_eq!(actual, plaintext[range.start as usize..range.start as usize + 64]);
    }
}
