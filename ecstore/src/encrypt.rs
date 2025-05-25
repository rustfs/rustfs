// encrypt.rs - 对象存储加密处理层
// 处理与对象存储加密相关的操作

use crate::store_api::{ObjectInfo, ObjectOptions};
use common::error::{Error, Result};
use http::HeaderMap;
use md5::Digest;
use ring::aead::{Aad, BoundKey, Nonce, NonceSequence, OpeningKey, SealingKey, CHACHA20_POLY1305};
use ring::digest::{digest, SHA256};
use ring::rand::{SecureRandom, SystemRandom};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

// 加密算法常量
pub const ENC_AES_256_GCM: &str = "AES256";
pub const ENC_CHACHA20_POLY1305: &str = "ChaCha20-Poly1305";

// 加密请求头常量
pub const HEADER_SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
pub const HEADER_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
pub const HEADER_SSE_C_KEY_MD5: &str = "x-amz-server-side-encryption-customer-key-MD5";
pub const HEADER_SSE_S3_ALGORITHM: &str = "x-amz-server-side-encryption";
pub const HEADER_SSE_S3_KEY_ID: &str = "x-amz-server-side-encryption-aws-kms-key-id";
pub const HEADER_SSE_KMS_KEY_ID: &str = "x-amz-server-side-encryption-kms-key-id";

// 加密元数据常量
pub const META_CRYPTO_IV: &str = "X-Amz-Meta-Crypto-Iv";
pub const META_CRYPTO_KEY: &str = "X-Amz-Meta-Crypto-Key";
pub const META_CRYPTO_KEY_ID: &str = "X-Amz-Meta-Crypto-Key-Id";
pub const META_CRYPTO_ALGORITHM: &str = "X-Amz-Meta-Crypto-Algorithm";
pub const META_CRYPTO_CONTEXT: &str = "X-Amz-Meta-Crypto-Context";
pub const META_UNENCRYPTED_CONTENT_LENGTH: &str = "X-Amz-Meta-Unencrypted-Content-Length";
pub const META_OBJECT_ENCRYPTION: &str = "X-Amz-Meta-Object-Encryption";

// 加密模式
#[derive(Debug, Clone, PartialEq)]
pub enum EncryptionMode {
    SSEC,  // SSE-C: 客户提供的密钥
    SSES3, // SSE-S3: 服务端托管密钥
    SSEKMS, // SSE-KMS: KMS托管密钥
}

/// 对象加密配置选项
#[derive(Debug, Clone)]
pub struct EncryptionOptions {
    pub mode: EncryptionMode,
    pub algorithm: String,
    pub customer_key: Option<Vec<u8>>,
    pub kms_key_id: Option<String>,
}

/// NonceSequence实现，用于AEAD加密
struct NonceGen {
    nonce: [u8; 12],
}

impl NonceGen {
    fn new(iv: &[u8]) -> Self {
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&iv[..12]);
        Self { nonce }
    }
}

impl NonceSequence for NonceGen {
    fn advance(&mut self) -> Result<Nonce, ring::error::Unspecified> {
        Ok(Nonce::assume_unique_for_key(self.nonce))
    }
}

/// 对象加密实现
pub struct ObjectEncryption;

impl ObjectEncryption {
    /// 从HTTP请求头中检测加密模式
    pub fn detect_encryption_mode(headers: &HeaderMap) -> Result<Option<EncryptionOptions>> {
        // 检测SSE-C
        if headers.contains_key(HEADER_SSE_C_ALGORITHM) {
            let algorithm = headers
                .get(HEADER_SSE_C_ALGORITHM)
                .ok_or_else(|| Error::msg("缺少SSE-C算法"))?
                .to_str()
                .map_err(|_| Error::msg("无效的SSE-C算法"))?;
            
            let key = headers
                .get(HEADER_SSE_C_KEY)
                .ok_or_else(|| Error::msg("缺少SSE-C密钥"))?
                .to_str()
                .map_err(|_| Error::msg("无效的SSE-C密钥"))?;
            
            // 将Base64编码的密钥转换为字节
            let key_bytes = BASE64_STANDARD.decode(key).map_err(|_| Error::msg("无效的SSE-C密钥格式"))?;
            
            // 验证MD5校验和
            if headers.contains_key(HEADER_SSE_C_KEY_MD5) {
                let key_md5 = headers
                    .get(HEADER_SSE_C_KEY_MD5)
                    .unwrap()
                    .to_str()
                    .map_err(|_| Error::msg("无效的SSE-C密钥MD5"))?;
                
                let computed_md5 = Self::compute_md5(&key_bytes);
                if key_md5 != computed_md5 {
                    return Err(Error::msg("SSE-C密钥MD5校验失败"));
                }
            }
            
            return Ok(Some(EncryptionOptions {
                mode: EncryptionMode::SSEC,
                algorithm: algorithm.to_string(),
                customer_key: Some(key_bytes),
                kms_key_id: None,
            }));
        }
        
        // 检测SSE-S3
        if headers.contains_key(HEADER_SSE_S3_ALGORITHM) {
            let algorithm = headers
                .get(HEADER_SSE_S3_ALGORITHM)
                .unwrap()
                .to_str()
                .map_err(|_| Error::msg("无效的SSE-S3算法"))?;
            
            if algorithm != "AES256" {
                return Err(Error::msg("不支持的SSE-S3加密算法"));
            }
            
            return Ok(Some(EncryptionOptions {
                mode: EncryptionMode::SSES3,
                algorithm: algorithm.to_string(),
                customer_key: None,
                kms_key_id: None,
            }));
        }
        
        // 检测SSE-KMS
        if headers.contains_key(HEADER_SSE_KMS_KEY_ID) {
            let key_id = headers
                .get(HEADER_SSE_KMS_KEY_ID)
                .unwrap()
                .to_str()
                .map_err(|_| Error::msg("无效的KMS密钥ID"))?
                .to_string();
            
            return Ok(Some(EncryptionOptions {
                mode: EncryptionMode::SSEKMS,
                algorithm: ENC_AES_256_GCM.to_string(),
                customer_key: None,
                kms_key_id: Some(key_id),
            }));
        }
        
        // 没有启用加密
        Ok(None)
    }

    /// 计算MD5摘要并返回Base64编码的字符串
    fn compute_md5(data: &[u8]) -> String {
        let mut hasher = md5::Md5::new();
        hasher.update(data);
        let digest = hasher.finalize();
        BASE64_STANDARD.encode::<&[u8]>(digest.as_slice())
    }

    /// 检查对象是否需要解密
    pub fn needs_decryption(info: &ObjectInfo) -> bool {
        if let Some(user_defined) = &info.user_defined {
            user_defined.contains_key(META_CRYPTO_ALGORITHM) ||
            user_defined.contains_key(META_OBJECT_ENCRYPTION)
        } else {
            false
        }
    }

    /// 加密对象数据
    pub async fn encrypt_object_data(
        data: &[u8],
        options: &EncryptionOptions,
    ) -> Result<(Vec<u8>, HashMap<String, String>)> {
        let mut metadata = HashMap::new();
        
        match options.mode {
            EncryptionMode::SSEC => {
                // 获取客户提供的密钥
                let customer_key = options.customer_key
                    .as_ref()
                    .ok_or_else(|| Error::msg("缺少SSE-C客户密钥"))?;
                
                // 生成随机IV
                let rng = SystemRandom::new();
                let mut iv = [0u8; 12]; // ChaCha20-Poly1305使用12字节的Nonce
                rng.fill(&mut iv).map_err(|_| Error::msg("无法生成随机IV"))?;
                
                // 使用ChaCha20-Poly1305加密
                let sealing_key = ring::aead::UnboundKey::new(&CHACHA20_POLY1305, customer_key)
                    .map_err(|_| Error::msg("无法创建加密密钥"))?;
                let mut sealing_key = SealingKey::new(sealing_key, NonceGen::new(&iv));
                
                // 准备输出缓冲区
                let mut in_out = data.to_vec();
                let tag_len = ring::aead::CHACHA20_POLY1305.tag_len();
                in_out.extend(vec![0u8; tag_len]);
                
                // 加密
                let aad = Aad::empty(); // 可以根据需要添加额外的认证数据
                sealing_key.seal_in_place_append_tag(aad, &mut in_out)
                    .map_err(|_| Error::msg("数据加密失败"))?;
                
                // 设置加密元数据
                metadata.insert(META_CRYPTO_IV.to_string(), BASE64_STANDARD.encode(&iv));
                metadata.insert(META_CRYPTO_ALGORITHM.to_string(), ENC_CHACHA20_POLY1305.to_string());
                metadata.insert(META_UNENCRYPTED_CONTENT_LENGTH.to_string(), data.len().to_string());
                metadata.insert(META_OBJECT_ENCRYPTION.to_string(), "SSE-C".to_string());
                
                Ok((in_out, metadata))
            },
            EncryptionMode::SSES3 => {
                // 生成随机数据密钥
                let rng = SystemRandom::new();
                let mut data_key = [0u8; 32];
                rng.fill(&mut data_key).map_err(|_| Error::msg("无法生成随机数据密钥"))?;
                
                // 生成随机IV
                let mut iv = [0u8; 12];
                rng.fill(&mut iv).map_err(|_| Error::msg("无法生成随机IV"))?;
                
                // 使用数据密钥和ChaCha20-Poly1305加密数据
                let sealing_key = ring::aead::UnboundKey::new(&CHACHA20_POLY1305, &data_key)
                    .map_err(|_| Error::msg("无法创建加密密钥"))?;
                let mut sealing_key = SealingKey::new(sealing_key, NonceGen::new(&iv));
                
                // 准备输出缓冲区
                let mut in_out = data.to_vec();
                let tag_len = ring::aead::CHACHA20_POLY1305.tag_len();
                in_out.extend(vec![0u8; tag_len]);
                
                // 加密
                let aad = Aad::empty();
                sealing_key.seal_in_place_append_tag(aad, &mut in_out)
                    .map_err(|_| Error::msg("数据加密失败"))?;
                
                // 使用SSE-S3主密钥加密数据密钥
                // 在实际实现中，这里应该调用密钥管理服务
                // 这里简化处理，直接将数据密钥放入元数据
                
                metadata.insert(META_CRYPTO_IV.to_string(), BASE64_STANDARD.encode(&iv));
                metadata.insert(META_CRYPTO_KEY.to_string(), BASE64_STANDARD.encode(&data_key));
                metadata.insert(META_CRYPTO_ALGORITHM.to_string(), ENC_AES_256_GCM.to_string());
                metadata.insert(META_UNENCRYPTED_CONTENT_LENGTH.to_string(), data.len().to_string());
                metadata.insert(META_OBJECT_ENCRYPTION.to_string(), "SSE-S3".to_string());
                
                Ok((in_out, metadata))
            },
            EncryptionMode::SSEKMS => {
                // 获取KMS密钥ID
                let key_id = options.kms_key_id
                    .as_ref()
                    .ok_or_else(|| Error::msg("缺少SSE-KMS密钥ID"))?;
                
                // 生成随机数据密钥
                let rng = SystemRandom::new();
                let mut data_key = [0u8; 32];
                rng.fill(&mut data_key).map_err(|_| Error::msg("无法生成随机数据密钥"))?;
                
                // 生成随机IV
                let mut iv = [0u8; 12];
                rng.fill(&mut iv).map_err(|_| Error::msg("无法生成随机IV"))?;
                
                // 使用数据密钥和ChaCha20-Poly1305加密数据
                let sealing_key = ring::aead::UnboundKey::new(&CHACHA20_POLY1305, &data_key)
                    .map_err(|_| Error::msg("无法创建加密密钥"))?;
                let mut sealing_key = SealingKey::new(sealing_key, NonceGen::new(&iv));
                
                // 准备输出缓冲区
                let mut in_out = data.to_vec();
                let tag_len = ring::aead::CHACHA20_POLY1305.tag_len();
                in_out.extend(vec![0u8; tag_len]);
                
                // 加密
                let aad = Aad::empty();
                sealing_key.seal_in_place_append_tag(aad, &mut in_out)
                    .map_err(|_| Error::msg("数据加密失败"))?;
                
                // 在实际实现中，这里应该调用KMS加密数据密钥
                // 这里简化处理，直接将数据密钥放入元数据
                
                metadata.insert(META_CRYPTO_IV.to_string(), BASE64_STANDARD.encode(&iv));
                metadata.insert(META_CRYPTO_KEY.to_string(), BASE64_STANDARD.encode(&data_key));
                metadata.insert(META_CRYPTO_KEY_ID.to_string(), key_id.clone());
                metadata.insert(META_CRYPTO_ALGORITHM.to_string(), ENC_AES_256_GCM.to_string());
                metadata.insert(META_UNENCRYPTED_CONTENT_LENGTH.to_string(), data.len().to_string());
                metadata.insert(META_OBJECT_ENCRYPTION.to_string(), "SSE-KMS".to_string());
                
                Ok((in_out, metadata))
            },
        }
    }

    /// 解密对象数据
    pub async fn decrypt_object_data(
        encrypted_data: &[u8],
        metadata: &HashMap<String, String>,
        customer_key: Option<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        // 确定加密模式
        let encryption_mode = metadata.get(META_OBJECT_ENCRYPTION)
            .ok_or_else(|| Error::msg("缺少加密模式元数据"))?;
        
        // 获取IV
        let iv_base64 = metadata.get(META_CRYPTO_IV)
            .ok_or_else(|| Error::msg("缺少IV元数据"))?;
        let iv = BASE64_STANDARD.decode(iv_base64).map_err(|_| Error::msg("无效的IV格式"))?;
        
        match encryption_mode.as_str() {
            "SSE-C" => {
                // 获取客户提供的密钥
                let key = customer_key
                    .ok_or_else(|| Error::msg("缺少SSE-C客户密钥"))?;
                
                // 使用ChaCha20-Poly1305解密
                let opening_key = ring::aead::UnboundKey::new(&CHACHA20_POLY1305, &key)
                    .map_err(|_| Error::msg("无法创建解密密钥"))?;
                let mut opening_key = OpeningKey::new(opening_key, NonceGen::new(&iv));
                
                // 解密
                let mut in_out = encrypted_data.to_vec();
                let aad = Aad::empty();
                let decrypted_data = opening_key.open_in_place(aad, &mut in_out)
                    .map_err(|_| Error::msg("数据解密失败"))?;
                
                Ok(decrypted_data.to_vec())
            },
            "SSE-S3" => {
                // 获取加密的数据密钥
                let key_base64 = metadata.get(META_CRYPTO_KEY)
                    .ok_or_else(|| Error::msg("缺少数据密钥元数据"))?;
                let data_key = BASE64_STANDARD.decode(key_base64).map_err(|_| Error::msg("无效的数据密钥格式"))?;
                
                // 使用数据密钥和ChaCha20-Poly1305解密
                let opening_key = ring::aead::UnboundKey::new(&CHACHA20_POLY1305, &data_key)
                    .map_err(|_| Error::msg("无法创建解密密钥"))?;
                let mut opening_key = OpeningKey::new(opening_key, NonceGen::new(&iv));
                
                // 解密
                let mut in_out = encrypted_data.to_vec();
                let aad = Aad::empty();
                let decrypted_data = opening_key.open_in_place(aad, &mut in_out)
                    .map_err(|_| Error::msg("数据解密失败"))?;
                
                Ok(decrypted_data.to_vec())
            },
            "SSE-KMS" => {
                // 获取加密的数据密钥
                let key_base64 = metadata.get(META_CRYPTO_KEY)
                    .ok_or_else(|| Error::msg("缺少数据密钥元数据"))?;
                let data_key = BASE64_STANDARD.decode(key_base64).map_err(|_| Error::msg("无效的数据密钥格式"))?;
                
                // 使用数据密钥和ChaCha20-Poly1305解密
                let opening_key = ring::aead::UnboundKey::new(&CHACHA20_POLY1305, &data_key)
                    .map_err(|_| Error::msg("无法创建解密密钥"))?;
                let mut opening_key = OpeningKey::new(opening_key, NonceGen::new(&iv));
                
                // 解密
                let mut in_out = encrypted_data.to_vec();
                let aad = Aad::empty();
                let decrypted_data = opening_key.open_in_place(aad, &mut in_out)
                    .map_err(|_| Error::msg("数据解密失败"))?;
                
                Ok(decrypted_data.to_vec())
            },
            _ => Err(Error::msg(format!("不支持的加密模式: {}", encryption_mode))),
        }
    }

    /// 将加密元数据添加到对象选项中
    pub fn add_encryption_metadata(opts: &mut ObjectOptions, metadata: HashMap<String, String>) {
        if opts.user_defined.is_none() {
            opts.user_defined = Some(metadata);
        } else if let Some(user_defined) = &mut opts.user_defined {
            user_defined.extend(metadata);
        }
    }
}