use std::io;

use aes_gcm::aead::{self, Aead, AeadCore, Payload, TagPosition};
use aes_gcm::{Aes256Gcm, Nonce};
use subtle::ConstantTimeEq;
use xkunlun::{
    KLError, KLAES_256, KLSymAttributes, KLSymCipher, KLSymCipherOperation, KLSymCipherPadding,
    KLSymCipherWorkMode,
};

pub struct XkunlunAes256Gcm {
    key: [u8; 32],
}

impl XkunlunAes256Gcm {
    pub fn new_from_key(key: &[u8; 32]) -> Self {
        Self { key: *key }
    }

    // 前置进明文的关联数据长度：88帧头||88块序号(u64le)
    pub const AAD_PREFIX_LEN: usize = 16;
    // 密文相对逻辑明文的固定增量:ADD 前缀 + GCM tag
    pub const CIPHERTEXT_OVERHEAD: usize = Self::AAD_PREFIX_LEN + 16;

    fn init(&self, nonce: &[u8; 12], op: KLSymCipherOperation) -> io::Result<KLAES_256> {
        let mut c = KLAES_256::new();
        let p = c.get_parameters();
        p.set_operation(op);
        p.set_work_mode(KLSymCipherWorkMode::GCM);
        p.set_padding_syntax(KLSymCipherPadding::None);
        p.set_tag_size(16);
        
        let km = c.get_key_material();
        km.set_secret_key(&self.key).map_err(to_io)?;
        km.set_init_vector(nonce).map_err(to_io)?;
        c.init_cipher().map_err(to_io)?;
        Ok(c)
    }

    fn seal(&self, nonce: &[u8; 12], aad: &[u8], msg: &[u8]) -> io::Result<Vec<u8>> {
        let mut c = self.init(nonce, KLSymCipherOperation::Encrypt)?;
        let mut prefixed = Vec::with_capacity(aad.len() + msg.len());
        prefixed.extend_from_slice(aad);
        prefixed.extend_from_slice(msg);

        let mut ct = vec![0u8; prefixed.len()];
        let n = c
            .update_cipher(&mut prefixed, &mut ct)
            .map_err(to_io)?;
        if n != prefixed.len() {
            return Err(io::Error::other("xkunlun short update"));
        }

        let tail_len = prefixed.len() % 16;
        let mut tail = vec![0u8; tail_len + 16];
        let m = c.final_cipher(&mut tail).map_err(to_io)?;
        ct.extend_from_slice(&tail[..m]);
        Ok(ct)
    }

    fn open(&self, nonce: &[u8; 12], aad: &[u8], ct_and_tag: &[u8]) -> io::Result<Vec<u8>> {
        let mut c = self.init(nonce, KLSymCipherOperation::Decrypt)?;
        c.update_cipher(ct_and_tag, &mut [])
            .map_err(to_io)?; // GCM 解密全部在 final 完成
        let pt_len = ct_and_tag
            .len()
            .checked_sub(16)
            .ok_or_else(|| io::Error::other("frame shorter than tag"))?;
        let mut pt = vec![0u8; pt_len];
        let n = c.final_cipher(&mut pt).map_err(to_io)?; // tag 不符 → KLError::InvalidData

        // 安全不变量：前缀必须与期望 AAD 一致（常量时间比较），否则 fail-closed
        let prefix = pt.get(..aad.len()).ok_or_else(|| io::Error::other("frame shorter than AAD prefix"))?;
        
        if !bool::from(prefix.ct_eq(aad)) {
            return Err(io::Error::other("frame AAD prefix mismatch"));
        }
        
        Ok(pt[aad.len()..n].to_vec())
    }
}

impl AeadCore for XkunlunAes256Gcm {
    type NonceSize = <Aes256Gcm as AeadCore>::NonceSize; // 12
    type TagSize = <Aes256Gcm as AeadCore>::TagSize; // 16
    const TAG_POSITION: TagPosition = TagPosition::Postfix;
}

impl Aead for XkunlunAes256Gcm {
    fn encrypt<'m, 'a>(
        &self,
        nonce: &Nonce<Self::NonceSize>,
        plaintext: impl Into<Payload<'m, 'a>>,
    ) -> aead::Result<Vec<u8>> {
        let p = plaintext.into();
        let n: &[u8; 12] = nonce.as_ref();
        self.seal(n, p.aad, p.msg).map_err(to_aead)
    }

    fn decrypt<'m, 'a>(
        &self,
        nonce: &Nonce<Self::NonceSize>,
        ciphertext: impl Into<Payload<'m, 'a>>,
    ) -> aead::Result<Vec<u8>> {
        let p = ciphertext.into();
        let n: &[u8; 12] = nonce.as_ref();
        self.open(n, p.aad, p.msg).map_err(to_aead)
    }
}

// ---------------------------------------------------------------------------
// Helper conversions
// ---------------------------------------------------------------------------

fn to_io(e: KLError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

fn to_aead(_e: io::Error) -> aead::Error {
    aead::Error
}

// (ct_eq helper removed; line 74 uses `prefix.ct_eq(aad)` via ConstantTimeEq directly.)
