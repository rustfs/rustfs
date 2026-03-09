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

//! sio-go compatible stream encryption for IAM config.
//! Header: salt(32) + alg_id(1) + nonce_prefix(8) = 41 bytes.
//! Body: DARE-style fragmented AEAD (bufSize=16384, per-fragment nonce).

#![allow(deprecated)] // AeadInPlace deprecated in favor of AeadInOut; keep for aead 0.6 compatibility

use crate::encdec::id::ID;
use crate::error::Error;
use aes_gcm::{
    Aes256Gcm,
    aead::{AeadCore, AeadInPlace, KeyInit as _, array::Array},
};
use chacha20poly1305::ChaCha20Poly1305;

const STREAM_IO_HEADER_LEN: usize = 41;
const SIO_BUF_SIZE: usize = 16384;
const SIO_NONCE_PREFIX_LEN: usize = 8;
const AES_GCM_OVERHEAD: usize = 16;
const CHACHA_OVERHEAD: usize = 16;

/// Decrypt data in stream_io (sio-go) format.
pub fn decrypt_stream_io(password: &[u8], data: &[u8]) -> Result<Vec<u8>, Error> {
    if data.len() < STREAM_IO_HEADER_LEN {
        return Err(Error::ErrUnexpectedHeader);
    }
    let salt = &data[0..32];
    let id = ID::try_from(data[32])?;
    let nonce_prefix = &data[33..41];
    let body = &data[STREAM_IO_HEADER_LEN..];

    let key = id.get_key(password, salt)?;

    match id {
        ID::Argon2idChaCHa20Poly1305 => decrypt_stream(
            ChaCha20Poly1305::new_from_slice(&key).map_err(|e| Error::ErrInvalidInput(e.to_string()))?,
            nonce_prefix,
            body,
            CHACHA_OVERHEAD,
        ),
        _ => decrypt_stream(
            Aes256Gcm::new_from_slice(&key).map_err(|e| Error::ErrInvalidInput(e.to_string()))?,
            nonce_prefix,
            body,
            AES_GCM_OVERHEAD,
        ),
    }
}

fn decrypt_stream<A>(aead: A, nonce_prefix: &[u8], body: &[u8], overhead: usize) -> Result<Vec<u8>, Error>
where
    A: AeadInPlace,
{
    let ciphertext_len = SIO_BUF_SIZE + overhead;
    let ad = build_associated_data(&aead, nonce_prefix)?;
    let mut plain = Vec::with_capacity(body.len());

    let mut seq_num: u32 = 1;
    let mut pos = 0;

    while pos < body.len() {
        let remaining = body.len() - pos;
        let frag_len = remaining.min(ciphertext_len);
        let is_last = (pos + frag_len) == body.len();

        if frag_len < overhead {
            return Err(Error::ErrDecryptFailed(aes_gcm::aead::Error));
        }

        let mut nonce = [0u8; 12];
        nonce[0..SIO_NONCE_PREFIX_LEN].copy_from_slice(nonce_prefix);
        nonce[8..12].copy_from_slice(&seq_num.to_le_bytes());

        let mut ad_mut = ad.clone();
        ad_mut[0] = if is_last { 0x80 } else { 0x00 };

        let fragment = &body[pos..pos + frag_len];
        let tag_len = overhead;
        let (ct, tag) = fragment.split_at(frag_len - tag_len);

        let mut buffer = ct.to_vec();
        let nonce_arr = Array::<u8, <A as AeadCore>::NonceSize>::try_from(&nonce[..])
            .map_err(|_| Error::ErrDecryptFailed(aes_gcm::aead::Error))?;
        let tag_arr =
            Array::<u8, <A as AeadCore>::TagSize>::try_from(tag).map_err(|_| Error::ErrDecryptFailed(aes_gcm::aead::Error))?;
        aead.decrypt_in_place_detached(&nonce_arr, &ad_mut, &mut buffer, &tag_arr)
            .map_err(|_| Error::ErrDecryptFailed(aes_gcm::aead::Error))?;
        plain.extend_from_slice(&buffer);

        pos += frag_len;
        seq_num += 1;

        if is_last {
            break;
        }
    }

    Ok(plain)
}

fn build_associated_data<A>(aead: &A, nonce_prefix: &[u8]) -> Result<Vec<u8>, Error>
where
    A: AeadInPlace,
{
    let mut nonce = [0u8; 12];
    nonce[0..SIO_NONCE_PREFIX_LEN].copy_from_slice(nonce_prefix);
    nonce[8..12].copy_from_slice(&0u32.to_le_bytes());

    let nonce_arr = Array::<u8, <A as AeadCore>::NonceSize>::try_from(&nonce[..])
        .map_err(|_| Error::ErrEncryptFailed(aes_gcm::aead::Error))?;
    let mut empty: [u8; 0] = [];
    let tag = aead
        .encrypt_in_place_detached(&nonce_arr, &[] as &[u8], &mut empty)
        .map_err(Error::ErrEncryptFailed)?;

    let mut ad = vec![0u8; 1 + tag.len()];
    ad[0] = 0x00;
    ad[1..].copy_from_slice(tag.as_slice());
    Ok(ad)
}

/// Encrypt data in stream_io (sio-go) format.
pub fn encrypt_stream_io(password: &[u8], data: &[u8]) -> Result<Vec<u8>, Error> {
    let salt: [u8; 32] = rand::random();

    #[cfg(feature = "fips")]
    let id = ID::Pbkdf2AESGCM;

    #[cfg(not(feature = "fips"))]
    let id = if crate::encdec::encrypt::native_aes() {
        ID::Argon2idAESGCM
    } else {
        ID::Argon2idChaCHa20Poly1305
    };

    let key = id.get_key(password, &salt)?;
    let nonce_prefix: [u8; SIO_NONCE_PREFIX_LEN] = rand::random();

    let mut out = Vec::with_capacity(STREAM_IO_HEADER_LEN + data.len() + 32);
    out.extend_from_slice(&salt);
    out.push(id as u8);
    out.extend_from_slice(&nonce_prefix);

    match id {
        ID::Argon2idChaCHa20Poly1305 => encrypt_stream(
            ChaCha20Poly1305::new_from_slice(&key).map_err(|e| Error::ErrInvalidInput(e.to_string()))?,
            &nonce_prefix,
            data,
            &mut out,
            CHACHA_OVERHEAD,
        )?,
        _ => encrypt_stream(
            Aes256Gcm::new_from_slice(&key).map_err(|e| Error::ErrInvalidInput(e.to_string()))?,
            &nonce_prefix,
            data,
            &mut out,
            AES_GCM_OVERHEAD,
        )?,
    }

    Ok(out)
}

fn encrypt_stream<A>(
    aead: A,
    nonce_prefix: &[u8; SIO_NONCE_PREFIX_LEN],
    data: &[u8],
    out: &mut Vec<u8>,
    _overhead: usize,
) -> Result<(), Error>
where
    A: AeadInPlace,
{
    let ad = build_associated_data(&aead, nonce_prefix)?;
    let mut seq_num: u32 = 1;
    let mut pos = 0;

    while pos < data.len() {
        let remaining = data.len() - pos;
        let is_last = remaining <= SIO_BUF_SIZE;

        let chunk_len = if is_last { remaining } else { SIO_BUF_SIZE };
        let chunk = &data[pos..pos + chunk_len];

        let mut nonce = [0u8; 12];
        nonce[0..SIO_NONCE_PREFIX_LEN].copy_from_slice(nonce_prefix);
        nonce[8..12].copy_from_slice(&seq_num.to_le_bytes());

        let mut ad_mut = ad.clone();
        ad_mut[0] = if is_last { 0x80 } else { 0x00 };

        let mut buffer = chunk.to_vec();
        let nonce_arr = Array::<u8, <A as AeadCore>::NonceSize>::try_from(&nonce[..])
            .map_err(|_| Error::ErrEncryptFailed(aes_gcm::aead::Error))?;
        let tag = aead
            .encrypt_in_place_detached(&nonce_arr, &ad_mut, &mut buffer)
            .map_err(Error::ErrEncryptFailed)?;
        out.extend_from_slice(&buffer);
        out.extend_from_slice(tag.as_slice());

        pos += chunk_len;
        seq_num += 1;
    }

    Ok(())
}
