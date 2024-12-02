#[cfg(any(test, feature = "crypto"))]
pub fn decrypt_data(password: &[u8], data: &[u8]) -> Result<Vec<u8>, crate::Error> {
    use crate::encdec::id::ID;
    use crate::error::Error;
    use aes_gcm::{Aes256Gcm, KeyInit as _};
    use chacha20poly1305::ChaCha20Poly1305;

    // 32: salt
    // 1: id
    // 12: nonce
    const HEADER_LENGTH: usize = 45;
    if data.len() < HEADER_LENGTH {
        return Err(Error::ErrUnexpectedHeader);
    }

    let (salt, id, nonce) = (&data[..32], ID::try_from(data[32])?, &data[33..45]);
    let data = &data[HEADER_LENGTH..];

    match id {
        ID::Argon2idChaCHa20Poly1305 => {
            let key = id.get_key(password, salt)?;
            decryp(ChaCha20Poly1305::new_from_slice(&key)?, nonce, data)
        }
        _ => {
            let key = id.get_key(password, salt)?;
            decryp(Aes256Gcm::new_from_slice(&key)?, nonce, data)
        }
    }
}

#[cfg(any(test, feature = "crypto"))]
#[inline]
fn decryp<T: aes_gcm::aead::Aead>(stream: T, nonce: &[u8], data: &[u8]) -> Result<Vec<u8>, crate::Error> {
    use crate::error::Error;
    stream
        .decrypt(aes_gcm::Nonce::from_slice(nonce), data)
        .map_err(Error::ErrDecryptFailed)
}

#[cfg(all(not(test), not(feature = "crypto")))]
pub fn decrypt_data(_password: &[u8], data: &[u8]) -> Result<Vec<u8>, crate::Error> {
    Ok(data.to_vec())
}
