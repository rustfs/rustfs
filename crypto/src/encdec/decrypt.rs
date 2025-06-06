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

// use argon2::{Argon2, PasswordHasher};
// use argon2::password_hash::{SaltString};
// use aes_gcm::{Aes256Gcm, Key, Nonce}; // For AES-GCM
// use chacha20poly1305::{ChaCha20Poly1305, Key as ChaChaKey, Nonce as ChaChaNonce}; // For ChaCha20
// use pbkdf2::pbkdf2;
// use sha2::Sha256;
// use std::io::{self, Read};
// use thiserror::Error;

// #[derive(Debug, Error)]
// pub enum DecryptError {
//     #[error("unexpected header")]
//     UnexpectedHeader,
//     #[error("invalid encryption algorithm ID")]
//     InvalidAlgorithmId,
//     #[error("IO error")]
//     Io(#[from] io::Error),
//     #[error("decryption error")]
//     DecryptionError,
// }

// pub fn decrypt_data2<R: Read>(password: &str, mut data: R) -> Result<Vec<u8>, DecryptError> {
//     // Parse the stream header
//     let mut hdr = [0u8; 32 + 1 + 8];
//     if data.read_exact(&mut hdr).is_err() {
//         return Err(DecryptError::UnexpectedHeader);
//     }

//     let salt = &hdr[0..32];
//     let id = hdr[32];
//     let nonce = &hdr[33..41];

//     let key = match id {
//         // Argon2id + AES-GCM
//         0x01 => {
//             let salt = SaltString::encode_b64(salt).map_err(|_| DecryptError::DecryptionError)?;
//             let argon2 = Argon2::default();
//             let hashed_key = argon2.hash_password(password.as_bytes(), &salt)
//                 .map_err(|_| DecryptError::DecryptionError)?;
//             hashed_key.hash.unwrap().as_bytes().to_vec()
//         }
//         // Argon2id + ChaCha20Poly1305
//         0x02 => {
//             let salt = SaltString::encode_b64(salt).map_err(|_| DecryptError::DecryptionError)?;
//             let argon2 = Argon2::default();
//             let hashed_key = argon2.hash_password(password.as_bytes(), &salt)
//                 .map_err(|_| DecryptError::DecryptionError)?;
//             hashed_key.hash.unwrap().as_bytes().to_vec()
//         }
//         // PBKDF2 + AES-GCM
//         // 0x03 => {
//         //     let mut key = [0u8; 32];
//         //     pbkdf2::<Sha256>(password.as_bytes(), salt, 10000, &mut key);
//         //     key.to_vec()
//         // }
//         _ => return Err(DecryptError::InvalidAlgorithmId),
//     };

//     // Decrypt data using the corresponding cipher
//     let mut encrypted_data = Vec::new();
//     data.read_to_end(&mut encrypted_data)?;

//     let plaintext = match id {
//         0x01 => {
//             let cipher = Aes256Gcm::new(Key::from_slice(&key));
//             let nonce = Nonce::from_slice(nonce);
//             cipher
//                 .decrypt(nonce, encrypted_data.as_ref())
//                 .map_err(|_| DecryptError::DecryptionError)?
//         }
//         0x02 => {
//             let cipher = ChaCha20Poly1305::new(ChaChaKey::from_slice(&key));
//             let nonce = ChaChaNonce::from_slice(nonce);
//             cipher
//                 .decrypt(nonce, encrypted_data.as_ref())
//                 .map_err(|_| DecryptError::DecryptionError)?
//         }
//         0x03 => {
            
//             let cipher = Aes256Gcm::new(Key::from_slice(&key));
//             let nonce = Nonce::from_slice(nonce);
//             cipher
//                 .decrypt(nonce, encrypted_data.as_ref())
//                 .map_err(|_| DecryptError::DecryptionError)?
//         }
//         _ => return Err(DecryptError::InvalidAlgorithmId),
//     };

//     Ok(plaintext)
// }

#[cfg(any(test, feature = "crypto"))]
#[inline]
fn decryp<T: aes_gcm::aead::Aead>(stream: T, nonce: &[u8], data: &[u8]) -> Result<Vec<u8>, crate::Error> {
    use crate::error::Error;
    stream
        .decrypt(aes_gcm::Nonce::from_slice(nonce), data)
        .map_err(Error::ErrDecryptFailed)
}

#[cfg(not(any(test, feature = "crypto")))]
pub fn decrypt_data(_password: &[u8], data: &[u8]) -> Result<Vec<u8>, crate::Error> {
    Ok(data.to_vec())
}
