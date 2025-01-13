use argon2::{Algorithm, Argon2, Params, Version};
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum ID {
    Argon2idAESGCM = 0x00,
    Argon2idChaCHa20Poly1305 = 0x01,
    Pbkdf2AESGCM = 0x02,
}

impl TryFrom<u8> for ID {
    type Error = crate::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Argon2idAESGCM),
            0x01 => Ok(Self::Argon2idChaCHa20Poly1305),
            0x02 => Ok(Self::Pbkdf2AESGCM),
            _ => Err(crate::Error::ErrInvalidAlgID(value)),
        }
    }
}

impl ID {
    pub(crate) fn get_key(&self, password: &[u8], salt: &[u8]) -> Result<[u8; 32], crate::Error> {
        let mut key = [0u8; 32];
        match self {
            ID::Pbkdf2AESGCM => pbkdf2_hmac::<Sha256>(password, salt, 8192, &mut key),
            _ => {
                let params = Params::new(64 * 1024, 1, 4, Some(32))?;
                let argon_2id = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
                let mut key = vec![0u8; 32];
                argon_2id.hash_password_into(password, salt, &mut key)?;
            }
        }

        Ok(key)
    }
}
