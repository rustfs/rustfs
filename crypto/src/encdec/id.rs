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
                argon_2id.hash_password_into(password, salt, &mut key)?;
            }
        }

        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_enum_values() {
        // Test enum discriminant values
        assert_eq!(ID::Argon2idAESGCM as u8, 0x00);
        assert_eq!(ID::Argon2idChaCHa20Poly1305 as u8, 0x01);
        assert_eq!(ID::Pbkdf2AESGCM as u8, 0x02);
    }

    #[test]
    fn test_id_try_from_valid_values() {
        // Test valid conversions from u8 to ID
        assert!(matches!(ID::try_from(0x00), Ok(ID::Argon2idAESGCM)));
        assert!(matches!(ID::try_from(0x01), Ok(ID::Argon2idChaCHa20Poly1305)));
        assert!(matches!(ID::try_from(0x02), Ok(ID::Pbkdf2AESGCM)));
    }

    #[test]
    fn test_id_try_from_invalid_values() {
        // Test invalid conversions from u8 to ID
        assert!(ID::try_from(0x03).is_err());
        assert!(ID::try_from(0xFF).is_err());
        assert!(ID::try_from(100).is_err());

        // Verify error type
        if let Err(crate::Error::ErrInvalidAlgID(value)) = ID::try_from(0x03) {
            assert_eq!(value, 0x03);
        } else {
            panic!("Expected ErrInvalidAlgID error");
        }
    }

    #[test]
    fn test_id_debug_format() {
        // Test Debug trait implementation
        let argon2_aes = ID::Argon2idAESGCM;
        let argon2_chacha = ID::Argon2idChaCHa20Poly1305;
        let pbkdf2 = ID::Pbkdf2AESGCM;

        assert_eq!(format!("{:?}", argon2_aes), "Argon2idAESGCM");
        assert_eq!(format!("{:?}", argon2_chacha), "Argon2idChaCHa20Poly1305");
        assert_eq!(format!("{:?}", pbkdf2), "Pbkdf2AESGCM");
    }

    #[test]
    fn test_id_clone_and_copy() {
        // Test Clone and Copy traits
        let original = ID::Argon2idAESGCM;
        let cloned = original.clone();
        let copied = original;

        assert!(matches!(cloned, ID::Argon2idAESGCM));
        assert!(matches!(copied, ID::Argon2idAESGCM));
    }

    #[test]
    fn test_pbkdf2_key_generation() {
        // Test PBKDF2 key generation
        let id = ID::Pbkdf2AESGCM;
        let password = b"test_password";
        let salt = b"test_salt_16bytes";

        let result = id.get_key(password, salt);
        assert!(result.is_ok());

        let key = result.unwrap();
        assert_eq!(key.len(), 32);

        // Verify deterministic behavior - same inputs should produce same output
        let result2 = id.get_key(password, salt);
        assert!(result2.is_ok());
        assert_eq!(key, result2.unwrap());
    }

    #[test]
    fn test_argon2_key_generation() {
        // Test Argon2id key generation
        let id = ID::Argon2idAESGCM;
        let password = b"test_password";
        let salt = b"test_salt_16bytes";

        let result = id.get_key(password, salt);
        assert!(result.is_ok());

        let key = result.unwrap();
        assert_eq!(key.len(), 32);

        // Verify deterministic behavior
        let result2 = id.get_key(password, salt);
        assert!(result2.is_ok());
        assert_eq!(key, result2.unwrap());
    }

    #[test]
    fn test_argon2_chacha_key_generation() {
        // Test Argon2id ChaCha20Poly1305 key generation
        let id = ID::Argon2idChaCHa20Poly1305;
        let password = b"test_password";
        let salt = b"test_salt_16bytes";

        let result = id.get_key(password, salt);
        assert!(result.is_ok());

        let key = result.unwrap();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn test_key_generation_with_different_passwords() {
        // Test that different passwords produce different keys
        let id = ID::Pbkdf2AESGCM;
        let salt = b"same_salt_for_all";

        let key1 = id.get_key(b"password1", salt).unwrap();
        let key2 = id.get_key(b"password2", salt).unwrap();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_key_generation_with_different_salts() {
        // Test that different salts produce different keys
        let id = ID::Pbkdf2AESGCM;
        let password = b"same_password";

        let key1 = id.get_key(password, b"salt1_16_bytes__").unwrap();
        let key2 = id.get_key(password, b"salt2_16_bytes__").unwrap();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_key_generation_with_empty_inputs() {
        // Test key generation with empty password and salt
        let id = ID::Pbkdf2AESGCM;

        let result1 = id.get_key(b"", b"salt");
        assert!(result1.is_ok());

        let result2 = id.get_key(b"password", b"");
        assert!(result2.is_ok());

        let result3 = id.get_key(b"", b"");
        assert!(result3.is_ok());
    }

    #[test]
    fn test_all_algorithms_produce_valid_keys() {
        // Test that all algorithm variants can generate valid keys
        let algorithms = [
            ID::Argon2idAESGCM,
            ID::Argon2idChaCHa20Poly1305,
            ID::Pbkdf2AESGCM,
        ];

        let password = b"test_password_123";
        let salt = b"test_salt_16bytes";

        for algorithm in &algorithms {
            let result = algorithm.get_key(password, salt);
            assert!(result.is_ok(), "Algorithm {:?} should generate valid key", algorithm);

            let key = result.unwrap();
            assert_eq!(key.len(), 32, "Key length should be 32 bytes for {:?}", algorithm);

            // Verify key is not all zeros (very unlikely with proper implementation)
            assert_ne!(key, [0u8; 32], "Key should not be all zeros for {:?}", algorithm);
        }
    }

    #[test]
    fn test_round_trip_conversion() {
        // Test round-trip conversion: ID -> u8 -> ID
        let original_ids = [
            ID::Argon2idAESGCM,
            ID::Argon2idChaCHa20Poly1305,
            ID::Pbkdf2AESGCM,
        ];

        for original in &original_ids {
            let as_u8 = *original as u8;
            let converted_back = ID::try_from(as_u8).unwrap();

            assert!(matches!((original, converted_back),
                (ID::Argon2idAESGCM, ID::Argon2idAESGCM) |
                (ID::Argon2idChaCHa20Poly1305, ID::Argon2idChaCHa20Poly1305) |
                (ID::Pbkdf2AESGCM, ID::Pbkdf2AESGCM)
            ));
        }
    }

    #[test]
    fn test_key_generation_consistency_across_algorithms() {
        // Test that different algorithms produce different keys for same input
        let password = b"consistent_password";
        let salt = b"consistent_salt_";

        let key_argon2_aes = ID::Argon2idAESGCM.get_key(password, salt).unwrap();
        let key_argon2_chacha = ID::Argon2idChaCHa20Poly1305.get_key(password, salt).unwrap();
        let key_pbkdf2 = ID::Pbkdf2AESGCM.get_key(password, salt).unwrap();

        // Different algorithms should produce different keys
        assert_ne!(key_argon2_aes, key_pbkdf2);
        assert_ne!(key_argon2_chacha, key_pbkdf2);
        // Note: Argon2 variants might produce same key since they use same algorithm
    }
}
