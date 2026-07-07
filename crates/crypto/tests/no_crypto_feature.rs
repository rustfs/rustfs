use rustfs_crypto::{Error, decrypt_data, encrypt_data};

#[test]
fn encrypt_data_returns_error_without_crypto_feature() {
    let plain = b"must not be returned as ciphertext";

    let result = encrypt_data(b"password", plain);

    assert!(matches!(result, Err(Error::ErrCryptoDisabled)));
}

#[test]
fn decrypt_data_returns_error_without_crypto_feature() {
    let ciphertext = b"must not be returned as plaintext";

    let result = decrypt_data(b"password", ciphertext);

    assert!(matches!(result, Err(Error::ErrCryptoDisabled)));
}
