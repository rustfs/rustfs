use crate::{decrypt_data, encrypt_data};

const PASSWORD: &[u8] = "test_password".as_bytes();

#[test_case::test_case("hello world".as_bytes())]
#[test_case::test_case(&[])]
#[test_case::test_case(&[1, 2, 3])]
#[test_case::test_case(&[3, 2, 1])]
fn test(input: &[u8]) -> Result<(), crate::Error> {
    let encrypted = encrypt_data(PASSWORD, input)?;
    let decrypted = decrypt_data(PASSWORD, &encrypted)?;
    assert_eq!(input, decrypted, "input is not equal output");
    Ok(())
}
