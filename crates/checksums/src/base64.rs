use base64_simd::STANDARD;
use std::error::Error;

#[derive(Debug)]
pub struct DecodeError(base64_simd::Error);

impl Error for DecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to decode base64")
    }
}

pub fn decode(input: impl AsRef<str>) -> Result<Vec<u8>, DecodeError> {
    STANDARD.decode_to_vec(input.as_ref()).map_err(DecodeError)
}

pub fn encode(input: impl AsRef<[u8]>) -> String {
    STANDARD.encode_to_string(input.as_ref())
}

pub fn encoded_length(length: usize) -> usize {
    STANDARD.encoded_length(length)
}
