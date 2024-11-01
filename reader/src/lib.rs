pub mod error;
pub mod hasher;
pub mod reader;

pub fn hex(data: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(data, hex_simd::AsciiCase::Lower)
}
