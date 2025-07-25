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

use crate::base64;
use http::header::{HeaderMap, HeaderValue};

use crate::Crc64Nvme;
use crate::{CRC_32_C_NAME, CRC_32_NAME, CRC_64_NVME_NAME, Checksum, Crc32, Crc32c, Md5, SHA_1_NAME, SHA_256_NAME, Sha1, Sha256};

pub const CRC_32_HEADER_NAME: &str = "x-amz-checksum-crc32";
pub const CRC_32_C_HEADER_NAME: &str = "x-amz-checksum-crc32c";
pub const SHA_1_HEADER_NAME: &str = "x-amz-checksum-sha1";
pub const SHA_256_HEADER_NAME: &str = "x-amz-checksum-sha256";
pub const CRC_64_NVME_HEADER_NAME: &str = "x-amz-checksum-crc64nvme";

pub(crate) static MD5_HEADER_NAME: &str = "content-md5";

pub const CHECKSUM_ALGORITHMS_IN_PRIORITY_ORDER: [&str; 5] =
    [CRC_64_NVME_NAME, CRC_32_C_NAME, CRC_32_NAME, SHA_1_NAME, SHA_256_NAME];

pub trait HttpChecksum: Checksum + Send + Sync {
    fn headers(self: Box<Self>) -> HeaderMap<HeaderValue> {
        let mut header_map = HeaderMap::new();
        header_map.insert(self.header_name(), self.header_value());

        header_map
    }

    fn header_name(&self) -> &'static str;

    fn header_value(self: Box<Self>) -> HeaderValue {
        let hash = self.finalize();
        HeaderValue::from_str(&base64::encode(&hash[..])).expect("base64 encoded bytes are always valid header values")
    }

    fn size(&self) -> u64 {
        let trailer_name_size_in_bytes = self.header_name().len();
        let base64_encoded_checksum_size_in_bytes = base64::encoded_length(Checksum::size(self) as usize);

        let size = trailer_name_size_in_bytes + ":".len() + base64_encoded_checksum_size_in_bytes;

        size as u64
    }
}

impl HttpChecksum for Crc32 {
    fn header_name(&self) -> &'static str {
        CRC_32_HEADER_NAME
    }
}

impl HttpChecksum for Crc32c {
    fn header_name(&self) -> &'static str {
        CRC_32_C_HEADER_NAME
    }
}

impl HttpChecksum for Crc64Nvme {
    fn header_name(&self) -> &'static str {
        CRC_64_NVME_HEADER_NAME
    }
}

impl HttpChecksum for Sha1 {
    fn header_name(&self) -> &'static str {
        SHA_1_HEADER_NAME
    }
}

impl HttpChecksum for Sha256 {
    fn header_name(&self) -> &'static str {
        SHA_256_HEADER_NAME
    }
}

impl HttpChecksum for Md5 {
    fn header_name(&self) -> &'static str {
        MD5_HEADER_NAME
    }
}

#[cfg(test)]
mod tests {
    use crate::base64;
    use bytes::Bytes;

    use crate::{CRC_32_C_NAME, CRC_32_NAME, CRC_64_NVME_NAME, ChecksumAlgorithm, SHA_1_NAME, SHA_256_NAME};

    use super::HttpChecksum;

    #[test]
    fn test_trailer_length_of_crc32_checksum_body() {
        let checksum = CRC_32_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        let expected_size = 29;
        let actual_size = HttpChecksum::size(&*checksum);
        assert_eq!(expected_size, actual_size)
    }

    #[test]
    fn test_trailer_value_of_crc32_checksum_body() {
        let checksum = CRC_32_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        // The CRC32 of an empty string is all zeroes
        let expected_value = Bytes::from_static(b"\0\0\0\0");
        let expected_value = base64::encode(&expected_value);
        let actual_value = checksum.header_value();
        assert_eq!(expected_value, actual_value)
    }

    #[test]
    fn test_trailer_length_of_crc32c_checksum_body() {
        let checksum = CRC_32_C_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        let expected_size = 30;
        let actual_size = HttpChecksum::size(&*checksum);
        assert_eq!(expected_size, actual_size)
    }

    #[test]
    fn test_trailer_value_of_crc32c_checksum_body() {
        let checksum = CRC_32_C_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        // The CRC32C of an empty string is all zeroes
        let expected_value = Bytes::from_static(b"\0\0\0\0");
        let expected_value = base64::encode(&expected_value);
        let actual_value = checksum.header_value();
        assert_eq!(expected_value, actual_value)
    }

    #[test]
    fn test_trailer_length_of_crc64nvme_checksum_body() {
        let checksum = CRC_64_NVME_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        let expected_size = 37;
        let actual_size = HttpChecksum::size(&*checksum);
        assert_eq!(expected_size, actual_size)
    }

    #[test]
    fn test_trailer_value_of_crc64nvme_checksum_body() {
        let checksum = CRC_64_NVME_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        // The CRC64NVME of an empty string is all zeroes
        let expected_value = Bytes::from_static(b"\0\0\0\0\0\0\0\0");
        let expected_value = base64::encode(&expected_value);
        let actual_value = checksum.header_value();
        assert_eq!(expected_value, actual_value)
    }

    #[test]
    fn test_trailer_length_of_sha1_checksum_body() {
        let checksum = SHA_1_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        let expected_size = 48;
        let actual_size = HttpChecksum::size(&*checksum);
        assert_eq!(expected_size, actual_size)
    }

    #[test]
    fn test_trailer_value_of_sha1_checksum_body() {
        let checksum = SHA_1_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        // The SHA1 of an empty string is da39a3ee5e6b4b0d3255bfef95601890afd80709
        let expected_value = Bytes::from_static(&[
            0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55, 0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07,
            0x09,
        ]);
        let expected_value = base64::encode(&expected_value);
        let actual_value = checksum.header_value();
        assert_eq!(expected_value, actual_value)
    }

    #[test]
    fn test_trailer_length_of_sha256_checksum_body() {
        let checksum = SHA_256_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        let expected_size = 66;
        let actual_size = HttpChecksum::size(&*checksum);
        assert_eq!(expected_size, actual_size)
    }

    #[test]
    fn test_trailer_value_of_sha256_checksum_body() {
        let checksum = SHA_256_NAME.parse::<ChecksumAlgorithm>().unwrap().into_impl();
        let expected_value = Bytes::from_static(&[
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41,
            0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
        ]);
        let expected_value = base64::encode(&expected_value);
        let actual_value = checksum.header_value();
        assert_eq!(expected_value, actual_value)
    }
}
