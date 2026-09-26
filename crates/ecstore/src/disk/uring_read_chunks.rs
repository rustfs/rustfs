// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::ffi::OsStr;

const MIN_READ_CHUNK_BYTES: usize = 4096;
const DEFAULT_READ_CHUNK_BYTES: usize = 128 << 20;

/// Validated logical length cap for one io_uring read operation. Direct-I/O
/// padding, parallel reads and application result allocations are not included.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ReadChunkSize(usize);

#[derive(Debug)]
pub(super) struct InvalidReadChunkSize;

impl std::fmt::Display for InvalidReadChunkSize {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("expected an unsigned decimal integer between 4096 and 134217728 bytes")
    }
}

impl std::error::Error for InvalidReadChunkSize {}

impl ReadChunkSize {
    pub(super) fn from_env_value(value: Option<&OsStr>) -> Result<Self, InvalidReadChunkSize> {
        let Some(value) = value else { return Ok(Self(DEFAULT_READ_CHUNK_BYTES)) };
        let text = value.to_str().ok_or(InvalidReadChunkSize)?;
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(InvalidReadChunkSize);
        }
        let size = text.parse::<usize>().map_err(|_| InvalidReadChunkSize)?;
        if !(MIN_READ_CHUNK_BYTES..=DEFAULT_READ_CHUNK_BYTES).contains(&size) {
            return Err(InvalidReadChunkSize);
        }
        Ok(Self(size))
    }

    pub(super) fn get(self) -> usize {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_configuration_preserves_128_mib_default() {
        assert_eq!(ReadChunkSize::from_env_value(None).expect("default config").get(), 128 << 20);
    }

    #[test]
    fn accepts_range_boundaries_and_non_block_multiple_caps() {
        for size in [4096, 4097, 65536, 128 << 20] {
            assert_eq!(
                ReadChunkSize::from_env_value(Some(OsStr::new(&size.to_string())))
                    .expect("valid cap")
                    .get(),
                size
            );
        }
    }

    #[test]
    fn invalid_configuration_never_becomes_the_default() {
        for value in [
            "",
            " ",
            "0",
            "4095",
            "134217729",
            "-4096",
            "+4096",
            " 4096",
            "4096 ",
            "4KiB",
            "4096.0",
            "184467440737095516160",
        ] {
            assert!(
                ReadChunkSize::from_env_value(Some(OsStr::new(value))).is_err(),
                "invalid cap accepted: {value}"
            );
        }
        assert!(ReadChunkSize::from_env_value(Some(OsStr::new(&usize::MAX.to_string()))).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_configuration_is_rejected() {
        use std::os::unix::ffi::OsStrExt;
        assert!(ReadChunkSize::from_env_value(Some(OsStr::from_bytes(&[0xff]))).is_err());
    }
}
