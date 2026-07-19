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

//! Input format detection: magic bytes first, file-name extension fallback.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Format {
    Zip,
    Gzip,
    Zstd,
    Tar,
    Plain,
}

/// Detects the container format from the first bytes of the stream.
///
/// `header` should hold up to the first 8 KiB (at least 265 bytes to detect
/// tar); `name` is only used as an extension fallback for short/ambiguous
/// headers. Streams decompressed from gzip/zstd are detected again, which is
/// how `.tar.gz` / `.tgz` / `.tar.zst` are supported.
pub(crate) fn detect(header: &[u8], name: &str) -> Format {
    if header.starts_with(b"PK\x03\x04") || header.starts_with(b"PK\x05\x06") {
        return Format::Zip;
    }
    if header.starts_with(&[0x1F, 0x8B]) {
        return Format::Gzip;
    }
    if header.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
        return Format::Zstd;
    }
    if header.len() >= 262 && &header[257..262] == b"ustar" {
        return Format::Tar;
    }

    let lower = name.to_ascii_lowercase();
    if lower.ends_with(".zip") {
        Format::Zip
    } else if lower.ends_with(".gz") || lower.ends_with(".tgz") {
        Format::Gzip
    } else if lower.ends_with(".zst") {
        Format::Zstd
    } else if lower.ends_with(".tar") {
        Format::Tar
    } else {
        Format::Plain
    }
}

/// Binary sniff on the first bytes: > 1% NUL bytes means "not a log file".
pub(crate) fn looks_binary(header: &[u8]) -> bool {
    if header.is_empty() {
        return false;
    }
    let nul = header.iter().filter(|b| **b == 0).count();
    nul * 100 > header.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_bytes_win_over_names() {
        assert_eq!(detect(b"PK\x03\x04rest", "x.log"), Format::Zip);
        assert_eq!(detect(&[0x1F, 0x8B, 0x08], "x.log"), Format::Gzip);
        assert_eq!(detect(&[0x28, 0xB5, 0x2F, 0xFD, 0x00], "x.log"), Format::Zstd);
        let mut tar = vec![0u8; 512];
        tar[257..262].copy_from_slice(b"ustar");
        assert_eq!(detect(&tar, "x.log"), Format::Tar);
    }

    #[test]
    fn extension_fallback() {
        assert_eq!(detect(b"", "a.zip"), Format::Zip);
        assert_eq!(detect(b"", "a.tar.gz"), Format::Gzip);
        assert_eq!(detect(b"", "a.TGZ"), Format::Gzip);
        assert_eq!(detect(b"", "a.log.zst"), Format::Zstd);
        assert_eq!(detect(b"", "a.tar"), Format::Tar);
        assert_eq!(detect(b"", "rustfs.log"), Format::Plain);
    }

    #[test]
    fn binary_sniff() {
        assert!(!looks_binary(b""));
        assert!(!looks_binary(b"plain text, no nul at all"));
        assert!(looks_binary(&[0u8; 128]));
        // exactly 1% is not "more than 1%"
        let mut buf = vec![b'a'; 100];
        buf[0] = 0;
        assert!(!looks_binary(&buf));
        let mut buf = vec![b'a'; 99];
        buf.push(0);
        buf.push(0);
        assert!(looks_binary(&buf));
    }
}
