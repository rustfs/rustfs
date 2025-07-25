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

use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct UnknownChecksumAlgorithmError {
    checksum_algorithm: String,
}

impl UnknownChecksumAlgorithmError {
    pub(crate) fn new(checksum_algorithm: impl Into<String>) -> Self {
        Self {
            checksum_algorithm: checksum_algorithm.into(),
        }
    }

    pub fn checksum_algorithm(&self) -> &str {
        &self.checksum_algorithm
    }
}

impl fmt::Display for UnknownChecksumAlgorithmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            r#"unknown checksum algorithm "{}", please pass a known algorithm name ("crc32", "crc32c", "sha1", "sha256", "md5")"#,
            self.checksum_algorithm
        )
    }
}

impl Error for UnknownChecksumAlgorithmError {}
