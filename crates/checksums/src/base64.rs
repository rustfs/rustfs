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

use base64_simd::STANDARD;
use std::error::Error;

#[derive(Debug)]
pub(crate) struct DecodeError(base64_simd::Error);

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

pub(crate) fn decode(input: impl AsRef<str>) -> Result<Vec<u8>, DecodeError> {
    STANDARD.decode_to_vec(input.as_ref()).map_err(DecodeError)
}

pub(crate) fn encode(input: impl AsRef<[u8]>) -> String {
    STANDARD.encode_to_string(input.as_ref())
}

pub(crate) fn encoded_length(length: usize) -> usize {
    STANDARD.encoded_length(length)
}
