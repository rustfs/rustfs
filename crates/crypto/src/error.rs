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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unexpected header")]
    ErrUnexpectedHeader,

    #[error("invalid encryption algorithm ID: {0}")]
    ErrInvalidAlgID(u8),

    #[error("invalid input: {0}")]
    ErrInvalidInput(String),

    #[error("invalid key length")]
    ErrInvalidKeyLength,

    #[cfg(any(test, feature = "crypto"))]
    #[error("{0}")]
    ErrInvalidLength(#[from] sha2::digest::InvalidLength),

    #[cfg(any(test, feature = "crypto"))]
    #[error("encrypt failed")]
    ErrEncryptFailed(aes_gcm::aead::Error),

    #[cfg(any(test, feature = "crypto"))]
    #[error("decrypt failed")]
    ErrDecryptFailed(aes_gcm::aead::Error),

    #[cfg(any(test, feature = "crypto"))]
    #[error("argon2 err: {0}")]
    ErrArgon2(#[from] argon2::Error),

    #[error("jwt err: {0}")]
    ErrJwt(#[from] jsonwebtoken::errors::Error),

    #[error("io error: {0}")]
    ErrIo(#[from] std::io::Error),

    #[error("invalid signature")]
    ErrInvalidSignature,

    #[error("invalid token")]
    ErrInvalidToken,
}
