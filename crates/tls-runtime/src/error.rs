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

use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TlsRuntimeError {
    #[error("TLS source path is empty")]
    EmptySourcePath,
    #[error("TLS directory does not exist: {path}")]
    DirectoryNotFound { path: PathBuf },
    #[error("TLS path is not a directory: {path}")]
    NotADirectory { path: PathBuf },
    #[error("TLS source I/O error at {path}: {source}")]
    SourceIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("incomplete TLS certificate/private key pair for {name}")]
    IncompleteIdentity { name: String },
    #[error("invalid TLS certificate material for {name}: {reason}")]
    InvalidCertificate { name: String, reason: String },
    #[error("invalid TLS private key for {name}: {reason}")]
    InvalidPrivateKey { name: String, reason: String },
    #[error("TLS certificate/private key validation failed for {name}: {source}")]
    InvalidCertifiedKey {
        name: String,
        #[source]
        source: rustls::Error,
    },
    #[error("invalid TLS trust anchor in {name}: {source}")]
    InvalidTrustAnchor {
        name: String,
        #[source]
        source: rustls::Error,
    },
    #[error("TLS certificate for {name} is outside its validity interval")]
    CertificateValidity { name: String },
    #[error("TLS certificate for {name} does not permit server authentication")]
    InvalidServerEku { name: String },
    #[error("TLS certificate for {name} does not permit client authentication")]
    InvalidClientEku { name: String },
    #[error("TLS source loading task failed: {0}")]
    SourceTaskJoin(#[from] tokio::task::JoinError),
    #[error("TLS material error: {0}")]
    Material(String),
    #[error("TLS publication error: {0}")]
    Publication(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
