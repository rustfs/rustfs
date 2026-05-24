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

use crate::error::TlsRuntimeError;
use rustfs_config::{
    RUSTFS_CA_CERT, RUSTFS_CLIENT_CA_CERT_FILENAME, RUSTFS_CLIENT_CERT_FILENAME, RUSTFS_CLIENT_KEY_FILENAME, RUSTFS_PUBLIC_CERT,
    RUSTFS_TLS_CERT, RUSTFS_TLS_KEY,
};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsSourceKind {
    Directory,
    ExplicitFiles,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFileLayout {
    pub server_cert_filename: String,
    pub server_key_filename: String,
    pub public_ca_filename: String,
    pub client_ca_filename: String,
    pub client_cert_filename: String,
    pub client_key_filename: String,
}

impl Default for TlsFileLayout {
    fn default() -> Self {
        Self {
            server_cert_filename: RUSTFS_TLS_CERT.to_string(),
            server_key_filename: RUSTFS_TLS_KEY.to_string(),
            public_ca_filename: RUSTFS_PUBLIC_CERT.to_string(),
            client_ca_filename: RUSTFS_CLIENT_CA_CERT_FILENAME.to_string(),
            client_cert_filename: RUSTFS_CLIENT_CERT_FILENAME.to_string(),
            client_key_filename: RUSTFS_CLIENT_KEY_FILENAME.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsSource {
    pub kind: TlsSourceKind,
    pub base_dir: PathBuf,
    pub layout: TlsFileLayout,
    pub fallback_ca_filename: String,
    pub trust_system_ca: bool,
    pub trust_leaf_as_ca: bool,
    pub server_mtls_enabled: bool,
}

impl TlsSource {
    pub fn from_directory(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            kind: TlsSourceKind::Directory,
            base_dir: base_dir.into(),
            layout: TlsFileLayout::default(),
            fallback_ca_filename: RUSTFS_CA_CERT.to_string(),
            trust_system_ca: false,
            trust_leaf_as_ca: false,
            server_mtls_enabled: false,
        }
    }

    pub fn validate_directory(&self) -> Result<&Path, TlsRuntimeError> {
        if self.base_dir.as_os_str().is_empty() {
            return Err(TlsRuntimeError::EmptySourcePath);
        }

        if !self.base_dir.exists() {
            return Err(TlsRuntimeError::DirectoryNotFound {
                path: self.base_dir.clone(),
            });
        }

        if !self.base_dir.is_dir() {
            return Err(TlsRuntimeError::NotADirectory {
                path: self.base_dir.clone(),
            });
        }

        Ok(self.base_dir.as_path())
    }
}
