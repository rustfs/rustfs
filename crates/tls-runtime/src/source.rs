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

/// A directory generation resolved once before loading any TLS material.
///
/// Projected Kubernetes volumes are read through their physical `..data`
/// directory. Ordinary directory and certificate-file symlinks remain supported
/// for layouts such as Certbot. Writers must keep pinned generations immutable.
#[derive(Debug, Clone)]
pub struct TlsSourceSnapshot {
    source: TlsSource,
    resolved_source: TlsSource,
}

impl TlsSource {
    pub fn snapshot(&self) -> Result<TlsSourceSnapshot, TlsRuntimeError> {
        self.validate_directory()?;
        let mut resolved_source = self.clone();
        let root = std::fs::canonicalize(&self.base_dir).map_err(|source| TlsRuntimeError::SourceIo {
            path: self.base_dir.clone(),
            source,
        })?;
        let projection = root.join("..data");
        resolved_source.base_dir = match std::fs::symlink_metadata(&projection) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                std::fs::canonicalize(&projection).map_err(|source| TlsRuntimeError::SourceIo {
                    path: projection,
                    source,
                })?
            }
            Ok(_) => root,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => root,
            Err(source) => {
                return Err(TlsRuntimeError::SourceIo {
                    path: projection,
                    source,
                });
            }
        };
        resolved_source.validate_directory()?;
        Ok(TlsSourceSnapshot {
            source: self.clone(),
            resolved_source,
        })
    }
}

impl TlsSourceSnapshot {
    pub fn source(&self) -> &TlsSource {
        &self.source
    }

    pub fn resolved_source(&self) -> &TlsSource {
        &self.resolved_source
    }

    pub fn base_dir(&self) -> &Path {
        &self.resolved_source.base_dir
    }

    /// Missing optional files are allowed only while the pinned source exists.
    pub(crate) fn validate_available(&self) -> Result<(), TlsRuntimeError> {
        std::fs::read_dir(self.base_dir()).map_err(|source| TlsRuntimeError::SourceIo {
            path: self.base_dir().to_path_buf(),
            source,
        })?;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    #[test]
    fn source_snapshot_pins_kubernetes_projection_generation() {
        let dir = TempDir::new().expect("create projection directory");
        for generation in ["..generation-a", "..generation-b"] {
            fs::create_dir(dir.path().join(generation)).expect("create generation");
            fs::write(dir.path().join(generation).join("marker"), generation).expect("write generation marker");
        }
        symlink("..generation-a", dir.path().join("..data")).expect("link initial generation");
        symlink("..data/marker", dir.path().join("marker")).expect("link projected file");
        let pinned = TlsSource::from_directory(dir.path()).snapshot().expect("pin projection");
        symlink("..generation-b", dir.path().join("..next-data")).expect("link next generation");
        fs::rename(dir.path().join("..next-data"), dir.path().join("..data")).expect("swap projection");
        assert_eq!(
            fs::read_to_string(pinned.base_dir().join("marker")).expect("read pinned file"),
            "..generation-a"
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("marker")).expect("read current projection"),
            "..generation-b"
        );
    }

    #[test]
    fn source_snapshot_pins_tls_path_directory_symlink() {
        let dir = TempDir::new().expect("create TLS directory");
        fs::create_dir(dir.path().join("a")).expect("create generation A");
        fs::create_dir(dir.path().join("b")).expect("create generation B");
        symlink("a", dir.path().join("live")).expect("link generation A");
        let pinned = TlsSource::from_directory(dir.path().join("live"))
            .snapshot()
            .expect("pin TLS source");
        fs::remove_file(dir.path().join("live")).expect("unlink source");
        symlink("b", dir.path().join("live")).expect("link generation B");
        assert_eq!(pinned.base_dir(), fs::canonicalize(dir.path().join("a")).expect("resolve generation A"));
    }

    #[test]
    fn source_snapshot_rejects_disappeared_generation() {
        let dir = TempDir::new().expect("create TLS directory");
        let generation = dir.path().join("..generation");
        fs::create_dir(&generation).expect("create generation");
        symlink("..generation", dir.path().join("..data")).expect("link generation");
        let pinned = TlsSource::from_directory(dir.path()).snapshot().expect("pin TLS source");
        fs::remove_dir(&generation).expect("remove pinned generation");
        assert!(matches!(pinned.validate_available(), Err(TlsRuntimeError::SourceIo { .. })));
    }
}
