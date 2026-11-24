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

use crate::config::Config;
use rustfs_gateway_adapter::rustfs_host_resolver;
use rustfs_gateway_corpus_recorder::{CorpusRecorderLayer, RecorderConfig, Sut};
use std::io::{Error, Result};
use std::net::IpAddr;
use std::path::{Path, PathBuf};

pub(super) fn validate_listener(ip: IpAddr) -> Result<()> {
    if !ip.is_loopback() {
        return Err(Error::other("corpus-record requires a loopback test listener"));
    }
    Ok(())
}

/// Only the S3 data plane of the test server writes corpus entries. Each process owns its file.
pub(super) fn recorder(config: &Config, domains: &[String]) -> Result<CorpusRecorderLayer> {
    let dir = std::env::var_os("RUSTFS_CORPUS_DIR")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| Error::other("corpus-record requires RUSTFS_CORPUS_DIR"))?;
    let dir = output_directory(&dir, &std::env::temp_dir())?;
    let source =
        std::env::var("RUSTFS_CORPUS_RECORD_SRC").map_err(|_| Error::other("corpus-record requires RUSTFS_CORPUS_RECORD_SRC"))?;
    let resolver = rustfs_host_resolver(domains).map_err(Error::other)?;
    let output = dir.join(format!("rustfs-{}.jsonl", uuid::Uuid::new_v4()));
    let config = RecorderConfig::new(output, source, Sut::RustfsServer, vec![config.access_key.clone()]);
    CorpusRecorderLayer::new_checked(config)
        .map(|layer| layer.with_host_resolver(resolver))
        .map_err(Error::other)
}

fn output_directory(path: &Path, temporary_root: &Path) -> Result<PathBuf> {
    let root = temporary_root.canonicalize()?;
    let dir = path.canonicalize()?;
    if dir == root || !dir.starts_with(&root) || !dir.is_dir() {
        return Err(Error::other(
            "RUSTFS_CORPUS_DIR must name a dedicated directory beneath the temporary directory",
        ));
    }
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_listener_must_be_loopback() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        assert!(validate_listener(Ipv4Addr::LOCALHOST.into()).is_ok());
        assert!(validate_listener(Ipv6Addr::LOCALHOST.into()).is_ok());
        assert!(validate_listener(Ipv4Addr::UNSPECIFIED.into()).is_err());
        assert!(validate_listener(Ipv6Addr::UNSPECIFIED.into()).is_err());
        assert!(validate_listener(Ipv4Addr::new(192, 0, 2, 1).into()).is_err());
    }

    #[test]
    fn corpus_output_must_be_a_dedicated_temporary_directory() {
        let temp = tempfile::tempdir().unwrap();
        let private = temp.path().join("capture");
        std::fs::create_dir(&private).unwrap();
        assert_eq!(output_directory(&private, temp.path()).unwrap(), private.canonicalize().unwrap());
        assert!(output_directory(temp.path(), temp.path()).is_err());
        assert!(output_directory(temp.path(), &private).is_err());
        let file = private.join("existing.jsonl");
        std::fs::write(&file, []).unwrap();
        assert!(output_directory(&file, temp.path()).is_err());
    }
}
