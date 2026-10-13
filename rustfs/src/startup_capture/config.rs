//! Bounded configuration for the opt-in native capture runtime.
use rustfs_capture_authority::{CaptureBinding, CaptureNode, NodeIdentity};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

const MAX_CONFIG_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CaptureMode {
    Create,
    Recover,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CaptureConfig {
    pub identity: NodeIdentity,
    pub database: PathBuf,
    pub reader_socket: PathBuf,
    pub peers: BTreeMap<u64, CaptureNode>,
    pub capture_binding: CaptureBinding,
    pub mode: CaptureMode,
    pub bootstrap: bool,
}

impl CaptureConfig {
    pub(super) fn read(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(invalid("native capture configuration exceeds 64 KiB"));
        }
        let config: Self = serde_json::from_slice(&bytes).map_err(|_| invalid("invalid native capture configuration JSON"))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> io::Result<()> {
        if self.identity.cluster_id == [0; 16]
            || self.capture_binding.binding_id == [0; 16]
            || self.capture_binding.target_id == [0; 16]
        {
            return Err(invalid("native capture identifiers must not be nil"));
        }
        if self.database.as_os_str().is_empty() || self.reader_socket.as_os_str().is_empty() {
            return Err(invalid("native capture paths must not be empty"));
        }
        if self.database == self.reader_socket {
            return Err(invalid("native capture database and reader socket paths must differ"));
        }
        if !self.peers.contains_key(&self.identity.node_id) {
            return Err(invalid("native capture peers must include the local node"));
        }
        for peer in self.peers.values() {
            peer.validate().map_err(|_| invalid("invalid native capture peer"))?;
        }
        if self.bootstrap && self.mode != CaptureMode::Create {
            return Err(invalid("native capture bootstrap requires create mode"));
        }
        Ok(())
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::io::Write;

    fn configuration() -> Value {
        json!({
            "identity": {"cluster_id": ([1u8; 16]), "node_id": 0},
            "database": "/tmp/capture.redb",
            "reader_socket": "/tmp/capture.sock",
            "peers": {"0": {"rpc_uri": "http://127.0.0.1:9000"}},
            "capture_binding": {"revision": 1, "binding_id": ([2u8; 16]), "target_id": ([3u8; 16])},
            "mode": "create",
            "bootstrap": true
        })
    }

    fn read(value: &Value) -> io::Result<CaptureConfig> {
        let mut file = tempfile::NamedTempFile::new().expect("configuration file");
        file.write_all(&serde_json::to_vec(value).expect("JSON"))
            .expect("write configuration");
        CaptureConfig::read(file.path())
    }

    #[test]
    fn native_capture_config_reads_create_and_recover() {
        let mut value = configuration();
        let config = read(&value).expect("create configuration");
        assert_eq!(config.mode, CaptureMode::Create);
        assert_eq!(config.identity.node_id, 0);
        assert!(config.bootstrap);
        value["mode"] = json!("recover");
        value["bootstrap"] = json!(false);
        assert_eq!(read(&value).expect("recover configuration").mode, CaptureMode::Recover);
    }

    #[test]
    fn native_capture_config_rejects_invalid_fields() {
        for (field, replacement) in [
            ("database", json!("")),
            ("reader_socket", json!("")),
            ("peers", json!({})),
            ("mode", json!("recover")),
            ("unknown", json!(true)),
        ] {
            let mut value = configuration();
            value[field] = replacement;
            assert_eq!(read(&value).expect_err(field).kind(), io::ErrorKind::InvalidData);
        }
        for (parent, field) in [
            ("identity", "cluster_id"),
            ("capture_binding", "binding_id"),
            ("capture_binding", "target_id"),
        ] {
            let mut value = configuration();
            value[parent][field] = json!(([0u8; 16]));
            assert_eq!(read(&value).expect_err(field).kind(), io::ErrorKind::InvalidData);
        }
        let mut value = configuration();
        value["peers"]["0"]["rpc_uri"] = json!("http://127.0.0.1:0");
        assert!(read(&value).is_err());
        value = configuration();
        value["identity"]["unknown"] = json!(true);
        assert!(read(&value).is_err());
    }

    #[test]
    fn native_capture_config_bounds_actual_read() {
        let mut file = tempfile::NamedTempFile::new().expect("configuration file");
        let mut bytes = serde_json::to_vec(&configuration()).expect("JSON");
        bytes.resize(MAX_CONFIG_BYTES as usize, b' ');
        file.write_all(&bytes).expect("write bounded configuration");
        CaptureConfig::read(file.path()).expect("exact limit accepted");
        file.write_all(b" ").expect("exceed limit");
        assert_eq!(CaptureConfig::read(file.path()).expect_err("oversize").kind(), io::ErrorKind::InvalidData);
    }
}
