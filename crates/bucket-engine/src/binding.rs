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

use crate::error::{EngineError, EngineResult};
use crate::name::EngineName;
use serde::{Deserialize, Serialize};

/// Bucket metadata extension entry that holds a bucket's engine binding.
pub const BUCKET_ENGINE_CONFIG_FILE: &str = "engine.json";

/// The erasure-coded object layer. Buckets without a binding use it.
pub const OBJECT_ENGINE: &str = "object";

/// Largest serialized binding accepted from a client or from disk.
pub const MAX_BINDING_BYTES: usize = 64 * 1024;

/// A bucket's engine and that engine's config, stored as the bucket metadata
/// entry [`BUCKET_ENGINE_CONFIG_FILE`]. The config is validated by the engine;
/// this type only bounds and shapes it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketEngineBinding {
    pub engine: EngineName,
    /// Schema version of `config`, checked against the engine's descriptor.
    pub config_version: u32,
    #[serde(default)]
    pub config: serde_json::Value,
}

impl BucketEngineBinding {
    /// Parse a binding from untrusted bytes: size-bounded, strict fields, valid names.
    pub fn from_slice(bytes: &[u8]) -> EngineResult<Self> {
        if bytes.len() > MAX_BINDING_BYTES {
            return Err(EngineError::InvalidBinding(format!(
                "binding is {} bytes, more than the {MAX_BINDING_BYTES}-byte limit",
                bytes.len()
            )));
        }
        let binding: Self = serde_json::from_slice(bytes).map_err(|err| EngineError::InvalidBinding(err.to_string()))?;
        if !(binding.config.is_object() || binding.config.is_null()) {
            return Err(EngineError::InvalidBinding("config must be a JSON object".to_owned()));
        }
        Ok(binding)
    }

    pub fn to_vec(&self) -> EngineResult<Vec<u8>> {
        let bytes = serde_json::to_vec(self).map_err(|err| EngineError::InvalidBinding(err.to_string()))?;
        if bytes.len() > MAX_BINDING_BYTES {
            return Err(EngineError::InvalidBinding(format!(
                "binding is {} bytes, more than the {MAX_BINDING_BYTES}-byte limit",
                bytes.len()
            )));
        }
        Ok(bytes)
    }

    pub fn is_object_engine(&self) -> bool {
        self.engine.as_str() == OBJECT_ENGINE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let binding = BucketEngineBinding {
            engine: EngineName::new("kv-cache").expect("valid engine name"),
            config_version: 1,
            config: serde_json::json!({ "replication_factor": 1 }),
        };
        let bytes = binding.to_vec().expect("binding serializes");
        assert_eq!(BucketEngineBinding::from_slice(&bytes).expect("binding parses"), binding);
        assert!(!binding.is_object_engine());
    }

    #[test]
    fn rejects_unknown_fields_bad_names_and_non_object_config() {
        for bad in [
            r#"{"engine":"kv-cache","config_version":1,"extra":true}"#,
            r#"{"engine":"KV Cache","config_version":1}"#,
            r#"{"engine":"kv-cache","config_version":1,"config":[1,2]}"#,
            r#"{"config_version":1}"#,
        ] {
            assert!(
                matches!(BucketEngineBinding::from_slice(bad.as_bytes()), Err(EngineError::InvalidBinding(_))),
                "{bad} must be refused"
            );
        }
    }

    #[test]
    fn rejects_oversized_input() {
        let padding = "x".repeat(MAX_BINDING_BYTES);
        let big = format!(r#"{{"engine":"kv-cache","config_version":1,"config":{{"pad":"{padding}"}}}}"#);
        assert!(matches!(
            BucketEngineBinding::from_slice(big.as_bytes()),
            Err(EngineError::InvalidBinding(_))
        ));
    }
}
