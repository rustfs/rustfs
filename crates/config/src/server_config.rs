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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{LazyLock, OnceLock, RwLock};

use crate::{COMMENT_KEY, DEFAULT_DELIMITER};

pub static DEFAULT_KVS: LazyLock<OnceLock<HashMap<String, KVS>>> = LazyLock::new(OnceLock::new);
pub static GLOBAL_SERVER_CONFIG: LazyLock<RwLock<Option<Config>>> = LazyLock::new(|| RwLock::new(None));

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KV {
    pub key: String,
    pub value: String,
    #[serde(default, alias = "hiddenIfEmpty")]
    pub hidden_if_empty: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KVS(pub Vec<KV>);

impl Default for KVS {
    fn default() -> Self {
        Self::new()
    }
}

impl KVS {
    pub fn new() -> Self {
        KVS(Vec::new())
    }

    pub fn get(&self, key: &str) -> String {
        if let Some(v) = self.lookup(key) { v } else { "".to_owned() }
    }

    pub fn lookup(&self, key: &str) -> Option<String> {
        for kv in self.0.iter() {
            if kv.key.as_str() == key {
                return Some(kv.value.clone());
            }
        }

        None
    }

    /// Check if KVS is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns a list of all keys for the current KVS.
    /// If the "comment" key does not exist, it will be added.
    pub fn keys(&self) -> Vec<String> {
        let mut found_comment = false;
        let mut keys: Vec<String> = self
            .0
            .iter()
            .map(|kv| {
                if kv.key == COMMENT_KEY {
                    found_comment = true;
                }
                kv.key.clone()
            })
            .collect();

        if !found_comment {
            keys.push(COMMENT_KEY.to_owned());
        }

        keys
    }

    /// Insert or update a pair of key/values in KVS
    pub fn insert(&mut self, key: String, value: String) {
        for kv in self.0.iter_mut() {
            if kv.key == key {
                kv.value = value;
                return;
            }
        }
        self.0.push(KV {
            key,
            value,
            hidden_if_empty: false,
        });
    }

    /// Merge all entries from another KVS to the current instance
    pub fn extend(&mut self, other: KVS) {
        for KV { key, value, .. } in other.0.into_iter() {
            self.insert(key, value);
        }
    }

    fn merged_with_defaults(&self, defaults: &KVS) -> KVS {
        let mut merged = defaults.clone();

        for kv in &self.0 {
            if let Some(existing) = merged.0.iter_mut().find(|entry| entry.key == kv.key) {
                *existing = kv.clone();
            } else {
                merged.0.push(kv.clone());
            }
        }

        merged
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config(pub HashMap<String, HashMap<String, KVS>>);

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    pub fn new() -> Self {
        let mut cfg = Config(HashMap::new());
        cfg.set_defaults();

        cfg
    }

    pub fn get_value(&self, sub_sys: &str, key: &str) -> Option<KVS> {
        if let Some(m) = self.0.get(sub_sys) {
            m.get(key).cloned()
        } else {
            None
        }
    }

    pub fn set_defaults(&mut self) {
        if let Some(defaults) = DEFAULT_KVS.get() {
            for (k, v) in defaults.iter() {
                if !self.0.contains_key(k) {
                    let mut default = HashMap::new();
                    default.insert(DEFAULT_DELIMITER.to_owned(), v.clone());
                    self.0.insert(k.clone(), default);
                } else if !self.0[k].contains_key(DEFAULT_DELIMITER)
                    && let Some(m) = self.0.get_mut(k)
                {
                    m.insert(DEFAULT_DELIMITER.to_owned(), v.clone());
                }
            }
        }
    }

    pub fn unmarshal(data: &[u8]) -> Result<Config, serde_json::Error> {
        let m: HashMap<String, HashMap<String, KVS>> = serde_json::from_slice(data)?;
        let mut cfg = Config(m);
        cfg.set_defaults();
        Ok(cfg)
    }

    pub fn marshal(&self) -> Result<Vec<u8>, serde_json::Error> {
        let data = serde_json::to_vec(&self.0)?;
        Ok(data)
    }

    pub fn merge(&self) -> Config {
        if let Some(defaults) = DEFAULT_KVS.get() {
            self.merge_with_defaults(defaults)
        } else {
            self.clone()
        }
    }

    fn merge_with_defaults(&self, defaults: &HashMap<String, KVS>) -> Config {
        let mut cfg = self.clone();

        for (sub_sys, default_kvs) in defaults {
            let targets = cfg.0.entry(sub_sys.clone()).or_default();
            let default_target = targets.entry(DEFAULT_DELIMITER.to_owned()).or_default();
            *default_target = default_target.merged_with_defaults(default_kvs);
        }

        cfg
    }
}

pub fn register_default_kvs(kvs: HashMap<String, KVS>) {
    let mut p = HashMap::new();
    for (k, v) in kvs {
        p.insert(k, v);
    }

    let _ = DEFAULT_KVS.set(p);
}

pub fn get_global_server_config() -> Option<Config> {
    GLOBAL_SERVER_CONFIG.read().ok().and_then(|guard| (*guard).clone())
}

pub fn set_global_server_config(cfg: Config) {
    if let Ok(mut guard) = GLOBAL_SERVER_CONFIG.write() {
        *guard = Some(cfg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kvs_preserves_lookup_insert_extend_and_keys_behavior() {
        let mut kvs = KVS::new();
        assert!(kvs.is_empty());

        kvs.insert("first".to_string(), "1".to_string());
        kvs.insert("first".to_string(), "2".to_string());
        kvs.extend(KVS(vec![KV {
            key: "second".to_string(),
            value: "3".to_string(),
            hidden_if_empty: true,
        }]));

        assert_eq!(kvs.get("first"), "2");
        assert_eq!(kvs.lookup("second"), Some("3".to_string()));
        assert_eq!(kvs.get("missing"), "");
        assert!(kvs.keys().contains(&COMMENT_KEY.to_string()));
    }

    #[test]
    fn kv_hidden_if_empty_accepts_legacy_camel_case_alias() {
        let kvs: KVS = serde_json::from_str(r#"[{"key":"token","value":"","hiddenIfEmpty":true}]"#)
            .expect("legacy hiddenIfEmpty alias should deserialize");

        assert!(kvs.0[0].hidden_if_empty);
    }

    #[test]
    fn config_marshal_unmarshal_preserves_internal_json_shape() {
        let mut kvs = KVS::new();
        kvs.insert("standard".to_string(), "EC:4".to_string());
        let cfg = Config(HashMap::from([(
            "storage_class".to_string(),
            HashMap::from([(DEFAULT_DELIMITER.to_string(), kvs)]),
        )]));

        let data = cfg.marshal().expect("config should marshal");
        let loaded = Config::unmarshal(&data).expect("config should unmarshal");

        assert_eq!(
            loaded
                .get_value("storage_class", DEFAULT_DELIMITER)
                .expect("storage class should exist")
                .get("standard"),
            "EC:4"
        );
        assert_eq!(loaded.merge(), loaded);
    }

    #[test]
    fn config_merge_fills_default_kvs_without_overwriting_user_values() {
        let defaults = HashMap::from([
            (
                "merge_test_existing".to_string(),
                KVS(vec![
                    KV {
                        key: "keep".to_string(),
                        value: "default-keep".to_string(),
                        hidden_if_empty: false,
                    },
                    KV {
                        key: "fill".to_string(),
                        value: "default-fill".to_string(),
                        hidden_if_empty: true,
                    },
                ]),
            ),
            (
                "merge_test_missing".to_string(),
                KVS(vec![KV {
                    key: "enabled".to_string(),
                    value: "on".to_string(),
                    hidden_if_empty: false,
                }]),
            ),
        ]);
        let cfg = Config(HashMap::from([
            (
                "merge_test_existing".to_string(),
                HashMap::from([(
                    DEFAULT_DELIMITER.to_string(),
                    KVS(vec![KV {
                        key: "keep".to_string(),
                        value: "user-keep".to_string(),
                        hidden_if_empty: false,
                    }]),
                )]),
            ),
            (
                "unknown_subsystem".to_string(),
                HashMap::from([(
                    DEFAULT_DELIMITER.to_string(),
                    KVS(vec![KV {
                        key: "custom".to_string(),
                        value: "value".to_string(),
                        hidden_if_empty: false,
                    }]),
                )]),
            ),
        ]));

        let merged = cfg.merge_with_defaults(&defaults);
        let existing = merged
            .get_value("merge_test_existing", DEFAULT_DELIMITER)
            .expect("existing subsystem should remain");
        assert_eq!(existing.lookup("keep"), Some("user-keep".to_string()));
        assert_eq!(existing.lookup("fill"), Some("default-fill".to_string()));
        assert!(
            existing.0.iter().any(|kv| kv.key == "fill" && kv.hidden_if_empty),
            "merged default entry should preserve default metadata"
        );

        let missing = merged
            .get_value("merge_test_missing", DEFAULT_DELIMITER)
            .expect("missing default subsystem should be added");
        assert_eq!(missing.lookup("enabled"), Some("on".to_string()));

        let unknown = merged
            .get_value("unknown_subsystem", DEFAULT_DELIMITER)
            .expect("unknown subsystem should be preserved");
        assert_eq!(unknown.lookup("custom"), Some("value".to_string()));
        assert!(
            cfg.get_value("merge_test_existing", DEFAULT_DELIMITER)
                .expect("original existing subsystem should remain")
                .lookup("fill")
                .is_none(),
            "merge should not mutate the source config"
        );
    }

    #[test]
    fn global_server_config_set_and_get_roundtrip() {
        let mut cfg = Config(HashMap::new());
        let mut kvs = KVS::new();
        kvs.insert("standard".to_string(), "EC:4".to_string());
        cfg.0
            .insert("storage_class".to_string(), HashMap::from([(DEFAULT_DELIMITER.to_string(), kvs)]));

        set_global_server_config(cfg.clone());

        assert_eq!(get_global_server_config(), Some(cfg));
    }
}
