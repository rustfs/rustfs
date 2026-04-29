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

use crate::admin::site_replication_identity::{deployment_id_for_endpoint, normalize_peer_map_by_identity_with};
use rustfs_ecstore::config::com::{read_config, save_config};
use rustfs_ecstore::error::Error as StorageError;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_madmin::PeerInfo;
use s3s::{S3Error, S3ErrorCode, S3Result};
use serde_json::{Map, Value};
use tracing::info;

const SITE_REPLICATION_STATE_PATH: &str = "config/site-replication/state.json";

fn normalize_peers_map(peers: &Map<String, Value>) -> Map<String, Value> {
    let mut valid_peers = std::collections::BTreeMap::<String, PeerInfo>::new();
    let mut passthrough_invalid = Vec::<(String, Value)>::new();

    for (key, value) in peers {
        match serde_json::from_value::<PeerInfo>(value.clone()) {
            Ok(mut peer) => {
                if peer.endpoint.is_empty() {
                    passthrough_invalid.push((key.clone(), value.clone()));
                    continue;
                }
                if peer.deployment_id.is_empty() {
                    peer.deployment_id = deployment_id_for_endpoint(&peer.endpoint);
                }
                valid_peers.insert(peer.deployment_id.clone(), peer);
            }
            Err(_) => passthrough_invalid.push((key.clone(), value.clone())),
        }
    }

    let deduped_by_deployment = normalize_peer_map_by_identity_with(valid_peers, |peer| peer);

    let mut normalized = Map::new();
    for (_, peer) in deduped_by_deployment {
        let key = peer.deployment_id.clone();
        if let Ok(value) = serde_json::to_value(peer) {
            normalized.insert(key, value);
        }
    }
    for (key, value) in passthrough_invalid {
        normalized.entry(key).or_insert(value);
    }
    normalized
}

fn normalize_site_replication_state_json(data: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let mut state: Value = serde_json::from_slice(data).map_err(|e| format!("invalid site replication state: {e}"))?;
    let Some(obj) = state.as_object_mut() else {
        return Ok(None);
    };

    let before = obj.get("peers").and_then(|v| v.as_object()).map(|v| v.len()).unwrap_or(0);

    let Some(peers_obj) = obj.get("peers").and_then(|v| v.as_object()) else {
        return Ok(None);
    };

    let normalized_peers = normalize_peers_map(peers_obj);
    if normalized_peers == *peers_obj {
        return Ok(None);
    }

    let after = normalized_peers.len();
    obj.insert("peers".to_string(), Value::Object(normalized_peers));
    let normalized =
        serde_json::to_vec(&state).map_err(|e| format!("serialize normalized site replication state failed: {e}"))?;
    info!("normalized site-replication peers during reload: {before} -> {after}");
    Ok(Some(normalized))
}

/// Reload persisted site-replication state.
///
/// RustFS does not currently keep a separate in-memory cache for this state,
/// so "reload" means validating that the persisted JSON is readable.
pub async fn reload_site_replication_runtime_state() -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    match read_config(store.clone(), SITE_REPLICATION_STATE_PATH).await {
        Ok(data) => {
            if let Some(normalized) =
                normalize_site_replication_state_json(&data).map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e))?
            {
                save_config(store, SITE_REPLICATION_STATE_PATH, normalized)
                    .await
                    .map_err(|e| {
                        S3Error::with_message(S3ErrorCode::InternalError, format!("normalize site replication state failed: {e}"))
                    })?;
            }
            Ok(())
        }
        Err(StorageError::ConfigNotFound) => Ok(()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication state: {err}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer_value(name: &str, endpoint: &str, deployment_id: &str) -> Value {
        serde_json::json!({
            "name": name,
            "endpoint": endpoint,
            "deployment_id": deployment_id,
            "sync_state": "",
            "default_bandwidth": {},
            "replicate_ilm_expiry": false,
            "object_naming_mode": "",
            "api_version": "1"
        })
    }

    #[test]
    fn test_normalize_state_json_deduplicates_http_https_peer() {
        let data = serde_json::to_vec(&serde_json::json!({
            "name": "local",
            "peers": {
                "remote-http": peer_value("remote", "http://node-a.example.com:9000", "remote-http"),
                "remote-https": peer_value("remote", "https://node-a.example.com:9000/", "remote-https")
            }
        }))
        .unwrap();

        let normalized = normalize_site_replication_state_json(&data)
            .unwrap()
            .expect("state should be normalized");
        let value: Value = serde_json::from_slice(&normalized).unwrap();
        let peers = value.get("peers").and_then(Value::as_object).unwrap();

        assert_eq!(peers.len(), 1);
        let endpoint = peers
            .values()
            .next()
            .and_then(|peer| peer.get("endpoint"))
            .and_then(Value::as_str)
            .unwrap();
        assert!(endpoint.starts_with("https://"));
    }

    #[test]
    fn test_normalize_state_json_is_idempotent() {
        let data = serde_json::to_vec(&serde_json::json!({
            "name": "local",
            "peers": {
                "remote": peer_value("remote", "https://node-a.example.com:9000", "remote")
            }
        }))
        .unwrap();

        let first = normalize_site_replication_state_json(&data).unwrap();
        let normalized_once = first.unwrap_or(data);
        let second = normalize_site_replication_state_json(&normalized_once).unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn test_normalize_state_json_tolerates_malformed_peer_entries() {
        let data = serde_json::to_vec(&serde_json::json!({
            "name": "local",
            "peers": {
                "broken": {"endpoint": 123},
                "remote": peer_value("remote", "https://node-a.example.com:9000", "remote")
            }
        }))
        .unwrap();

        let normalized = normalize_site_replication_state_json(&data).unwrap();
        let out = normalized.unwrap_or(data);
        let value: Value = serde_json::from_slice(&out).unwrap();
        let peers = value.get("peers").and_then(Value::as_object).unwrap();

        assert!(peers.contains_key("broken"));
        assert!(!peers.is_empty());
    }
}
