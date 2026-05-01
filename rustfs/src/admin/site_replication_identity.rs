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

use rustfs_madmin::PeerInfo;
use std::collections::{BTreeMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use url::Url;

fn has_http_scheme(endpoint: &str) -> bool {
    endpoint.get(..7).is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
        || endpoint
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
}

pub fn canonical_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    let candidate = if has_http_scheme(trimmed) {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };

    Url::parse(&candidate)
        .ok()
        .map(|url| {
            let scheme = url.scheme().to_ascii_lowercase();
            let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
            let port = url.port_or_known_default();
            match port {
                Some(port) => format!("{scheme}://{host}:{port}"),
                None => format!("{scheme}://{host}"),
            }
        })
        .unwrap_or_else(|| trimmed.to_ascii_lowercase())
}

pub fn site_identity_key(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    let candidate = if has_http_scheme(trimmed) {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };

    Url::parse(&candidate)
        .ok()
        .map(|url| {
            let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
            match url.port_or_known_default() {
                Some(port) => format!("{host}:{port}"),
                None => host,
            }
        })
        .unwrap_or_else(|| trimmed.to_ascii_lowercase())
}

pub fn deployment_id_for_endpoint(endpoint: &str) -> String {
    let mut hasher = DefaultHasher::new();
    endpoint.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub fn same_identity_endpoint(left: &str, right: &str) -> bool {
    site_identity_key(left) == site_identity_key(right)
}

fn is_https_endpoint(endpoint: &str) -> bool {
    canonical_endpoint(endpoint).starts_with("https://")
}

fn merge_identity_peer(existing: PeerInfo, incoming: PeerInfo) -> PeerInfo {
    let existing_https = is_https_endpoint(&existing.endpoint);
    let incoming_https = is_https_endpoint(&incoming.endpoint);
    let mut merged = if incoming_https && !existing_https {
        incoming.clone()
    } else {
        existing.clone()
    };
    let fallback = if merged.deployment_id == incoming.deployment_id {
        existing
    } else {
        incoming
    };

    if merged.deployment_id.is_empty() {
        merged.deployment_id = fallback.deployment_id;
    }
    if merged.name.is_empty() {
        merged.name = fallback.name;
    }
    if merged.api_version.is_none() {
        merged.api_version = fallback.api_version;
    }
    merged.replicate_ilm_expiry |= fallback.replicate_ilm_expiry;
    merged
}

pub fn normalize_peer_map_by_identity_with<F>(peers: BTreeMap<String, PeerInfo>, mut normalize: F) -> BTreeMap<String, PeerInfo>
where
    F: FnMut(PeerInfo) -> PeerInfo,
{
    let mut peers_by_identity = BTreeMap::<String, PeerInfo>::new();
    for (_, peer) in peers {
        let normalized_peer = normalize(peer);
        let identity = site_identity_key(&normalized_peer.endpoint);
        if let Some(existing) = peers_by_identity.remove(&identity) {
            peers_by_identity.insert(identity, normalize(merge_identity_peer(existing, normalized_peer)));
        } else {
            peers_by_identity.insert(identity, normalized_peer);
        }
    }

    let mut normalized = BTreeMap::<String, PeerInfo>::new();
    for (_, mut peer) in peers_by_identity {
        if peer.deployment_id.is_empty() {
            peer.deployment_id = deployment_id_for_endpoint(&peer.endpoint);
        }

        let mut deployment_id = peer.deployment_id.clone();
        if let Some(existing) = normalized.get(&deployment_id)
            && site_identity_key(&existing.endpoint) != site_identity_key(&peer.endpoint)
        {
            deployment_id = format!("{deployment_id}-{}", deployment_id_for_endpoint(&peer.endpoint));
            peer.deployment_id = deployment_id.clone();
        }

        if let Some(existing) = normalized.get(&deployment_id).cloned() {
            normalized.insert(deployment_id, normalize(merge_identity_peer(existing, peer)));
        } else {
            normalized.insert(deployment_id, peer);
        }
    }

    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_madmin::{BucketBandwidth, SyncStatus};

    fn peer(name: &str, endpoint: &str) -> PeerInfo {
        PeerInfo {
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            deployment_id: name.to_string(),
            sync_state: SyncStatus::Unknown,
            default_bandwidth: BucketBandwidth::default(),
            replicate_ilm_expiry: false,
            object_naming_mode: String::new(),
            api_version: None,
        }
    }

    #[test]
    fn canonical_endpoint_accepts_case_insensitive_scheme() {
        assert_eq!(
            canonical_endpoint(" HTTPS://Node-A.Example.Com:9000/ "),
            "https://node-a.example.com:9000"
        );
    }

    #[test]
    fn site_identity_key_accepts_case_insensitive_scheme() {
        assert_eq!(site_identity_key("HTTPS://Node-A.Example.Com:9000/"), "node-a.example.com:9000");
        assert!(same_identity_endpoint(
            "HTTPS://Node-A.Example.Com:9000/",
            "http://node-a.example.com:9000"
        ));
    }

    #[test]
    fn normalize_peer_map_deduplicates_case_insensitive_scheme() {
        let peers = BTreeMap::from([
            ("remote-http".to_string(), peer("remote-http", "http://node-a.example.com:9000")),
            ("remote-https".to_string(), peer("remote-https", "HTTPS://Node-A.Example.Com:9000/")),
        ]);

        let normalized = normalize_peer_map_by_identity_with(peers, |peer| peer);

        assert_eq!(normalized.len(), 1);
        let peer = normalized.values().next().expect("normalized peer should exist");
        assert_eq!(peer.endpoint, "HTTPS://Node-A.Example.Com:9000/");
        assert_eq!(peer.deployment_id, "remote-https");
    }
}
