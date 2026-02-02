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

//! Google Cloud Platform (GCP) metadata fetching implementation for identifying trusted proxy ranges.

use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::AppError;
use crate::CloudMetadataFetcher;

/// Fetcher for GCP-specific metadata.
#[derive(Debug, Clone)]
pub struct GcpMetadataFetcher {
    client: Client,
    metadata_endpoint: String,
}

impl GcpMetadataFetcher {
    /// Creates a new `GcpMetadataFetcher`.
    pub fn new(timeout: Duration) -> Self {
        let client = Client::builder().timeout(timeout).build().unwrap_or_else(|_| Client::new());

        Self {
            client,
            metadata_endpoint: "http://metadata.google.internal".to_string(),
        }
    }

    /// Retrieves metadata from the GCP Compute Engine metadata server.
    async fn get_metadata(&self, path: &str) -> Result<String, AppError> {
        let url = format!("{}/computeMetadata/v1/{}", self.metadata_endpoint, path);

        debug!("Fetching GCP metadata from: {}", url);

        match self.client.get(&url).header("Metadata-Flavor", "Google").send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let text = response
                        .text()
                        .await
                        .map_err(|e| AppError::cloud(format!("Failed to read GCP metadata response: {}", e)))?;
                    Ok(text)
                } else {
                    debug!("GCP metadata request failed with status: {}", response.status());
                    Err(AppError::cloud(format!("GCP metadata API returned status: {}", response.status())))
                }
            }
            Err(e) => {
                debug!("GCP metadata request failed: {}", e);
                Err(AppError::cloud(format!("GCP metadata request failed: {}", e)))
            }
        }
    }

    /// Converts a dotted-decimal subnet mask to a CIDR prefix length.
    fn subnet_mask_to_prefix_length(mask: &str) -> Result<u8, AppError> {
        let parts: Vec<&str> = mask.split('.').collect();
        if parts.len() != 4 {
            return Err(AppError::cloud(format!("Invalid subnet mask format: {}", mask)));
        }

        let mut prefix_length = 0;
        for part in parts {
            let octet: u8 = part
                .parse()
                .map_err(|_| AppError::cloud(format!("Invalid octet in subnet mask: {}", part)))?;

            let mut remaining = octet;
            while remaining > 0 {
                if remaining & 0x80 == 0x80 {
                    prefix_length += 1;
                    remaining <<= 1;
                } else {
                    break;
                }
            }

            if remaining != 0 {
                return Err(AppError::cloud("Non-contiguous subnet mask detected"));
            }
        }

        Ok(prefix_length)
    }
}

#[async_trait]
impl CloudMetadataFetcher for GcpMetadataFetcher {
    fn provider_name(&self) -> &str {
        "gcp"
    }

    async fn fetch_network_cidrs(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        // Attempt to list network interfaces from GCP metadata.
        match self.get_metadata("instance/network-interfaces/").await {
            Ok(interfaces_metadata) => {
                let interface_indices: Vec<usize> = interfaces_metadata
                    .lines()
                    .filter_map(|line| {
                        let line = line.trim().trim_end_matches('/');
                        if line.chars().all(|c| c.is_ascii_digit()) {
                            line.parse().ok()
                        } else {
                            None
                        }
                    })
                    .collect();

                if interface_indices.is_empty() {
                    warn!("No network interfaces found in GCP metadata");
                    return Self::default_gcp_network_ranges();
                }

                let mut cidrs = Vec::new();

                for index in interface_indices {
                    // Try to get IP and subnet mask for each interface.
                    let ip_path = format!("instance/network-interfaces/{}/ip", index);
                    let mask_path = format!("instance/network-interfaces/{}/subnetmask", index);

                    match tokio::try_join!(self.get_metadata(&ip_path), self.get_metadata(&mask_path)) {
                        Ok((ip, mask)) => {
                            let ip = ip.trim();
                            let mask = mask.trim();

                            if let (Ok(ip_addr), Ok(prefix_len)) =
                                (std::net::Ipv4Addr::from_str(ip), Self::subnet_mask_to_prefix_length(mask))
                            {
                                let cidr_str = format!("{}/{}", ip_addr, prefix_len);
                                if let Ok(network) = ipnetwork::IpNetwork::from_str(&cidr_str) {
                                    cidrs.push(network);
                                }
                            }
                        }
                        Err(e) => {
                            debug!("Failed to get IP/mask for GCP interface {}: {}", index, e);
                        }
                    }
                }

                if cidrs.is_empty() {
                    warn!("Could not determine network CIDRs from GCP metadata, falling back to defaults");
                    Self::default_gcp_network_ranges()
                } else {
                    info!("Successfully fetched {} network CIDRs from GCP metadata", cidrs.len());
                    Ok(cidrs)
                }
            }
            Err(e) => {
                warn!("Failed to fetch GCP network metadata: {}", e);
                Self::default_gcp_network_ranges()
            }
        }
    }

    async fn fetch_public_ip_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        self.fetch_gcp_ip_ranges().await
    }
}

impl GcpMetadataFetcher {
    /// Fetches GCP public IP ranges from the official Google source.
    async fn fetch_gcp_ip_ranges(&self) -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        let url = "https://www.gstatic.com/ipranges/cloud.json";

        #[derive(Debug, Deserialize)]
        struct GcpIpRanges {
            prefixes: Vec<GcpPrefix>,
        }

        #[derive(Debug, Deserialize)]
        struct GcpPrefix {
            ipv4_prefix: Option<String>,
        }

        debug!("Fetching GCP IP ranges from: {}", url);

        match self.client.get(url).timeout(Duration::from_secs(10)).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let ip_ranges: GcpIpRanges = response
                        .json()
                        .await
                        .map_err(|e| AppError::cloud(format!("Failed to parse GCP IP ranges JSON: {}", e)))?;

                    let mut networks = Vec::new();

                    for prefix in ip_ranges.prefixes {
                        if let Some(ipv4_prefix) = prefix.ipv4_prefix
                            && let Ok(network) = ipnetwork::IpNetwork::from_str(&ipv4_prefix)
                        {
                            networks.push(network);
                        }
                    }

                    info!("Successfully fetched {} GCP public IP ranges", networks.len());
                    Ok(networks)
                } else {
                    debug!("Failed to fetch GCP IP ranges: HTTP {}", response.status());
                    Self::default_gcp_ip_ranges()
                }
            }
            Err(e) => {
                debug!("Failed to fetch GCP IP ranges: {}", e);
                Self::default_gcp_ip_ranges()
            }
        }
    }

    /// Returns a set of default GCP public IP ranges as a fallback.
    fn default_gcp_ip_ranges() -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        let ranges = vec![
            "8.34.208.0/20",
            "8.35.192.0/20",
            "8.35.208.0/20",
            "23.236.48.0/20",
            "23.251.128.0/19",
            "34.0.0.0/15",
            "34.2.0.0/16",
            "34.3.0.0/23",
            "34.4.0.0/14",
            "34.8.0.0/13",
            "34.16.0.0/12",
            "34.32.0.0/11",
            "34.64.0.0/10",
            "34.128.0.0/10",
            "35.184.0.0/13",
            "35.192.0.0/14",
            "35.196.0.0/15",
            "35.198.0.0/16",
            "35.200.0.0/13",
            "35.208.0.0/12",
            "35.224.0.0/12",
            "35.240.0.0/13",
            "104.154.0.0/15",
            "104.196.0.0/14",
            "107.167.160.0/19",
            "107.178.192.0/18",
            "108.59.80.0/20",
            "108.170.192.0/18",
            "108.177.0.0/17",
            "130.211.0.0/16",
            "136.112.0.0/12",
            "142.250.0.0/15",
            "146.148.0.0/17",
            "172.217.0.0/16",
            "172.253.0.0/16",
            "173.194.0.0/16",
            "192.178.0.0/15",
            "209.85.128.0/17",
            "216.58.192.0/19",
            "216.239.32.0/19",
            "2001:4860::/32",
            "2404:6800::/32",
            "2600:1900::/28",
            "2607:f8b0::/32",
            "2620:15c::/36",
            "2800:3f0::/32",
            "2a00:1450::/32",
            "2c0f:fb50::/32",
        ];

        let networks: Result<Vec<_>, _> = ranges.into_iter().map(ipnetwork::IpNetwork::from_str).collect();

        match networks {
            Ok(networks) => {
                debug!("Using default GCP public IP ranges");
                Ok(networks)
            }
            Err(e) => Err(AppError::cloud(format!("Failed to parse default GCP ranges: {}", e))),
        }
    }

    /// Returns a set of default GCP VPC ranges as a fallback.
    fn default_gcp_network_ranges() -> Result<Vec<ipnetwork::IpNetwork>, AppError> {
        let ranges = vec![
            "10.0.0.0/8",     // Large VPCs
            "172.16.0.0/12",  // Medium VPCs
            "192.168.0.0/16", // Small VPCs
            "100.64.0.0/10",  // GCP reserved range
        ];

        let networks: Result<Vec<_>, _> = ranges.into_iter().map(ipnetwork::IpNetwork::from_str).collect();

        match networks {
            Ok(networks) => {
                debug!("Using default GCP VPC network ranges");
                Ok(networks)
            }
            Err(e) => Err(AppError::cloud(format!("Failed to parse default GCP network ranges: {}", e))),
        }
    }
}
