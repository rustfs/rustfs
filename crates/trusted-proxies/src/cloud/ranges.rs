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

//! Static and dynamic IP range definitions for various cloud providers.

use std::str::FromStr;
use std::time::Duration;

use ipnetwork::IpNetwork;
use reqwest::Client;
use tracing::{debug, info};

use crate::error::AppError;

/// Utility for fetching Cloudflare IP ranges.
pub struct CloudflareIpRanges;

impl CloudflareIpRanges {
    /// Returns a static list of Cloudflare IP ranges.
    pub async fn fetch() -> Result<Vec<IpNetwork>, AppError> {
        let ranges = vec![
            // IPv4 ranges
            "103.21.244.0/22",
            "103.22.200.0/22",
            "103.31.4.0/22",
            "104.16.0.0/13",
            "104.24.0.0/14",
            "108.162.192.0/18",
            "131.0.72.0/22",
            "141.101.64.0/18",
            "162.158.0.0/15",
            "172.64.0.0/13",
            "173.245.48.0/20",
            "188.114.96.0/20",
            "190.93.240.0/20",
            "197.234.240.0/22",
            "198.41.128.0/17",
            // IPv6 ranges
            "2400:cb00::/32",
            "2606:4700::/32",
            "2803:f800::/32",
            "2405:b500::/32",
            "2405:8100::/32",
            "2a06:98c0::/29",
            "2c0f:f248::/32",
        ];

        let networks: Result<Vec<_>, _> = ranges.into_iter().map(IpNetwork::from_str).collect();

        match networks {
            Ok(networks) => {
                info!(
                    event = "trusted_proxies.cloud_ranges",
                    component = "trusted_proxies",
                    subsystem = "cloud_ranges",
                    provider = "cloudflare",
                    source = "static",
                    result = "loaded",
                    range_count = networks.len(),
                    "trusted proxy cloud ranges loaded"
                );
                Ok(networks)
            }
            Err(e) => Err(AppError::cloud(format!("Failed to parse static Cloudflare IP ranges: {}", e))),
        }
    }

    /// Fetches the latest Cloudflare IP ranges from their official API.
    pub async fn fetch_from_api() -> Result<Vec<IpNetwork>, AppError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| AppError::cloud(format!("Failed to create HTTP client: {}", e)))?;

        let urls = ["https://www.cloudflare.com/ips-v4", "https://www.cloudflare.com/ips-v6"];

        let mut all_ranges = Vec::new();

        for url in urls {
            match client.get(url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        let text = response
                            .text()
                            .await
                            .map_err(|e| AppError::cloud(format!("Failed to read response from {}: {}", url, e)))?;

                        let ranges: Result<Vec<_>, _> = text
                            .lines()
                            .map(|line| line.trim())
                            .filter(|line| !line.is_empty())
                            .map(IpNetwork::from_str)
                            .collect();

                        match ranges {
                            Ok(mut networks) => {
                                debug!(
                                    event = "trusted_proxies.cloud_ranges",
                                    component = "trusted_proxies",
                                    subsystem = "cloud_ranges",
                                    provider = "cloudflare",
                                    source = %url,
                                    result = "loaded",
                                    range_count = networks.len(),
                                    "trusted proxy cloud ranges fetched"
                                );
                                all_ranges.append(&mut networks);
                            }
                            Err(e) => {
                                debug!(
                                    event = "trusted_proxies.cloud_ranges",
                                    component = "trusted_proxies",
                                    subsystem = "cloud_ranges",
                                    provider = "cloudflare",
                                    source = %url,
                                    result = "parse_failed",
                                    error = %e,
                                    "trusted proxy cloud ranges parse failed"
                                );
                            }
                        }
                    } else {
                        debug!(
                            event = "trusted_proxies.cloud_ranges",
                            component = "trusted_proxies",
                            subsystem = "cloud_ranges",
                            provider = "cloudflare",
                            source = %url,
                            result = "http_error",
                            status = %response.status(),
                            "trusted proxy cloud ranges fetch failed"
                        );
                    }
                }
                Err(e) => {
                    debug!(
                        event = "trusted_proxies.cloud_ranges",
                        component = "trusted_proxies",
                        subsystem = "cloud_ranges",
                        provider = "cloudflare",
                        source = %url,
                        result = "request_failed",
                        error = %e,
                        "trusted proxy cloud ranges fetch failed"
                    );
                }
            }
        }

        if all_ranges.is_empty() {
            // Fallback to static list if API requests fail.
            Self::fetch().await
        } else {
            info!(
                event = "trusted_proxies.cloud_ranges",
                component = "trusted_proxies",
                subsystem = "cloud_ranges",
                provider = "cloudflare",
                source = "api",
                result = "loaded",
                range_count = all_ranges.len(),
                "trusted proxy cloud ranges loaded"
            );
            Ok(all_ranges)
        }
    }
}

/// Utility for fetching DigitalOcean IP ranges.
pub struct DigitalOceanIpRanges;

impl DigitalOceanIpRanges {
    /// Returns a static list of DigitalOcean IP ranges.
    pub async fn fetch() -> Result<Vec<IpNetwork>, AppError> {
        let ranges = vec![
            // Datacenter IP ranges
            "64.227.0.0/16",
            "138.197.0.0/16",
            "139.59.0.0/16",
            "157.230.0.0/16",
            "159.65.0.0/16",
            "167.99.0.0/16",
            "178.128.0.0/16",
            "206.189.0.0/16",
            "207.154.0.0/16",
            "209.97.0.0/16",
            // Load Balancer IP ranges
            "144.126.0.0/16",
            "143.198.0.0/16",
            "161.35.0.0/16",
        ];

        let networks: Result<Vec<_>, _> = ranges.into_iter().map(IpNetwork::from_str).collect();

        match networks {
            Ok(networks) => {
                info!(
                    event = "trusted_proxies.cloud_ranges",
                    component = "trusted_proxies",
                    subsystem = "cloud_ranges",
                    provider = "digitalocean",
                    source = "static",
                    result = "loaded",
                    range_count = networks.len(),
                    "trusted proxy cloud ranges loaded"
                );
                Ok(networks)
            }
            Err(e) => Err(AppError::cloud(format!("Failed to parse static DigitalOcean IP ranges: {}", e))),
        }
    }
}

/// Utility for fetching Google Cloud IP ranges.
pub struct GoogleCloudIpRanges;

impl GoogleCloudIpRanges {
    /// Fetches the latest Google Cloud IP ranges from their official source.
    pub async fn fetch() -> Result<Vec<IpNetwork>, AppError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| AppError::cloud(format!("Failed to create HTTP client: {}", e)))?;

        let url = "https://www.gstatic.com/ipranges/cloud.json";

        #[derive(Debug, serde::Deserialize)]
        struct GoogleIpRanges {
            prefixes: Vec<GooglePrefix>,
        }

        #[derive(Debug, serde::Deserialize)]
        struct GooglePrefix {
            ipv4_prefix: Option<String>,
        }

        match client.get(url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let ip_ranges: GoogleIpRanges = response
                        .json()
                        .await
                        .map_err(|e| AppError::cloud(format!("Failed to parse Google IP ranges JSON: {}", e)))?;

                    let mut networks = Vec::new();

                    for prefix in ip_ranges.prefixes {
                        if let Some(ipv4_prefix) = prefix.ipv4_prefix
                            && let Ok(network) = IpNetwork::from_str(&ipv4_prefix)
                        {
                            networks.push(network);
                        }
                    }

                    info!(
                        event = "trusted_proxies.cloud_ranges",
                        component = "trusted_proxies",
                        subsystem = "cloud_ranges",
                        provider = "gcp",
                        source = "api",
                        result = "loaded",
                        range_count = networks.len(),
                        "trusted proxy cloud ranges loaded"
                    );
                    Ok(networks)
                } else {
                    debug!(
                        event = "trusted_proxies.cloud_ranges",
                        component = "trusted_proxies",
                        subsystem = "cloud_ranges",
                        provider = "gcp",
                        source = %url,
                        result = "http_error",
                        status = %response.status(),
                        "trusted proxy cloud ranges fetch failed"
                    );
                    Ok(Vec::new())
                }
            }
            Err(e) => {
                debug!(
                    event = "trusted_proxies.cloud_ranges",
                    component = "trusted_proxies",
                    subsystem = "cloud_ranges",
                    provider = "gcp",
                    source = %url,
                    result = "request_failed",
                    error = %e,
                    "trusted proxy cloud ranges fetch failed"
                );
                Ok(Vec::new())
            }
        }
    }
}
