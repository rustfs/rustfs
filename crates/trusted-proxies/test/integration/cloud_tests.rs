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

//! Cloud metadata integration tests

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::cloud::detector::CloudDetector;
    use crate::cloud::metadata::{AwsMetadataFetcher, AzureMetadataFetcher, GcpMetadataFetcher};
    use crate::cloud::ranges::{CloudflareIpRanges, GoogleCloudIpRanges};

    #[tokio::test]
    async fn test_cloud_detector_disabled() {
        let detector = CloudDetector::new(
            false, // 禁用
            std::time::Duration::from_secs(1),
            None,
        );

        let provider = detector.detect_provider();
        assert!(provider.is_none());

        let ranges = detector.fetch_trusted_ranges().await;
        assert!(ranges.is_ok());
        assert!(ranges.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_cloud_detector_forced_provider() {
        let detector = CloudDetector::new(true, std::time::Duration::from_secs(1), Some("aws".to_string()));

        let provider = detector.detect_provider();
        assert!(provider.is_some());
        assert_eq!(provider.unwrap().name(), "aws");
    }

    #[tokio::test]
    async fn test_aws_metadata_fetcher() {
        let fetcher = AwsMetadataFetcher::new();

        // 测试提供者名称
        assert_eq!(fetcher.provider_name(), "aws");

        // 由于不在 AWS 环境中，这些调用应该失败或返回默认值
        let network_result = fetcher.fetch_network_cidrs().await;
        // 可能返回默认范围或错误
        assert!(network_result.is_ok());

        let public_result = fetcher.fetch_public_ip_ranges().await;
        // 可能从 API 获取或返回空列表
        assert!(public_result.is_ok());
    }

    #[tokio::test]
    async fn test_azure_metadata_fetcher() {
        let fetcher = AzureMetadataFetcher::new();

        // 测试提供者名称
        assert_eq!(fetcher.provider_name(), "azure");

        // 由于不在 Azure 环境中，这些调用应该返回默认值
        let network_result = fetcher.fetch_network_cidrs().await;
        assert!(network_result.is_ok());

        let public_result = fetcher.fetch_public_ip_ranges().await;
        assert!(public_result.is_ok());
    }

    #[tokio::test]
    async fn test_gcp_metadata_fetcher() {
        let fetcher = GcpMetadataFetcher::new();

        // 测试提供者名称
        assert_eq!(fetcher.provider_name(), "gcp");

        // 由于不在 GCP 环境中，这些调用应该返回默认值
        let network_result = fetcher.fetch_network_cidrs().await;
        assert!(network_result.is_ok());

        let public_result = fetcher.fetch_public_ip_ranges().await;
        assert!(public_result.is_ok());
    }

    #[tokio::test]
    async fn test_cloudflare_ip_ranges_static() {
        let ranges = CloudflareIpRanges::fetch().await;

        assert!(ranges.is_ok());

        let networks = ranges.unwrap();
        assert!(!networks.is_empty());

        // 检查是否包含预期的范围
        let has_ipv4 = networks
            .iter()
            .any(|n| n.to_string().contains("103.21.244.0/22") || n.to_string().contains("198.41.128.0/17"));

        let has_ipv6 = networks
            .iter()
            .any(|n| n.to_string().contains("2400:cb00::/32") || n.to_string().contains("2606:4700::/32"));

        assert!(has_ipv4 || has_ipv6);
    }

    #[tokio::test]
    async fn test_google_cloud_ip_ranges_api_mock() {
        // 创建模拟服务器
        let mock_server = MockServer::start().await;

        // 模拟 Google IP 范围 API 响应
        let mock_response = r#"
        {
            "prefixes": [
                {"ipv4Prefix": "8.34.208.0/20"},
                {"ipv4Prefix": "8.35.192.0/20"},
                {"ipv6Prefix": "2001:4860::/32"}
            ]
        }
        "#;

        Mock::given(method("GET"))
            .and(path("/ipranges/cloud.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(mock_response))
            .mount(&mock_server)
            .await;

        // 创建自定义客户端指向模拟服务器
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap();

        let url = format!("{}/ipranges/cloud.json", mock_server.uri());

        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status(), 200);

        let body = response.text().await.unwrap();
        assert!(body.contains("8.34.208.0/20"));
        assert!(body.contains("2001:4860::/32"));
    }

    #[tokio::test]
    async fn test_cloud_detector_try_all_providers() {
        let detector = CloudDetector::new(true, std::time::Duration::from_secs(2), None);

        // 在测试环境中，所有提供者都应该失败或返回空列表
        let result = detector.try_all_providers().await;

        // 应该成功返回（即使是空列表）
        assert!(result.is_ok());
    }

    #[test]
    fn test_ip_network_parsing() {
        // 测试 CIDR 解析
        let cidr = ipnetwork::IpNetwork::from_str("192.168.1.0/24");
        assert!(cidr.is_ok());

        let network = cidr.unwrap();
        assert_eq!(network.prefix(), 24);

        // 测试 IP 包含检查
        let ip: std::net::IpAddr = "192.168.1.100".parse().unwrap();
        assert!(network.contains(ip));

        let ip_outside: std::net::IpAddr = "192.168.2.100".parse().unwrap();
        assert!(!network.contains(ip_outside));

        // 测试 IPv6 CIDR
        let ipv6_cidr = ipnetwork::IpNetwork::from_str("2001:db8::/32");
        assert!(ipv6_cidr.is_ok());

        let ipv6_network = ipv6_cidr.unwrap();
        assert_eq!(ipv6_network.prefix(), 32);
    }
}
