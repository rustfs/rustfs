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

use rustfs_trusted_proxies::ValidationUtils;
use std::net::IpAddr;

#[test]
fn test_email_validation() {
    assert!(ValidationUtils::is_valid_email("user@example.com"));
    assert!(!ValidationUtils::is_valid_email("invalid-email"));
}

#[test]
fn test_url_validation() {
    assert!(ValidationUtils::is_valid_url("https://example.com"));
    assert!(!ValidationUtils::is_valid_url("invalid"));
}

#[test]
fn test_x_forwarded_for_validation() {
    assert!(ValidationUtils::validate_x_forwarded_for("203.0.113.195"));
    assert!(!ValidationUtils::validate_x_forwarded_for("invalid"));
}

#[test]
fn test_forwarded_header_validation() {
    assert!(ValidationUtils::validate_forwarded_header("for=192.0.2.60"));
    assert!(!ValidationUtils::validate_forwarded_header("invalid"));
}

#[test]
fn test_ip_in_range_validation() {
    let cidr_ranges = vec!["10.0.0.0/8".to_string(), "192.168.0.0/16".to_string()];
    let ip: IpAddr = "10.0.1.1".parse().unwrap();
    assert!(ValidationUtils::validate_ip_in_range(&ip, &cidr_ranges));
}

#[test]
fn test_header_value_validation() {
    assert!(ValidationUtils::validate_header_value("text/plain"));
    assert!(!ValidationUtils::validate_header_value(&"a".repeat(8193)));
}

#[test]
fn test_port_validation() {
    assert!(ValidationUtils::validate_port(80));
    assert!(!ValidationUtils::validate_port(0));
}

#[test]
fn test_cidr_validation() {
    assert!(ValidationUtils::validate_cidr("192.168.1.0/24"));
    assert!(!ValidationUtils::validate_cidr("invalid"));
}
