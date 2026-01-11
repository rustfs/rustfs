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

//! Validation utility unit tests

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use std::net::IpAddr;

    use crate::utils::ip::IpUtils;
    use crate::utils::validation::ValidationUtils;

    /// 测试电子邮件验证
    #[test]
    fn test_email_validation() {
        // 有效的电子邮件地址
        assert!(ValidationUtils::is_valid_email("user@example.com"));
        assert!(ValidationUtils::is_valid_email("first.last@example.co.uk"));
        assert!(ValidationUtils::is_valid_email("user123@example.org"));
        assert!(ValidationUtils::is_valid_email("user+tag@example.com"));
        assert!(ValidationUtils::is_valid_email("user_name@example-domain.com"));

        // 无效的电子邮件地址
        assert!(!ValidationUtils::is_valid_email(""));
        assert!(!ValidationUtils::is_valid_email("invalid-email"));
        assert!(!ValidationUtils::is_valid_email("user@"));
        assert!(!ValidationUtils::is_valid_email("@example.com"));
        assert!(!ValidationUtils::is_valid_email("user@.com"));
        assert!(!ValidationUtils::is_valid_email("user@example."));
        assert!(!ValidationUtils::is_valid_email("user@example..com"));
        assert!(!ValidationUtils::is_valid_email("user@example_com"));
        assert!(!ValidationUtils::is_valid_email("user@[127.0.0.1]"));
        assert!(!ValidationUtils::is_valid_email("user name@example.com"));
        assert!(!ValidationUtils::is_valid_email("user@exa mple.com"));
        assert!(!ValidationUtils::is_valid_email("user@example.c"));
    }

    /// 测试 URL 验证
    #[test]
    fn test_url_validation() {
        // 有效的 URL
        assert!(ValidationUtils::is_valid_url("https://example.com"));
        assert!(ValidationUtils::is_valid_url("http://example.com"));
        assert!(ValidationUtils::is_valid_url("example.com"));
        assert!(ValidationUtils::is_valid_url("sub.example.com"));
        assert!(ValidationUtils::is_valid_url("example.co.uk"));
        assert!(ValidationUtils::is_valid_url("example.com/path"));
        assert!(ValidationUtils::is_valid_url("example.com/path/to/resource"));
        assert!(ValidationUtils::is_valid_url("example.com/?query=param"));
        assert!(ValidationUtils::is_valid_url("sub-domain.example-domain.com"));

        // 无效的 URL
        assert!(!ValidationUtils::is_valid_url(""));
        assert!(!ValidationUtils::is_valid_url("invalid"));
        assert!(!ValidationUtils::is_valid_url("example"));
        assert!(!ValidationUtils::is_valid_url("example."));
        assert!(!ValidationUtils::is_valid_url(".com"));
        assert!(!ValidationUtils::is_valid_url("http://"));
        assert!(!ValidationUtils::is_valid_url("https://"));
        assert!(!ValidationUtils::is_valid_url("://example.com"));
        assert!(!ValidationUtils::is_valid_url("example..com"));
        assert!(!ValidationUtils::is_valid_url("-example.com"));
        assert!(!ValidationUtils::is_valid_url("example-.com"));
        assert!(!ValidationUtils::is_valid_url("example_com"));
    }

    /// 测试 X-Forwarded-For 头部验证
    #[test]
    fn test_x_forwarded_for_validation() {
        // 有效的 X-Forwarded-For 头部
        assert!(ValidationUtils::validate_x_forwarded_for("203.0.113.195"));
        assert!(ValidationUtils::validate_x_forwarded_for("203.0.113.195, 198.51.100.1"));
        assert!(ValidationUtils::validate_x_forwarded_for("203.0.113.195,198.51.100.1,10.0.1.100"));
        assert!(ValidationUtils::validate_x_forwarded_for("2001:db8::1"));
        assert!(ValidationUtils::validate_x_forwarded_for("2001:db8::1, 2001:db8::2"));
        assert!(ValidationUtils::validate_x_forwarded_for("203.0.113.195:8080, 198.51.100.1:443")); // 带端口

        // 无效的 X-Forwarded-For 头部
        assert!(!ValidationUtils::validate_x_forwarded_for("")); // 空字符串
        assert!(!ValidationUtils::validate_x_forwarded_for(" ")); // 只有空格
        assert!(!ValidationUtils::validate_x_forwarded_for("invalid")); // 无效 IP
        assert!(!ValidationUtils::validate_x_forwarded_for("203.0.113.195, invalid")); // 部分无效
        assert!(!ValidationUtils::validate_x_forwarded_for("203.0.113.195, ")); // 尾部逗号加空格
        assert!(!ValidationUtils::validate_x_forwarded_for(",203.0.113.195")); // 开头逗号
        assert!(!ValidationUtils::validate_x_forwarded_for("203.0.113.195,,198.51.100.1")); // 连续逗号
        assert!(!ValidationUtils::validate_x_forwarded_for("256.256.256.256")); // 超出范围的 IP
    }

    /// 测试 Forwarded 头部验证 (RFC 7239)
    #[test]
    fn test_forwarded_header_validation() {
        // 有效的 Forwarded 头部
        assert!(ValidationUtils::validate_forwarded_header("for=192.0.2.60"));
        assert!(ValidationUtils::validate_forwarded_header("for=192.0.2.60;proto=http"));
        assert!(ValidationUtils::validate_forwarded_header("for=\"[2001:db8:cafe::17]\";proto=https"));
        assert!(ValidationUtils::validate_forwarded_header("for=192.0.2.43, for=198.51.100.17"));
        assert!(ValidationUtils::validate_forwarded_header("for=192.0.2.60;proto=http;by=203.0.113.43"));
        assert!(ValidationUtils::validate_forwarded_header(
            "by=203.0.113.43;for=192.0.2.60;host=example.com;proto=https"
        ));

        // 无效的 Forwarded 头部
        assert!(!ValidationUtils::validate_forwarded_header("")); // 空字符串
        assert!(!ValidationUtils::validate_forwarded_header(" ")); // 只有空格
        assert!(!ValidationUtils::validate_forwarded_header("invalid")); // 无效格式
        assert!(!ValidationUtils::validate_forwarded_header("for=192.0.2.60 proto=http")); // 缺少分号
        assert!(!ValidationUtils::validate_forwarded_header("for;192.0.2.60")); // 缺少等号
        assert!(!ValidationUtils::validate_forwarded_header("=192.0.2.60")); // 缺少键
        assert!(!ValidationUtils::validate_forwarded_header("for=")); // 缺少值
    }

    /// 测试 IP 范围验证
    #[test]
    fn test_ip_in_range_validation() {
        let cidr_ranges = vec![
            "10.0.0.0/8".to_string(),
            "192.168.0.0/16".to_string(),
            "172.16.0.0/12".to_string(),
            "2001:db8::/32".to_string(),
        ];

        // IP 在范围内
        let ip_in_range: IpAddr = "10.0.1.1".parse().unwrap();
        assert!(ValidationUtils::validate_ip_in_range(&ip_in_range, &cidr_ranges));

        let ip_in_range2: IpAddr = "192.168.1.100".parse().unwrap();
        assert!(ValidationUtils::validate_ip_in_range(&ip_in_range2, &cidr_ranges));

        let ip_in_range3: IpAddr = "172.16.0.1".parse().unwrap();
        assert!(ValidationUtils::validate_ip_in_range(&ip_in_range3, &cidr_ranges));

        let ipv6_in_range: IpAddr = "2001:db8::1".parse().unwrap();
        assert!(ValidationUtils::validate_ip_in_range(&ipv6_in_range, &cidr_ranges));

        // IP 不在范围内
        let ip_not_in_range: IpAddr = "8.8.8.8".parse().unwrap();
        assert!(!ValidationUtils::validate_ip_in_range(&ip_not_in_range, &cidr_ranges));

        let ip_not_in_range2: IpAddr = "203.0.113.1".parse().unwrap();
        assert!(!ValidationUtils::validate_ip_in_range(&ip_not_in_range2, &cidr_ranges));

        let ipv6_not_in_range: IpAddr = "2001:4860::1".parse().unwrap();
        assert!(!ValidationUtils::validate_ip_in_range(&ipv6_not_in_range, &cidr_ranges));

        // 空范围列表
        assert!(!ValidationUtils::validate_ip_in_range(&ip_in_range, &Vec::new()));

        // 无效的 CIDR 范围（应该被忽略）
        let invalid_ranges = vec![
            "invalid".to_string(),
            "10.0.0.0/8".to_string(), // 这个有效
        ];

        let test_ip: IpAddr = "10.0.1.1".parse().unwrap();
        // 即使有无效范围，只要有一个有效范围包含 IP，就应该返回 true
        assert!(ValidationUtils::validate_ip_in_range(&test_ip, &invalid_ranges));
    }

    /// 测试头部值验证
    #[test]
    fn test_header_value_validation() {
        // 有效的头部值
        assert!(ValidationUtils::validate_header_value("text/plain"));
        assert!(ValidationUtils::validate_header_value("application/json; charset=utf-8"));
        assert!(ValidationUtils::validate_header_value("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"));
        assert!(ValidationUtils::validate_header_value("127.0.0.1:8080"));
        assert!(ValidationUtils::validate_header_value("")); // 空字符串是有效的
        assert!(ValidationUtils::validate_header_value("normal text with spaces")); // 普通文本
        assert!(ValidationUtils::validate_header_value("value\twith\ttabs")); // 包含制表符

        // 长但有效的头部值（边界情况）
        let long_value = "a".repeat(8192);
        assert!(ValidationUtils::validate_header_value(&long_value));

        // 无效的头部值
        let too_long_value = "a".repeat(8193); // 超过长度限制
        assert!(!ValidationUtils::validate_header_value(&too_long_value));

        // 包含控制字符（除了制表符、换行符、回车符）
        assert!(!ValidationUtils::validate_header_value("value\x00with_null")); // 空字符
        assert!(!ValidationUtils::validate_header_value("value\x01with_soh")); // 标题开始
        assert!(!ValidationUtils::validate_header_value("value\x1fwith_us")); // 单元分隔符

        // 换行符和回车符是允许的（在某些上下文中）
        assert!(ValidationUtils::validate_header_value("line1\nline2"));
        assert!(ValidationUtils::validate_header_value("line1\r\nline2"));
    }

    /// 测试头部映射验证
    #[test]
    fn test_headers_validation() {
        let mut valid_headers = HeaderMap::new();
        valid_headers.insert("Content-Type", "application/json".parse().unwrap());
        valid_headers.insert("Authorization", "Bearer token123".parse().unwrap());
        valid_headers.insert("X-Forwarded-For", "203.0.113.195".parse().unwrap());

        assert!(ValidationUtils::validate_headers(&valid_headers));

        // 测试头部名称过长
        let mut invalid_headers = HeaderMap::new();
        let long_name = "X-".to_string() + &"A".repeat(300); // 超过 256 字符
        invalid_headers.insert(long_name, "value".parse().unwrap());

        assert!(!ValidationUtils::validate_headers(&invalid_headers));

        // 测试头部值过长
        let mut invalid_headers2 = HeaderMap::new();
        let long_value = "A".repeat(8193); // 超过 8192 字节
        invalid_headers2.insert("X-Custom-Header", long_value.parse().unwrap());

        assert!(!ValidationUtils::validate_headers(&invalid_headers2));

        // 测试二进制数据（无法转换为字符串）
        let mut binary_headers = HeaderMap::new();
        let binary_data = vec![0x00, 0x01, 0x02, 0x03];
        binary_headers.insert("X-Binary-Data", http::HeaderValue::from_bytes(&binary_data).unwrap());

        // 二进制数据应该通过验证（只要长度不超过限制）
        assert!(ValidationUtils::validate_headers(&binary_headers));

        // 测试过长的二进制数据
        let mut long_binary_headers = HeaderMap::new();
        let long_binary_data = vec![0x00; 8193]; // 超过 8192 字节
        long_binary_headers.insert("X-Long-Binary", http::HeaderValue::from_bytes(&long_binary_data).unwrap());

        assert!(!ValidationUtils::validate_headers(&long_binary_headers));
    }

    /// 测试端口号验证
    #[test]
    fn test_port_validation() {
        // 有效端口号
        assert!(ValidationUtils::validate_port(1));
        assert!(ValidationUtils::validate_port(80));
        assert!(ValidationUtils::validate_port(443));
        assert!(ValidationUtils::validate_port(8080));
        assert!(ValidationUtils::validate_port(65535));

        // 无效端口号
        assert!(!ValidationUtils::validate_port(0)); // 端口 0 是保留的
        assert!(!ValidationUtils::validate_port(65536)); // 超过最大值
        assert!(!ValidationUtils::validate_port(70000)); // 远超过最大值
    }

    /// 测试 CIDR 表示法验证
    #[test]
    fn test_cidr_validation() {
        // 有效的 CIDR 表示法
        assert!(ValidationUtils::validate_cidr("192.168.1.0/24"));
        assert!(ValidationUtils::validate_cidr("10.0.0.0/8"));
        assert!(ValidationUtils::validate_cidr("0.0.0.0/0")); // 默认路由
        assert!(ValidationUtils::validate_cidr("2001:db8::/32"));
        assert!(ValidationUtils::validate_cidr("::/0")); // IPv6 默认路由
        assert!(ValidationUtils::validate_cidr("192.168.1.1/32")); // 单个主机
        assert!(ValidationUtils::validate_cidr("2001:db8::1/128")); // 单个 IPv6 主机

        // 无效的 CIDR 表示法
        assert!(!ValidationUtils::validate_cidr("")); // 空字符串
        assert!(!ValidationUtils::validate_cidr("invalid")); // 无效格式
        assert!(!ValidationUtils::validate_cidr("192.168.1.0")); // 缺少前缀长度
        assert!(!ValidationUtils::validate_cidr("192.168.1.0/33")); // 前缀长度过大
        assert!(!ValidationUtils::validate_cidr("256.256.256.256/24")); // 无效 IP
        assert!(!ValidationUtils::validate_cidr("192.168.1.0/24/extra")); // 多余的部分
        assert!(!ValidationUtils::validate_cidr("192.168.1.0/-1")); // 负的前缀长度
        assert!(!ValidationUtils::validate_cidr("192.168.1.0/abc")); // 非数字前缀长度
    }

    /// 测试代理链长度验证
    #[test]
    fn test_proxy_chain_length_validation() {
        let chain = vec![
            "203.0.113.195".parse().unwrap(),
            "198.51.100.1".parse().unwrap(),
            "10.0.1.100".parse().unwrap(),
        ];

        // 链长度在限制内
        assert!(ValidationUtils::validate_proxy_chain_length(&chain, 3));
        assert!(ValidationUtils::validate_proxy_chain_length(&chain, 5));
        assert!(ValidationUtils::validate_proxy_chain_length(&chain, 10));

        // 链长度超过限制
        assert!(!ValidationUtils::validate_proxy_chain_length(&chain, 2));
        assert!(!ValidationUtils::validate_proxy_chain_length(&chain, 1));
        assert!(!ValidationUtils::validate_proxy_chain_length(&chain, 0));

        // 空链
        let empty_chain: Vec<IpAddr> = Vec::new();
        assert!(ValidationUtils::validate_proxy_chain_length(&empty_chain, 0));
        assert!(ValidationUtils::validate_proxy_chain_length(&empty_chain, 1));
        assert!(ValidationUtils::validate_proxy_chain_length(&empty_chain, 10));
    }

    /// 测试代理链连续性验证
    #[test]
    fn test_proxy_chain_continuity_validation() {
        // 连续链（无重复相邻 IP）
        let continuous_chain = vec![
            "203.0.113.195".parse().unwrap(),
            "198.51.100.1".parse().unwrap(),
            "10.0.1.100".parse().unwrap(),
        ];
        assert!(ValidationUtils::validate_proxy_chain_continuity(&continuous_chain));

        // 不连续链（有重复相邻 IP）
        let discontinuous_chain = vec![
            "203.0.113.195".parse().unwrap(),
            "198.51.100.1".parse().unwrap(),
            "198.51.100.1".parse().unwrap(), // 重复
            "10.0.1.100".parse().unwrap(),
        ];
        assert!(!ValidationUtils::validate_proxy_chain_continuity(&discontinuous_chain));

        // 短链（应该总是连续的）
        let short_chain = vec!["203.0.113.195".parse().unwrap()];
        assert!(ValidationUtils::validate_proxy_chain_continuity(&short_chain));

        let two_item_chain = vec!["203.0.113.195".parse().unwrap(), "198.51.100.1".parse().unwrap()];
        assert!(ValidationUtils::validate_proxy_chain_continuity(&two_item_chain));

        // 空链（应该总是连续的）
        let empty_chain: Vec<IpAddr> = Vec::new();
        assert!(ValidationUtils::validate_proxy_chain_continuity(&empty_chain));

        // 有多个重复的链
        let multi_duplicate_chain = vec![
            "203.0.113.195".parse().unwrap(),
            "203.0.113.195".parse().unwrap(), // 重复 1
            "198.51.100.1".parse().unwrap(),
            "198.51.100.1".parse().unwrap(), // 重复 2
        ];
        assert!(!ValidationUtils::validate_proxy_chain_continuity(&multi_duplicate_chain));
    }

    /// 测试安全字符串验证
    #[test]
    fn test_safe_string_validation() {
        // 安全字符串
        assert!(ValidationUtils::is_safe_string("example"));
        assert!(ValidationUtils::is_safe_string("example123"));
        assert!(ValidationUtils::is_safe_string("example-test"));
        assert!(ValidationUtils::is_safe_string("example.test"));
        assert!(ValidationUtils::is_safe_string("example~test"));
        assert!(ValidationUtils::is_safe_string("http://example.com/path"));
        assert!(ValidationUtils::is_safe_string("https://example.com/?query=param"));
        assert!(ValidationUtils::is_safe_string("user@example.com"));
        assert!(ValidationUtils::is_safe_string("192.168.1.1:8080"));
        assert!(ValidationUtils::is_safe_string("[2001:db8::1]:8080"));

        // 不安全字符串
        assert!(!ValidationUtils::is_safe_string("")); // 空字符串
        assert!(!ValidationUtils::is_safe_string("example test")); // 包含空格
        assert!(!ValidationUtils::is_safe_string("example\ttest")); // 包含制表符
        assert!(!ValidationUtils::is_safe_string("example\ntest")); // 包含换行符
        assert!(!ValidationUtils::is_safe_string("example<script>alert('xss')</script>")); // 包含尖括号
        assert!(!ValidationUtils::is_safe_string("example\"test")); // 包含双引号
        assert!(!ValidationUtils::is_safe_string("example'test")); // 包含单引号
        assert!(!ValidationUtils::is_safe_string("example\\test")); // 包含反斜杠
        assert!(!ValidationUtils::is_safe_string("example`test")); // 包含反引号
        assert!(!ValidationUtils::is_safe_string("example|test")); // 包含竖线
        assert!(!ValidationUtils::is_safe_string("example$test")); // 包含美元符号
        assert!(!ValidationUtils::is_safe_string("example%test")); // 包含百分号
        assert!(!ValidationUtils::is_safe_string("example^test")); // 包含脱字符
        assert!(!ValidationUtils::is_safe_string("example&test")); // 包含和号
        assert!(!ValidationUtils::is_safe_string("example(test")); // 包含括号
        assert!(!ValidationUtils::is_safe_string("example)test")); // 包含括号
        assert!(!ValidationUtils::is_safe_string("example[test")); // 包含方括号
        assert!(!ValidationUtils::is_safe_string("example]test")); // 包含方括号
        assert!(!ValidationUtils::is_safe_string("example{test")); // 包含花括号
        assert!(!ValidationUtils::is_safe_string("example}test")); // 包含花括号
    }

    /// 测试速率限制参数验证
    #[test]
    fn test_rate_limit_params_validation() {
        // 有效的速率限制参数
        assert!(ValidationUtils::validate_rate_limit_params(1, 1)); // 最小值
        assert!(ValidationUtils::validate_rate_limit_params(100, 60)); // 典型值
        assert!(ValidationUtils::validate_rate_limit_params(10000, 86400)); // 最大值

        // 无效的速率限制参数
        assert!(!ValidationUtils::validate_rate_limit_params(0, 60)); // 请求数为 0
        assert!(!ValidationUtils::validate_rate_limit_params(10001, 60)); // 请求数超过最大值
        assert!(!ValidationUtils::validate_rate_limit_params(100, 0)); // 周期为 0
        assert!(!ValidationUtils::validate_rate_limit_params(100, 86401)); // 周期超过最大值
        assert!(!ValidationUtils::validate_rate_limit_params(0, 0)); // 两者都为 0
        assert!(!ValidationUtils::validate_rate_limit_params(100001, 100000)); // 两者都超过最大值
    }

    /// 测试缓存参数验证
    #[test]
    fn test_cache_params_validation() {
        // 有效的缓存参数
        assert!(ValidationUtils::validate_cache_params(1, 1)); // 最小值
        assert!(ValidationUtils::validate_cache_params(10000, 300)); // 典型值
        assert!(ValidationUtils::validate_cache_params(1000000, 86400)); // 最大值

        // 无效的缓存参数
        assert!(!ValidationUtils::validate_cache_params(0, 300)); // 容量为 0
        assert!(!ValidationUtils::validate_cache_params(1000001, 300)); // 容量超过最大值
        assert!(!ValidationUtils::validate_cache_params(10000, 0)); // TTL 为 0
        assert!(!ValidationUtils::validate_cache_params(10000, 86401)); // TTL 超过最大值
        assert!(!ValidationUtils::validate_cache_params(0, 0)); // 两者都为 0
        assert!(!ValidationUtils::validate_cache_params(2000000, 100000)); // 两者都超过最大值
    }

    /// 测试敏感数据脱敏
    #[test]
    fn test_sensitive_data_masking() {
        let sensitive_patterns = vec!["password", "token", "secret", "authorization", "api_key"];

        // 测试各种敏感字段的脱敏
        let test_cases = vec![
            (
                r#"{"username":"john","password":"secret123"}"#,
                r#"{"username":"john","password:[REDACTED]"}"#,
            ),
            (r#"token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9&user=john"#, r#"token:[REDACTED]&user=john"#),
            (
                r#"Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"#,
                r#"Authorization:[REDACTED]"#,
            ),
            (r#"api_key=sk_test_1234567890abcdef"#, r#"api_key:[REDACTED]"#),
            (r#"secret_key=abc123&public_key=xyz789"#, r#"secret_key:[REDACTED]&public_key=xyz789"#),
            (
                r#"password=123&password_confirmation=123"#,
                r#"password:[REDACTED]&password_confirmation:[REDACTED]"#,
            ),
        ];

        for (input, expected) in test_cases {
            let result = ValidationUtils::mask_sensitive_data(input, &sensitive_patterns);
            assert_eq!(result, expected, "Failed to mask: {}", input);
        }

        // 测试不包含敏感数据的情况
        let safe_data = r#"{"name":"John","age":30,"city":"New York"}"#;
        let result = ValidationUtils::mask_sensitive_data(safe_data, &sensitive_patterns);
        assert_eq!(result, safe_data);

        // 测试空模式列表
        let sensitive_data = r#"password=secret123"#;
        let result = ValidationUtils::mask_sensitive_data(sensitive_data, &Vec::new());
        assert_eq!(result, sensitive_data);

        // 测试空输入
        let result = ValidationUtils::mask_sensitive_data("", &sensitive_patterns);
        assert_eq!(result, "");
    }

    /// 测试组合验证场景
    #[test]
    fn test_combined_validation_scenarios() {
        // 场景 1：完整的代理请求验证
        let proxy_chain = vec![
            "203.0.113.195".parse().unwrap(),
            "198.51.100.1".parse().unwrap(),
            "10.0.1.100".parse().unwrap(),
        ];

        assert!(ValidationUtils::validate_proxy_chain_length(&proxy_chain, 10));
        assert!(ValidationUtils::validate_proxy_chain_continuity(&proxy_chain));

        // 场景 2：包含无效数据的头部验证
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("X-Forwarded-For", "203.0.113.195, invalid, 10.0.1.100".parse().unwrap());

        // 头部映射本身是有效的（即使包含无效的 X-Forwarded-For）
        assert!(ValidationUtils::validate_headers(&headers));

        // 但 X-Forwarded-For 内容无效
        let xff_value = headers.get("X-Forwarded-For").unwrap().to_str().unwrap();
        assert!(!ValidationUtils::validate_x_forwarded_for(xff_value));

        // 场景 3：配置参数验证组合
        let cache_capacity = 10000;
        let cache_ttl = 300;
        let rate_limit_requests = 100;
        let rate_limit_period = 60;

        assert!(ValidationUtils::validate_cache_params(cache_capacity, cache_ttl));
        assert!(ValidationUtils::validate_rate_limit_params(rate_limit_requests, rate_limit_period));

        // 场景 4：IP 和 CIDR 验证组合
        let ip: IpAddr = "10.0.1.1".parse().unwrap();
        let cidr = "10.0.0.0/8";

        assert!(IpUtils::is_private_ip(&ip));
        assert!(ValidationUtils::validate_cidr(cidr));
        assert!(ValidationUtils::validate_ip_in_range(&ip, &[cidr.to_string()]));
    }

    /// 测试边缘情况和边界值
    #[test]
    fn test_edge_cases_and_boundaries() {
        // 测试头部值的边界长度
        let max_length_value = "a".repeat(8192);
        let over_length_value = "a".repeat(8193);

        assert!(ValidationUtils::validate_header_value(&max_length_value));
        assert!(!ValidationUtils::validate_header_value(&over_length_value));

        // 测试端口边界值
        assert!(!ValidationUtils::validate_port(0)); // 最小无效值
        assert!(ValidationUtils::validate_port(1)); // 最小有效值
        assert!(ValidationUtils::validate_port(65535)); // 最大有效值
        assert!(!ValidationUtils::validate_port(65536)); // 超过最大值

        // 测试 CIDR 前缀长度边界值
        assert!(ValidationUtils::validate_cidr("192.168.1.0/0")); // 最小有效前缀
        assert!(ValidationUtils::validate_cidr("192.168.1.0/32")); // 最大有效前缀
        assert!(!ValidationUtils::validate_cidr("192.168.1.0/33")); // 超过最大值

        // IPv6 CIDR 前缀长度
        assert!(ValidationUtils::validate_cidr("2001:db8::/0")); // 最小有效前缀
        assert!(ValidationUtils::validate_cidr("2001:db8::/128")); // 最大有效前缀

        // 测试代理链边界情况
        let empty_chain: Vec<IpAddr> = Vec::new();
        assert!(ValidationUtils::validate_proxy_chain_length(&empty_chain, 0));
        assert!(ValidationUtils::validate_proxy_chain_continuity(&empty_chain));

        // 测试单个 IP 的链
        let single_ip_chain = vec!["192.168.1.1".parse().unwrap()];
        assert!(ValidationUtils::validate_proxy_chain_length(&single_ip_chain, 1));
        assert!(ValidationUtils::validate_proxy_chain_continuity(&single_ip_chain));

        // 测试速率限制边界值
        assert!(!ValidationUtils::validate_rate_limit_params(0, 60));
        assert!(ValidationUtils::validate_rate_limit_params(1, 1));
        assert!(ValidationUtils::validate_rate_limit_params(10000, 86400));
        assert!(!ValidationUtils::validate_rate_limit_params(10001, 86400));
        assert!(!ValidationUtils::validate_rate_limit_params(10000, 86401));

        // 测试缓存参数边界值
        assert!(!ValidationUtils::validate_cache_params(0, 300));
        assert!(ValidationUtils::validate_cache_params(1, 1));
        assert!(ValidationUtils::validate_cache_params(1000000, 86400));
        assert!(!ValidationUtils::validate_cache_params(1000001, 86400));
        assert!(!ValidationUtils::validate_cache_params(1000000, 86401));
    }

    /// 测试性能敏感场景
    #[test]
    fn test_performance_sensitive_scenarios() {
        // 测试长代理链的处理
        let mut long_chain = Vec::new();
        for i in 0..100 {
            let ip = format!("10.0.{}.1", i % 256).parse().unwrap();
            long_chain.push(ip);
        }

        // 应该能快速处理长链
        assert!(ValidationUtils::validate_proxy_chain_length(&long_chain, 100));
        assert!(ValidationUtils::validate_proxy_chain_continuity(&long_chain));

        // 测试大量 CIDR 范围的验证
        let mut cidr_ranges = Vec::new();
        for i in 0..1000 {
            let cidr = format!("10.{}.0.0/16", i % 256);
            cidr_ranges.push(cidr);
        }

        let test_ip: IpAddr = "10.128.1.1".parse().unwrap();
        // 应该能快速在大范围列表中查找
        let start = std::time::Instant::now();
        let result = ValidationUtils::validate_ip_in_range(&test_ip, &cidr_ranges);
        let duration = start.elapsed();

        assert!(result);
        // 验证时间应该在合理范围内（比如小于 10 毫秒）
        assert!(duration < std::time::Duration::from_millis(10));

        // 测试头部值验证的性能
        let large_header_value = "x".repeat(10000); // 超过 8192，应该快速拒绝
        let start = std::time::Instant::now();
        let result = ValidationUtils::validate_header_value(&large_header_value);
        let duration = start.elapsed();

        assert!(!result); // 应该拒绝
        assert!(duration < std::time::Duration::from_millis(1)); // 应该非常快
    }

    /// 测试实际代理场景模拟
    #[test]
    fn test_real_world_proxy_scenarios() {
        // 场景 1：典型的反向代理配置
        let typical_xff = "203.0.113.195, 198.51.100.1, 10.0.1.100";
        assert!(ValidationUtils::validate_x_forwarded_for(typical_xff));

        let typical_proxy_chain: Vec<IpAddr> = typical_xff.split(',').map(|s| s.trim().parse().unwrap()).collect();

        assert_eq!(typical_proxy_chain.len(), 3);
        assert!(ValidationUtils::validate_proxy_chain_length(&typical_proxy_chain, 10));
        assert!(ValidationUtils::validate_proxy_chain_continuity(&typical_proxy_chain));

        // 场景 2：负载均衡器场景
        let lb_scenario = "2001:db8::1, 203.0.113.195, 198.51.100.1";
        assert!(ValidationUtils::validate_x_forwarded_for(lb_scenario));

        // 场景 3：可能被攻击的头部
        let attack_headers = vec![
            ("X-Forwarded-For", "127.0.0.1, 8.8.8.8, 192.168.1.1"),
            ("X-Real-IP", "8.8.8.8"),
            ("X-Forwarded-Host", "evil.com"),
        ];

        let mut headers = HeaderMap::new();
        for (name, value) in attack_headers {
            headers.insert(name, value.parse().unwrap());
        }

        // 头部格式本身应该是有效的
        assert!(ValidationUtils::validate_headers(&headers));

        // 但内容可能需要进一步验证
        let xff_value = headers.get("X-Forwarded-For").unwrap().to_str().unwrap();
        assert!(ValidationUtils::validate_x_forwarded_for(xff_value));

        // 场景 4：RFC 7239 格式
        let rfc7239_header = "for=192.0.2.60;proto=https;by=203.0.113.43";
        assert!(ValidationUtils::validate_forwarded_header(rfc7239_header));
    }
}
