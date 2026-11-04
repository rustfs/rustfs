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

use super::func::InnerFunc;
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize, de::Visitor};
use std::{collections::HashMap, net::IpAddr};

pub type AddrFunc = InnerFunc<AddrFuncValue>;

impl AddrFunc {
    pub(crate) fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        for inner in self.0.iter() {
            let rvalues = values.get(inner.key.name().as_str()).map(|t| t.iter()).unwrap_or_default();

            for r in rvalues {
                let Ok(ip) = r.parse::<IpAddr>() else {
                    return false;
                };

                for ip_net in inner.values.0.iter() {
                    let canonical = ip.to_canonical();
                    let matches_mapped_network = match (ip_net, canonical) {
                        (IpNetwork::V6(network), IpAddr::V4(address))
                            if network.prefix() >= 96 && network.ip().to_ipv4_mapped().is_some() =>
                        {
                            network.contains(address.to_ipv6_mapped())
                        }
                        _ => false,
                    };
                    if ip_net.contains(ip) || ip_net.contains(canonical) || matches_mapped_network {
                        return true;
                    }
                }
            }
        }

        false
    }
}

#[derive(Serialize, Clone, PartialEq, Eq, Debug)]
#[serde(transparent)]
pub struct AddrFuncValue(Vec<IpNetwork>);

impl<'de> Deserialize<'de> for AddrFuncValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AddrFuncValueVisitor;
        impl<'d> Visitor<'d> for AddrFuncValueVisitor {
            type Value = AddrFuncValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("cidr string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(AddrFuncValue(vec![Self::cidr::<E>(v)?]))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'d>,
            {
                Ok(AddrFuncValue({
                    let mut data = Vec::with_capacity(seq.size_hint().unwrap_or_default());
                    while let Some(v) = seq.next_element::<&str>()? {
                        data.push(Self::cidr::<A::Error>(v)?)
                    }
                    data
                }))
            }
        }

        impl AddrFuncValueVisitor {
            fn cidr<E: serde::de::Error>(v: &str) -> Result<IpNetwork, E> {
                v.parse::<IpNetwork>()
                    .map_err(|_| E::custom(format!("{v} can not be parsed to CIDR")))
            }
        }

        deserializer.deserialize_any(AddrFuncValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::{AddrFunc, AddrFuncValue};
    use crate::policy::function::func::FuncKeyValue;
    use crate::policy::function::{
        key::Key,
        key_name::AwsKeyName::*,
        key_name::KeyName::{self, *},
    };
    use test_case::test_case;

    #[test]
    fn source_ip_matches_ipv4_and_mapped_forms_across_protocols() {
        for network in ["203.0.113.0/24", "::ffff:203.0.113.0/120"] {
            let function: AddrFunc =
                serde_json::from_str(&format!(r#"{{"aws:SourceIp":"{network}"}}"#)).expect("parse source IP condition");
            for address in ["203.0.113.9", "::ffff:203.0.113.9"] {
                assert!(
                    function.evaluate(&std::collections::HashMap::from([("SourceIp".to_string(), vec![address.to_string()]),])),
                    "{network} must match {address}"
                );
            }
            for address in ["198.51.100.9", "::ffff:198.51.100.9", "2001:db8::9"] {
                assert!(
                    !function.evaluate(&std::collections::HashMap::from([("SourceIp".to_string(), vec![address.to_string()]),])),
                    "{network} must not match {address}"
                );
            }
        }
    }

    #[test]
    fn bare_address_matches_only_the_named_host() {
        for (host, neighbor, prefix) in [
            ("2001:db8:85a3::8a2e:370:7334", "2001:db8:ffff::1", 128),
            ("203.0.113.7", "203.0.113.8", 32),
        ] {
            let function: AddrFunc =
                serde_json::from_str(&format!(r#"{{"aws:SourceIp":"{host}"}}"#)).expect("parse bare IP address condition");
            assert_eq!(function.0[0].values.0[0].prefix(), prefix);
            assert!(function.evaluate(&std::collections::HashMap::from([("SourceIp".to_string(), vec![host.to_string()])])));
            assert!(!function.evaluate(&std::collections::HashMap::from([("SourceIp".to_string(), vec![neighbor.to_string()])])));
        }
    }

    fn new_func(name: KeyName, variable: Option<String>, value: Vec<&str>) -> AddrFunc {
        AddrFunc {
            0: vec![FuncKeyValue {
                key: Key { name, variable },
                values: AddrFuncValue(value.into_iter().filter_map(|x| x.parse().ok()).collect()),
            }],
        }
    }

    #[test_case(r#"{"aws:SourceIp": "203.0.113.0/24"}"#, new_func(Aws(AWSSourceIP), None, vec!["203.0.113.0/24"]); "1")]
    #[test_case(r#"{"aws:SourceIp": "203.0.113.0"}"#, new_func(Aws(AWSSourceIP), None, vec!["203.0.113.0/32"]); "2")]
    #[test_case(r#"{"aws:SourceIp": "2001:DB8:1234:5678::/64"}"#, new_func(Aws(AWSSourceIP),None, vec!["2001:DB8:1234:5678::/64"]); "3")]
    #[test_case(r#"{"aws:SourceIp": "2001:DB8:1234:5678::"}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/128"]); "4")]
    #[test_case(r#"{"aws:SourceIp": ["203.0.113.0/24","203.0.113.0"]}"#, new_func(Aws(AWSSourceIP), None, vec!["203.0.113.0/24", "203.0.113.0/32"]); "5")]
    #[test_case(r#"{"aws:SourceIp": ["2001:DB8:1234:5678::/64","203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/64", "203.0.113.0/24"]); "6")]
    #[test_case(r#"{"aws:SourceIp": ["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::"]}"#, new_func(Aws(AWSSourceIP),None, vec!["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::/128"]); "7")]
    #[test_case(r#"{"aws:SourceIp": ["2001:DB8:1234:5678::", "203.0.113.0"]}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/128", "203.0.113.0/32"]); "8")]
    #[test_case(r#"{"aws:SourceIp/a": "203.0.113.0/24"}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24"]); "9")]
    #[test_case(r#"{"aws:SourceIp/a": "203.0.113.0/24"}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24"]); "10")]
    #[test_case(r#"{"aws:SourceIp/a": "203.0.113.0"}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/32"]); "11")]
    #[test_case(r#"{"aws:SourceIp/a": "2001:DB8:1234:5678::/64"}"#, new_func(Aws(AWSSourceIP),Some("a".into()), vec!["2001:DB8:1234:5678::/64"]); "12")]
    #[test_case(r#"{"aws:SourceIp/a": "2001:DB8:1234:5678::"}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/128"]); "13")]
    #[test_case(r#"{"aws:SourceIp/a": ["203.0.113.0/24", "203.0.113.0"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24", "203.0.113.0/32"]); "14")]
    #[test_case(r#"{"aws:SourceIp/a": ["2001:DB8:1234:5678::/64", "203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/64", "203.0.113.0/24"]); "15")]
    #[test_case(r#"{"aws:SourceIp/a": ["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::"]}"#, new_func(Aws(AWSSourceIP),Some("a".into()), vec!["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::/128"]); "16")]
    #[test_case(r#"{"aws:SourceIp/a": ["2001:DB8:1234:5678::", "203.0.113.0"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/128", "203.0.113.0/32"]); "17")]
    fn test_deser(input: &str, expect: AddrFunc) -> Result<(), serde_json::Error> {
        let v: AddrFunc = serde_json::from_str(input)?;
        assert_eq!(v, expect);
        Ok(())
    }

    #[test_case(r#"{"aws:SourceIp":["203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), None, vec!["203.0.113.0/24"]); "1")]
    #[test_case(r#"{"aws:SourceIp":["203.0.113.0/32"]}"#, new_func(Aws(AWSSourceIP), None, vec!["203.0.113.0/32"]); "2")]
    #[test_case(r#"{"aws:SourceIp":["2001:db8:1234:5678::/64"]}"#, new_func(Aws(AWSSourceIP),None, vec!["2001:DB8:1234:5678::/64"]); "3")]
    #[test_case(r#"{"aws:SourceIp":["2001:db8:1234:5678::/32"]}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/32"]); "4")]
    #[test_case(r#"{"aws:SourceIp":["203.0.113.0/24","203.0.113.0/32"]}"#, new_func(Aws(AWSSourceIP), None, vec!["203.0.113.0/24", "203.0.113.0/32"]); "5")]
    #[test_case(r#"{"aws:SourceIp":["2001:db8:1234:5678::/64","203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/64", "203.0.113.0/24"]); "6")]
    #[test_case(r#"{"aws:SourceIp":["2001:db8:1234:5678::/64","2001:db8:1234:5678::/32"]}"#, new_func(Aws(AWSSourceIP),None, vec!["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::/32"]); "7")]
    #[test_case(r#"{"aws:SourceIp":["2001:db8:1234:5678::/32","203.0.113.0/32"]}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/32", "203.0.113.0/32"]); "8")]
    #[test_case(r#"{"aws:SourceIp/a":["203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24"]); "9")]
    #[test_case(r#"{"aws:SourceIp/a":["203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24"]); "10")]
    #[test_case(r#"{"aws:SourceIp/a":["203.0.113.0/32"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/32"]); "11")]
    #[test_case(r#"{"aws:SourceIp/a":["2001:db8:1234:5678::/64"]}"#, new_func(Aws(AWSSourceIP),Some("a".into()), vec!["2001:DB8:1234:5678::/64"]); "12")]
    #[test_case(r#"{"aws:SourceIp/a":["2001:db8:1234:5678::/32"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/32"]); "13")]
    #[test_case(r#"{"aws:SourceIp/a":["203.0.113.0/24","203.0.113.0/32"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24", "203.0.113.0/32"]); "14")]
    #[test_case(r#"{"aws:SourceIp/a":["2001:db8:1234:5678::/64","203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/64", "203.0.113.0/24"]); "15")]
    #[test_case(r#"{"aws:SourceIp/a":["2001:db8:1234:5678::/64","2001:db8:1234:5678::/32"]}"#, new_func(Aws(AWSSourceIP),Some("a".into()), vec!["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::/32"]); "16")]
    #[test_case(r#"{"aws:SourceIp/a":["2001:db8:1234:5678::/32","203.0.113.0/32"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/32", "203.0.113.0/32"]); "17")]
    fn test_ser(expect: &str, input: AddrFunc) -> Result<(), serde_json::Error> {
        let v = serde_json::to_string(&input)?;
        assert_eq!(v, expect);
        Ok(())
    }
}
