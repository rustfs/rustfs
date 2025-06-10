use super::func::InnerFunc;
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize, de::Visitor};
use std::{borrow::Cow, collections::HashMap, net::IpAddr};

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
                    if ip_net.contains(ip) {
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
                let mut cidr_str = Cow::from(v);
                if v.find('/').is_none() {
                    cidr_str.to_mut().push_str("/32");
                }

                cidr_str
                    .parse::<IpNetwork>()
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
    #[test_case(r#"{"aws:SourceIp": "2001:DB8:1234:5678::"}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/32"]); "4")]
    #[test_case(r#"{"aws:SourceIp": ["203.0.113.0/24","203.0.113.0"]}"#, new_func(Aws(AWSSourceIP), None, vec!["203.0.113.0/24", "203.0.113.0/32"]); "5")]
    #[test_case(r#"{"aws:SourceIp": ["2001:DB8:1234:5678::/64","203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/64", "203.0.113.0/24"]); "6")]
    #[test_case(r#"{"aws:SourceIp": ["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::"]}"#, new_func(Aws(AWSSourceIP),None, vec!["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::/32"]); "7")]
    #[test_case(r#"{"aws:SourceIp": ["2001:DB8:1234:5678::", "203.0.113.0"]}"#, new_func(Aws(AWSSourceIP), None, vec!["2001:DB8:1234:5678::/32", "203.0.113.0/32"]); "8")]
    #[test_case(r#"{"aws:SourceIp/a": "203.0.113.0/24"}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24"]); "9")]
    #[test_case(r#"{"aws:SourceIp/a": "203.0.113.0/24"}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24"]); "10")]
    #[test_case(r#"{"aws:SourceIp/a": "203.0.113.0"}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/32"]); "11")]
    #[test_case(r#"{"aws:SourceIp/a": "2001:DB8:1234:5678::/64"}"#, new_func(Aws(AWSSourceIP),Some("a".into()), vec!["2001:DB8:1234:5678::/64"]); "12")]
    #[test_case(r#"{"aws:SourceIp/a": "2001:DB8:1234:5678::"}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/32"]); "13")]
    #[test_case(r#"{"aws:SourceIp/a": ["203.0.113.0/24", "203.0.113.0"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["203.0.113.0/24", "203.0.113.0/32"]); "14")]
    #[test_case(r#"{"aws:SourceIp/a": ["2001:DB8:1234:5678::/64", "203.0.113.0/24"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/64", "203.0.113.0/24"]); "15")]
    #[test_case(r#"{"aws:SourceIp/a": ["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::"]}"#, new_func(Aws(AWSSourceIP),Some("a".into()), vec!["2001:DB8:1234:5678::/64", "2001:DB8:1234:5678::/32"]); "16")]
    #[test_case(r#"{"aws:SourceIp/a": ["2001:DB8:1234:5678::", "203.0.113.0"]}"#, new_func(Aws(AWSSourceIP), Some("a".into()), vec!["2001:DB8:1234:5678::/32", "203.0.113.0/32"]); "17")]
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
