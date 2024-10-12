use crate::error::{Error, Result};
use crate::{
    bucket::policy::condition::keyname::COMMOM_KEYS,
    utils::{self, wildcard},
};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

// 定义ResourceARNType枚举类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, Default)]
pub enum ResourceARNType {
    #[default]
    UnknownARN,
    ResourceARNS3,
    ResourceARNKMS,
}

impl fmt::Display for ResourceARNType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceARNType::UnknownARN => write!(f, ""),
            ResourceARNType::ResourceARNS3 => write!(f, "{}", RESOURCE_ARN_PREFIX),
            ResourceARNType::ResourceARNKMS => write!(f, "{}", RESOURCE_ARN_KMS_PREFIX),
        }
    }
}

// 定义资源ARN前缀
const RESOURCE_ARN_PREFIX: &str = "arn:aws:s3:::";
const RESOURCE_ARN_KMS_PREFIX: &str = "arn:rustfs:kms::::";

// 定义Resource结构体
#[derive(Debug, Default, PartialEq, Eq, Hash, Clone)]
pub struct Resource {
    pattern: String,
    rtype: ResourceARNType,
}

impl Resource {
    pub fn validate_bucket(&self, bucket: &str) -> Result<()> {
        self.validate()?;
        if !wildcard::match_pattern(&self.pattern, bucket)
            && !wildcard::match_as_pattern_prefix(&self.pattern, format!("{}/", bucket).as_str())
        {
            return Err(Error::msg("bucket name does not match"));
        }
        Ok(())
    }
    pub fn validate(&self) -> Result<()> {
        if !self.is_valid() {
            Err(Error::msg("invalid resource"))
        } else {
            Ok(())
        }
    }
    pub fn is_valid(&self) -> bool {
        if self.rtype == ResourceARNType::UnknownARN {
            return false;
        }
        if self.is_s3() && self.pattern.starts_with("/") {
            return false;
        }
        if self.is_kms() && self.pattern.as_bytes().iter().any(|&v| v == b'/' || v == b'\\' || v == b'.') {
            return false;
        }

        !self.pattern.is_empty()
    }
    pub fn is_s3(&self) -> bool {
        self.rtype == ResourceARNType::ResourceARNS3
    }
    pub fn is_kms(&self) -> bool {
        self.rtype == ResourceARNType::ResourceARNKMS
    }
    pub fn is_bucket_pattern(&self) -> bool {
        !self.pattern.contains("/") || self.pattern.eq("*")
    }
    pub fn is_object_pattern(&self) -> bool {
        self.pattern.contains("/") || self.pattern.contains("*")
    }
    pub fn is_match(&self, res: &str, condition_values: &HashMap<String, Vec<String>>) -> bool {
        let mut pattern = res.to_string();
        if !condition_values.is_empty() {
            for key in COMMOM_KEYS.iter() {
                if let Some(vals) = condition_values.get(key.name()) {
                    if let Some(v0) = vals.first() {
                        pattern = pattern.replace(key.name(), v0);
                    }
                }
            }
        }

        let cp = utils::path::clean(res);

        if cp != "." && cp == pattern {
            return true;
        }

        wildcard::match_pattern(&pattern, res)
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.rtype, self.pattern)
    }
}

impl FromStr for Resource {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(RESOURCE_ARN_PREFIX) {
            let pattern = {
                if let Some(val) = s.strip_prefix(RESOURCE_ARN_PREFIX) {
                    val.to_string()
                } else {
                    s.to_string()
                }
            };
            Ok(Self {
                rtype: ResourceARNType::ResourceARNS3,
                pattern,
            })
        } else if s.starts_with(RESOURCE_ARN_KMS_PREFIX) {
            let pattern = {
                if let Some(val) = s.strip_prefix(RESOURCE_ARN_KMS_PREFIX) {
                    val.to_string()
                } else {
                    s.to_string()
                }
            };
            Ok(Self {
                rtype: ResourceARNType::ResourceARNS3,
                pattern,
            })
        } else {
            Ok(Self {
                rtype: ResourceARNType::UnknownARN,
                pattern: "".to_string(),
            })
        }
    }
}

impl Serialize for Resource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for Resource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Resource;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("string resource")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match Resource::from_str(value) {
                    Ok(res) => Ok(res),
                    Err(_) => Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(value), &self)),
                }
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct ResourceSet(HashSet<Resource>);

impl ResourceSet {
    pub fn validate_bucket(&self, bucket: &str) -> Result<()> {
        for res in self.0.iter() {
            res.validate_bucket(bucket)?;
        }
        Ok(())
    }

    pub fn is_match(&self, res: &str, condition_values: &HashMap<String, Vec<String>>) -> bool {
        for item in self.0.iter() {
            if item.is_match(res, condition_values) {
                return true;
            }
        }

        false
    }
    pub fn object_resource_exists(&self) -> bool {
        for res in self.0.iter() {
            if res.is_object_pattern() {
                return true;
            }
        }
        false
    }
    pub fn bucket_resource_exists(&self) -> bool {
        for res in self.0.iter() {
            if res.is_bucket_pattern() {
                return true;
            }
        }
        false
    }
}

impl AsRef<HashSet<Resource>> for ResourceSet {
    fn as_ref(&self) -> &HashSet<Resource> {
        &self.0
    }
}

// impl Serialize for ResourceSet {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         let ress: Vec<Resource> = self.0.iter().cloned().collect();
//         serializer.collect_seq(ress)
//     }
// }

// impl<'de> Deserialize<'de> for ResourceSet {
//     fn deserialize<D>(deserializer: D) -> Result<ResourceSet, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let vec: Vec<Resource> = Deserialize::deserialize(deserializer)?;
//         let ha: HashSet<Resource> = vec.into_iter().collect();
//         Ok(ResourceSet(ha))
//     }
// }
