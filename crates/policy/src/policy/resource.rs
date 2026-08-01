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

use crate::error::{Error, Result};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, Error as DeError, Visitor},
};
use std::{collections::HashMap, fmt, ops::Deref};

use super::{
    Error as IamError, Validator,
    function::key_name::KeyName,
    utils::{path, wildcard},
    variables::PolicyVariableResolver,
};

#[derive(Clone, Default, Debug)]
pub struct ResourceSet(pub Vec<Resource>);

impl Serialize for ResourceSet {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for resource in &self.0 {
            let resource_str = match resource {
                Resource::S3(value) => format!("{}{}", Resource::S3_PREFIX, value),
                Resource::Kms(value) => format!("{}{}", Resource::KMS_PREFIX, value),
            };
            seq.serialize_element(&resource_str)?;
        }
        seq.end()
    }
}

impl ResourceSet {
    /// Returns true if the resource set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn push_unique(&mut self, resource: Resource) {
        if !self.0.contains(&resource) {
            self.0.push(resource);
        }
    }

    pub async fn is_match(&self, resource: &str, conditions: &HashMap<String, Vec<String>>) -> bool {
        self.is_match_with_resolver(resource, conditions, None).await
    }

    pub async fn is_match_with_resolver(
        &self,
        resource: &str,
        conditions: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        for re in self.0.iter() {
            if re.is_match_with_resolver(resource, conditions, resolver).await {
                return true;
            }
        }

        false
    }

    pub async fn match_resource(&self, resource: &str) -> bool {
        for re in self.0.iter() {
            if re.match_resource(resource).await {
                return true;
            }
        }

        false
    }
}

impl Deref for ResourceSet {
    type Target = [Resource];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Validator for ResourceSet {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        for resource in self.0.iter() {
            resource.is_valid()?;
        }

        Ok(())
    }
}

impl PartialEq for ResourceSet {
    fn eq(&self, other: &Self) -> bool {
        self.0.iter().all(|x| other.0.contains(x)) && other.0.iter().all(|x| self.0.contains(x))
    }
}

impl<'de> Deserialize<'de> for ResourceSet {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ResourceOrVecVisitor;

        impl<'de> Visitor<'de> for ResourceOrVecVisitor {
            type Value = ResourceSet;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or an array of strings")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                let resource = Resource::try_from(value).map_err(|e| E::custom(format!("invalid resource: {}", e)))?;
                let mut resources = ResourceSet::default();
                resources.push_unique(resource);
                Ok(resources)
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
                A::Error: DeError,
            {
                let mut resources = ResourceSet(Vec::with_capacity(seq.size_hint().unwrap_or(0)));
                while let Some(value) = seq.next_element::<String>()? {
                    match Resource::try_from(value.as_str()) {
                        Ok(resource) => {
                            resources.push_unique(resource);
                        }
                        Err(e) => {
                            return Err(A::Error::custom(format!("invalid resource: {}", e)));
                        }
                    }
                }
                Ok(resources)
            }
        }

        deserializer.deserialize_any(ResourceOrVecVisitor)
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum Resource {
    S3(String),
    Kms(String),
}

impl Resource {
    pub const S3_PREFIX: &'static str = "arn:aws:s3:::";
    /// KMS ARNs use the same empty-account form as [`Self::S3_PREFIX`]; the suffix is
    /// `key/<key_id>` (wildcards allowed in the id), `alias/<name>`, or a bare `*`.
    pub const KMS_PREFIX: &'static str = "arn:aws:kms:::";
    /// Resource-type segment for key ids; request-side KMS resource strings are
    /// `key/<key_id>` so they line up with these patterns.
    pub const KMS_KEY_SEGMENT: &'static str = "key/";
    /// Resource-type segment reserved for key aliases. Alias patterns parse and
    /// validate, but requests are always evaluated against `key/<key_id>` strings,
    /// so an alias pattern matches nothing until alias resolution lands.
    pub const KMS_ALIAS_SEGMENT: &'static str = "alias/";

    pub fn is_kms(&self) -> bool {
        matches!(self, Resource::Kms(_))
    }

    pub async fn is_match(&self, resource: &str, conditions: &HashMap<String, Vec<String>>) -> bool {
        self.is_match_with_resolver(resource, conditions, None).await
    }

    pub async fn is_match_with_resolver(
        &self,
        resource: &str,
        conditions: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        let pattern = match self {
            Resource::S3(s) => s.to_owned(),
            Resource::Kms(s) => s.to_owned(),
        };

        let patterns = if let Some(res) = resolver {
            super::variables::resolve_aws_variables(&pattern, res).await
        } else {
            vec![pattern.clone()]
        };

        for pattern in patterns {
            let mut resolved_pattern = pattern;

            // Apply condition substitutions
            if !conditions.is_empty() {
                for key in KeyName::COMMON_KEYS {
                    if let Some(rvalue) = conditions.get(key.name())
                        && matches!(rvalue.first().map(|c| !c.is_empty()), Some(true))
                    {
                        resolved_pattern = resolved_pattern.replace(&key.var_name(), &rvalue[0]);
                    }
                }
            }

            let cp = path::clean(resource);
            if cp != "." && cp == resolved_pattern.as_str() {
                return true;
            }

            if cp != "." && wildcard::is_match(resolved_pattern, &cp) {
                return true;
            }
        }

        false
    }

    pub async fn match_resource(&self, resource: &str) -> bool {
        self.is_match(resource, &HashMap::new()).await
    }
}

impl TryFrom<&str> for Resource {
    type Error = Error;
    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let resource = if let Some(suffix) = value.strip_prefix(Self::S3_PREFIX) {
            Resource::S3(suffix.into())
        } else if let Some(suffix) = value.strip_prefix(Self::KMS_PREFIX) {
            Resource::Kms(suffix.into())
        } else {
            return Err(IamError::InvalidResource("unknown".into(), value.into()).into());
        };

        resource.is_valid()?;
        Ok(resource)
    }
}

impl Validator for Resource {
    type Error = Error;
    fn is_valid(&self) -> std::result::Result<(), Error> {
        match self {
            Self::S3(pattern) => {
                if pattern.is_empty() || pattern.starts_with('/') {
                    return Err(IamError::InvalidResource("s3".into(), pattern.into()).into());
                }
            }
            Self::Kms(pattern) => {
                // A bare `*` matches every key resource; anything else must carry a
                // resource-type segment. Key ids never contain separators (the KMS
                // backends reject them), while alias names may nest ("alias/aws/s3").
                let well_formed = pattern == "*"
                    || pattern
                        .strip_prefix(Self::KMS_KEY_SEGMENT)
                        .is_some_and(|id| !id.is_empty() && !id.contains('/') && !id.contains('\\'))
                    || pattern
                        .strip_prefix(Self::KMS_ALIAS_SEGMENT)
                        .is_some_and(|name| !name.is_empty() && !name.contains('\\'));
                if !well_formed {
                    return Err(IamError::InvalidResource("kms".into(), pattern.into()).into());
                }
            }
        }
        Ok(())
    }
}

impl Serialize for Resource {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Resource::S3(s) => serializer.serialize_str(&format!("{}{}", Self::S3_PREFIX, s)),
            Resource::Kms(s) => serializer.serialize_str(&format!("{}{}", Self::KMS_PREFIX, s)),
        }
    }
}

impl<'de> Deserialize<'de> for Resource {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Resource::try_from(value.as_str()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use crate::policy::resource::Resource;
    use std::collections::HashMap;
    use test_case::test_case;

    #[test_case("arn:aws:s3:::*","mybucket" => true; "1")]
    #[test_case("arn:aws:s3:::*","mybucket/myobject" => true; "2")]
    #[test_case("arn:aws:s3:::mybucket*","mybucket" => true; "3")]
    #[test_case("arn:aws:s3:::mybucket*","mybucket/myobject" => true; "4")]
    #[test_case("arn:aws:s3:::*/*","mybucket/myobject"=> true; "5")]
    #[test_case("arn:aws:s3:::mybucket/*","mybucket/myobject" => true; "6")]
    #[test_case("arn:aws:s3:::mybucket*/myobject","mybucket/myobject" => true; "7")]
    #[test_case("arn:aws:s3:::mybucket*/myobject","mybucket100/myobject" => true; "8")]
    #[test_case("arn:aws:s3:::mybucket?0/2010/photos/*","mybucket20/2010/photos/1.jpg" => true; "9")]
    #[test_case("arn:aws:s3:::mybucket","mybucket" => true; "10")]
    #[test_case("arn:aws:s3:::mybucket?0","mybucket30" => true; "11")]
    #[test_case("arn:aws:s3:::*/*","mybucket" => false; "12")]
    #[test_case("arn:aws:s3:::mybucket/*","mybucket10/myobject" => false; "13")]
    #[test_case("arn:aws:s3:::mybucket?0/2010/photos/*","mybucket0/2010/photos/1.jpg" => false; "14")]
    #[test_case("arn:aws:s3:::mybucket","mybucket/myobject" => false; "15")]
    #[test_case("arn:aws:s3:::attacker-bucket/*","attacker-bucket/../victim-bucket/evil.txt" => false; "16")]
    #[test_case("arn:aws:s3:::attacker-bucket/*","attacker-bucket/safe/../../victim-bucket/evil.txt" => false; "17")]
    #[test_case("arn:aws:kms:::key/mykey","key/mykey" => true; "kms exact key")]
    #[test_case("arn:aws:kms:::key/mykey","key/otherkey" => false; "kms wrong key")]
    #[test_case("arn:aws:kms:::key/*","key/mykey" => true; "kms key wildcard")]
    #[test_case("arn:aws:kms:::key/app-*","key/app-primary" => true; "kms key prefix wildcard")]
    #[test_case("arn:aws:kms:::key/app-*","key/backup-primary" => false; "kms key prefix mismatch")]
    #[test_case("arn:aws:kms:::key/mykey?","key/mykey1" => true; "kms key question mark")]
    #[test_case("arn:aws:kms:::*","key/mykey" => true; "kms bare star")]
    #[test_case("arn:aws:kms:::key/mykey","key/mykey/../otherkey" => false; "kms traversal cleaned")]
    #[test_case("arn:aws:kms:::alias/myalias","key/myalias" => false; "kms alias never matches key")]
    #[test_case("arn:aws:kms:::alias/*","key/mykey" => false; "kms alias wildcard never matches key")]
    #[test_case("arn:aws:kms:::key/mykey","mykey" => false; "kms bare id lacks key segment")]
    fn test_resource_is_match(resource: &str, object: &str) -> bool {
        let resource: Resource = resource.try_into().unwrap();

        pollster::block_on(resource.is_match(object, &HashMap::new()))
    }

    #[test_case("arn:aws:kms:::key/mykey" => true; "key id parses")]
    #[test_case("arn:aws:kms:::key/*" => true; "key wildcard parses")]
    #[test_case("arn:aws:kms:::key/app-key.v2" => true; "key id with dot parses")]
    #[test_case("arn:aws:kms:::alias/myalias" => true; "alias parses")]
    #[test_case("arn:aws:kms:::alias/aws/s3" => true; "nested alias parses")]
    #[test_case("arn:aws:kms:::*" => true; "bare star parses")]
    #[test_case("arn:aws:kms:::" => false; "empty suffix rejected")]
    #[test_case("arn:aws:kms:::key/" => false; "empty key id rejected")]
    #[test_case("arn:aws:kms:::alias/" => false; "empty alias name rejected")]
    #[test_case("arn:aws:kms:::mykey" => false; "missing resource type segment rejected")]
    #[test_case("arn:aws:kms:::key/a/b" => false; "separator in key id rejected")]
    #[test_case("arn:aws:kms:::key/a\\b" => false; "backslash in key id rejected")]
    #[test_case("arn:aws:kms:us-east-1:123456789012:key/mykey" => false; "region and account form rejected")]
    fn test_kms_resource_parse(resource: &str) -> bool {
        Resource::try_from(resource).is_ok()
    }

    #[test]
    fn test_kms_resource_serialization_round_trip() {
        for raw in [
            "arn:aws:kms:::key/mykey",
            "arn:aws:kms:::key/*",
            "arn:aws:kms:::alias/myalias",
            "arn:aws:kms:::*",
        ] {
            let resource = Resource::try_from(raw).expect("KMS resource should parse");
            assert!(resource.is_kms());

            let json = serde_json::to_string(&resource).expect("KMS resource should serialize");
            assert_eq!(json, format!("\"{raw}\""), "serialization must write back the full ARN");

            let round_trip: Resource = serde_json::from_str(&json).expect("serialized KMS resource should deserialize");
            assert_eq!(round_trip, resource);
        }
    }

    #[test]
    fn test_kms_resource_set_serialization_round_trip() {
        use crate::policy::resource::ResourceSet;

        let set: ResourceSet =
            serde_json::from_str(r#"["arn:aws:kms:::key/app-*","arn:aws:kms:::alias/myalias"]"#).expect("set should parse");
        assert_eq!(set.len(), 2);

        let json = serde_json::to_string(&set).expect("set should serialize");
        assert_eq!(json, r#"["arn:aws:kms:::key/app-*","arn:aws:kms:::alias/myalias"]"#);

        let round_trip: ResourceSet = serde_json::from_str(&json).expect("serialized set should deserialize");
        assert_eq!(round_trip, set);
    }
}
