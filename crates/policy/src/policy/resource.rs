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
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    ops::Deref,
};

use super::{
    Error as IamError, Validator,
    function::key_name::KeyName,
    utils::{path, wildcard},
    variables::PolicyVariableResolver,
};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct ResourceSet(pub HashSet<Resource>);

impl ResourceSet {
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
    type Target = HashSet<Resource>;

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
        self.len() == other.len() && self.0.iter().all(|x| other.0.contains(x))
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum Resource {
    S3(String),
    Kms(String),
}

impl Resource {
    pub const S3_PREFIX: &'static str = "arn:aws:s3:::";

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
                    if let Some(rvalue) = conditions.get(key.name()) {
                        if matches!(rvalue.first().map(|c| !c.is_empty()), Some(true)) {
                            resolved_pattern = resolved_pattern.replace(&key.var_name(), &rvalue[0]);
                        }
                    }
                }
            }

            let cp = path::clean(resource);
            if cp != "." && cp == resolved_pattern.as_str() {
                return true;
            }

            if wildcard::is_match(resolved_pattern, resource) {
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
        let resource = if value.starts_with(Self::S3_PREFIX) {
            Resource::S3(value.strip_prefix(Self::S3_PREFIX).unwrap().into())
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
                if pattern.is_empty()
                    || pattern
                        .char_indices()
                        .find(|&(_, c)| c == '/' || c == '\\' || c == '.')
                        .map(|(i, _)| i)
                        .is_some()
                {
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
            Resource::Kms(s) => serializer.serialize_str(s),
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
    fn test_resource_is_match(resource: &str, object: &str) -> bool {
        let resource: Resource = resource.try_into().unwrap();

        pollster::block_on(resource.is_match(object, &HashMap::new()))
    }
}
