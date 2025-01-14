use ecstore::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    ops::Deref,
};

use crate::sys::Validator;

use super::{
    function::key_name::KeyName,
    utils::{path, wildcard},
    Error as IamError,
};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct ResourceSet(pub HashSet<Resource>);

impl ResourceSet {
    pub fn is_match(&self, resource: &str, conditons: &HashMap<String, Vec<String>>) -> bool {
        for re in self.0.iter() {
            if re.is_match(resource, conditons) {
                return true;
            }
        }

        false
    }

    pub fn match_resource(&self, resource: &str) -> bool {
        for re in self.0.iter() {
            if re.match_resource(resource) {
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

#[derive(Hash, Eq, PartialEq, Serialize, Deserialize, Clone, Debug)]
pub enum Resource {
    S3(String),
    Kms(String),
}

impl Resource {
    pub const S3_PREFIX: &'static str = "arn:aws:s3:::";

    pub fn is_match(&self, resource: &str, conditons: &HashMap<String, Vec<String>>) -> bool {
        let mut pattern = match self {
            Resource::S3(s) => s.to_owned(),
            Resource::Kms(s) => s.to_owned(),
        };
        if !conditons.is_empty() {
            for key in KeyName::COMMON_KEYS {
                if let Some(rvalue) = conditons.get(key.name()) {
                    if matches!(rvalue.first().map(|c| !c.is_empty()), Some(true)) {
                        pattern = pattern.replace(&key.var_name(), &rvalue[0]);
                    }
                }
            }
        }

        let cp = path::clean(resource);
        if cp != "." && cp == pattern.as_str() {
            return true;
        }

        wildcard::is_match(pattern, resource)
    }

    pub fn match_resource(&self, resource: &str) -> bool {
        self.is_match(resource, &HashMap::new())
    }
}

impl TryFrom<&str> for Resource {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
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
    fn is_valid(&self) -> Result<(), Error> {
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
        resource.is_match(object, &HashMap::new())
    }
}
