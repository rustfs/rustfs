use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    ops::Deref,
};

use serde::{Deserialize, Serialize};

use super::{
    function::key_name::KeyName,
    utils::{path, wildcard},
    Error, Validator,
};

#[derive(Serialize, Deserialize, Clone, Default)]
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
}

impl Deref for ResourceSet {
    type Target = HashSet<Resource>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Validator for ResourceSet {
    fn is_valid(&self) -> Result<(), Error> {
        for resource in self.0.iter() {
            resource.is_valid()?;
        }

        Ok(())
    }
}

#[derive(Hash, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub enum Resource {
    S3(String),
    Kms(String),
}

impl Resource {
    pub const S3_PREFIX: &str = "arn:aws:s3:::";

    pub fn is_match(&self, resource: &str, conditons: &HashMap<String, Vec<String>>) -> bool {
        let mut pattern = match self {
            Resource::S3(s) => s.to_owned(),
            Resource::Kms(s) => s.to_owned(),
        };

        if !conditons.is_empty() {
            for key in KeyName::COMMON_KEYS {
                if let Some(rvalue) = conditons.get(key.name()) {
                    if matches!(rvalue.first().map(|c| !c.is_empty()), Some(true)) {
                        pattern = pattern.replace(key.name(), &rvalue[0]);
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
}

impl TryFrom<&str> for Resource {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let resource = if value.starts_with(Self::S3_PREFIX) {
            Resource::S3(value[Self::S3_PREFIX.len() + 1..].into())
        } else {
            return Err(Error::InvalidResource("unknown".into(), value.into()));
        };

        resource.is_valid()?;
        Ok(resource)
    }
}

impl Validator for Resource {
    fn is_valid(&self) -> Result<(), Error> {
        match self {
            Self::S3(pattern) => {
                if pattern.is_empty() || pattern.starts_with('/') {
                    return Err(Error::InvalidResource("s3".into(), pattern.into()));
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
                    return Err(Error::InvalidResource("kms".into(), pattern.into()));
                }
            }
        }
        Ok(())
    }
}
