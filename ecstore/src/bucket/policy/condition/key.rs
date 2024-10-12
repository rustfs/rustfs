use super::keyname::{KeyName, ALL_SUPPORT_KEYS};
use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt, str::FromStr};

// 定义Key结构体
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    pub name: KeyName,
    pub variable: String,
}

impl Key {
    pub fn new(name: KeyName, variable: String) -> Self {
        Key { name, variable }
    }
    // IsValid - checks if key is valid or not.
    fn is_valid(&self) -> bool {
        ALL_SUPPORT_KEYS.iter().any(|supported| self.name == *supported)
    }

    // Is - checks if this key has the same key name or not.
    pub fn is(&self, name: &KeyName) -> bool {
        self.name == *name
    }

    // VarName - returns variable key name, such as "${aws:username}"
    pub fn var_name(&self) -> String {
        self.name.var_name()
    }

    // Name - returns key name which is stripped value of prefixes "aws:" and "s3:"
    pub fn name(&self) -> String {
        if !self.variable.is_empty() {
            format!("{}{}", self.name.name(), self.variable)
        } else {
            self.name.name().to_string()
        }
    }
}

impl FromStr for Key {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (name, variable) = if let Some(pos) = s.find('/') {
            (&s[..pos], &s[pos + 1..])
        } else {
            (s, "")
        };

        let keyname = KeyName::from_str(name)?;

        let key = Key {
            name: keyname,
            variable: variable.to_string(),
        };

        if key.is_valid() {
            Ok(key)
        } else {
            Err(Error::msg(format!("invalid condition key '{}'", s)))
        }
    }
}

impl Serialize for Key {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Key::from_str(s.as_str()).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.variable.is_empty() {
            write!(f, "{}/{}", self.name.as_str(), self.variable)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

#[derive(Debug, Default)]
pub struct KeySet(HashSet<Key>);

impl KeySet {
    pub fn new() -> Self {
        KeySet(HashSet::new())
    }
    // Add - add a key to key set
    pub fn add(&mut self, key: Key) {
        self.0.insert(key);
    }

    // Merge merges two key sets, duplicates are overwritten
    pub fn merge(&mut self, other: &KeySet) {
        for key in &other.0 {
            self.add(key.clone());
        }
    }

    // Match matches the input key name with current keySet
    pub fn match_key(&self, key: &Key) -> bool {
        self.0.contains(key)
    }

    // Difference - returns a key set contains difference of two keys
    pub fn difference(&self, other: &KeySet) -> KeySet {
        let mut result = KeySet::default();
        for key in &self.0 {
            if !other.match_key(key) {
                result.add(key.clone());
            }
        }
        result
    }

    // IsEmpty - returns whether key set is empty or not
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    // ToSlice - returns slice of keys
    fn to_slice(&self) -> Vec<Key> {
        self.0.iter().cloned().collect()
    }

    // NewKeySet - returns new KeySet contains given keys
    pub fn from_keys(keys: &Vec<Key>) -> KeySet {
        let mut set = KeySet::default();
        for key in keys {
            set.add(key.clone());
        }
        set
    }
}

impl fmt::Display for KeySet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.to_slice())
    }
}
