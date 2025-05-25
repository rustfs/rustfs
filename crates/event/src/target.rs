use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

///TargetID - Holds the identity and name string of the notification target
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TargetID {
    /// Destination ID
    pub id: String,
    /// Target name
    pub name: String,
}

impl TargetID {
    /// Create a new TargetID
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
        }
    }

    /// Convert to ARN
    pub fn to_arn(&self, region: &str) -> ARN {
        ARN {
            target_id: self.clone(),
            region: region.to_string(),
        }
    }

    /// The parsed string is TargetID
    pub fn parse(s: &str) -> Result<Self, String> {
        let tokens: Vec<&str> = s.split(':').collect();
        if tokens.len() != 2 {
            return Err(format!("Invalid TargetID format '{}'", s));
        }

        Ok(Self {
            id: tokens[0].to_string(),
            name: tokens[1].to_string(),
        })
    }
}

impl fmt::Display for TargetID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.id, self.name)
    }
}

impl Serialize for TargetID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TargetID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        TargetID::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// ARN - Amazon Resource Name structure
#[derive(Debug, Clone)]
pub struct ARN {
    /// Destination ID
    pub target_id: TargetID,
    /// region
    pub region: String,
}
