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

use crate::TargetError;
use rustfs_config::notify::{ARN_PREFIX, DEFAULT_ARN_PARTITION, DEFAULT_ARN_SERVICE};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TargetIDError {
    #[error("Invalid TargetID format '{0}', expect 'ID:Name'")]
    InvalidFormat(String),
}

/// Target ID, used to identify notification targets
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct TargetID {
    pub id: String,
    pub name: String,
}

impl TargetID {
    pub fn new(id: String, name: String) -> Self {
        Self { id, name }
    }

    /// Convert to string representation
    pub fn to_id_string(&self) -> String {
        format!("{}:{}", self.id, self.name)
    }

    /// Create an ARN
    pub fn to_arn(&self, region: &str) -> ARN {
        ARN {
            target_id: self.clone(),
            region: region.to_string(),
            service: DEFAULT_ARN_SERVICE.to_string(),     // Default Service
            partition: DEFAULT_ARN_PARTITION.to_string(), // Default partition
        }
    }
}

impl fmt::Display for TargetID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.id, self.name)
    }
}

impl FromStr for TargetID {
    type Err = TargetIDError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        if parts.len() == 2 {
            Ok(TargetID {
                id: parts[0].to_string(),
                name: parts[1].to_string(),
            })
        } else {
            Err(TargetIDError::InvalidFormat(s.to_string()))
        }
    }
}

impl Serialize for TargetID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_id_string())
    }
}

impl<'de> Deserialize<'de> for TargetID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        TargetID::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Error)]
pub enum ArnError {
    #[error("Invalid ARN format '{0}'")]
    InvalidFormat(String),
    #[error("ARN component missing")]
    MissingComponents,
}

/// ARN - AWS resource name representation
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ARN {
    pub target_id: TargetID,
    pub region: String,
    // Service types, such as "sqs", "sns", "lambda", etc. This defaults to "sqs" to match the Go example.
    pub service: String,
    // Partitions such as "aws", "aws-cn", or customizations such as "rustfs", etc.
    pub partition: String,
}

impl ARN {
    pub fn new(target_id: TargetID, region: String) -> Self {
        ARN {
            target_id,
            region,
            service: DEFAULT_ARN_SERVICE.to_string(),     // Default is sqs
            partition: DEFAULT_ARN_PARTITION.to_string(), // Default is rustfs partition
        }
    }

    /// Returns the string representation of ARN
    /// Returns the ARN string in the format "{ARN_PREFIX}:{region}:{target_id}"
    #[allow(clippy::inherent_to_string)]
    pub fn to_arn_string(&self) -> String {
        if self.target_id.id.is_empty() && self.target_id.name.is_empty() && self.region.is_empty() {
            return String::new();
        }
        format!("{}:{}:{}", ARN_PREFIX, self.region, self.target_id.to_id_string())
    }

    /// Parsing ARN from string
    pub fn parse(s: &str) -> Result<Self, TargetError> {
        if !s.starts_with(ARN_PREFIX) {
            return Err(TargetError::InvalidARN(s.to_string()));
        }

        let tokens: Vec<&str> = s.split(':').collect();
        if tokens.len() != 6 {
            return Err(TargetError::InvalidARN(s.to_string()));
        }

        if tokens[4].is_empty() || tokens[5].is_empty() {
            return Err(TargetError::InvalidARN(s.to_string()));
        }

        Ok(ARN {
            region: tokens[3].to_string(),
            target_id: TargetID {
                id: tokens[4].to_string(),
                name: tokens[5].to_string(),
            },
            service: tokens[2].to_string(),   // Service Type
            partition: tokens[1].to_string(), // Partition
        })
    }
}

impl fmt::Display for ARN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.target_id.id.is_empty() && self.target_id.name.is_empty() && self.region.is_empty() {
            // Returns an empty string if all parts are empty
            return Ok(());
        }
        write!(
            f,
            "arn:{}:{}:{}:{}:{}",
            self.partition, self.service, self.region, self.target_id.id, self.target_id.name
        )
    }
}

impl FromStr for ARN {
    type Err = ArnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() < 6 {
            return Err(ArnError::InvalidFormat(s.to_string()));
        }

        if parts[0] != "arn" {
            return Err(ArnError::InvalidFormat(s.to_string()));
        }

        let partition = parts[1].to_string();
        let service = parts[2].to_string();
        let region = parts[3].to_string();
        let id = parts[4].to_string();
        let name = parts[5..].join(":"); // The name section may contain colons, although this is not usually the case in SQS ARN

        if id.is_empty() || name.is_empty() {
            return Err(ArnError::MissingComponents);
        }

        Ok(ARN {
            target_id: TargetID { id, name },
            region,
            service,
            partition,
        })
    }
}

// Serialization implementation
impl Serialize for ARN {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_arn_string())
    }
}

impl<'de> Deserialize<'de> for ARN {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // deserializer.deserialize_str(ARNVisitor)
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            // Handle an empty ARN string, for example, creating an empty or default Arn instance
            // Or return an error based on business logic
            // Here we create an empty TargetID and region Arn
            return Ok(ARN {
                target_id: TargetID {
                    id: String::new(),
                    name: String::new(),
                },
                region: String::new(),
                service: DEFAULT_ARN_SERVICE.to_string(),
                partition: DEFAULT_ARN_PARTITION.to_string(),
            });
        }
        ARN::from_str(&s).map_err(serde::de::Error::custom)
    }
}
