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

use crate::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};

// Define the QuotaType enum
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaType {
    Hard,
}

// Define the BucketQuota structure
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketQuota {
    quota: Option<u64>, // Use Option to represent optional fields

    size: u64,

    rate: u64,

    requests: u64,

    quota_type: Option<QuotaType>,
}

impl BucketQuota {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketQuota = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
