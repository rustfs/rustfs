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

use crate::error::Error;

#[derive(PartialEq, Eq, Debug)]
pub enum ServiceType {
    S3,
    STS,
}

impl TryFrom<&str> for ServiceType {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let service_type = match value {
            "s3" => Self::S3,
            "sts" => Self::STS,
            _ => return Err(Error::InvalidServiceType(value.to_owned())),
        };

        Ok(service_type)
    }
}
