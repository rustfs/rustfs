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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Object not found")]
    NotFound,
    #[error("Other error: {0}")]
    Other(String),
}

impl From<MetadataError> for rustfs_ecstore::error::Error {
    fn from(err: MetadataError) -> Self {
        match err {
            MetadataError::NotFound => rustfs_ecstore::error::Error::other("Object not found"),
            _ => rustfs_ecstore::error::Error::other(err.to_string()),
        }
    }
}
