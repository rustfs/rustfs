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

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogIdentifierError {
    Empty,
    TooLong { max: usize },
    NamespaceTooLong { max: usize },
    InvalidCharacter,
    InvalidBoundary,
    Ambiguous,
}

impl fmt::Display for CatalogIdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("catalog identifier segment is empty"),
            Self::TooLong { max } => write!(f, "catalog identifier segment exceeds {max} characters"),
            Self::NamespaceTooLong { max } => write!(f, "catalog namespace exceeds {max} characters"),
            Self::InvalidCharacter => f.write_str("catalog identifier segment contains invalid characters"),
            Self::InvalidBoundary => {
                f.write_str("catalog identifier segment must start and end with a lowercase letter or digit")
            }
            Self::Ambiguous => f.write_str("catalog identifier segment is ambiguous"),
        }
    }
}

impl std::error::Error for CatalogIdentifierError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableObjectMutationError {
    ReservedCatalogObject,
}

impl fmt::Display for TableObjectMutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedCatalogObject => f.write_str("object key is reserved for the table catalog"),
        }
    }
}

impl std::error::Error for TableObjectMutationError {}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableCatalogStoreError {
    NotFound(String),
    NamespaceNotFound(String),
    TableNotFound(String),
    AlreadyExists(String),
    Conflict(String),
    Invalid(String),
    Unsupported(String),
    Internal(String),
}

impl fmt::Display for TableCatalogStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(message) => write!(f, "table catalog entry not found: {message}"),
            Self::NamespaceNotFound(message) => write!(f, "table catalog namespace not found: {message}"),
            Self::TableNotFound(message) => write!(f, "table catalog table not found: {message}"),
            Self::AlreadyExists(message) => write!(f, "table catalog entry already exists: {message}"),
            Self::Conflict(message) => write!(f, "table catalog conflict: {message}"),
            Self::Invalid(message) => write!(f, "invalid table catalog entry: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported table catalog operation: {message}"),
            Self::Internal(message) => write!(f, "table catalog store error: {message}"),
        }
    }
}

impl std::error::Error for TableCatalogStoreError {}

pub(crate) type TableCatalogStoreResult<T> = Result<T, TableCatalogStoreError>;
