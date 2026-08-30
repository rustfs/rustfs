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

use datafusion::sql::sqlparser::ast::Statement;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonPathSegment {
    Key { name: String, quoted: bool },
    Index(usize),
    ArrayWildcard,
    ObjectWildcard,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JsonSource {
    path: Arc<[JsonPathSegment]>,
    scalar_column: Option<String>,
}

impl JsonSource {
    pub fn new(path: Vec<JsonPathSegment>, scalar_column: Option<String>) -> Self {
        Self {
            path: path.into(),
            scalar_column,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_path(path: Vec<JsonPathSegment>) -> Self {
        Self::new(path, None)
    }

    pub fn path(&self) -> &[JsonPathSegment] {
        &self.path
    }

    pub fn scalar_column(&self) -> Option<&str> {
        self.scalar_column.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtStatement {
    /// ANSI SQL AST node
    SqlStatement(Box<Statement>),
    // we can expand command
}
