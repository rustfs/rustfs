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

use std::collections::VecDeque;

use datafusion::sql::sqlparser::dialect::Dialect;

use super::ast::ExtStatement;
use crate::QueryResult;

#[derive(Debug, Default)]
pub struct RustFsDialect;

impl Dialect for RustFsDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '#' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '@' || ch == '$' || ch == '#' || ch == '_'
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }
}

pub trait Parser {
    fn parse(&self, sql: &str) -> QueryResult<VecDeque<ExtStatement>>;
}
