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

use std::{
    collections::VecDeque,
    sync::atomic::{AtomicUsize, Ordering},
};

use datafusion::sql::sqlparser::{
    ast::Expr,
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser as SqlParser, ParserError},
    tokenizer::Token,
};

use super::ast::ExtStatement;
use crate::QueryResult;

/// Maximum number of infix and postfix operators in one S3 Select expression.
///
/// sqlparser's recursion limit only guards nested sub-expressions. Operator chains
/// (`1+1+...+1`, `a OR b OR ...`, `x IS NULL IS NULL ...`) are parsed in a loop where
/// each operator nests the expression built so far one level deeper. The resulting
/// AST is later cloned, dropped, and planned recursively on a worker-thread stack,
/// so operator count must be bounded during parsing.
pub const MAX_SELECT_OPERATORS: usize = 256;

/// Maximum number of SQL set-operation keywords (`UNION`, `EXCEPT`, `INTERSECT`,
/// `MINUS`) allowed before parsing. sqlparser builds `SetExpr::SetOperation` chains
/// in a loop without touching `Dialect::parse_infix`, so unbounded chains would
/// overflow the stack on drop even though S3 Select rejects set operations.
pub const MAX_SELECT_SET_OPERATORS: usize = 16;

/// Maximum nested field-access segments (`a.b.c...` or `a[0][1]...`) in one expression,
/// bounding `CoreFunctionPlanner::plan_compound_identifier` recursion depth.
pub const MAX_SELECT_FIELD_ACCESS_DEPTH: usize = 64;

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

    fn supports_partiql(&self) -> bool {
        true
    }
}

/// [`RustFsDialect`] that rejects expressions with more than [`MAX_SELECT_OPERATORS`]
/// infix or postfix operators. The budget is per instance, so construct a fresh
/// dialect for every parse.
#[derive(Debug, Default)]
pub struct BoundedRustFsDialect {
    operators: AtomicUsize,
}

impl Dialect for BoundedRustFsDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        RustFsDialect.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        RustFsDialect.is_identifier_part(ch)
    }

    fn supports_group_by_expr(&self) -> bool {
        RustFsDialect.supports_group_by_expr()
    }

    fn supports_partiql(&self) -> bool {
        RustFsDialect.supports_partiql()
    }

    /// Called by sqlparser before every infix or postfix operator.
    fn parse_infix(&self, _parser: &mut SqlParser, _expr: &Expr, _precedence: u8) -> Option<Result<Expr, ParserError>> {
        if self.operators.load(Ordering::Relaxed) >= MAX_SELECT_OPERATORS {
            // `RecursionLimitExceeded` is the only error sqlparser's speculative
            // `maybe_parse` does not swallow, and matches nested-parenthesis errors.
            return Some(Err(ParserError::RecursionLimitExceeded));
        }
        self.operators.fetch_add(1, Ordering::Relaxed);
        // `None` hands the operator back to sqlparser's standard infix parsing.
        None
    }
}

/// Rejects token streams with more than [`MAX_SELECT_SET_OPERATORS`] set-operation
/// keywords before sqlparser constructs a deeply nested `SetExpr::SetOperation` tree.
pub fn validate_set_operator_budget(tokens: &[Token]) -> Result<(), ParserError> {
    let mut set_operators = 0usize;
    for token in tokens {
        if let Token::Word(word) = token
            && matches!(word.keyword, Keyword::UNION | Keyword::EXCEPT | Keyword::INTERSECT | Keyword::MINUS)
        {
            set_operators += 1;
            if set_operators > MAX_SELECT_SET_OPERATORS {
                return Err(ParserError::RecursionLimitExceeded);
            }
        }
    }
    Ok(())
}

pub trait Parser {
    fn parse(&self, sql: &str) -> QueryResult<VecDeque<ExtStatement>>;
}
