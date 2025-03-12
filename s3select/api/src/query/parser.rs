use std::collections::VecDeque;

use super::ast::ExtStatement;
use crate::QueryResult;

pub trait Parser {
    fn parse(&self, sql: &str) -> QueryResult<VecDeque<ExtStatement>>;
}
