use std::{collections::VecDeque, fmt::Display};

use api::{
    query::{ast::ExtStatement, parser::Parser as RustFsParser},
    ParserSnafu,
};
use datafusion::sql::sqlparser::{
    dialect::Dialect,
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use snafu::ResultExt;

use super::dialect::RustFsDialect;

pub type Result<T, E = ParserError> = std::result::Result<T, E>;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

#[derive(Default)]
pub struct DefaultParser {}

impl RustFsParser for DefaultParser {
    fn parse(&self, sql: &str) -> api::QueryResult<VecDeque<ExtStatement>> {
        ExtParser::parse_sql(sql).context(ParserSnafu)
    }
}

/// SQL Parser
pub struct ExtParser<'a> {
    parser: Parser<'a>,
}

impl<'a> ExtParser<'a> {
    /// Parse the specified tokens with dialect
    fn new_with_dialect(sql: &str, dialect: &'a dyn Dialect) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;
        Ok(ExtParser {
            parser: Parser::new(dialect).with_tokens(tokens),
        })
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql(sql: &str) -> Result<VecDeque<ExtStatement>> {
        let dialect = &RustFsDialect {};
        ExtParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(sql: &str, dialect: &dyn Dialect) -> Result<VecDeque<ExtStatement>> {
        let mut parser = ExtParser::new_with_dialect(sql, dialect)?;
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }

        // debug!("Parser sql: {}, stmts: {:#?}", sql, stmts);

        Ok(stmts)
    }

    /// Parse a new expression
    fn parse_statement(&mut self) -> Result<ExtStatement> {
        Ok(ExtStatement::SqlStatement(Box::new(self.parser.parse_statement()?)))
    }

    // Report unexpected token
    fn expected<T>(&self, expected: &str, found: impl Display) -> Result<T> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }
}
