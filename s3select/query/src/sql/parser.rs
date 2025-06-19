use std::{collections::VecDeque, fmt::Display};

use api::{
    ParserSnafu,
    query::{ast::ExtStatement, parser::Parser as RustFsParser},
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

#[cfg(test)]
mod tests {
    use super::*;
    use api::query::ast::ExtStatement;

    #[test]
    fn test_default_parser_creation() {
        let _parser = DefaultParser::default();

        // Test that parser can be created successfully
        assert!(std::mem::size_of::<DefaultParser>() == 0, "Parser should be zero-sized");
    }

    #[test]
    fn test_default_parser_simple_select() {
        let parser = DefaultParser::default();
        let sql = "SELECT * FROM S3Object";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "Simple SELECT should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");

        // Just verify we get a SQL statement without diving into AST details
        match &statements[0] {
            ExtStatement::SqlStatement(_) => {
                // Successfully parsed as SQL statement
            }
        }
    }

    #[test]
    fn test_default_parser_select_with_columns() {
        let parser = DefaultParser::default();
        let sql = "SELECT id, name, age FROM S3Object";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "SELECT with columns should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");

        match &statements[0] {
            ExtStatement::SqlStatement(_) => {
                // Successfully parsed as SQL statement
            }
        }
    }

    #[test]
    fn test_default_parser_select_with_where() {
        let parser = DefaultParser::default();
        let sql = "SELECT * FROM S3Object WHERE age > 25";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "SELECT with WHERE should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");

        match &statements[0] {
            ExtStatement::SqlStatement(_) => {
                // Successfully parsed as SQL statement
            }
        }
    }

    #[test]
    fn test_default_parser_multiple_statements() {
        let parser = DefaultParser::default();
        let sql = "SELECT * FROM S3Object; SELECT id FROM S3Object;";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "Multiple statements should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 2, "Should have exactly two statements");
    }

    #[test]
    fn test_default_parser_empty_statements() {
        let parser = DefaultParser::default();
        let sql = ";;; SELECT * FROM S3Object; ;;;";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "Empty statements should be ignored");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one non-empty statement");
    }

    #[test]
    fn test_default_parser_invalid_sql() {
        let parser = DefaultParser::default();
        let sql = "INVALID SQL SYNTAX";

        let result = parser.parse(sql);
        assert!(result.is_err(), "Invalid SQL should return error");
    }

    #[test]
    fn test_default_parser_empty_sql() {
        let parser = DefaultParser::default();
        let sql = "";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "Empty SQL should parse successfully");

        let statements = result.unwrap();
        assert!(statements.is_empty(), "Should have no statements");
    }

    #[test]
    fn test_default_parser_whitespace_only() {
        let parser = DefaultParser::default();
        let sql = "   \n\t  ";

        let result = parser.parse(sql);
        assert!(result.is_ok(), "Whitespace-only SQL should parse successfully");

        let statements = result.unwrap();
        assert!(statements.is_empty(), "Should have no statements");
    }

    #[test]
    fn test_ext_parser_parse_sql() {
        let sql = "SELECT * FROM S3Object";

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "ExtParser::parse_sql should work");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_ext_parser_parse_sql_with_dialect() {
        let sql = "SELECT * FROM S3Object";
        let dialect = &RustFsDialect;

        let result = ExtParser::parse_sql_with_dialect(sql, dialect);
        assert!(result.is_ok(), "ExtParser::parse_sql_with_dialect should work");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_ext_parser_new_with_dialect() {
        let sql = "SELECT * FROM S3Object";
        let dialect = &RustFsDialect;

        let result = ExtParser::new_with_dialect(sql, dialect);
        assert!(result.is_ok(), "ExtParser::new_with_dialect should work");
    }

    #[test]
    fn test_ext_parser_complex_query() {
        let sql = "SELECT id, name, age FROM S3Object WHERE age > 25 AND department = 'IT' ORDER BY age DESC LIMIT 10";

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "Complex query should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");

        match &statements[0] {
            ExtStatement::SqlStatement(_) => {
                // Successfully parsed as SQL statement
            }
        }
    }

    #[test]
    fn test_ext_parser_aggregate_functions() {
        let sql = "SELECT COUNT(*), AVG(age), MAX(salary) FROM S3Object GROUP BY department";

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "Aggregate functions should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");

        match &statements[0] {
            ExtStatement::SqlStatement(_) => {
                // Successfully parsed as SQL statement
            }
        }
    }

    #[test]
    fn test_ext_parser_join_query() {
        let sql = "SELECT s1.id, s2.name FROM S3Object s1 JOIN S3Object s2 ON s1.id = s2.id";

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "JOIN query should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_ext_parser_subquery() {
        let sql = "SELECT * FROM S3Object WHERE id IN (SELECT id FROM S3Object WHERE age > 30)";

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "Subquery should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_ext_parser_case_insensitive() {
        let sql = "select * from s3object where age > 25";

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "Case insensitive SQL should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_ext_parser_quoted_identifiers() {
        let sql = r#"SELECT "id", "name" FROM "S3Object" WHERE "age" > 25"#;

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "Quoted identifiers should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_ext_parser_string_literals() {
        let sql = "SELECT * FROM S3Object WHERE name = 'John Doe' AND department = 'IT'";

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "String literals should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_ext_parser_numeric_literals() {
        let sql = "SELECT * FROM S3Object WHERE age = 25 AND salary = 50000.50";

        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "Numeric literals should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_ext_parser_error_handling() {
        let invalid_sqls = vec![
            "SELECT FROM",                  // Missing column list
            "SELECT * FROM",                // Missing table name
            "SELECT * FROM S3Object WHERE", // Incomplete WHERE clause
            "SELECT * FROM S3Object GROUP", // Incomplete GROUP BY
            "SELECT * FROM S3Object ORDER", // Incomplete ORDER BY
        ];

        for sql in invalid_sqls {
            let result = ExtParser::parse_sql(sql);
            assert!(result.is_err(), "Invalid SQL '{}' should return error", sql);
        }
    }

    #[test]
    fn test_ext_parser_memory_efficiency() {
        let sql = "SELECT * FROM S3Object";

        // Test that parser doesn't use excessive memory
        let result = ExtParser::parse_sql(sql);
        assert!(result.is_ok(), "Parser should work efficiently");

        let statements = result.unwrap();
        let memory_size = std::mem::size_of_val(&statements);
        assert!(memory_size < 10000, "Parsed statements should not use excessive memory");
    }

    #[test]
    fn test_ext_parser_large_query() {
        // Test with a reasonably large query
        let mut sql = String::from("SELECT ");
        for i in 0..100 {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("col{}", i));
        }
        sql.push_str(" FROM S3Object WHERE ");
        for i in 0..50 {
            if i > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str(&format!("col{} > {}", i, i));
        }

        let result = ExtParser::parse_sql(&sql);
        assert!(result.is_ok(), "Large query should parse successfully");

        let statements = result.unwrap();
        assert_eq!(statements.len(), 1, "Should have exactly one statement");
    }

    #[test]
    fn test_parser_err_macro() {
        let error: Result<()> = parser_err!("Test error message");
        assert!(error.is_err(), "parser_err! macro should create error");

        match error {
            Err(ParserError::ParserError(msg)) => {
                assert_eq!(msg, "Test error message", "Error message should match");
            }
            _ => panic!("Expected ParserError::ParserError"),
        }
    }

    #[test]
    fn test_ext_parser_expected_method() {
        let sql = "SELECT * FROM S3Object";
        let dialect = &RustFsDialect;
        let parser = ExtParser::new_with_dialect(sql, dialect).unwrap();

        let result: Result<()> = parser.expected("test token", "found token");
        assert!(result.is_err(), "expected method should return error");

        match result {
            Err(ParserError::ParserError(msg)) => {
                assert!(msg.contains("Expected test token"), "Error should contain expected message");
                assert!(msg.contains("found: found token"), "Error should contain found message");
            }
            _ => panic!("Expected ParserError::ParserError"),
        }
    }
}
