use datafusion::sql::sqlparser::ast::Statement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtStatement {
    /// ANSI SQL AST node
    SqlStatement(Box<Statement>),
    // we can expand command
}
