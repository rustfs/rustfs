use datafusion::sql::sqlparser::dialect::Dialect;

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
