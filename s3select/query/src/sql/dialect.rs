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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rustfs_dialect_creation() {
        let _dialect = RustFsDialect;

        // Test that dialect can be created successfully
        assert!(std::mem::size_of::<RustFsDialect>() == 0, "Dialect should be zero-sized");
    }

    #[test]
    fn test_rustfs_dialect_debug() {
        let dialect = RustFsDialect;

        let debug_str = format!("{dialect:?}");
        assert!(!debug_str.is_empty(), "Debug output should not be empty");
        assert!(debug_str.contains("RustFsDialect"), "Debug output should contain dialect name");
    }

    #[test]
    fn test_is_identifier_start_alphabetic() {
        let dialect = RustFsDialect;

        // Test alphabetic characters
        assert!(dialect.is_identifier_start('a'), "Lowercase letter should be valid identifier start");
        assert!(dialect.is_identifier_start('A'), "Uppercase letter should be valid identifier start");
        assert!(dialect.is_identifier_start('z'), "Last lowercase letter should be valid identifier start");
        assert!(dialect.is_identifier_start('Z'), "Last uppercase letter should be valid identifier start");

        // Test Unicode alphabetic characters
        assert!(dialect.is_identifier_start('α'), "Greek letter should be valid identifier start");
        assert!(dialect.is_identifier_start('中'), "Chinese character should be valid identifier start");
        assert!(dialect.is_identifier_start('ñ'), "Accented letter should be valid identifier start");
    }

    #[test]
    fn test_is_identifier_start_special_chars() {
        let dialect = RustFsDialect;

        // Test special characters that are allowed
        assert!(dialect.is_identifier_start('_'), "Underscore should be valid identifier start");
        assert!(dialect.is_identifier_start('#'), "Hash should be valid identifier start");
        assert!(dialect.is_identifier_start('@'), "At symbol should be valid identifier start");
    }

    #[test]
    fn test_is_identifier_start_invalid_chars() {
        let dialect = RustFsDialect;

        // Test characters that should not be valid identifier starts
        assert!(!dialect.is_identifier_start('0'), "Digit should not be valid identifier start");
        assert!(!dialect.is_identifier_start('9'), "Digit should not be valid identifier start");
        assert!(!dialect.is_identifier_start('$'), "Dollar sign should not be valid identifier start");
        assert!(!dialect.is_identifier_start(' '), "Space should not be valid identifier start");
        assert!(!dialect.is_identifier_start('\t'), "Tab should not be valid identifier start");
        assert!(!dialect.is_identifier_start('\n'), "Newline should not be valid identifier start");
        assert!(!dialect.is_identifier_start('.'), "Dot should not be valid identifier start");
        assert!(!dialect.is_identifier_start(','), "Comma should not be valid identifier start");
        assert!(!dialect.is_identifier_start(';'), "Semicolon should not be valid identifier start");
        assert!(!dialect.is_identifier_start('('), "Left paren should not be valid identifier start");
        assert!(!dialect.is_identifier_start(')'), "Right paren should not be valid identifier start");
        assert!(!dialect.is_identifier_start('['), "Left bracket should not be valid identifier start");
        assert!(!dialect.is_identifier_start(']'), "Right bracket should not be valid identifier start");
        assert!(!dialect.is_identifier_start('{'), "Left brace should not be valid identifier start");
        assert!(!dialect.is_identifier_start('}'), "Right brace should not be valid identifier start");
        assert!(!dialect.is_identifier_start('='), "Equals should not be valid identifier start");
        assert!(!dialect.is_identifier_start('+'), "Plus should not be valid identifier start");
        assert!(!dialect.is_identifier_start('-'), "Minus should not be valid identifier start");
        assert!(!dialect.is_identifier_start('*'), "Asterisk should not be valid identifier start");
        assert!(!dialect.is_identifier_start('/'), "Slash should not be valid identifier start");
        assert!(!dialect.is_identifier_start('%'), "Percent should not be valid identifier start");
        assert!(!dialect.is_identifier_start('<'), "Less than should not be valid identifier start");
        assert!(!dialect.is_identifier_start('>'), "Greater than should not be valid identifier start");
        assert!(!dialect.is_identifier_start('!'), "Exclamation should not be valid identifier start");
        assert!(!dialect.is_identifier_start('?'), "Question mark should not be valid identifier start");
        assert!(!dialect.is_identifier_start('&'), "Ampersand should not be valid identifier start");
        assert!(!dialect.is_identifier_start('|'), "Pipe should not be valid identifier start");
        assert!(!dialect.is_identifier_start('^'), "Caret should not be valid identifier start");
        assert!(!dialect.is_identifier_start('~'), "Tilde should not be valid identifier start");
        assert!(!dialect.is_identifier_start('`'), "Backtick should not be valid identifier start");
        assert!(!dialect.is_identifier_start('"'), "Double quote should not be valid identifier start");
        assert!(!dialect.is_identifier_start('\''), "Single quote should not be valid identifier start");
    }

    #[test]
    fn test_is_identifier_part_alphabetic() {
        let dialect = RustFsDialect;

        // Test alphabetic characters
        assert!(dialect.is_identifier_part('a'), "Lowercase letter should be valid identifier part");
        assert!(dialect.is_identifier_part('A'), "Uppercase letter should be valid identifier part");
        assert!(dialect.is_identifier_part('z'), "Last lowercase letter should be valid identifier part");
        assert!(dialect.is_identifier_part('Z'), "Last uppercase letter should be valid identifier part");

        // Test Unicode alphabetic characters
        assert!(dialect.is_identifier_part('α'), "Greek letter should be valid identifier part");
        assert!(dialect.is_identifier_part('中'), "Chinese character should be valid identifier part");
        assert!(dialect.is_identifier_part('ñ'), "Accented letter should be valid identifier part");
    }

    #[test]
    fn test_is_identifier_part_digits() {
        let dialect = RustFsDialect;

        // Test ASCII digits
        assert!(dialect.is_identifier_part('0'), "Digit 0 should be valid identifier part");
        assert!(dialect.is_identifier_part('1'), "Digit 1 should be valid identifier part");
        assert!(dialect.is_identifier_part('5'), "Digit 5 should be valid identifier part");
        assert!(dialect.is_identifier_part('9'), "Digit 9 should be valid identifier part");
    }

    #[test]
    fn test_is_identifier_part_special_chars() {
        let dialect = RustFsDialect;

        // Test special characters that are allowed
        assert!(dialect.is_identifier_part('_'), "Underscore should be valid identifier part");
        assert!(dialect.is_identifier_part('#'), "Hash should be valid identifier part");
        assert!(dialect.is_identifier_part('@'), "At symbol should be valid identifier part");
        assert!(dialect.is_identifier_part('$'), "Dollar sign should be valid identifier part");
    }

    #[test]
    fn test_is_identifier_part_invalid_chars() {
        let dialect = RustFsDialect;

        // Test characters that should not be valid identifier parts
        assert!(!dialect.is_identifier_part(' '), "Space should not be valid identifier part");
        assert!(!dialect.is_identifier_part('\t'), "Tab should not be valid identifier part");
        assert!(!dialect.is_identifier_part('\n'), "Newline should not be valid identifier part");
        assert!(!dialect.is_identifier_part('.'), "Dot should not be valid identifier part");
        assert!(!dialect.is_identifier_part(','), "Comma should not be valid identifier part");
        assert!(!dialect.is_identifier_part(';'), "Semicolon should not be valid identifier part");
        assert!(!dialect.is_identifier_part('('), "Left paren should not be valid identifier part");
        assert!(!dialect.is_identifier_part(')'), "Right paren should not be valid identifier part");
        assert!(!dialect.is_identifier_part('['), "Left bracket should not be valid identifier part");
        assert!(!dialect.is_identifier_part(']'), "Right bracket should not be valid identifier part");
        assert!(!dialect.is_identifier_part('{'), "Left brace should not be valid identifier part");
        assert!(!dialect.is_identifier_part('}'), "Right brace should not be valid identifier part");
        assert!(!dialect.is_identifier_part('='), "Equals should not be valid identifier part");
        assert!(!dialect.is_identifier_part('+'), "Plus should not be valid identifier part");
        assert!(!dialect.is_identifier_part('-'), "Minus should not be valid identifier part");
        assert!(!dialect.is_identifier_part('*'), "Asterisk should not be valid identifier part");
        assert!(!dialect.is_identifier_part('/'), "Slash should not be valid identifier part");
        assert!(!dialect.is_identifier_part('%'), "Percent should not be valid identifier part");
        assert!(!dialect.is_identifier_part('<'), "Less than should not be valid identifier part");
        assert!(!dialect.is_identifier_part('>'), "Greater than should not be valid identifier part");
        assert!(!dialect.is_identifier_part('!'), "Exclamation should not be valid identifier part");
        assert!(!dialect.is_identifier_part('?'), "Question mark should not be valid identifier part");
        assert!(!dialect.is_identifier_part('&'), "Ampersand should not be valid identifier part");
        assert!(!dialect.is_identifier_part('|'), "Pipe should not be valid identifier part");
        assert!(!dialect.is_identifier_part('^'), "Caret should not be valid identifier part");
        assert!(!dialect.is_identifier_part('~'), "Tilde should not be valid identifier part");
        assert!(!dialect.is_identifier_part('`'), "Backtick should not be valid identifier part");
        assert!(!dialect.is_identifier_part('"'), "Double quote should not be valid identifier part");
        assert!(!dialect.is_identifier_part('\''), "Single quote should not be valid identifier part");
    }

    #[test]
    fn test_supports_group_by_expr() {
        let dialect = RustFsDialect;

        assert!(dialect.supports_group_by_expr(), "RustFsDialect should support GROUP BY expressions");
    }

    #[test]
    fn test_identifier_validation_comprehensive() {
        let dialect = RustFsDialect;

        // Test valid identifier patterns
        let valid_starts = ['a', 'A', 'z', 'Z', '_', '#', '@', 'α', '中'];
        let valid_parts = ['a', 'A', '0', '9', '_', '#', '@', '$', 'α', '中'];

        for start_char in valid_starts {
            assert!(
                dialect.is_identifier_start(start_char),
                "Character '{start_char}' should be valid identifier start"
            );

            for part_char in valid_parts {
                assert!(
                    dialect.is_identifier_part(part_char),
                    "Character '{part_char}' should be valid identifier part"
                );
            }
        }
    }

    #[test]
    fn test_identifier_edge_cases() {
        let dialect = RustFsDialect;

        // Test edge cases with control characters
        assert!(!dialect.is_identifier_start('\0'), "Null character should not be valid identifier start");
        assert!(!dialect.is_identifier_part('\0'), "Null character should not be valid identifier part");

        assert!(
            !dialect.is_identifier_start('\x01'),
            "Control character should not be valid identifier start"
        );
        assert!(
            !dialect.is_identifier_part('\x01'),
            "Control character should not be valid identifier part"
        );

        assert!(!dialect.is_identifier_start('\x7F'), "DEL character should not be valid identifier start");
        assert!(!dialect.is_identifier_part('\x7F'), "DEL character should not be valid identifier part");
    }

    #[test]
    fn test_identifier_unicode_support() {
        let dialect = RustFsDialect;

        // Test various Unicode categories
        let unicode_letters = ['α', 'β', 'γ', 'Α', 'Β', 'Γ', '中', '文', '日', '本', 'ñ', 'ü', 'ç'];

        for ch in unicode_letters {
            assert!(
                dialect.is_identifier_start(ch),
                "Unicode letter '{ch}' should be valid identifier start"
            );
            assert!(dialect.is_identifier_part(ch), "Unicode letter '{ch}' should be valid identifier part");
        }
    }

    #[test]
    fn test_identifier_ascii_digits() {
        let dialect = RustFsDialect;

        // Test all ASCII digits
        for digit in '0'..='9' {
            assert!(
                !dialect.is_identifier_start(digit),
                "ASCII digit '{digit}' should not be valid identifier start"
            );
            assert!(
                dialect.is_identifier_part(digit),
                "ASCII digit '{digit}' should be valid identifier part"
            );
        }
    }

    #[test]
    fn test_dialect_consistency() {
        let dialect = RustFsDialect;

        // Test that all valid identifier starts are also valid identifier parts
        let test_chars = [
            'a', 'A', 'z', 'Z', '_', '#', '@', 'α', '中', 'ñ', '0', '9', '$', ' ', '.', ',', ';', '(', ')', '=', '+', '-',
        ];

        for ch in test_chars {
            if dialect.is_identifier_start(ch) {
                assert!(
                    dialect.is_identifier_part(ch),
                    "Character '{ch}' that is valid identifier start should also be valid identifier part"
                );
            }
        }
    }

    #[test]
    fn test_dialect_memory_efficiency() {
        let dialect = RustFsDialect;

        // Test that dialect doesn't use excessive memory
        let dialect_size = std::mem::size_of_val(&dialect);
        assert!(dialect_size < 100, "Dialect should not use excessive memory");
    }

    #[test]
    fn test_dialect_trait_implementation() {
        let dialect = RustFsDialect;

        // Test that dialect properly implements the Dialect trait
        let dialect_ref: &dyn Dialect = &dialect;

        // Test basic functionality through trait
        assert!(dialect_ref.is_identifier_start('a'), "Trait method should work for valid start");
        assert!(!dialect_ref.is_identifier_start('0'), "Trait method should work for invalid start");
        assert!(dialect_ref.is_identifier_part('a'), "Trait method should work for valid part");
        assert!(dialect_ref.is_identifier_part('0'), "Trait method should work for digit part");
        assert!(
            dialect_ref.supports_group_by_expr(),
            "Trait method should return true for GROUP BY support"
        );
    }

    #[test]
    fn test_dialect_clone_and_default() {
        let dialect1 = RustFsDialect;
        let dialect2 = RustFsDialect;

        // Test that multiple instances behave the same
        let test_chars = ['a', 'A', '0', '_', '#', '@', '$', ' ', '.'];

        for ch in test_chars {
            assert_eq!(
                dialect1.is_identifier_start(ch),
                dialect2.is_identifier_start(ch),
                "Different instances should behave the same for is_identifier_start"
            );
            assert_eq!(
                dialect1.is_identifier_part(ch),
                dialect2.is_identifier_part(ch),
                "Different instances should behave the same for is_identifier_part"
            );
        }

        assert_eq!(
            dialect1.supports_group_by_expr(),
            dialect2.supports_group_by_expr(),
            "Different instances should behave the same for supports_group_by_expr"
        );
    }
}
