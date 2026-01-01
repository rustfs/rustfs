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

use wildmatch::WildMatch;

/// Create new pattern string based on prefix and suffix。
///
/// The rule is similar to event.NewPattern in the Go version:
/// - If a prefix is provided and does not end with '*', '*' is appended.
/// - If a suffix is provided and does not start with '*', then prefix '*'.
/// - Replace "**" with "*".
pub fn new_pattern(prefix: Option<&str>, suffix: Option<&str>) -> String {
    let mut pattern = String::new();

    // Process the prefix part
    if let Some(p) = prefix
        && !p.is_empty()
    {
        pattern.push_str(p);
        if !p.ends_with('*') {
            pattern.push('*');
        }
    }

    // Process the suffix part
    if let Some(s) = suffix
        && !s.is_empty()
    {
        let mut s_to_append = s.to_string();
        if !s.starts_with('*') {
            s_to_append.insert(0, '*');
        }

        // If the pattern is empty (only suffixes are provided), then the pattern is the suffix
        // Otherwise, append the suffix to the pattern
        if pattern.is_empty() {
            pattern = s_to_append;
        } else {
            pattern.push_str(&s_to_append);
        }
    }

    // Replace "**" with "*"
    pattern = pattern.replace("**", "*");

    pattern
}

/// Simple matching object names and patterns。
pub fn match_simple(pattern_str: &str, object_name: &str) -> bool {
    if pattern_str == "*" {
        // AWS S3 docs: A single asterisk (*) in the rule matches all objects.
        return true;
    }
    // WildMatch considers an empty pattern to not match anything, which is usually desired.
    // If pattern_str is empty, it means no specific filter, so it depends on interpretation.
    // Go's wildcard.MatchSimple might treat empty pattern differently.
    // For now, assume empty pattern means no match unless it's explicitly "*".
    if pattern_str.is_empty() {
        return false; // Or true if an empty pattern means "match all" in some contexts.
        // Given Go's NewRulesMap defaults to "*", an empty pattern from Filter is unlikely to mean "match all".
    }
    WildMatch::new(pattern_str).matches(object_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_pattern() {
        assert_eq!(new_pattern(Some("images/"), Some(".jpg")), "images/*.jpg");
        assert_eq!(new_pattern(Some("images/"), None), "images/*");
        assert_eq!(new_pattern(None, Some(".jpg")), "*.jpg");
        assert_eq!(new_pattern(Some("foo"), Some("bar")), "foo*bar"); // foo* + *bar -> foo**bar -> foo*bar
        assert_eq!(new_pattern(Some("foo*"), Some("bar")), "foo*bar"); // foo* + *bar -> foo**bar -> foo*bar
        assert_eq!(new_pattern(Some("foo"), Some("*bar")), "foo*bar"); // foo* + *bar -> foo**bar -> foo*bar
        assert_eq!(new_pattern(Some("foo*"), Some("*bar")), "foo*bar"); // foo* + *bar -> foo**bar -> foo*bar
        assert_eq!(new_pattern(Some("*"), Some("*")), "*"); // * + * -> ** -> *
        assert_eq!(new_pattern(Some("a"), Some("")), "a*");
        assert_eq!(new_pattern(Some(""), Some("b")), "*b");
        assert_eq!(new_pattern(None, None), "");
        assert_eq!(new_pattern(Some("prefix"), Some("suffix")), "prefix*suffix");
        assert_eq!(new_pattern(Some("prefix/"), Some("/suffix")), "prefix/*/suffix"); // prefix/* + */suffix -> prefix/**/suffix -> prefix/*/suffix
    }

    #[test]
    fn test_match_simple() {
        assert!(match_simple("foo*", "foobar"));
        assert!(!match_simple("foo*", "barfoo"));
        assert!(match_simple("*.jpg", "photo.jpg"));
        assert!(!match_simple("*.jpg", "photo.png"));
        assert!(match_simple("*", "anything.anything"));
        assert!(match_simple("foo*bar", "foobazbar"));
        assert!(!match_simple("foo*bar", "foobar_baz"));
        assert!(match_simple("a*b*c", "axbyc"));
        assert!(match_simple("a*b*c", "abc"));
        assert!(match_simple("a*b*c", "axbc"));
    }
}
