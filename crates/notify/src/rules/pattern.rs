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
    // An empty pattern matches nothing, which is usually desired: an empty
    // pattern_str means no specific filter was supplied. Go's NewRulesMap
    // defaults to "*", so an empty pattern reaching here is unlikely to mean
    // "match all" — treat it as no match unless it is explicitly "*".
    if pattern_str.is_empty() {
        return false; // Or true if an empty pattern means "match all" in some contexts.
        // Given Go's NewRulesMap defaults to "*", an empty pattern from Filter is unlikely to mean "match all".
    }
    glob_match_star_only(pattern_str, object_name)
}

/// Matches `object_name` against a glob `pattern` using **S3 filter semantics**:
/// only `*` is a wildcard (matching any run of characters, including none), and
/// every other character — crucially including `?` — is matched literally.
///
/// S3 event notification prefix/suffix filters treat `*` as the only wildcard;
/// AWS does not assign any special meaning to `?`. A generic glob matcher that
/// interprets `?` as a single-character wildcard would let a literal `?` in a
/// prefix/suffix filter (or object key) over-match and misroute events
/// (backlog#979), so this matcher is hand-rolled to keep `?` literal.
fn glob_match_star_only(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();

    let mut p = 0usize; // cursor into `pattern`
    let mut t = 0usize; // cursor into `text`
    // Backtracking state: the last `*` we saw and where in `text` we matched it.
    let mut star_at: Option<usize> = None;
    let mut star_match_t = 0usize;

    while t < text.len() {
        if p < pattern.len() && pattern[p] == '*' {
            // Record the star and tentatively let it consume nothing.
            star_at = Some(p);
            star_match_t = t;
            p += 1;
        } else if p < pattern.len() && pattern[p] == text[t] {
            // Literal match (any non-`*` char, including `?`).
            p += 1;
            t += 1;
        } else if let Some(star_p) = star_at {
            // Mismatch: let the previous `*` absorb one more character and retry.
            p = star_p + 1;
            star_match_t += 1;
            t = star_match_t;
        } else {
            return false;
        }
    }

    // Consume any trailing `*` in the pattern.
    while p < pattern.len() && pattern[p] == '*' {
        p += 1;
    }

    p == pattern.len()
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

    /// Regression test for backlog#979 (c): S3 filter semantics treat `*` as the
    /// only wildcard. `?` must be matched literally, not as a single-character
    /// wildcard (the way a generic glob matcher would treat it).
    #[test]
    fn question_mark_is_literal_not_single_char_wildcard() {
        // `?` is a literal: it only matches a literal `?`, never an arbitrary char.
        assert!(!match_simple("a?c", "abc"));
        assert!(match_simple("a?c", "a?c"));

        // Prefix-derived pattern containing a literal `?`.
        let prefix_pattern = new_pattern(Some("foo?"), None);
        assert_eq!(prefix_pattern, "foo?*");
        assert!(!match_simple(&prefix_pattern, "fooX/bar")); // `?` must not match `X`
        assert!(match_simple(&prefix_pattern, "foo?bar")); // literal `?` matches
        assert!(!match_simple(&prefix_pattern, "foobar")); // `?` is required literally

        // Suffix-derived pattern containing a literal `?`.
        let suffix_pattern = new_pattern(None, Some("?.log"));
        assert_eq!(suffix_pattern, "*?.log");
        assert!(match_simple(&suffix_pattern, "app?.log"));
        assert!(!match_simple(&suffix_pattern, "appX.log"));

        // `*` continues to work as a multi-character wildcard alongside literal `?`.
        assert!(match_simple("a?*c", "a?bbbc"));
        assert!(!match_simple("a?*c", "aXbbbc"));
    }
}
