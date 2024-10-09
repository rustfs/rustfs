pub fn match_simple(pattern: &str, name: &str) -> bool {
    if pattern.is_empty() {
        return name == pattern;
    }
    if pattern == "*" {
        return true;
    }
    // Do an extended wildcard '*' and '?' match.
    deep_match_rune(name, pattern, true)
}

pub fn match_pattern(pattern: &str, name: &str) -> bool {
    if pattern.is_empty() {
        return name == pattern;
    }
    if pattern == "*" {
        return true;
    }
    // Do an extended wildcard '*' and '?' match.
    deep_match_rune(name, pattern, false)
}

fn deep_match_rune(str_: &str, pattern: &str, simple: bool) -> bool {
    let (mut str_, mut pattern) = (str_.as_bytes(), pattern.as_bytes());
    while !pattern.is_empty() {
        match pattern[0] as char {
            '*' => {
                return if pattern.len() == 1 {
                    true
                } else if deep_match_rune(&str_[..], &pattern[1..], simple)
                    || (!str_.is_empty() && deep_match_rune(&str_[1..], pattern, simple))
                {
                    true
                } else {
                    false
                };
            }
            '?' => {
                if str_.is_empty() {
                    return simple;
                }
            }
            _ => {
                if str_.is_empty() || str_[0] != pattern[0] {
                    return false;
                }
            }
        }
        str_ = &str_[1..];
        pattern = &pattern[1..];
    }
    str_.is_empty() && pattern.is_empty()
}

pub fn match_as_pattern_prefix(pattern: &str, text: &str) -> bool {
    let mut i = 0;
    while i < text.len() && i < pattern.len() {
        match pattern.as_bytes()[i] as char {
            '*' => return true,
            '?' => i += 1,
            _ => {
                if pattern.as_bytes()[i] != text.as_bytes()[i] {
                    return false;
                }
            }
        }
        i += 1;
    }
    text.len() <= pattern.len()
}
