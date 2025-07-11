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

use rand::{Rng, RngCore};
use regex::Regex;
use std::io::{Error, Result};
use std::sync::LazyLock;

pub fn parse_bool(str: &str) -> Result<bool> {
    match str {
        "1" | "t" | "T" | "true" | "TRUE" | "True" | "on" | "ON" | "On" | "enabled" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" | "off" | "OFF" | "Off" | "disabled" => Ok(false),
        _ => Err(Error::other(format!("ParseBool: parsing {str}"))),
    }
}

pub fn match_simple(pattern: &str, name: &str) -> bool {
    if pattern.is_empty() {
        return name == pattern;
    }
    if pattern == "*" {
        return true;
    }
    // Do an extended wildcard '*' and '?' match.
    deep_match_rune(name.as_bytes(), pattern.as_bytes(), true)
}

pub fn match_pattern(pattern: &str, name: &str) -> bool {
    if pattern.is_empty() {
        return name == pattern;
    }
    if pattern == "*" {
        return true;
    }
    // Do an extended wildcard '*' and '?' match.
    deep_match_rune(name.as_bytes(), pattern.as_bytes(), false)
}

pub fn has_pattern(patterns: &[&str], match_str: &str) -> bool {
    for pattern in patterns {
        if match_simple(pattern, match_str) {
            return true;
        }
    }
    false
}

pub fn has_string_suffix_in_slice(str: &str, list: &[&str]) -> bool {
    let str = str.to_lowercase();
    for v in list {
        if *v == "*" {
            return true;
        }

        if str.ends_with(&v.to_lowercase()) {
            return true;
        }
    }
    false
}

fn deep_match_rune(str_: &[u8], pattern: &[u8], simple: bool) -> bool {
    let (mut str_, mut pattern) = (str_, pattern);
    while !pattern.is_empty() {
        match pattern[0] as char {
            '*' => {
                return if pattern.len() == 1 {
                    true
                } else {
                    deep_match_rune(str_, &pattern[1..], simple)
                        || (!str_.is_empty() && deep_match_rune(&str_[1..], pattern, simple))
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

static ELLIPSES_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(.*)(\{[0-9a-z]*\.\.\.[0-9a-z]*\})(.*)").unwrap());

/// Ellipses constants
const OPEN_BRACES: &str = "{";
const CLOSE_BRACES: &str = "}";
const ELLIPSES: &str = "...";

/// ellipses pattern, describes the range and also the
/// associated prefix and suffixes.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Pattern {
    pub prefix: String,
    pub suffix: String,
    pub seq: Vec<String>,
}

impl Pattern {
    /// expands a ellipses pattern.
    pub fn expand(&self) -> Vec<String> {
        let mut ret = Vec::with_capacity(self.suffix.len());

        for v in self.seq.iter() {
            match (self.prefix.is_empty(), self.suffix.is_empty()) {
                (false, true) => ret.push(format!("{}{}", self.prefix, v)),
                (true, false) => ret.push(format!("{}{}", v, self.suffix)),
                (true, true) => ret.push(v.to_string()),
                (false, false) => ret.push(format!("{}{}{}", self.prefix, v, self.suffix)),
            }
        }

        ret
    }

    pub fn len(&self) -> usize {
        self.seq.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seq.is_empty()
    }
}

/// contains a list of patterns provided in the input.
#[derive(Debug, PartialEq, Eq)]
pub struct ArgPattern {
    inner: Vec<Pattern>,
}

impl AsRef<Vec<Pattern>> for ArgPattern {
    fn as_ref(&self) -> &Vec<Pattern> {
        &self.inner
    }
}

impl AsMut<Vec<Pattern>> for ArgPattern {
    fn as_mut(&mut self) -> &mut Vec<Pattern> {
        &mut self.inner
    }
}

impl ArgPattern {
    pub fn new(inner: Vec<Pattern>) -> Self {
        Self { inner }
    }

    /// expands all the ellipses patterns in the given argument.
    pub fn expand(&self) -> Vec<Vec<String>> {
        let ret: Vec<Vec<String>> = self.inner.iter().map(|v| v.expand()).collect();

        Self::arg_expander(&ret)
    }

    /// recursively expands labels into its respective forms.
    fn arg_expander(lbs: &[Vec<String>]) -> Vec<Vec<String>> {
        if lbs.len() == 1 {
            return lbs[0].iter().map(|v| vec![v.to_string()]).collect();
        }

        let mut ret = Vec::new();
        let (first, others) = lbs.split_at(1);

        for bs in first[0].iter() {
            let ots = Self::arg_expander(others);
            for mut obs in ots {
                obs.push(bs.to_string());
                ret.push(obs);
            }
        }

        ret
    }

    /// returns the total number of sizes in the given patterns.
    pub fn total_sizes(&self) -> usize {
        self.inner.iter().fold(1, |acc, v| acc * v.seq.len())
    }
}

/// finds all ellipses patterns, recursively and parses the ranges numerically.
pub fn find_ellipses_patterns(arg: &str) -> Result<ArgPattern> {
    let mut parts = match ELLIPSES_RE.captures(arg) {
        Some(caps) => caps,
        None => {
            return Err(Error::other(format!(
                "Invalid ellipsis format in ({arg}), Ellipsis range must be provided in format {{N...M}} where N and M are positive integers, M must be greater than N,  with an allowed minimum range of 4"
            )));
        }
    };

    let mut patterns = Vec::new();
    while let Some(prefix) = parts.get(1) {
        let seq = parse_ellipses_range(parts[2].into())?;

        match ELLIPSES_RE.captures(prefix.into()) {
            Some(cs) => {
                patterns.push(Pattern {
                    seq,
                    prefix: String::new(),
                    suffix: parts[3].into(),
                });
                parts = cs;
            }
            None => {
                patterns.push(Pattern {
                    seq,
                    prefix: prefix.as_str().to_owned(),
                    suffix: parts[3].into(),
                });
                break;
            }
        };
    }

    // Check if any of the prefix or suffixes now have flower braces
    // left over, in such a case we generally think that there is
    // perhaps a typo in users input and error out accordingly.
    for p in patterns.iter() {
        if p.prefix.contains(OPEN_BRACES)
            || p.prefix.contains(CLOSE_BRACES)
            || p.suffix.contains(OPEN_BRACES)
            || p.suffix.contains(CLOSE_BRACES)
        {
            return Err(Error::other(format!(
                "Invalid ellipsis format in ({arg}), Ellipsis range must be provided in format {{N...M}} where N and M are positive integers, M must be greater than N,  with an allowed minimum range of 4"
            )));
        }
    }

    Ok(ArgPattern::new(patterns))
}

/// returns true if input arg has ellipses type pattern.
pub fn has_ellipses<T: AsRef<str>>(s: &[T]) -> bool {
    let pattern = [ELLIPSES, OPEN_BRACES, CLOSE_BRACES];

    s.iter().any(|v| pattern.iter().any(|p| v.as_ref().contains(p)))
}

/// Parses an ellipses range pattern of following style
///
/// example:
/// {1...64}
/// {33...64}
pub fn parse_ellipses_range(pattern: &str) -> Result<Vec<String>> {
    if !pattern.contains(OPEN_BRACES) {
        return Err(Error::other("Invalid argument"));
    }
    if !pattern.contains(CLOSE_BRACES) {
        return Err(Error::other("Invalid argument"));
    }

    let ellipses_range: Vec<&str> = pattern
        .trim_start_matches(OPEN_BRACES)
        .trim_end_matches(CLOSE_BRACES)
        .split(ELLIPSES)
        .collect();

    if ellipses_range.len() != 2 {
        return Err(Error::other("Invalid argument"));
    }

    // TODO: Add support for hexadecimals.
    let start = ellipses_range[0].parse::<usize>().map_err(Error::other)?;
    let end = ellipses_range[1].parse::<usize>().map_err(Error::other)?;

    if start > end {
        return Err(Error::other("Invalid argument:range start cannot be bigger than end"));
    }

    let mut ret: Vec<String> = Vec::with_capacity(end - start + 1);
    for i in start..=end {
        if ellipses_range[0].starts_with('0') && ellipses_range[0].len() > 1 {
            ret.push(format!("{:0width$}", i, width = ellipses_range[1].len()));
        } else {
            ret.push(format!("{i}"));
        }
    }

    Ok(ret)
}

pub fn gen_access_key(length: usize) -> Result<String> {
    const ALPHA_NUMERIC_TABLE: [char; 36] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
        'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    if length < 3 {
        return Err(Error::other("access key length is too short"));
    }

    let mut result = String::with_capacity(length);
    let mut rng = rand::rng();

    for _ in 0..length {
        result.push(ALPHA_NUMERIC_TABLE[rng.random_range(0..ALPHA_NUMERIC_TABLE.len())]);
    }

    Ok(result)
}

pub fn gen_secret_key(length: usize) -> Result<String> {
    use base64_simd::URL_SAFE_NO_PAD;

    if length < 8 {
        return Err(Error::other("secret key length is too short"));
    }
    let mut rng = rand::rng();

    let mut key = vec![0u8; URL_SAFE_NO_PAD.estimated_decoded_length(length)];
    rng.fill_bytes(&mut key);

    let encoded = URL_SAFE_NO_PAD.encode_to_string(&key);
    let key_str = encoded.replace("/", "+");

    Ok(key_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_ellipses() {
        // Tests for all args without ellipses.
        let test_cases = [
            (1, vec!["64"], false),
            // Found flower braces, still attempt to parse and throw an error.
            (2, vec!["{1..64}"], true),
            (3, vec!["{1..2..}"], true),
            // Test for valid input.
            (4, vec!["1...64"], true),
            (5, vec!["{1...2O}"], true),
            (6, vec!["..."], true),
            (7, vec!["{-1...1}"], true),
            (8, vec!["{0...-1}"], true),
            (9, vec!["{1....4}"], true),
            (10, vec!["{1...64}"], true),
            (11, vec!["{...}"], true),
            (12, vec!["{1...64}", "{65...128}"], true),
            (13, vec!["http://rustfs{2...3}/export/set{1...64}"], true),
            (
                14,
                vec![
                    "http://rustfs{2...3}/export/set{1...64}",
                    "http://rustfs{2...3}/export/set{65...128}",
                ],
                true,
            ),
            (15, vec!["mydisk-{a...z}{1...20}"], true),
            (16, vec!["mydisk-{1...4}{1..2.}"], true),
        ];

        for (i, args, expected) in test_cases {
            let ret = has_ellipses(&args);
            assert_eq!(ret, expected, "Test{i}: Expected {expected}, got {ret}");
        }
    }

    #[test]
    fn test_find_ellipses_patterns() {
        #[derive(Default)]
        struct TestCase<'a> {
            num: usize,
            pattern: &'a str,
            success: bool,
            want: Vec<Vec<&'a str>>,
        }

        let test_cases = [
            TestCase {
                num: 1,
                pattern: "{1..64}",
                ..Default::default()
            },
            TestCase {
                num: 2,
                pattern: "1...64",
                ..Default::default()
            },
            TestCase {
                num: 2,
                pattern: "...",
                ..Default::default()
            },
            TestCase {
                num: 3,
                pattern: "{1...",
                ..Default::default()
            },
            TestCase {
                num: 4,
                pattern: "...64}",
                ..Default::default()
            },
            TestCase {
                num: 5,
                pattern: "{...}",
                ..Default::default()
            },
            TestCase {
                num: 6,
                pattern: "{-1...1}",
                ..Default::default()
            },
            TestCase {
                num: 7,
                pattern: "{0...-1}",
                ..Default::default()
            },
            TestCase {
                num: 8,
                pattern: "{1...2O}",
                ..Default::default()
            },
            TestCase {
                num: 9,
                pattern: "{64...1}",
                ..Default::default()
            },
            TestCase {
                num: 10,
                pattern: "{1....4}",
                ..Default::default()
            },
            TestCase {
                num: 11,
                pattern: "mydisk-{a...z}{1...20}",
                ..Default::default()
            },
            TestCase {
                num: 12,
                pattern: "mydisk-{1...4}{1..2.}",
                ..Default::default()
            },
            TestCase {
                num: 13,
                pattern: "{1..2.}-mydisk-{1...4}",
                ..Default::default()
            },
            TestCase {
                num: 14,
                pattern: "{{1...4}}",
                ..Default::default()
            },
            TestCase {
                num: 16,
                pattern: "{4...02}",
                ..Default::default()
            },
            TestCase {
                num: 17,
                pattern: "{f...z}",
                ..Default::default()
            },
            // Test for valid input.
            TestCase {
                num: 18,
                pattern: "{1...64}",
                success: true,
                want: vec![
                    vec!["1"],
                    vec!["2"],
                    vec!["3"],
                    vec!["4"],
                    vec!["5"],
                    vec!["6"],
                    vec!["7"],
                    vec!["8"],
                    vec!["9"],
                    vec!["10"],
                    vec!["11"],
                    vec!["12"],
                    vec!["13"],
                    vec!["14"],
                    vec!["15"],
                    vec!["16"],
                    vec!["17"],
                    vec!["18"],
                    vec!["19"],
                    vec!["20"],
                    vec!["21"],
                    vec!["22"],
                    vec!["23"],
                    vec!["24"],
                    vec!["25"],
                    vec!["26"],
                    vec!["27"],
                    vec!["28"],
                    vec!["29"],
                    vec!["30"],
                    vec!["31"],
                    vec!["32"],
                    vec!["33"],
                    vec!["34"],
                    vec!["35"],
                    vec!["36"],
                    vec!["37"],
                    vec!["38"],
                    vec!["39"],
                    vec!["40"],
                    vec!["41"],
                    vec!["42"],
                    vec!["43"],
                    vec!["44"],
                    vec!["45"],
                    vec!["46"],
                    vec!["47"],
                    vec!["48"],
                    vec!["49"],
                    vec!["50"],
                    vec!["51"],
                    vec!["52"],
                    vec!["53"],
                    vec!["54"],
                    vec!["55"],
                    vec!["56"],
                    vec!["57"],
                    vec!["58"],
                    vec!["59"],
                    vec!["60"],
                    vec!["61"],
                    vec!["62"],
                    vec!["63"],
                    vec!["64"],
                ],
            },
            TestCase {
                num: 19,
                pattern: "{1...5} {65...70}",
                success: true,
                want: vec![
                    vec!["1 ", "65"],
                    vec!["2 ", "65"],
                    vec!["3 ", "65"],
                    vec!["4 ", "65"],
                    vec!["5 ", "65"],
                    vec!["1 ", "66"],
                    vec!["2 ", "66"],
                    vec!["3 ", "66"],
                    vec!["4 ", "66"],
                    vec!["5 ", "66"],
                    vec!["1 ", "67"],
                    vec!["2 ", "67"],
                    vec!["3 ", "67"],
                    vec!["4 ", "67"],
                    vec!["5 ", "67"],
                    vec!["1 ", "68"],
                    vec!["2 ", "68"],
                    vec!["3 ", "68"],
                    vec!["4 ", "68"],
                    vec!["5 ", "68"],
                    vec!["1 ", "69"],
                    vec!["2 ", "69"],
                    vec!["3 ", "69"],
                    vec!["4 ", "69"],
                    vec!["5 ", "69"],
                    vec!["1 ", "70"],
                    vec!["2 ", "70"],
                    vec!["3 ", "70"],
                    vec!["4 ", "70"],
                    vec!["5 ", "70"],
                ],
            },
            TestCase {
                num: 20,
                pattern: "{01...036}",
                success: true,
                want: vec![
                    vec!["001"],
                    vec!["002"],
                    vec!["003"],
                    vec!["004"],
                    vec!["005"],
                    vec!["006"],
                    vec!["007"],
                    vec!["008"],
                    vec!["009"],
                    vec!["010"],
                    vec!["011"],
                    vec!["012"],
                    vec!["013"],
                    vec!["014"],
                    vec!["015"],
                    vec!["016"],
                    vec!["017"],
                    vec!["018"],
                    vec!["019"],
                    vec!["020"],
                    vec!["021"],
                    vec!["022"],
                    vec!["023"],
                    vec!["024"],
                    vec!["025"],
                    vec!["026"],
                    vec!["027"],
                    vec!["028"],
                    vec!["029"],
                    vec!["030"],
                    vec!["031"],
                    vec!["032"],
                    vec!["033"],
                    vec!["034"],
                    vec!["035"],
                    vec!["036"],
                ],
            },
            TestCase {
                num: 21,
                pattern: "{001...036}",
                success: true,
                want: vec![
                    vec!["001"],
                    vec!["002"],
                    vec!["003"],
                    vec!["004"],
                    vec!["005"],
                    vec!["006"],
                    vec!["007"],
                    vec!["008"],
                    vec!["009"],
                    vec!["010"],
                    vec!["011"],
                    vec!["012"],
                    vec!["013"],
                    vec!["014"],
                    vec!["015"],
                    vec!["016"],
                    vec!["017"],
                    vec!["018"],
                    vec!["019"],
                    vec!["020"],
                    vec!["021"],
                    vec!["022"],
                    vec!["023"],
                    vec!["024"],
                    vec!["025"],
                    vec!["026"],
                    vec!["027"],
                    vec!["028"],
                    vec!["029"],
                    vec!["030"],
                    vec!["031"],
                    vec!["032"],
                    vec!["033"],
                    vec!["034"],
                    vec!["035"],
                    vec!["036"],
                ],
            },
        ];

        for test_case in test_cases {
            let ret = find_ellipses_patterns(test_case.pattern);
            match ret {
                Ok(v) => {
                    if !test_case.success {
                        panic!("Test{}: Expected failure but passed instead", test_case.num);
                    }

                    let got = v.expand();
                    if got.len() != test_case.want.len() {
                        panic!("Test{}: Expected {}, got {}", test_case.num, test_case.want.len(), got.len());
                    }

                    assert_eq!(got, test_case.want, "Test{}: Expected {:?}, got {:?}", test_case.num, test_case.want, got);
                }
                Err(e) => {
                    if test_case.success {
                        panic!("Test{}: Expected success but failed instead {:?}", test_case.num, e);
                    }
                }
            }
        }
    }
}
