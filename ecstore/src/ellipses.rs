use lazy_static::*;

use anyhow::{Error, Result};
use regex::Regex;

lazy_static! {
    static ref ELLIPSES_RE: Regex = Regex::new(r"(.*)(\{[0-9a-z]*\.\.\.[0-9a-z]*\})(.*)").unwrap();
}

// Ellipses constants
const OPEN_BRACES: &str = "{";
const CLOSE_BRACES: &str = "}";
const ELLIPSES: &str = "...";

#[derive(Debug, Default)]
pub struct Pattern {
    pub prefix: String,
    pub suffix: String,
    pub seq: Vec<String>,
}

impl Pattern {
    #[allow(dead_code)]
    pub fn expand(&self) -> Vec<String> {
        let mut ret = Vec::with_capacity(self.suffix.len());
        for v in self.seq.iter() {
            if !self.prefix.is_empty() && self.suffix.is_empty() {
                ret.push(format!("{}{}", self.prefix, v))
            } else if self.prefix.is_empty() && !self.suffix.is_empty() {
                ret.push(format!("{}{}", v, self.suffix))
            } else if self.prefix.is_empty() && self.suffix.is_empty() {
                ret.push(v.to_string())
            } else {
                ret.push(format!("{}{}{}", self.prefix, v, self.suffix));
            }
        }

        ret
    }
}

#[derive(Debug)]
pub struct ArgPattern {
    pub inner: Vec<Pattern>,
}

impl ArgPattern {
    #[allow(dead_code)]
    pub fn new(inner: Vec<Pattern>) -> Self {
        Self { inner }
    }

    #[allow(dead_code)]
    pub fn expand(&self) -> Vec<Vec<String>> {
        let mut ret = Vec::new();
        for v in self.inner.iter() {
            ret.push(v.expand());
        }

        Self::arg_expander(&ret)
    }

    fn arg_expander(lbs: &Vec<Vec<String>>) -> Vec<Vec<String>> {
        let mut ret = Vec::new();

        if lbs.len() == 1 {
            let arr = lbs.get(0).unwrap();
            for bs in arr {
                ret.push(vec![bs.to_string()])
            }

            return ret;
        }

        let first = &lbs[0];
        let (_, other) = lbs.split_at(1);
        let others = Vec::from(other);
        // let other = lbs[1..lbs.len()];
        for bs in first {
            let ots = Self::arg_expander(&others);
            for obs in ots {
                let mut v = obs;
                v.push(bs.to_string());
                ret.push(v);
            }
        }
        ret
    }
}

#[allow(dead_code)]
pub fn find_ellipses_patterns(arg: &str) -> Result<ArgPattern> {
    let mut caps = match ELLIPSES_RE.captures(arg) {
        Some(caps) => caps,
        None => return Err(Error::msg("Invalid argument")),
    };

    if caps.len() == 0 {
        return Err(Error::msg("Invalid format"));
    }

    let mut pattens = Vec::new();

    loop {
        let m = match caps.get(1) {
            Some(m) => m,
            None => break,
        };

        let cs = match ELLIPSES_RE.captures(m.into()) {
            Some(cs) => cs,
            None => {
                break;
            }
        };

        let seq = caps
            .get(2)
            .map(|m| parse_ellipses_range(m.into()).unwrap_or(Vec::new()))
            .unwrap();
        let suffix = caps
            .get(3)
            .map(|m| m.as_str().to_string())
            .unwrap_or(String::new());
        pattens.push(Pattern {
            suffix,
            seq,
            ..Default::default()
        });

        if cs.len() > 0 {
            caps = cs;
            continue;
        }

        break;
    }

    if caps.len() > 0 {
        let seq = caps
            .get(2)
            .map(|m| parse_ellipses_range(m.into()).unwrap_or(Vec::new()))
            .unwrap();
        let suffix = caps
            .get(3)
            .map(|m| m.as_str().to_string())
            .unwrap_or(String::new());
        let prefix = caps
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or(String::new());
        pattens.push(Pattern {
            prefix,
            suffix,
            seq,
            ..Default::default()
        });
    }

    Ok(ArgPattern::new(pattens))
}

// has_ellipse return ture if has
#[allow(dead_code)]
pub fn has_ellipses(s: &Vec<String>) -> bool {
    let mut ret = true;
    for v in s {
        ret =
            ret && (v.contains(ELLIPSES) || (v.contains(OPEN_BRACES) && v.contains(CLOSE_BRACES)));
    }

    ret
}
// Parses an ellipses range pattern of following style
// `{1...64}`
// `{33...64}`
#[allow(dead_code)]
pub fn parse_ellipses_range(partten: &str) -> Result<Vec<String>> {
    if !partten.contains(OPEN_BRACES) {
        return Err(Error::msg("Invalid argument"));
    }
    if !partten.contains(OPEN_BRACES) {
        return Err(Error::msg("Invalid argument"));
    }

    let v: Vec<&str> = partten
        .trim_start_matches(OPEN_BRACES)
        .trim_end_matches(CLOSE_BRACES)
        .split(ELLIPSES)
        .collect();

    if v.len() != 2 {
        return Err(Error::msg("Invalid argument"));
    }

    // let start = usize::from_str_radix(v[0], 16)?;
    // let end = usize::from_str_radix(v[1], 16)?;

    let start = v[0].parse::<usize>()?;
    let end = v[1].parse::<usize>()?;

    if start > end {
        return Err(Error::msg(
            "Invalid argument:range start cannot be bigger than end",
        ));
    }

    let mut ret: Vec<String> = Vec::with_capacity(end + 1);

    for i in start..end + 1 {
        if v[0].starts_with('0') && v[0].len() > 1 {
            ret.push(format!("{:0witdth$}", i, witdth = v[0].len()));
        } else {
            ret.push(format!("{}", i));
        }
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_ellipses() {
        assert_eq!(has_ellipses(vec!["/sdf".to_string()].as_ref()), false);
        assert_eq!(has_ellipses(vec!["{1...3}".to_string()].as_ref()), true);
    }

    #[test]
    fn test_parse_ellipses_range() {
        let s = "{1...16}";

        match parse_ellipses_range(s) {
            Ok(res) => {
                println!("{:?}", res)
            }
            Err(err) => println!("{err:?}"),
        };
    }

    #[test]
    fn test_find_ellipses_patterns() {
        use std::result::Result::Ok;
        let pattern = "http://rustfs{1...2}:9000/mnt/disk{1...16}";
        // let pattern = "http://[2001:3984:3989::{01...f}]/disk{1...10}";
        match find_ellipses_patterns(pattern) {
            Ok(caps) => println!("caps{caps:?}"),
            Err(err) => println!("{err:?}"),
        }
    }
}
