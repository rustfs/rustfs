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

use std::path::Path;
use std::path::PathBuf;

pub const GLOBAL_DIR_SUFFIX: &str = "__XLDIR__";

pub const SLASH_SEPARATOR: &str = "/";

pub const GLOBAL_DIR_SUFFIX_WITH_SLASH: &str = "__XLDIR__/";

pub fn has_suffix(s: &str, suffix: &str) -> bool {
    if cfg!(target_os = "windows") {
        s.to_lowercase().ends_with(&suffix.to_lowercase())
    } else {
        s.ends_with(suffix)
    }
}

pub fn encode_dir_object(object: &str) -> String {
    if has_suffix(object, SLASH_SEPARATOR) {
        format!("{}{}", object.trim_end_matches(SLASH_SEPARATOR), GLOBAL_DIR_SUFFIX)
    } else {
        object.to_string()
    }
}

pub fn is_dir_object(object: &str) -> bool {
    let obj = encode_dir_object(object);
    obj.ends_with(GLOBAL_DIR_SUFFIX)
}

#[allow(dead_code)]
pub fn decode_dir_object(object: &str) -> String {
    if has_suffix(object, GLOBAL_DIR_SUFFIX) {
        format!("{}{}", object.trim_end_matches(GLOBAL_DIR_SUFFIX), SLASH_SEPARATOR)
    } else {
        object.to_string()
    }
}

pub fn retain_slash(s: &str) -> String {
    if s.is_empty() {
        return s.to_string();
    }
    if s.ends_with(SLASH_SEPARATOR) {
        s.to_string()
    } else {
        format!("{s}{SLASH_SEPARATOR}")
    }
}

pub fn strings_has_prefix_fold(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && (s[..prefix.len()] == *prefix || s[..prefix.len()].eq_ignore_ascii_case(prefix))
}

pub fn has_prefix(s: &str, prefix: &str) -> bool {
    if cfg!(target_os = "windows") {
        return strings_has_prefix_fold(s, prefix);
    }

    s.starts_with(prefix)
}

pub fn path_join(elem: &[PathBuf]) -> PathBuf {
    let mut joined_path = PathBuf::new();

    for path in elem {
        joined_path.push(path);
    }

    joined_path
}

pub fn path_join_buf(elements: &[&str]) -> String {
    let trailing_slash = !elements.is_empty() && elements.last().unwrap().ends_with(SLASH_SEPARATOR);

    let mut dst = String::new();
    let mut added = 0;

    for e in elements {
        if added > 0 || !e.is_empty() {
            if added > 0 {
                dst.push_str(SLASH_SEPARATOR);
            }
            dst.push_str(e);
            added += e.len();
        }
    }

    let result = dst.to_string();
    let cpath = Path::new(&result).components().collect::<PathBuf>();
    let clean_path = cpath.to_string_lossy();

    if trailing_slash {
        return format!("{clean_path}{SLASH_SEPARATOR}");
    }
    clean_path.to_string()
}

pub fn path_to_bucket_object_with_base_path(bash_path: &str, path: &str) -> (String, String) {
    let path = path.trim_start_matches(bash_path).trim_start_matches(SLASH_SEPARATOR);
    if let Some(m) = path.find(SLASH_SEPARATOR) {
        return (path[..m].to_string(), path[m + SLASH_SEPARATOR.len()..].to_string());
    }

    (path.to_string(), "".to_string())
}

pub fn path_to_bucket_object(s: &str) -> (String, String) {
    path_to_bucket_object_with_base_path("", s)
}

pub fn base_dir_from_prefix(prefix: &str) -> String {
    let mut base_dir = dir(prefix).to_owned();
    if base_dir == "." || base_dir == "./" || base_dir == "/" {
        base_dir = "".to_owned();
    }
    if !prefix.contains('/') {
        base_dir = "".to_owned();
    }
    if !base_dir.is_empty() && !base_dir.ends_with(SLASH_SEPARATOR) {
        base_dir.push_str(SLASH_SEPARATOR);
    }
    base_dir
}

pub struct LazyBuf {
    s: String,
    buf: Option<Vec<u8>>,
    w: usize,
}

impl LazyBuf {
    pub fn new(s: String) -> Self {
        LazyBuf { s, buf: None, w: 0 }
    }

    pub fn index(&self, i: usize) -> u8 {
        if let Some(ref buf) = self.buf {
            buf[i]
        } else {
            self.s.as_bytes()[i]
        }
    }

    pub fn append(&mut self, c: u8) {
        if self.buf.is_none() {
            if self.w < self.s.len() && self.s.as_bytes()[self.w] == c {
                self.w += 1;
                return;
            }
            let mut new_buf = vec![0; self.s.len()];
            new_buf[..self.w].copy_from_slice(&self.s.as_bytes()[..self.w]);
            self.buf = Some(new_buf);
        }

        if let Some(ref mut buf) = self.buf {
            buf[self.w] = c;
            self.w += 1;
        }
    }

    pub fn string(&self) -> String {
        if let Some(ref buf) = self.buf {
            String::from_utf8(buf[..self.w].to_vec()).unwrap()
        } else {
            self.s[..self.w].to_string()
        }
    }
}

pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }

    let rooted = path.starts_with('/');
    let n = path.len();
    let mut out = LazyBuf::new(path.to_string());
    let mut r = 0;
    let mut dotdot = 0;

    if rooted {
        out.append(b'/');
        r = 1;
        dotdot = 1;
    }

    while r < n {
        match path.as_bytes()[r] {
            b'/' => {
                // Empty path element
                r += 1;
            }
            b'.' if r + 1 == n || path.as_bytes()[r + 1] == b'/' => {
                // . element
                r += 1;
            }
            b'.' if path.as_bytes()[r + 1] == b'.' && (r + 2 == n || path.as_bytes()[r + 2] == b'/') => {
                // .. element: remove to last /
                r += 2;

                if out.w > dotdot {
                    // Can backtrack
                    out.w -= 1;
                    while out.w > dotdot && out.index(out.w) != b'/' {
                        out.w -= 1;
                    }
                } else if !rooted {
                    // Cannot backtrack but not rooted, so append .. element.
                    if out.w > 0 {
                        out.append(b'/');
                    }
                    out.append(b'.');
                    out.append(b'.');
                    dotdot = out.w;
                }
            }
            _ => {
                // Real path element.
                // Add slash if needed
                if (rooted && out.w != 1) || (!rooted && out.w != 0) {
                    out.append(b'/');
                }

                // Copy element
                while r < n && path.as_bytes()[r] != b'/' {
                    out.append(path.as_bytes()[r]);
                    r += 1;
                }
            }
        }
    }

    // Turn empty string into "."
    if out.w == 0 {
        return ".".to_string();
    }

    out.string()
}

pub fn split(path: &str) -> (&str, &str) {
    // Find the last occurrence of the '/' character
    if let Some(i) = path.rfind('/') {
        // Return the directory (up to and including the last '/') and the file name
        return (&path[..i + 1], &path[i + 1..]);
    }
    // If no '/' is found, return an empty string for the directory and the whole path as the file name
    (path, "")
}

pub fn dir(path: &str) -> String {
    let (a, _) = split(path);
    clean(a)
}

pub fn trim_etag(etag: &str) -> String {
    etag.trim_matches('"').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_dir_from_prefix() {
        let a = "da/";
        // Test base_dir_from_prefix function
        let result = base_dir_from_prefix(a);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_clean() {
        assert_eq!(clean(""), ".");
        assert_eq!(clean("abc"), "abc");
        assert_eq!(clean("abc/def"), "abc/def");
        assert_eq!(clean("a/b/c"), "a/b/c");
        assert_eq!(clean("."), ".");
        assert_eq!(clean(".."), "..");
        assert_eq!(clean("../.."), "../..");
        assert_eq!(clean("../../abc"), "../../abc");
        assert_eq!(clean("/abc"), "/abc");
        assert_eq!(clean("/"), "/");
        assert_eq!(clean("abc/"), "abc");
        assert_eq!(clean("abc/def/"), "abc/def");
        assert_eq!(clean("a/b/c/"), "a/b/c");
        assert_eq!(clean("./"), ".");
        assert_eq!(clean("../"), "..");
        assert_eq!(clean("../../"), "../..");
        assert_eq!(clean("/abc/"), "/abc");
        assert_eq!(clean("abc//def//ghi"), "abc/def/ghi");
        assert_eq!(clean("//abc"), "/abc");
        assert_eq!(clean("///abc"), "/abc");
        assert_eq!(clean("//abc//"), "/abc");
        assert_eq!(clean("abc//"), "abc");
        assert_eq!(clean("abc/./def"), "abc/def");
        assert_eq!(clean("/./abc/def"), "/abc/def");
        assert_eq!(clean("abc/."), "abc");
        assert_eq!(clean("abc/./../def"), "def");
        assert_eq!(clean("abc//./../def"), "def");
        assert_eq!(clean("abc/../../././../def"), "../../def");

        assert_eq!(clean("abc/def/ghi/../jkl"), "abc/def/jkl");
        assert_eq!(clean("abc/def/../ghi/../jkl"), "abc/jkl");
        assert_eq!(clean("abc/def/.."), "abc");
        assert_eq!(clean("abc/def/../.."), ".");
        assert_eq!(clean("/abc/def/../.."), "/");
        assert_eq!(clean("abc/def/../../.."), "..");
        assert_eq!(clean("/abc/def/../../.."), "/");
        assert_eq!(clean("abc/def/../../../ghi/jkl/../../../mno"), "../../mno");
    }
}
