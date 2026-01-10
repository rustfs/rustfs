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

#[cfg(target_os = "windows")]
const SLASH_SEPARATOR: char = '\\';
#[cfg(not(target_os = "windows"))]
const SLASH_SEPARATOR: char = '/';

/// GLOBAL_DIR_SUFFIX is a special suffix used to denote directory objects
/// in object storage systems that do not have a native directory concept.
pub const GLOBAL_DIR_SUFFIX: &str = "__XLDIR__";

/// SLASH_SEPARATOR_STR is the string representation of the path separator
/// used in the current operating system.
pub const SLASH_SEPARATOR_STR: &str = if cfg!(target_os = "windows") { "\\" } else { "/" };

/// GLOBAL_DIR_SUFFIX_WITH_SLASH is a special suffix used to denote directory objects
/// in object storage systems that do not have a native directory concept.
pub const GLOBAL_DIR_SUFFIX_WITH_SLASH: &str = "__XLDIR__/"; // Note: Keep as "/" for compatibility, but ensure usage is adjusted

/// has_suffix checks if the string `s` ends with the specified `suffix`,
/// performing a case-insensitive comparison on Windows platforms.
///
/// # Arguments
/// * `s` - A string slice that holds the string to be checked.
/// * `suffix` - A string slice that holds the suffix to check for.
///
/// # Returns
/// A boolean indicating whether `s` ends with `suffix`.
///
pub fn has_suffix(s: &str, suffix: &str) -> bool {
    if cfg!(target_os = "windows") {
        s.to_lowercase().ends_with(&suffix.to_lowercase())
    } else {
        s.ends_with(suffix)
    }
}

/// encode_dir_object encodes a directory object by appending
/// a special suffix if it ends with a slash.
///
/// # Arguments
/// * `object` - A string slice that holds the object to be encoded.
///
/// # Returns
/// A `String` representing the encoded directory object.
///
pub fn encode_dir_object(object: &str) -> String {
    if has_suffix(object, SLASH_SEPARATOR_STR) {
        format!("{}{}", object.trim_end_matches(SLASH_SEPARATOR), GLOBAL_DIR_SUFFIX)
    } else {
        object.to_string()
    }
}

/// is_dir_object checks if the given object string represents
/// a directory object by verifying if it ends with the special suffix.
///
/// # Arguments
/// * `object` - A string slice that holds the object to be checked.
///
/// # Returns
/// A boolean indicating whether the object is a directory object.
///
pub fn is_dir_object(object: &str) -> bool {
    let obj = encode_dir_object(object);
    obj.ends_with(GLOBAL_DIR_SUFFIX)
}

/// decode_dir_object decodes a directory object by removing
/// the special suffix if it is present.
///
/// # Arguments
/// * `object` - A string slice that holds the object to be decoded.
///
///# Returns
/// A `String` representing the decoded directory object.
///
#[allow(dead_code)]
pub fn decode_dir_object(object: &str) -> String {
    if has_suffix(object, GLOBAL_DIR_SUFFIX) {
        format!("{}{}", object.trim_end_matches(GLOBAL_DIR_SUFFIX), SLASH_SEPARATOR)
    } else {
        object.to_string()
    }
}

/// retain_slash ensures that the given string `s` ends with a slash.
/// If it does not, a slash is appended.
///
/// # Arguments
/// * `s` - A string slice that holds the string to be processed.
///
/// # Returns
/// A `String` that ends with a slash.
///
pub fn retain_slash(s: &str) -> String {
    if s.is_empty() {
        return s.to_string();
    }
    if s.ends_with(SLASH_SEPARATOR_STR) {
        s.to_string()
    } else {
        format!("{s}{SLASH_SEPARATOR_STR}")
    }
}

/// strings_has_prefix_fold checks if the string `s` starts with the specified `prefix`,
/// performing a case-insensitive comparison on Windows platforms.
///
/// # Arguments
/// * `s` - A string slice that holds the string to be checked.
/// * `prefix` - A string slice that holds the prefix to check for.
///
/// # Returns
/// A boolean indicating whether `s` starts with `prefix`.
///
pub fn strings_has_prefix_fold(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && (s[..prefix.len()] == *prefix || s[..prefix.len()].eq_ignore_ascii_case(prefix))
}

/// has_prefix checks if the string `s` starts with the specified `prefix`,
/// performing a case-insensitive comparison on Windows platforms.
///
/// # Arguments
/// * `s` - A string slice that holds the string to be checked.
/// * `prefix` - A string slice that holds the prefix to check for.
///
/// # Returns
/// A boolean indicating whether `s` starts with `prefix`.
///
pub fn has_prefix(s: &str, prefix: &str) -> bool {
    if cfg!(target_os = "windows") {
        return strings_has_prefix_fold(s, prefix);
    }

    s.starts_with(prefix)
}

/// path_join joins multiple path elements into a single PathBuf,
/// ensuring that the resulting path is clean and properly formatted.
///
/// # Arguments
/// * `elem` - A slice of path elements to be joined.
///
/// # Returns
/// A PathBuf representing the joined path.
///
pub fn path_join<P: AsRef<Path>>(elem: &[P]) -> PathBuf {
    if elem.is_empty() {
        return PathBuf::new();
    }
    let mut result = elem[0].as_ref().to_path_buf();
    for e in &elem[1..] {
        result.push(e.as_ref());
    }
    result
}

/// path_join_buf joins multiple string path elements into a single String,
/// ensuring that the resulting path is clean and properly formatted.
///
/// # Arguments
/// * `elements` - A slice of string path elements to be joined.
///
/// # Returns
/// A String representing the joined path.
///
pub fn path_join_buf(elements: &[&str]) -> String {
    let trailing_slash = !elements.is_empty() && elements.last().is_some_and(|last| last.ends_with(SLASH_SEPARATOR));

    let mut dst = String::new();
    let mut added = 0;

    for e in elements {
        if added > 0 || !e.is_empty() {
            if added > 0 {
                dst.push(SLASH_SEPARATOR);
            }
            dst.push_str(e);
            added += e.len();
        }
    }

    if path_needs_clean(dst.as_bytes()) {
        let mut clean_path = clean(&dst);
        if trailing_slash {
            clean_path.push(SLASH_SEPARATOR);
        }
        return clean_path;
    }

    if trailing_slash {
        dst.push(SLASH_SEPARATOR);
    }

    dst
}

/// path_needs_clean returns whether path cleaning may change the path.
/// Will detect all cases that will be cleaned,
/// but may produce false positives on non-trivial paths.
///
/// # Arguments
/// * `path` - A byte slice that holds the path to be checked.
///
/// # Returns
/// A boolean indicating whether the path needs cleaning.
///
fn path_needs_clean(path: &[u8]) -> bool {
    if path.is_empty() {
        return true;
    }

    let sep = SLASH_SEPARATOR as u8;

    for (i, &b) in path.iter().enumerate() {
        if b == sep {
            if i > 0 && path[i - 1] == sep {
                return true;
            }
            if i > 0 && path[i - 1] == b'.' && (i == 1 || path[i - 2] == sep) {
                return true;
            }
            if i > 1 && path[i - 1] == b'.' && path[i - 2] == b'.' && (i == 2 || path[i - 3] == sep) {
                return true;
            }
        }
    }

    false
}

/// path_to_bucket_object_with_base_path splits a given path into bucket and object components,
/// considering a base path to trim from the start.
///
/// # Arguments
/// * `bash_path` - A string slice that holds the base path to be trimmed.
/// * `path` - A string slice that holds the path to be split.
///
/// # Returns
/// A tuple containing the bucket and object as `String`s.
///
pub fn path_to_bucket_object_with_base_path(bash_path: &str, path: &str) -> (String, String) {
    let path = path.trim_start_matches(bash_path).trim_start_matches(SLASH_SEPARATOR);
    if let Some(m) = path.find(SLASH_SEPARATOR) {
        return (path[..m].to_string(), path[m + SLASH_SEPARATOR_STR.len()..].to_string());
    }

    (path.to_string(), "".to_string())
}

/// path_to_bucket_object splits a given path into bucket and object components.
///
/// # Arguments
/// * `s` - A string slice that holds the path to be split.
///
/// # Returns
/// A tuple containing the bucket and object as `String`s.
///
pub fn path_to_bucket_object(s: &str) -> (String, String) {
    path_to_bucket_object_with_base_path("", s)
}

/// base_dir_from_prefix extracts the base directory from a given prefix.
///
/// # Arguments
/// * `prefix` - A string slice that holds the prefix to be processed.
///
/// # Returns
/// A `String` representing the base directory extracted from the prefix.
///
pub fn base_dir_from_prefix(prefix: &str) -> String {
    let mut base_dir = dir(prefix).to_owned();
    if base_dir == "." || base_dir == "./" || base_dir == "/" {
        base_dir = "".to_owned();
    }
    if !prefix.contains('/') {
        base_dir = "".to_owned();
    }
    if !base_dir.is_empty() && !base_dir.ends_with(SLASH_SEPARATOR) {
        base_dir.push_str(SLASH_SEPARATOR_STR);
    }
    base_dir
}

/// LazyBuf is a structure that efficiently builds a byte buffer
/// from a string by delaying the allocation of the buffer until
/// a modification is necessary. It allows appending bytes and
/// retrieving the current string representation.
pub struct LazyBuf {
    s: String,
    buf: Option<Vec<u8>>,
    w: usize,
}

impl LazyBuf {
    /// Creates a new LazyBuf with the given string.
    ///
    /// # Arguments
    /// * `s` - A string to initialize the LazyBuf.
    ///
    /// # Returns
    /// A new instance of LazyBuf.
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

/// clean returns the shortest path name equivalent to path
/// by purely lexical processing. It applies the following rules
/// iteratively until no further processing can be done:
///
/// 1. Replace multiple slashes with a single slash.
/// 2. Eliminate each . path name element (the current directory).
/// 3. Eliminate each inner .. path name element (the parent directory)
///    along with the non-.. element that precedes it.
/// 4. Eliminate .. elements that begin a rooted path,
///    that is, replace "/.." by "/" at the beginning of a path.
///
/// If the result of this process is an empty string, clean returns the string ".".
///
/// This function is adapted to work cross-platform by using the appropriate path separator.
/// It does not handle Windows drive letters or UNC paths.
///
/// # Arguments
/// * `path` - A string slice that holds the path to be cleaned.
///
/// # Returns
/// A `String` representing the cleaned path.
///
pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }

    let sep = SLASH_SEPARATOR;
    let path_bytes = path.as_bytes();
    let mut result = Vec::new();
    let mut r = 0;

    // Skip leading separators
    while r < path_bytes.len() && path_bytes[r] == sep as u8 {
        r += 1;
    }

    let mut in_segment_start: Option<usize> = None;
    let mut dot_count = 0;

    while r < path_bytes.len() {
        let b = path_bytes[r];
        if b == sep as u8 {
            if let Some(start) = in_segment_start {
                // let _segment_len = r - start;
                if dot_count == 0 {
                    // Regular segment
                    if !result.is_empty() {
                        result.push(sep as u8);
                    }
                    result.extend_from_slice(&path_bytes[start..r]);
                } else if dot_count == 1 {
                    // Single dot, ignore
                } else if dot_count == 2 {
                    // Double dot, pop previous segment
                    if !result.is_empty() {
                        let last_sep = result.iter().rposition(|&x| x == sep as u8);
                        if let Some(pos) = last_sep {
                            result.truncate(pos);
                        } else {
                            result.clear();
                        }
                    }
                }
                in_segment_start = None;
                dot_count = 0;
            }
        } else if b == b'.' {
            if in_segment_start.is_none() {
                in_segment_start = Some(r);
            }
            dot_count += 1;
        } else if in_segment_start.is_none() {
            in_segment_start = Some(r);
            dot_count = 0;
        } else {
            dot_count = 0;
        }
        r += 1;
    }

    // Handle last segment
    if let Some(start) = in_segment_start {
        if dot_count == 0 {
            if !result.is_empty() {
                result.push(sep as u8);
            }
            result.extend_from_slice(&path_bytes[start..r]);
        } else if dot_count == 1 {
            // Ignore
        } else if dot_count == 2 && !result.is_empty() {
            let last_sep = result.iter().rposition(|&x| x == sep as u8);
            if let Some(pos) = last_sep {
                result.truncate(pos);
            } else {
                result.clear();
            }
        }
    }

    if result.is_empty() {
        ".".to_string()
    } else {
        String::from_utf8(result).unwrap_or_else(|_| path.to_string())
    }
}

/// split splits path immediately after the final slash,
/// separating it into a directory and file name component.
/// If there is no slash in path, split returns
/// ("", path).
///
/// # Arguments
/// * `path` - A string slice that holds the path to be split.
///
/// # Returns
/// A tuple containing the directory and file name as string slices.
///
pub fn split(path: &str) -> (&str, &str) {
    // Find the last occurrence of the '/' character
    if let Some(i) = path.rfind('/') {
        // Return the directory (up to and including the last '/') and the file name
        return (&path[..i + 1], &path[i + 1..]);
    }
    // If no '/' is found, return an empty string for the directory and the whole path as the file name
    (path, "")
}

/// dir returns all but the last element of path,
/// typically the path's directory. After dropping the final
/// element, the path is cleaned. If the path is empty,
/// dir returns ".".
///
/// # Arguments
/// * `path` - A string slice that holds the path to be processed.
///
/// # Returns
/// A `String` representing the directory part of the path.
///
pub fn dir(path: &str) -> String {
    let (a, _) = split(path);
    clean(a)
}

/// trim_etag removes surrounding double quotes from an ETag string.
///
/// # Arguments
/// * `etag` - A string slice that holds the ETag to be trimmed.
///
/// # Returns
/// A `String` representing the trimmed ETag.
///
pub fn trim_etag(etag: &str) -> String {
    etag.trim_matches('"').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_join_buf() {
        #[cfg(not(target_os = "windows"))]
        {
            assert_eq!(path_join_buf(&["a", "b"]), "a/b");
            assert_eq!(path_join_buf(&["a/", "b"]), "a/b");
        }
        #[cfg(target_os = "windows")]
        {
            assert_eq!(path_join_buf(&["a", "b"]), "a\\b");
            assert_eq!(path_join_buf(&["a\\", "b"]), "a\\b");
        }
    }

    #[test]
    fn test_trim_etag() {
        // Test with quoted ETag
        assert_eq!(trim_etag("\"abc123\""), "abc123");

        // Test with unquoted ETag
        assert_eq!(trim_etag("abc123"), "abc123");

        // Test with empty string
        assert_eq!(trim_etag(""), "");

        // Test with only quotes
        assert_eq!(trim_etag("\"\""), "");

        // Test with MD5 hash
        assert_eq!(trim_etag("\"2c7ab85a893283e98c931e9511add182\""), "2c7ab85a893283e98c931e9511add182");

        // Test with multipart ETag format
        assert_eq!(trim_etag("\"098f6bcd4621d373cade4e832627b4f6-2\""), "098f6bcd4621d373cade4e832627b4f6-2");
    }

    #[test]
    fn test_base_dir_from_prefix() {
        let a = "da/";
        // Test base_dir_from_prefix function
        let result = base_dir_from_prefix(a);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_clean() {
        #[cfg(not(target_os = "windows"))]
        {
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

        #[cfg(target_os = "windows")]
        {
            assert_eq!(clean("a\\b\\..\\c"), "a\\c");
            assert_eq!(clean("a\\\\b"), "a\\b");
        }
    }

    #[test]
    fn test_path_needs_clean() {
        struct PathTest {
            path: &'static str,
            result: &'static str,
        }

        let cleantests = vec![
            // Already clean
            PathTest { path: "", result: "." },
            PathTest {
                path: "abc",
                result: "abc",
            },
            PathTest {
                path: "abc/def",
                result: "abc/def",
            },
            PathTest {
                path: "a/b/c",
                result: "a/b/c",
            },
            PathTest { path: ".", result: "." },
            PathTest {
                path: "..",
                result: "..",
            },
            PathTest {
                path: "../..",
                result: "../..",
            },
            PathTest {
                path: "../../abc",
                result: "../../abc",
            },
            PathTest {
                path: "/abc",
                result: "/abc",
            },
            PathTest {
                path: "/abc/def",
                result: "/abc/def",
            },
            PathTest { path: "/", result: "/" },
            // Remove trailing slash
            PathTest {
                path: "abc/",
                result: "abc",
            },
            PathTest {
                path: "abc/def/",
                result: "abc/def",
            },
            PathTest {
                path: "a/b/c/",
                result: "a/b/c",
            },
            PathTest { path: "./", result: "." },
            PathTest {
                path: "../",
                result: "..",
            },
            PathTest {
                path: "../../",
                result: "../..",
            },
            PathTest {
                path: "/abc/",
                result: "/abc",
            },
            // Remove doubled slash
            PathTest {
                path: "abc//def//ghi",
                result: "abc/def/ghi",
            },
            PathTest {
                path: "//abc",
                result: "/abc",
            },
            PathTest {
                path: "///abc",
                result: "/abc",
            },
            PathTest {
                path: "//abc//",
                result: "/abc",
            },
            PathTest {
                path: "abc//",
                result: "abc",
            },
            // Remove . elements
            PathTest {
                path: "abc/./def",
                result: "abc/def",
            },
            PathTest {
                path: "/./abc/def",
                result: "/abc/def",
            },
            PathTest {
                path: "abc/.",
                result: "abc",
            },
            // Remove .. elements
            PathTest {
                path: "abc/def/ghi/../jkl",
                result: "abc/def/jkl",
            },
            PathTest {
                path: "abc/def/../ghi/../jkl",
                result: "abc/jkl",
            },
            PathTest {
                path: "abc/def/..",
                result: "abc",
            },
            PathTest {
                path: "abc/def/../..",
                result: ".",
            },
            PathTest {
                path: "/abc/def/../..",
                result: "/",
            },
            PathTest {
                path: "abc/def/../../..",
                result: "..",
            },
            PathTest {
                path: "/abc/def/../../..",
                result: "/",
            },
            PathTest {
                path: "abc/def/../../../ghi/jkl/../../../mno",
                result: "../../mno",
            },
            // Combinations
            PathTest {
                path: "abc/./../def",
                result: "def",
            },
            PathTest {
                path: "abc//./../def",
                result: "def",
            },
            PathTest {
                path: "abc/../../././../def",
                result: "../../def",
            },
        ];

        for test in cleantests {
            let want = test.path != test.result;
            let got = path_needs_clean(test.path.as_bytes());
            if want && !got {
                panic!("input: {:?}, want {}, got {}", test.path, want, got);
            }

            assert_eq!(clean(test.path), test.result);
        }
    }

    #[test]
    fn test_path_join() {
        // Test empty input
        let result = path_join::<&str>(&[]);
        assert_eq!(result, PathBuf::from("."));

        // Test single path
        let result = path_join(&[PathBuf::from("abc")]);
        assert_eq!(result, PathBuf::from("abc"));

        // Test single absolute path
        let result = path_join(&[PathBuf::from("/abc")]);
        assert_eq!(result, PathBuf::from("/abc"));

        // Test multiple relative paths
        let result = path_join(&[PathBuf::from("a"), PathBuf::from("b"), PathBuf::from("c")]);
        assert_eq!(result, PathBuf::from("a/b/c"));

        // Test absolute path with relative paths
        let result = path_join(&[PathBuf::from("/a"), PathBuf::from("b"), PathBuf::from("c")]);
        assert_eq!(result, PathBuf::from("/a/b/c"));

        // Test paths with dots
        let result = path_join(&[PathBuf::from("a"), PathBuf::from("."), PathBuf::from("b")]);
        assert_eq!(result, PathBuf::from("a/b"));

        // Test paths with double dots
        let result = path_join(&[
            PathBuf::from("a"),
            PathBuf::from("b"),
            PathBuf::from(".."),
            PathBuf::from("c"),
        ]);
        assert_eq!(result, PathBuf::from("a/c"));

        // Test paths that need cleaning
        let result = path_join(&[PathBuf::from("a//b"), PathBuf::from("c")]);
        assert_eq!(result, PathBuf::from("a/b/c"));

        // Test trailing slash preservation
        let result = path_join(&[PathBuf::from("a"), PathBuf::from("b/")]);
        assert_eq!(result, PathBuf::from("a/b/"));

        // Test empty path in middle
        let result = path_join(&[PathBuf::from("a"), PathBuf::from(""), PathBuf::from("b")]);
        assert_eq!(result, PathBuf::from("a/b"));

        // Test multiple absolute paths (should concatenate)
        let result = path_join(&[PathBuf::from("/a"), PathBuf::from("/b"), PathBuf::from("c")]);
        assert_eq!(result, PathBuf::from("/a/b/c"));

        // Test complex case with various path elements
        let result = path_join(&[
            PathBuf::from("a"),
            PathBuf::from(".."),
            PathBuf::from("b"),
            PathBuf::from("."),
            PathBuf::from("c"),
        ]);
        assert_eq!(result, PathBuf::from("b/c"));
    }
}
