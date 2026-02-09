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

#[inline]
pub fn is_separator(c: u8) -> bool {
    c == b'/' || (cfg!(target_os = "windows") && c == b'\\')
}

/// Checks if the string `s` ends with `suffix`.
///
/// On Windows, this comparison is case-insensitive.
/// On other platforms, it is case-sensitive.
pub fn has_suffix(s: &str, suffix: &str) -> bool {
    if cfg!(target_os = "windows") {
        s.to_lowercase().ends_with(&suffix.to_lowercase())
    } else {
        s.ends_with(suffix)
    }
}

/// Encodes a directory object name by replacing the trailing slash with `GLOBAL_DIR_SUFFIX`.
///
/// If the object name ends with a slash, it is considered a directory object.
/// The trailing slash is removed and `GLOBAL_DIR_SUFFIX` is appended.
/// If it does not end with a slash, the name is returned as is.
pub fn encode_dir_object(object: &str) -> String {
    if has_suffix(object, SLASH_SEPARATOR) {
        format!("{}{}", object.trim_end_matches(SLASH_SEPARATOR), GLOBAL_DIR_SUFFIX)
    } else {
        object.to_string()
    }
}

/// Checks if the given object name represents a directory object.
///
/// Returns true if the object name ends with `GLOBAL_DIR_SUFFIX`.
pub fn is_dir_object(object: &str) -> bool {
    let obj = encode_dir_object(object);
    obj.ends_with(GLOBAL_DIR_SUFFIX)
}

/// Decodes a directory object name by replacing `GLOBAL_DIR_SUFFIX` with a trailing slash.
///
/// If the object name ends with `GLOBAL_DIR_SUFFIX`, it is replaced with a slash.
/// Otherwise, the name is returned as is.
#[allow(dead_code)]
pub fn decode_dir_object(object: &str) -> String {
    if has_suffix(object, GLOBAL_DIR_SUFFIX) {
        format!("{}{}", object.trim_end_matches(GLOBAL_DIR_SUFFIX), SLASH_SEPARATOR)
    } else {
        object.to_string()
    }
}

/// Ensures that the string ends with a trailing slash if it is not empty.
///
/// If the string is empty, it is returned as is.
/// If it already ends with a slash, it is returned as is.
/// Otherwise, a slash is appended.
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

/// Checks if string `s` starts with `prefix` using case-insensitive comparison.
pub fn strings_has_prefix_fold(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && (s[..prefix.len()] == *prefix || s[..prefix.len()].eq_ignore_ascii_case(prefix))
}

/// Checks if string `s` starts with `prefix`.
///
/// On Windows, this comparison is case-insensitive.
/// On other platforms, it is case-sensitive.
pub fn has_prefix(s: &str, prefix: &str) -> bool {
    if cfg!(target_os = "windows") {
        return strings_has_prefix_fold(s, prefix);
    }

    s.starts_with(prefix)
}

/// Joins multiple path components into a single `PathBuf`.
///
/// This function normalizes the path separators to forward slashes (`/`) internally
/// and cleans the path (resolving `.` and `..`).
///
/// On Windows, backslashes in input components are treated as separators and normalized.
pub fn path_join<P: AsRef<Path>>(elem: &[P]) -> PathBuf {
    let trailing_slash = !elem.is_empty()
        && elem.last().is_some_and(|last| {
            let s = last.as_ref().to_string_lossy();
            s.ends_with(SLASH_SEPARATOR) || (cfg!(target_os = "windows") && s.ends_with('\\'))
        });

    let len = elem.iter().map(|s| s.as_ref().to_string_lossy().len()).sum::<usize>() + elem.len();
    let mut dst = String::with_capacity(len);
    let mut added = 0;

    for e in elem {
        let s = e.as_ref().to_string_lossy();
        if added > 0 || !s.is_empty() {
            if added > 0 {
                dst.push_str(SLASH_SEPARATOR);
            }
            dst.push_str(&s);
            added += s.len();
        }
    }

    if path_needs_clean(dst.as_bytes()) {
        let mut clean_path = clean(&dst);
        if trailing_slash {
            clean_path.push_str(SLASH_SEPARATOR);
        }
        return clean_path.into();
    }

    if trailing_slash {
        dst.push_str(SLASH_SEPARATOR);
    }

    dst.into()
}

/// Joins multiple string path components into a single `String`.
///
/// This function normalizes the path separators to forward slashes (`/`) internally
/// and cleans the path (resolving `.` and `..`).
///
/// On Windows, backslashes in input components are treated as separators and normalized.
pub fn path_join_buf(elements: &[&str]) -> String {
    let trailing_slash = !elements.is_empty()
        && elements
            .last()
            .is_some_and(|last| last.ends_with(SLASH_SEPARATOR) || (cfg!(target_os = "windows") && last.ends_with('\\')));

    let len = elements.iter().map(|s| s.len()).sum::<usize>() + elements.len();
    let mut dst = String::with_capacity(len);
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

    if path_needs_clean(dst.as_bytes()) {
        let mut clean_path = clean(&dst);
        if trailing_slash {
            clean_path.push_str(SLASH_SEPARATOR);
        }
        return clean_path;
    }

    if trailing_slash {
        dst.push_str(SLASH_SEPARATOR);
    }

    dst
}

/// path_needs_clean returns whether path cleaning may change the path.
/// Will detect all cases that will be cleaned,
/// but may produce false positives on non-trivial paths.
fn path_needs_clean(path: &[u8]) -> bool {
    if path.is_empty() {
        return true;
    }

    // Check for backslashes on Windows
    if cfg!(target_os = "windows") && path.contains(&b'\\') {
        return true;
    }

    let rooted = path[0] == b'/';
    let n = path.len();

    let (mut r, mut w) = if rooted { (1, 1) } else { (0, 0) };

    while r < n {
        match path[r] {
            b if b > 127 => {
                // Non ascii.
                return true;
            }
            b'/' => {
                // multiple / elements
                return true;
            }
            b'.' => {
                if r + 1 == n || path[r + 1] == b'/' {
                    // . element - assume it has to be cleaned.
                    return true;
                }
                if r + 1 < n && path[r + 1] == b'.' && (r + 2 == n || path[r + 2] == b'/') {
                    // .. element: remove to last / - assume it has to be cleaned.
                    return true;
                }
                // Handle single dot case
                if r + 1 == n {
                    // . element - assume it has to be cleaned.
                    return true;
                }
                // Copy the dot
                w += 1;
                r += 1;
            }
            _ => {
                // real path element.
                // add slash if needed
                if (rooted && w != 1) || (!rooted && w != 0) {
                    w += 1;
                }
                // copy element
                while r < n && path[r] != b'/' {
                    if cfg!(target_os = "windows") && path[r] == b'\\' {
                        return true;
                    }
                    w += 1;
                    r += 1;
                }
                // allow one slash, not at end
                if r < n - 1 && path[r] == b'/' {
                    r += 1;
                }
            }
        }
    }

    // Turn empty string into "."
    if w == 0 {
        return true;
    }

    false
}

/// Splits a path into bucket and object names, removing a specified base path.
///
/// The path is first trimmed of the `base_path` prefix and any leading slashes.
/// Then it is split at the first slash into bucket and object.
/// If no slash is found, the whole path is returned as the bucket name, and object name is empty.
///
/// On Windows, this function handles backslashes as separators and performs case-insensitive prefix matching for `base_path`.
/// The returned bucket and object names are normalized to use forward slashes.
pub fn path_to_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    let mut p = path;

    // 1. Trim base_path
    if cfg!(target_os = "windows") {
        if strings_has_prefix_fold(p, base_path) {
            p = &p[base_path.len()..];
        }
    } else if p.starts_with(base_path) {
        p = &p[base_path.len()..];
    }

    // 2. Trim leading separators
    while let Some(c) = p.chars().next() {
        if is_separator(c as u8) {
            p = &p[c.len_utf8()..];
        } else {
            break;
        }
    }

    // 3. Find split point
    let idx = if cfg!(target_os = "windows") {
        p.find(['/', '\\'])
    } else {
        p.find('/')
    };

    match idx {
        Some(i) => {
            let bucket = &p[..i];
            let object = &p[i + 1..]; // Skip the separator

            // Normalize separators on Windows
            let bucket = if cfg!(target_os = "windows") {
                bucket.replace('\\', "/")
            } else {
                bucket.to_string()
            };
            let object = if cfg!(target_os = "windows") {
                object.replace('\\', "/")
            } else {
                object.to_string()
            };

            (bucket, object)
        }
        None => {
            let bucket = if cfg!(target_os = "windows") {
                p.replace('\\', "/")
            } else {
                p.to_string()
            };
            (bucket, "".to_string())
        }
    }
}

/// Splits a path into bucket and object names.
///
/// The path is trimmed of any leading slashes.
/// Then it is split at the first slash into bucket and object.
/// If no slash is found, the whole path is returned as the bucket name, and object name is empty.
pub fn path_to_bucket_object(s: &str) -> (String, String) {
    path_to_bucket_object_with_base_path("", s)
}

/// Extracts the base directory from a prefix string.
///
/// It returns the directory part of the prefix.
/// If the prefix does not contain a separator, or resolves to root/current dir, an empty string is returned.
/// The result ensures a trailing slash if not empty.
pub fn base_dir_from_prefix(prefix: &str) -> String {
    let mut base_dir = dir(prefix).to_owned();
    if base_dir == "." || base_dir == "./" || base_dir == "/" {
        base_dir = "".to_owned();
    }

    let has_separator = if cfg!(target_os = "windows") {
        prefix.contains(['/', '\\'])
    } else {
        prefix.contains('/')
    };

    if !has_separator {
        base_dir = "".to_owned();
    }
    if !base_dir.is_empty() && !base_dir.ends_with(SLASH_SEPARATOR) {
        base_dir.push_str(SLASH_SEPARATOR);
    }
    base_dir
}

/// A helper struct for lazy buffer allocation during string processing.
pub struct LazyBuf {
    s: String,
    buf: Option<Vec<u8>>,
    w: usize,
}

impl LazyBuf {
    /// Creates a new `LazyBuf` with the given source string.
    pub fn new(s: String) -> Self {
        LazyBuf { s, buf: None, w: 0 }
    }

    /// Returns the byte at index `i`, either from the buffer or the source string.
    pub fn index(&self, i: usize) -> u8 {
        if let Some(ref buf) = self.buf {
            buf[i]
        } else {
            self.s.as_bytes()[i]
        }
    }

    /// Appends a byte to the buffer.
    ///
    /// If the buffer hasn't been allocated yet, it checks if the byte matches the source string
    /// at the current write position. If it matches, it just advances the write position.
    /// If it doesn't match, it allocates the buffer and copies the prefix.
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

    /// Returns the resulting string.
    pub fn string(&self) -> String {
        if let Some(ref buf) = self.buf {
            String::from_utf8(buf[..self.w].to_vec()).unwrap()
        } else {
            self.s[..self.w].to_string()
        }
    }
}

/// Returns the shortest path name equivalent to the given path.
///
/// It applies the following rules iteratively until no further processing can be done:
/// 1. Replace multiple separators with a single one.
/// 2. Eliminate each `.` path name element (the current directory).
/// 3. Eliminate each inner `..` path name element (the parent directory) along with the non-`..` element that precedes it.
/// 4. Eliminate `..` elements that begin a rooted path: that is, replace `/..` by `/` at the beginning of a path.
///
/// On Windows, backslashes are converted to forward slashes.
/// The returned path ends in a slash only if it represents a root directory, such as `/` on Unix or `C:/` on Windows.
///
/// If the result of this process is an empty string, `clean` returns the string `.`.
pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }

    let rooted = path.starts_with('/') || (cfg!(target_os = "windows") && path.starts_with('\\'));
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
        let c = path.as_bytes()[r];

        if is_separator(c) {
            // Empty path element
            r += 1;
        } else if c == b'.' && (r + 1 == n || is_separator(path.as_bytes()[r + 1])) {
            // . element
            r += 1;
        } else if c == b'.'
            && (r + 1 < n && path.as_bytes()[r + 1] == b'.')
            && (r + 2 == n || is_separator(path.as_bytes()[r + 2]))
        {
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
        } else {
            // Real path element.
            // Add slash if needed
            if (rooted && out.w != 1) || (!rooted && out.w != 0) {
                out.append(b'/');
            }

            // Copy element
            while r < n {
                let c = path.as_bytes()[r];
                if is_separator(c) {
                    break;
                }
                out.append(c);
                r += 1;
            }
        }
    }

    // Turn empty string into "."
    if out.w == 0 {
        return ".".to_string();
    }

    out.string()
}

/// Splits path immediately following the final separator, separating it into a directory and file name component.
///
/// If there is no separator in path, `split` returns an empty dir and file set to path.
/// The returned values have the property that path = dir + file.
///
/// On Windows, both `/` and `\` are treated as separators.
pub fn split(path: &str) -> (&str, &str) {
    // Find the last occurrence of the separator
    let idx = if cfg!(target_os = "windows") {
        path.rfind(['/', '\\'])
    } else {
        path.rfind('/')
    };

    if let Some(i) = idx {
        // Return the directory (up to and including the last separator) and the file name
        return (&path[..i + 1], &path[i + 1..]);
    }
    // If no separator is found, return an empty string for the directory and the whole path as the file name
    (path, "")
}

/// Returns all but the last element of path, typically the path's directory.
///
/// After dropping the final element, `dir` calls `clean` on the path and trails the result.
/// If the path is empty, `dir` returns ".".
/// If the path consists entirely of separators, `dir` returns a single separator.
/// The returned path does not end in a separator unless it is the root directory.
pub fn dir(path: &str) -> String {
    let (a, _) = split(path);
    clean(a)
}

/// Trims double quotes from an ETag string.
pub fn trim_etag(etag: &str) -> String {
    etag.trim_matches('"').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    #[cfg(target_os = "windows")]
    fn test_windows_paths() {
        // Test clean with backslashes
        assert_eq!(clean("a\\b\\c"), "a/b/c");
        assert_eq!(clean("a\\b/c"), "a/b/c");
        assert_eq!(clean("a\\.\\b"), "a/b");
        assert_eq!(clean("a\\b\\..\\c"), "a/c");
        assert_eq!(clean("\\a\\b"), "/a/b");
        assert_eq!(clean("a\\\\b"), "a/b");

        // Test path_join with backslashes
        let result = path_join(&[PathBuf::from("a"), PathBuf::from("b\\c")]);
        assert_eq!(result, PathBuf::from("a/b/c"));

        // Test trailing backslash
        let result = path_join(&[PathBuf::from("a"), PathBuf::from("b\\")]);
        assert_eq!(result, PathBuf::from("a/b/"));

        // Test path_needs_clean
        assert!(path_needs_clean(b"a\\b"));

        // Test path_to_bucket_object_with_base_path
        let (bucket, object) = path_to_bucket_object_with_base_path("C:\\tmp", "C:\\tmp\\bucket\\object");
        assert_eq!(bucket, "bucket");
        assert_eq!(object, "object");

        let (bucket, object) = path_to_bucket_object_with_base_path("c:\\tmp", "C:\\tmp\\bucket\\object");
        assert_eq!(bucket, "bucket");
        assert_eq!(object, "object");
    }
}
