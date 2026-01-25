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

// #[cfg(target_os = "windows")]
// const SLASH_SEPARATOR: char = '\\';
// #[cfg(not(target_os = "windows"))]
// const SLASH_SEPARATOR: char = '/';

/// GLOBAL_DIR_SUFFIX is a special suffix used to denote directory objects
/// in object storage systems that do not have a native directory concept.
pub const GLOBAL_DIR_SUFFIX: &str = "__XLDIR__";

/// SLASH_SEPARATOR_STR is the string representation of the path separator
/// used in the current operating system.
pub const SLASH_SEPARATOR_STR: &str = if cfg!(target_os = "windows") { "\\" } else { "/" };

/// GLOBAL_DIR_SUFFIX_WITH_SLASH is the directory suffix followed by the
/// platform-specific path separator, used to denote directory objects.
#[cfg(target_os = "windows")]
pub const GLOBAL_DIR_SUFFIX_WITH_SLASH: &str = "__XLDIR__\\";
#[cfg(not(target_os = "windows"))]
pub const GLOBAL_DIR_SUFFIX_WITH_SLASH: &str = "__XLDIR__/";

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

/// Normalize path separators to forward slash (canonical form for object storage).
/// This converts Windows backslashes to forward slashes for storage operations.
/// Internal representation uses forward slash to match S3 and HTTP standards.
#[inline]
fn normalize_to_forward_slash(s: &str) -> String {
    #[cfg(target_os = "windows")]
    {
        s.replace('\\', "/")
    }
    #[cfg(not(target_os = "windows"))]
    {
        s.to_string()
    }
}

/// Normalize path separators to platform-specific style.
/// This is only used when interfacing with OS-level operations.
/// For storage operations, always use forward slash.
#[cfg(target_os = "windows")]
fn normalize_to_platform_separator(s: &str) -> String {
    s.replace('/', "\\")
}

#[cfg(not(target_os = "windows"))]
#[allow(dead_code)]
fn normalize_to_platform_separator(s: &str) -> String {
    s.to_string()
}

/// encode_dir_object encodes a directory object by appending
/// a special suffix if it ends with a slash.
/// CRITICAL FIX: Always use forward slash internally for storage consistency.
///
/// # Arguments
/// * `object` - A string slice that holds the object to be encoded.
///
/// # Returns
/// A `String` representing the encoded directory object (with forward slashes).
///
pub fn encode_dir_object(object: &str) -> String {
    // FIX: First normalize to forward slash (canonical form)
    let canonical = normalize_to_forward_slash(object);

    if canonical.ends_with('/') {
        let trimmed = canonical.trim_end_matches('/');
        format!("{}{}", trimmed, GLOBAL_DIR_SUFFIX)
    } else {
        canonical
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
/// # Returns
/// A `String` representing the decoded directory object (with forward slashes).
///
#[allow(dead_code)]
pub fn decode_dir_object(object: &str) -> String {
    if has_suffix(object, GLOBAL_DIR_SUFFIX) {
        let trimmed = object.trim_end_matches(GLOBAL_DIR_SUFFIX);
        format!("{}/", trimmed) // Always use forward slash
    } else {
        object.to_string()
    }
}

/// Check if the string ends with any path separator (forward slash or backslash).
/// Accepts both separators but prefers canonical forward slash.
#[allow(dead_code)]
fn is_ends_with_any_sep(s: &str) -> bool {
    s.ends_with('/') || s.ends_with('\\')
}

/// Remove all path separators at the end of the string.
/// Handles both forward slash and backslash.
#[allow(dead_code)]
fn trim_trailing_seps(s: &str) -> &str {
    s.trim_end_matches(['/', '\\'])
}

/// retain_slash ensures that the given string `s` ends with a forward slash.
/// Always uses forward slash for consistency with storage systems.
///
/// # Arguments
/// * `s` - A string slice that holds the string to be processed.
///
/// # Returns
/// A `String` that ends with a forward slash.
///
pub fn retain_slash(s: &str) -> String {
    if s.is_empty() {
        return s.to_string();
    }
    // Normalize to forward slash first
    let canonical = normalize_to_forward_slash(s);
    if canonical.ends_with('/') {
        canonical
    } else {
        format!("{}/", canonical)
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
/// Returns paths with forward slashes for consistency.
///
/// # Arguments
/// * `elem` - A slice of path elements to be joined.
///
/// # Returns
/// A PathBuf representing the joined path.
///
pub fn path_join<P: AsRef<Path>>(elem: &[P]) -> PathBuf {
    if elem.is_empty() {
        return PathBuf::from(".");
    }
    // Collect components as owned Strings (lossy for non-UTF8)
    let strs: Vec<String> = elem.iter().map(|p| p.as_ref().to_string_lossy().into_owned()).collect();
    // Convert to slice of &str for path_join_buf
    let refs: Vec<&str> = strs.iter().map(|s| s.as_str()).collect();
    PathBuf::from(path_join_buf(&refs))
}

/// path_join_buf joins multiple string path elements into a single String,
/// ensuring that the resulting path is clean and properly formatted.
/// Returns paths with forward slashes for consistency with storage systems.
///
/// # Arguments
/// * `elements` - A slice of string path elements to be joined.
///
/// # Returns
/// A String representing the joined path (with forward slashes).
///
pub fn path_join_buf(elements: &[&str]) -> String {
    // FIX: Check for trailing slash before normalization
    let trailing_slash = !elements.is_empty()
        && elements
            .last()
            .is_some_and(|last| last.ends_with('/') || last.ends_with('\\'));

    let mut dst = String::new();
    let mut added = 0;

    for e in elements {
        if added > 0 || !e.is_empty() {
            if added > 0 {
                dst.push('/'); // Always use forward slash internally
            }
            // Normalize incoming element to forward slash
            let normalized = normalize_to_forward_slash(e);
            dst.push_str(&normalized);
            added += normalized.len();
        }
    }

    if path_needs_clean(dst.as_bytes()) {
        let mut clean_path = clean(&dst);
        if trailing_slash && !clean_path.ends_with('/') {
            clean_path.push('/');
        }
        return clean_path;
    }

    if trailing_slash && !dst.ends_with('/') {
        dst.push('/');
    }

    dst
}

/// Platform-aware separator check for byte analysis.
/// On Windows, both / and \ are valid separators in paths.
/// On Unix, only / is valid.
fn is_sep(b: u8) -> bool {
    #[cfg(target_os = "windows")]
    {
        b == b'/' || b == b'\\'
    }
    #[cfg(not(target_os = "windows"))]
    {
        b == b'/'
    }
}

/// path_needs_clean returns whether path cleaning may change the path.
/// Will detect all cases that will be cleaned.
/// Note: Input should be normalized to forward slash before calling.
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

    let n = path.len();

    // On Windows: we may still see backslashes in analysis, so check for that
    #[cfg(target_os = "windows")]
    {
        if path.iter().any(|&b| b == b'\\') {
            return true;
        }
    }

    let mut i = 0usize;
    let mut prev_was_sep = false;

    // Handle leading separators
    if n >= 1 && is_sep(path[0]) {
        #[cfg(target_os = "windows")]
        {
            if n >= 2 && is_sep(path[1]) {
                if n >= 3 && is_sep(path[2]) {
                    return true;
                }
                i = 2;
                prev_was_sep = false;
            } else {
                i = 1;
                prev_was_sep = true;
            }
        }

        #[cfg(not(target_os = "windows"))]
        {
            if n >= 2 && is_sep(path[1]) {
                return true;
            }
            i = 1;
            prev_was_sep = true;
        }
    } else {
        #[cfg(target_os = "windows")]
        {
            if n >= 2 && path[1] == b':' && (path[0] as char).is_ascii_alphabetic() {
                i = 2;
                if i < n && is_sep(path[i]) {
                    i += 1;
                    if i < n && is_sep(path[i]) {
                        return true;
                    }
                    prev_was_sep = true;
                } else {
                    prev_was_sep = false;
                }
            }
        }
    }

    // Generic scan for repeated separators and dot / dot-dot components
    while i < n {
        let b = path[i];
        if is_sep(b) {
            if prev_was_sep {
                return true;
            }
            prev_was_sep = true;
            i += 1;
            continue;
        }

        let start = i;
        while i < n && !is_sep(path[i]) {
            i += 1;
        }
        let len = i - start;
        if len == 1 && path[start] == b'.' {
            return true;
        }
        if len == 2 && path[start] == b'.' && path[start + 1] == b'.' {
            return true;
        }
        prev_was_sep = false;
    }

    // Trailing separator handling
    if n > 1 && is_sep(path[n - 1]) {
        #[cfg(not(target_os = "windows"))]
        {
            return true;
        }
        #[cfg(target_os = "windows")]
        {
            if n == 2 && is_sep(path[0]) && is_sep(path[1]) {
                return false;
            }
            if n == 3 && path[1] == b':' && (path[0] as char).is_ascii_alphabetic() && is_sep(path[2]) {
                return false;
            }
            return true;
        }
    }

    false
}

/// path_to_bucket_object_with_base_path splits a given path into bucket and object components,
/// considering a base path to trim from the start.
/// FIX: Works with normalized forward slash paths only.
///
/// # Arguments
/// * `base_path` - A string slice that holds the base path to be trimmed.
/// * `path` - A string slice that holds the path to be split (should use forward slashes).
///
/// # Returns
/// A tuple containing the bucket and object as `String`s (both with forward slashes).
///
pub fn path_to_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    // FIX: Normalize input to forward slash
    let normalized_path = normalize_to_forward_slash(path);
    let normalized_base = normalize_to_forward_slash(base_path);

    let mut remaining_path = normalized_path.trim_start_matches(&normalized_base);
    remaining_path = remaining_path.trim_start_matches('/');

    if let Some(sep_pos) = remaining_path.find('/') {
        return (remaining_path[..sep_pos].to_string(), remaining_path[sep_pos + 1..].to_string());
    }

    (remaining_path.to_string(), "".to_string())
}

/// path_to_bucket_object splits a given path into bucket and object components.
///
/// # Arguments
/// * `s` - A string slice that holds the path to be split.
///
/// # Returns
/// A tuple containing the bucket and object as `String`s (both with forward slashes).
///
pub fn path_to_bucket_object(s: &str) -> (String, String) {
    path_to_bucket_object_with_base_path("", s)
}

/// contains_any_sep_str checks if the given string contains any path separators.
/// Returns true if either forward slash or backslash is found.
///
/// # Arguments
/// * `s` - A string slice that holds the string to be checked.
///
/// # Returns
/// A boolean indicating whether the string contains any path separators.
fn contains_any_sep_str(s: &str) -> bool {
    s.contains('/') || s.contains('\\')
}

/// base_dir_from_prefix extracts the base directory from a given prefix.
/// Handles both forward and backward slashes in input.
/// Always returns path with forward slash.
///
/// # Arguments
/// * `prefix` - A string slice that holds the prefix to be processed.
///
/// # Returns
/// A `String` representing the base directory (with forward slash).
///
pub fn base_dir_from_prefix(prefix: &str) -> String {
    if !contains_any_sep_str(prefix) {
        return String::new();
    }
    let mut base_dir = dir(prefix);
    if base_dir == "." || base_dir == "/" {
        base_dir.clear();
    }
    if !base_dir.is_empty() && !base_dir.ends_with('/') {
        base_dir.push('/');
    }
    base_dir
}

/// clean returns the shortest path name equivalent to path
/// by purely lexical processing.  It applies the following rules:
///
/// 1. Replace multiple slashes with a single slash.
/// 2. Eliminate each . path name element (the current directory).
/// 3. Eliminate each inner .. path name element (the parent directory)
///    along with the non-.. element that precedes it.
/// 4.  Eliminate ..  elements that begin a rooted path.
///
/// If the result is an empty string, returns ".".
/// Always uses forward slash in output for consistency.
///
/// # Arguments
/// * `path` - A string slice that holds the path to be cleaned.
///
/// # Returns
/// A `String` representing the cleaned path (with forward slashes).
///
pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }

    // FIX: Always work with forward-slash normalized paths
    let normalized_path = normalize_to_forward_slash(path);
    let binding = normalized_path.clone();
    let bytes = binding.as_bytes();
    let n = bytes.len();

    let rooted = bytes[0] == b'/';
    let mut out = LazyBuf::new(normalized_path);
    let mut r = 0;
    let mut dotdot = 0;

    if rooted {
        out.append(b'/');
        r = 1;
        dotdot = 1;
    }

    while r < n {
        match bytes[r] {
            b'/' => {
                r += 1;
            }
            b'.' if r + 1 == n || bytes[r + 1] == b'/' => {
                r += 1;
            }
            b'.' if bytes[r + 1] == b'.' && (r + 2 == n || bytes[r + 2] == b'/') => {
                r += 2;

                if out.w > dotdot {
                    out.w -= 1;
                    while out.w > dotdot && out.index(out.w) != b'/' {
                        out.w -= 1;
                    }
                } else if !rooted {
                    if out.w > 0 {
                        out.append(b'/');
                    }
                    out.append(b'.');
                    out.append(b'.');
                    dotdot = out.w;
                }
            }
            _ => {
                if (rooted && out.w != 1) || (!rooted && out.w != 0) {
                    out.append(b'/');
                }

                while r < n && bytes[r] != b'/' {
                    out.append(bytes[r]);
                    r += 1;
                }
            }
        }
    }

    if out.w == 0 {
        return ".".to_string();
    }

    let result = out.string();

    // Ensure trailing slash is removed (except for root)
    if result.len() > 1 && result.ends_with('/') {
        result[..result.len() - 1].to_string()
    } else {
        result
    }
}

/// split splits path immediately after the final slash,
/// separating it into a directory and file name component.
/// If there is no slash in path, returns ("", path).
///
/// FIX: Now correctly handles both / and \ in input,
/// always returns paths with forward slashes.
///
/// # Arguments
/// * `path` - A string slice that holds the path to be split.
///
/// # Returns
/// A tuple containing the directory and file name as `String`s (with forward slashes).
///
pub fn split(path: &str) -> (String, String) {
    // FIX:  Normalize to forward slash first
    let normalized = normalize_to_forward_slash(path);

    if let Some(pos) = normalized.rfind('/') {
        (normalized[..pos + 1].to_string(), normalized[pos + 1..].to_string())
    } else {
        ("".to_string(), normalized)
    }
}

/// dir returns all but the last element of path,
/// typically the path's directory.  After dropping the final
/// element, the path is cleaned.  If the path is empty,
/// dir returns ". ".
///
/// # Arguments
/// * `path` - A string slice that holds the path to be processed.
///
/// # Returns
/// A `String` representing the directory part of the path (with forward slash).
///
pub fn dir(path: &str) -> String {
    let (dir_part, _) = split(path);
    clean(&dir_part)
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

/// LazyBuf is a structure that efficiently builds a byte buffer
/// from a string by delaying the allocation of the buffer until
/// a modification is necessary.  It allows appending bytes and
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_join_buf() {
        // All tests should produce forward slash paths
        assert_eq!(path_join_buf(&["a", "b"]), "a/b");
        assert_eq!(path_join_buf(&["a/", "b"]), "a/b");
        assert_eq!(path_join_buf(&[]), ".");
        assert_eq!(path_join_buf(&["a"]), "a");
        assert_eq!(path_join_buf(&["a", "b", "c"]), "a/b/c");
        assert_eq!(path_join_buf(&["a/", "b/"]), "a/b/");
        assert_eq!(path_join_buf(&["a", ".", "b"]), "a/b");
        assert_eq!(path_join_buf(&["a", "..", "b"]), "b");
        assert_eq!(path_join_buf(&["a", "b", ".."]), "a");
        assert_eq!(path_join_buf(&["a", "b/"]), "a/b/");
        assert_eq!(path_join_buf(&["a/", "b/"]), "a/b/");
        assert_eq!(path_join_buf(&["a", "", "b"]), "a/b");
        assert_eq!(path_join_buf(&["a//", "b"]), "a/b");

        #[cfg(target_os = "windows")]
        {
            // Windows: input with backslashes should be normalized to forward slashes
            assert_eq!(path_join_buf(&["a\\b", "c"]), "a/b/c");
            assert_eq!(path_join_buf(&["a\\", "b/c"]), "a/b/c");
            assert_eq!(path_join_buf(&["a\\\\", "b"]), "a/b");
        }
    }

    #[test]
    fn test_split() {
        // Test forward slash paths
        assert_eq!(split("a/b"), ("a/".to_string(), "b".to_string()));
        assert_eq!(split("a/b/c"), ("a/b/".to_string(), "c".to_string()));
        assert_eq!(split("abc"), ("".to_string(), "abc".to_string()));
        assert_eq!(split("a/b/"), ("a/b/".to_string(), "".to_string()));
        assert_eq!(split("/abc"), ("/".to_string(), "abc".to_string()));

        #[cfg(target_os = "windows")]
        {
            // Windows: backslash input should be normalized to forward slash output
            assert_eq!(split("a\\b"), ("a/".to_string(), "b".to_string()));
            assert_eq!(split("a\\b\\c"), ("a/b/".to_string(), "c".to_string()));
            // Mixed separators should be normalized
            assert_eq!(split("a\\b/c"), ("a/b/".to_string(), "c".to_string()));
            assert_eq!(split("a/b\\c"), ("a/b/".to_string(), "c".to_string()));
        }
    }

    #[test]
    fn test_trim_etag() {
        assert_eq!(trim_etag("\"abc123\""), "abc123");
        assert_eq!(trim_etag("abc123"), "abc123");
        assert_eq!(trim_etag(""), "");
        assert_eq!(trim_etag("\"\""), "");
        assert_eq!(trim_etag("\"2c7ab85a893283e98c931e9511add182\""), "2c7ab85a893283e98c931e9511add182");
        assert_eq!(trim_etag("\"098f6bcd4621d373cade4e832627b4f6-2\""), "098f6bcd4621d373cade4e832627b4f6-2");
    }

    #[test]
    fn test_base_dir_from_prefix() {
        let result = base_dir_from_prefix("da/");
        assert!(!result.is_empty());
        assert_eq!(result, "da/");

        #[cfg(target_os = "windows")]
        {
            let result = base_dir_from_prefix("da\\");
            assert!(!result.is_empty());
            assert_eq!(result, "da/"); // Normalized to forward slash
        }
    }

    #[test]
    fn test_clean() {
        assert_eq!(clean(""), ".");
        assert_eq!(clean("abc"), "abc");
        assert_eq!(clean("abc/def"), "abc/def");
        assert_eq!(clean("a/b/c"), "a/b/c");
        assert_eq!(clean(". "), ".");
        assert_eq!(clean(".."), "..");
        assert_eq!(clean("../.. "), "../..");
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
        assert_eq!(clean("abc/. "), "abc");
        assert_eq!(clean("abc/. /../def"), "def");
        assert_eq!(clean("abc//. /../def"), "def");
        assert_eq!(clean("abc/../../././../def"), "../../def");
        assert_eq!(clean("abc/def/ghi/../jkl"), "abc/def/jkl");
        assert_eq!(clean("abc/def/../ghi/../jkl"), "abc/jkl");
        assert_eq!(clean("abc/def/.. "), "abc");
        assert_eq!(clean("abc/def/../.. "), ".");
        assert_eq!(clean("/abc/def/../.."), "/");
        assert_eq!(clean("abc/def/../../.."), "..");
        assert_eq!(clean("/abc/def/../../.."), "/");
        assert_eq!(clean("abc/def/../../../ghi/jkl/../../../mno"), "../../mno");
    }

    #[test]
    fn test_path_to_bucket_object() {
        let (bucket, object) = path_to_bucket_object("mybucket/myobject");
        assert_eq!(bucket, "mybucket");
        assert_eq!(object, "myobject");

        let (bucket, object) = path_to_bucket_object("mybucket/");
        assert_eq!(bucket, "mybucket");
        assert_eq!(object, "");

        let (bucket, object) = path_to_bucket_object("mybucket/dir/file.txt");
        assert_eq!(bucket, "mybucket");
        assert_eq!(object, "dir/file.txt");

        #[cfg(target_os = "windows")]
        {
            // Windows backslash should be normalized
            let (bucket, object) = path_to_bucket_object("mybucket\\myobject");
            assert_eq!(bucket, "mybucket");
            assert_eq!(object, "myobject");

            // Mixed separators should be normalized
            let (bucket, object) = path_to_bucket_object("mybucket/dir\\file");
            assert_eq!(bucket, "mybucket");
            assert_eq!(object, "dir/file");
        }
    }

    #[test]
    fn test_encode_decode_dir_object() {
        assert_eq!(encode_dir_object("dir/"), "dir__XLDIR__");
        assert!(is_dir_object("dir/"));
        assert!(!is_dir_object("file"));

        assert_eq!(decode_dir_object("dir__XLDIR__"), "dir/");
        assert_eq!(decode_dir_object("file"), "file");

        #[cfg(target_os = "windows")]
        {
            // Windows backslash should be normalized to forward slash
            assert_eq!(encode_dir_object("dir\\"), "dir__XLDIR__");
            assert_eq!(is_dir_object("dir\\"), true);

            // Mixed separators
            let encoded = encode_dir_object("dir/subdir\\");
            assert_eq!(encoded, "dir/subdir__XLDIR__");
        }
    }

    #[test]
    fn test_normalize_to_forward_slash() {
        #[cfg(target_os = "windows")]
        {
            assert_eq!(normalize_to_forward_slash("a\\b\\c"), "a/b/c");
            assert_eq!(normalize_to_forward_slash("a\\b/c"), "a/b/c");
            assert_eq!(normalize_to_forward_slash("a/b/c"), "a/b/c");
        }

        #[cfg(not(target_os = "windows"))]
        {
            assert_eq!(normalize_to_forward_slash("a/b/c"), "a/b/c");
            // On Unix, backslashes are valid filename characters, so they're preserved
            assert_eq!(normalize_to_forward_slash("a\\b/c"), "a\\b/c");
        }
    }

    #[test]
    fn test_has_suffix_prefix() {
        assert!(has_suffix("myfile. txt", ". txt"));
        assert!(!has_suffix("myfile. txt", ". doc"));

        assert!(has_prefix("myfile.txt", "my"));
        assert!(!has_prefix("myfile.txt", "other"));

        #[cfg(target_os = "windows")]
        {
            assert!(has_suffix("MYFILE.TXT", ".txt"));
            assert!(has_prefix("MYFILE", "my"));
        }
    }

    #[test]
    fn test_retain_slash() {
        assert_eq!(retain_slash("abc"), "abc/");
        assert_eq!(retain_slash("abc/"), "abc/");
        assert_eq!(retain_slash(""), "");

        #[cfg(target_os = "windows")]
        {
            // Should normalize to forward slash
            assert_eq!(retain_slash("abc\\"), "abc/");
            assert_eq!(retain_slash("abc"), "abc/");
        }
    }

    #[test]
    fn test_dir() {
        assert_eq!(dir("a/b/c"), "a/b");
        assert_eq!(dir("a/b"), "a");
        assert_eq!(dir("a"), "");
        assert_eq!(dir(""), ".");

        #[cfg(target_os = "windows")]
        {
            // Windows paths should be normalized
            assert_eq!(dir("a\\b\\c"), "a/b");
            assert_eq!(dir("a\\b"), "a");
        }
    }
}
