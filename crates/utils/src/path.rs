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

/// encode_dir_object encodes a directory object by appending
/// a special suffix if it ends with a slash.
/// Normalizes mixed separators on Windows before encoding.
///
/// # Arguments
/// * `object` - A string slice that holds the object to be encoded.
///
/// # Returns
/// A `String` representing the encoded directory object.
///
pub fn encode_dir_object(object: &str) -> String {
    if is_ends_with_any_sep(object) {
        let trimmed = trim_trailing_seps(object);
        // Normalize mixed separators on Windows
        #[cfg(target_os = "windows")]
        let normalized = normalize_path_separators(trimmed);
        #[cfg(not(target_os = "windows"))]
        let normalized = trimmed;

        format!("{}{}", normalized, GLOBAL_DIR_SUFFIX)
    } else {
        // Also normalize single file paths
        #[cfg(target_os = "windows")]
        let normalized = normalize_path_separators(object);
        #[cfg(not(target_os = "windows"))]
        let normalized = object;

        normalized.to_string()
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
/// A `String` representing the decoded directory object.
///
#[allow(dead_code)]
pub fn decode_dir_object(object: &str) -> String {
    if has_suffix(object, GLOBAL_DIR_SUFFIX) {
        let trimmed = object.trim_end_matches(GLOBAL_DIR_SUFFIX);
        format!("{}{}", trimmed, SLASH_SEPARATOR_STR)
    } else {
        object.to_string()
    }
}

/// Normalize path separators to platform-specific style.
/// On Windows, converts forward slashes to backslashes.
/// On Unix-like systems, converts backslashes to forward slashes.
#[cfg(target_os = "windows")]
fn normalize_path_separators(s: &str) -> String {
    s.replace('/', "\\")
}

#[cfg(not(target_os = "windows"))]
fn normalize_path_separators(s: &str) -> String {
    s.replace('\\', "/")
}

/// Check if the string ends with any platform-specific separator
/// On Windows, checks both forward slash and backslash.
/// On Unix-like systems, checks only forward slash.
fn is_ends_with_any_sep(s: &str) -> bool {
    #[cfg(target_os = "windows")]
    {
        s.ends_with('/') || s.ends_with('\\')
    }
    #[cfg(not(target_os = "windows"))]
    {
        s.ends_with('/')
    }
}

/// Remove all platform-specific separators at the end of the string
/// On Windows, removes both forward slashes and backslashes.
/// On Unix-like systems, removes only forward slashes.
fn trim_trailing_seps(s: &str) -> &str {
    #[cfg(target_os = "windows")]
    {
        s.trim_end_matches(|c| c == '/' || c == '\\')
    }
    #[cfg(not(target_os = "windows"))]
    {
        s.trim_end_matches('/')
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
    if is_ends_with_any_sep(s) {
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
///
/// # Arguments
/// * `elements` - A slice of string path elements to be joined.
///
/// # Returns
/// A String representing the joined path.
///
pub fn path_join_buf(elements: &[&str]) -> String {
    let trailing_slash = !elements.is_empty() && elements.last().is_some_and(|last| is_ends_with_any_sep(last));

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

/// Platform-aware separator check
/// Returns true if the byte represents a path separator on the current platform.
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
/// Will detect all cases that will be cleaned,
/// but may produce false positives on non-trivial paths.
/// Enhanced to properly handle Windows-specific paths and mixed separators.
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

    // On Windows: any forward slash indicates normalization to backslash is required.
    #[cfg(target_os = "windows")]
    {
        if path.iter().any(|&b| b == b'/') {
            return true;
        }
    }

    // Initialize scan index and previous-separator flag.
    let mut i = 0usize;
    let mut prev_was_sep = false;

    // Platform-aware prefix handling to avoid flagging meaningful leading sequences:
    // - Windows: handle drive letter "C:" and UNC leading "\\"
    // - Non-Windows: detect and flag double leading '/' (e.g.  "//abc") as needing clean
    if n >= 1 && is_sep(path[0]) {
        #[cfg(target_os = "windows")]
        {
            // If starts with two separators -> UNC prefix:  allow exactly two without flag
            if n >= 2 && is_sep(path[1]) {
                // If a third leading separator exists, that's redundant (e.g. "///... ") -> needs clean
                if n >= 3 && is_sep(path[2]) {
                    return true;
                }
                // Skip the two UNC leading separators for scanning; do not mark prev_was_sep true
                i = 2;
                prev_was_sep = false;
            } else {
                // Single leading separator (rooted) -> mark as seen separator so immediate next sep is duplicate
                i = 1;
                prev_was_sep = true;
            }
        }

        #[cfg(not(target_os = "windows"))]
        {
            // POSIX: double leading '/' is redundant and should be cleaned (e.g. "//abc" -> "/abc")
            if n >= 2 && is_sep(path[1]) {
                return true;
            }
            i = 1;
            prev_was_sep = true;
        }
    } else {
        // If not starting with separator, check for Windows drive-letter prefix like "C:"
        #[cfg(target_os = "windows")]
        {
            if n >= 2 && path[1] == b':' && (path[0] as char).is_ascii_alphabetic() {
                // Position after "C:"
                i = 2;
                // If a separator immediately follows the drive (rooted like "C:\"),
                // treat that first separator as seen; if more separators follow, it's redundant.
                if i < n && is_sep(path[i]) {
                    i += 1; // consume the single allowed separator after drive
                    if i < n && is_sep(path[i]) {
                        // multiple separators after drive like "C:\\..." -> needs clean
                        return true;
                    }
                    prev_was_sep = true;
                } else {
                    prev_was_sep = false;
                }
            }
        }
    }

    // Generic scan for repeated separators and dot / dot-dot components.
    while i < n {
        let b = path[i];
        if is_sep(b) {
            if prev_was_sep {
                // Multiple separators (except allowed UNC prefix handled above)
                return true;
            }
            prev_was_sep = true;
            i += 1;
            continue;
        }

        // Not a separator:  parse current path element
        let start = i;
        while i < n && !is_sep(path[i]) {
            i += 1;
        }
        let len = i - start;
        if len == 1 && path[start] == b'.' {
            // single "." element -> needs cleaning
            return true;
        }
        if len == 2 && path[start] == b'.' && path[start + 1] == b'.' {
            // ". ." element -> needs cleaning
            return true;
        }
        prev_was_sep = false;
    }

    // Trailing separator:  if last byte is a separator and path length > 1, then usually needs cleaning,
    // except when the path is a platform-specific root form (e.g. "/" on POSIX, "\\" or "C: \" on Windows).
    if n > 1 && is_sep(path[n - 1]) {
        #[cfg(not(target_os = "windows"))]
        {
            // POSIX: any trailing separator except the single-root "/" needs cleaning.
            return true;
        }
        #[cfg(target_os = "windows")]
        {
            // Windows special root forms that are acceptable with trailing separator:
            // - UNC root: exactly two leading separators "\" "\" (i.e. "\\") -> n == 2
            if n == 2 && is_sep(path[0]) && is_sep(path[1]) {
                return false;
            }
            // - Drive root: pattern "C: \" or "C: /" (len == 3)
            if n == 3 && path[1] == b':' && (path[0] as char).is_ascii_alphabetic() && is_sep(path[2]) {
                return false;
            }
            // Otherwise, trailing separator should be cleaned.
            return true;
        }
    }

    // No conditions triggered: assume path is already clean.
    false
}

/// path_to_bucket_object_with_base_path splits a given path into bucket and object components,
/// considering a base path to trim from the start.
/// FIXED: Uses byte-position-based separator detection to handle both / and \ correctly.
///
/// # Arguments
/// * `base_path` - A string slice that holds the base path to be trimmed.
/// * `path` - A string slice that holds the path to be split.
///
/// # Returns
/// A tuple containing the bucket and object as `String`s.
///
pub fn path_to_bucket_object_with_base_path(base_path: &str, path: &str) -> (String, String) {
    let mut remaining_path = path.trim_start_matches(base_path);
    remaining_path = trim_leading_seps(remaining_path);

    if let Some(sep_byte_pos) = find_first_sep_byte_pos(remaining_path) {
        // FIXED: Byte position is used directly.  Separator is always 1 byte.
        let after_sep_pos = sep_byte_pos + 1;
        return (
            remaining_path[..sep_byte_pos].to_string(),
            remaining_path.get(after_sep_pos..).unwrap_or("").to_string(),
        );
    }

    (remaining_path.to_string(), "".to_string())
}

/// Remove all platform-specific separators at the beginning of the string
/// On Windows, removes both forward slashes and backslashes.
/// On Unix-like systems, removes only forward slashes.
fn trim_leading_seps(s: &str) -> &str {
    #[cfg(target_os = "windows")]
    {
        s.trim_start_matches(|c| c == '/' || c == '\\')
    }
    #[cfg(not(target_os = "windows"))]
    {
        s.trim_start_matches('/')
    }
}

/// Find the byte position of the first separator in the path
/// Returns the byte offset of the separator character itself.
/// On Windows, finds either forward slash or backslash.
/// On Unix-like systems, finds only forward slash.
/// FIXED: Uses byte iteration instead of char iteration for ASCII separators.
fn find_first_sep_byte_pos(path: &str) -> Option<usize> {
    #[cfg(target_os = "windows")]
    {
        // Both / and \ are single-byte ASCII, so we can use byte iteration
        path.as_bytes().iter().position(|&b| b == b'/' || b == b'\\')
    }
    #[cfg(not(target_os = "windows"))]
    {
        path.as_bytes().iter().position(|&b| b == b'/')
    }
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

/// contains_any_sep_str checks if the given string contains any path separators.
/// On Windows, checks for both forward slash and backslash.
/// On Unix-like systems, checks for only forward slash.
///
/// # Arguments
/// * `s` - A string slice that holds the string to be checked.
///
/// # Returns
/// A boolean indicating whether the string contains any path separators.
fn contains_any_sep_str(s: &str) -> bool {
    #[cfg(target_os = "windows")]
    {
        s.contains('/') || s.contains('\\')
    }
    #[cfg(not(target_os = "windows"))]
    {
        s.contains('/')
    }
}

/// base_dir_from_prefix extracts the base directory from a given prefix.
/// Handles mixed separators on Windows.
///
/// # Arguments
/// * `prefix` - A string slice that holds the prefix to be processed.
///
/// # Returns
/// A `String` representing the base directory extracted from the prefix.
///
pub fn base_dir_from_prefix(prefix: &str) -> String {
    if !contains_any_sep_str(prefix) {
        return String::new();
    }
    let mut base_dir = dir(prefix);
    if base_dir == "." || base_dir == SLASH_SEPARATOR_STR {
        base_dir.clear();
    }
    if !base_dir.is_empty() && !is_ends_with_any_sep(&base_dir) {
        base_dir.push_str(SLASH_SEPARATOR_STR);
    }
    base_dir
}

/// clean returns the shortest path name equivalent to path
/// by purely lexical processing.  It applies the following rules
/// iteratively until no further processing can be done:
///
/// 1. Replace multiple slashes with a single slash.
/// 2. Eliminate each .  path name element (the current directory).
/// 3. Eliminate each inner ..  path name element (the parent directory)
///    along with the non-..  element that precedes it.
/// 4. Eliminate ..  elements that begin a rooted path,
///    that is, replace "/. ." by "/" at the beginning of a path.
///
/// If the result of this process is an empty string, clean returns the string ". ".
///
/// This function is adapted to work cross-platform by using the appropriate path separator.
/// On Windows, this function is aware of drive letters (e.g., `C: `) and UNC paths
/// (e.g., `\\server\share`) and cleans them using the appropriate separator.
/// Mixed separators are normalized to the platform-specific separator.
///
/// # Arguments
/// * `path` - A string slice that holds the path to be cleaned.
///
/// # Returns
/// A `String` representing the cleaned path.
///
pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ". ".to_string();
    }

    #[cfg(target_os = "windows")]
    {
        use std::borrow::Cow;
        let bytes = path.as_bytes();
        let n = bytes.len();
        // Windows-aware handling
        let mut i = 0usize;
        let mut drive: Option<char> = None;
        let mut rooted = false;
        let mut preserve_leading_double_sep = false;

        // Drive letter detection
        if n >= 2 && bytes[1] == b':' && (bytes[0] as char).is_ascii_alphabetic() {
            drive = Some(bytes[0] as char);
            i = 2;
            // If next is separator, it's an absolute drive-root (e.g., "C:\")
            if i < n && is_sep(bytes[i]) {
                rooted = true;
                // consume all leading separators after drive
                while i < n && is_sep(bytes[i]) {
                    i += 1;
                }
            }
        } else {
            // UNC or absolute by separators
            if n >= 2 && is_sep(bytes[0]) && is_sep(bytes[1]) {
                rooted = true;
                preserve_leading_double_sep = true;
                i = 2;
                // consume extra leading separators
                while i < n && is_sep(bytes[i]) {
                    i += 1;
                }
            } else if is_sep(bytes[0]) {
                rooted = true;
                i = 1;
                while i < n && is_sep(bytes[i]) {
                    i += 1;
                }
            }
        }

        // Component stack
        let mut comps: Vec<Cow<'_, str>> = Vec::new();
        let mut r = i;
        while r < n {
            // find next sep or end
            let start = r;
            while r < n && !is_sep(bytes[r]) {
                r += 1;
            }
            // component bytes [start..r)
            let comp = String::from_utf8_lossy(&bytes[start..r]);
            if comp == "." {
                // skip
            } else if comp == ".." {
                if !comps.is_empty() {
                    // pop last component
                    comps.pop();
                } else if !rooted {
                    // relative path with ..  at front must be kept
                    comps.push(Cow::Owned("..".to_string()));
                } else {
                    // rooted and at root => ignore
                }
            } else {
                comps.push(comp);
            }
            // skip separators
            while r < n && is_sep(bytes[r]) {
                r += 1;
            }
        }

        // Build result
        let mut out = String::new();
        if let Some(d) = drive {
            out.push(d);
            out.push(':');
            if rooted {
                out.push(SLASH_SEPARATOR);
            }
        } else if preserve_leading_double_sep {
            out.push(SLASH_SEPARATOR);
            out.push(SLASH_SEPARATOR);
        } else if rooted {
            out.push(SLASH_SEPARATOR);
        }

        // Join components
        for _c in comps.iter() {
            if !out.is_empty() && !out.ends_with(SLASH_SEPARATOR_STR) {
                out.push(SLASH_SEPARATOR);
            }
            out.push_str(_c);
        }

        // Special cases:
        if out.is_empty() {
            // No drive, no components -> "."
            return ".".to_string();
        }

        // If output is just "C:" (drive without components and not rooted), keep as "C:"
        if drive.is_some() {
            if out.len() == 2 && out.as_bytes()[1] == b':' {
                return out;
            }
            // If drive+colon+sep and no components, return "C: \"
            if out.len() == 3 && out.as_bytes()[1] == b':' && is_sep(out.as_bytes()[2]) {
                return out;
            }
        }

        // Remove trailing separator unless it's a root form (single leading sep or drive root or UNC)
        if out.len() > 1 && out.ends_with(SLASH_SEPARATOR_STR) {
            // Determine if it's a root form:  "/" or "\\" or "C:\"
            let is_root = {
                // "/" (non-drive single sep)
                if out == SLASH_SEPARATOR_STR {
                    true
                } else if out.starts_with(SLASH_SEPARATOR_STR) && out == format!("{}{}", SLASH_SEPARATOR_STR, SLASH_SEPARATOR_STR)
                {
                    // only double separator
                    true
                } else {
                    // drive root "C:\" length >=3 with pattern X:\
                    if out.len() == 3 && out.as_bytes()[1] == b':' && is_sep(out.as_bytes()[2]) {
                        true
                    } else {
                        false
                    }
                }
            };
            if !is_root {
                out.pop();
            }
        }

        out
    }

    #[cfg(not(target_os = "windows"))]
    {
        // POSIX-like behavior (original implementation but simplified)
        let rooted = path.starts_with('/');
        let n = path.len();
        let mut out = LazyBuf::new(path.to_string());
        let mut r = 0usize;
        let mut dotdot = 0usize;

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
                    // .  element
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
                        // Cannot backtrack but not rooted, so append ..  element.
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
}

/// split splits path immediately after the final slash,
/// separating it into a directory and file name component.
/// If there is no slash in path, split returns
/// ("", path).
///
/// Note: This function supports cross-platform separator detection (Windows supports \ and /)
/// FIXED: Uses byte-position-based detection to correctly handle both / and \ separators.
///
/// # Arguments
/// * `path` - A string slice that holds the path to be split.
///
/// # Returns
/// A tuple containing the directory and file name as string slices.
///
pub fn split(path: &str) -> (&str, &str) {
    // Find the byte position of the last separator (cross-platform support)
    if let Some(sep_byte_pos) = find_last_sep_byte_pos(path) {
        // Returns the directory section (including the last separator) and the file name section
        return (&path[..sep_byte_pos + 1], &path[sep_byte_pos + 1..]);
    }
    // If no delimiter is found, return an empty string as the directory and the entire path as the file name
    ("", path)
}

/// Find the byte position of the last separator in the path
/// Returns the byte offset of the separator character itself.
/// On Windows, finds either forward slash or backslash.
/// On Unix-like systems, finds only forward slash.
/// FIXED: Uses byte iteration instead of char iteration for better performance with ASCII separators.
fn find_last_sep_byte_pos(path: &str) -> Option<usize> {
    #[cfg(target_os = "windows")]
    {
        // On Windows, search for both / and \
        // We need to find the rightmost occurrence
        let mut last_pos = None;
        for (i, b) in path.as_bytes().iter().enumerate() {
            if *b == b'/' || *b == b'\\' {
                last_pos = Some(i);
            }
        }
        last_pos
    }
    #[cfg(not(target_os = "windows"))]
    {
        path.as_bytes().iter().rposition(|&b| b == b'/')
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
        #[cfg(not(target_os = "windows"))]
        {
            // Basic joining
            assert_eq!(path_join_buf(&["a", "b"]), "a/b");
            assert_eq!(path_join_buf(&["a/", "b"]), "a/b");

            // Empty array input
            assert_eq!(path_join_buf(&[]), ".");

            // Single element
            assert_eq!(path_join_buf(&["a"]), "a");

            // Multiple elements
            assert_eq!(path_join_buf(&["a", "b", "c"]), "a/b/c");

            // Elements with trailing separators
            assert_eq!(path_join_buf(&["a/", "b/"]), "a/b/");

            // Elements requiring cleaning (with "." and "..")
            assert_eq!(path_join_buf(&["a", ".", "b"]), "a/b");
            assert_eq!(path_join_buf(&["a", "..", "b"]), "b");
            assert_eq!(path_join_buf(&["a", "b", ".."]), "a");

            // Preservation of trailing slashes
            assert_eq!(path_join_buf(&["a", "b/"]), "a/b/");
            assert_eq!(path_join_buf(&["a/", "b/"]), "a/b/");

            // Empty elements
            assert_eq!(path_join_buf(&["a", "", "b"]), "a/b");

            // Double slashes (cleaning)
            assert_eq!(path_join_buf(&["a//", "b"]), "a/b");
        }
        #[cfg(target_os = "windows")]
        {
            // Basic joining
            assert_eq!(path_join_buf(&["a", "b"]), "a\\b");
            assert_eq!(path_join_buf(&["a\\", "b"]), "a\\b");

            // Empty array input
            assert_eq!(path_join_buf(&[]), ".");

            // Single element
            assert_eq!(path_join_buf(&["a"]), "a");

            // Multiple elements
            assert_eq!(path_join_buf(&["a", "b", "c"]), "a\\b\\c");

            // Elements with trailing separators
            assert_eq!(path_join_buf(&["a\\", "b\\"]), "a\\b\\");

            // Elements requiring cleaning (with "." and "..")
            assert_eq!(path_join_buf(&["a", ".", "b"]), "a\\b");
            assert_eq!(path_join_buf(&["a", "..", "b"]), "b");
            assert_eq!(path_join_buf(&["a", "b", ".."]), "a");

            // Mixed separator handling - FIX FOR ISSUE 1424
            assert_eq!(path_join_buf(&["a/b", "c"]), "a\\b\\c");
            assert_eq!(path_join_buf(&["a\\", "b/c"]), "a\\b\\c");

            // Preservation of trailing slashes
            assert_eq!(path_join_buf(&["a", "b\\"]), "a\\b\\");
            assert_eq!(path_join_buf(&["a\\", "b\\"]), "a\\b\\");

            // Empty elements
            assert_eq!(path_join_buf(&["a", "", "b"]), "a\\b");

            // Double slashes (cleaning)
            assert_eq!(path_join_buf(&["a\\\\", "b"]), "a\\b");
        }
    }

    #[test]
    fn test_split() {
        #[cfg(not(target_os = "windows"))]
        {
            // Basic split
            assert_eq!(split("a/b"), ("a/", "b"));
            assert_eq!(split("a/b/c"), ("a/b/", "c"));

            // No separator
            assert_eq!(split("abc"), ("", "abc"));

            // Trailing slash
            assert_eq!(split("a/b/"), ("a/b/", ""));

            // Root
            assert_eq!(split("/abc"), ("/", "abc"));
        }
        #[cfg(target_os = "windows")]
        {
            // Windows backslash separator - FIX FOR ISSUE 1554
            assert_eq!(split("a\\b"), ("a\\", "b"));
            assert_eq!(split("a\\b\\c"), ("a\\b\\", "c"));

            // Windows forward slash separator - FIX FOR ISSUE 1554
            assert_eq!(split("a/b"), ("a/", "b"));
            assert_eq!(split("a/b/c"), ("a/b/", "c"));

            // Mixed separators - FIX FOR ISSUE 1554
            assert_eq!(split("a\\b/c"), ("a\\b/", "c"));
            assert_eq!(split("a/b\\c"), ("a/b\\", "c"));

            // No separator
            assert_eq!(split("abc"), ("", "abc"));

            // Drive letter
            assert_eq!(split("C:\\a\\b"), ("C:\\a\\", "b"));
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
            assert_eq!(clean("../../"), "../. .");
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
            assert_eq!(clean("abc/def/../../.. "), "..");
            assert_eq!(clean("/abc/def/../../.."), "/");
            assert_eq!(clean("abc/def/../../../ghi/jkl/../../../mno"), "../../mno");
        }

        #[cfg(target_os = "windows")]
        {
            assert_eq!(clean("a\\b\\.. \\c"), "a\\c");
            assert_eq!(clean("a\\\\b"), "a\\b");
            assert_eq!(clean("C:\\"), "C:\\");
            assert_eq!(clean("C:\\a\\.. \\b"), "C:\\b");
            assert_eq!(clean("C: a\\b\\..\\c"), "C:a\\c");
            assert_eq!(clean("\\\\server\\share\\a\\\\b"), "\\\\server\\share\\a\\b");
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
                path: ". .",
                result: ". .",
            },
            PathTest {
                path: "../..",
                result: "../. .",
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
                result: "../. .",
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
            // Remove .  elements
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
                path: "abc/def/. .",
                result: "abc",
            },
            PathTest {
                path: "abc/def/../. .",
                result: ".",
            },
            PathTest {
                path: "/abc/def/../. .",
                result: "/",
            },
            PathTest {
                path: "abc/def/../../. .",
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
                path: "abc/. /../def",
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
        assert_eq!(result, PathBuf::from(". "));

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
        let result = path_join(&[PathBuf::from("a"), PathBuf::from(". "), PathBuf::from("b")]);
        assert_eq!(result, PathBuf::from("a/b"));

        // Test paths with double dots
        let result = path_join(&[
            PathBuf::from("a"),
            PathBuf::from("b"),
            PathBuf::from(".. "),
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
    fn test_path_to_bucket_object() {
        // Test basic splitting
        let (bucket, object) = path_to_bucket_object("mybucket/myobject");
        assert_eq!(bucket, "mybucket");
        assert_eq!(object, "myobject");

        // Test with trailing slash
        let (bucket, object) = path_to_bucket_object("mybucket/");
        assert_eq!(bucket, "mybucket");
        assert_eq!(object, "");

        // Test nested paths
        let (bucket, object) = path_to_bucket_object("mybucket/dir/file.txt");
        assert_eq!(bucket, "mybucket");
        assert_eq!(object, "dir/file.txt");

        // Test with base path
        let (bucket, object) = path_to_bucket_object_with_base_path("/base", "/base/mybucket/obj");
        assert_eq!(bucket, "mybucket");
        assert_eq!(object, "obj");

        #[cfg(target_os = "windows")]
        {
            // Windows backslash separator - FIX FOR ISSUE 1554
            let (bucket, object) = path_to_bucket_object("mybucket\\myobject");
            assert_eq!(bucket, "mybucket");
            assert_eq!(object, "myobject");

            // Mixed separators
            let (bucket, object) = path_to_bucket_object("mybucket/subdir\\file");
            assert_eq!(bucket, "mybucket");
            assert_eq!(object, "subdir\\file");
        }
    }

    #[test]
    fn test_encode_decode_dir_object() {
        // Test encode
        assert_eq!(encode_dir_object("dir/"), "dir__XLDIR__");
        assert_eq!(is_dir_object("dir/"), true);
        assert_eq!(is_dir_object("file"), false);

        // Test decode
        assert_eq!(decode_dir_object("dir__XLDIR__"), "dir/");
        assert_eq!(decode_dir_object("file"), "file");

        #[cfg(target_os = "windows")]
        {
            // Windows backslash separator
            assert_eq!(encode_dir_object("dir\\"), "dir__XLDIR__");
            assert_eq!(is_dir_object("dir\\"), true);

            // Mixed separators should be normalized
            assert_eq!(encode_dir_object("dir/"), "dir__XLDIR__");
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
            // Case insensitive on Windows
            assert!(has_suffix("MYFILE.TXT", ".txt"));
            assert!(has_prefix("MYFILE", "my"));
        }
    }

    #[test]
    fn test_normalize_path_separators() {
        #[cfg(target_os = "windows")]
        {
            // Forward slashes should be converted to backslashes
            assert_eq!(normalize_path_separators("a/b/c"), "a\\b\\c");
            assert_eq!(normalize_path_separators("a/b\\c"), "a\\b\\c");
        }

        #[cfg(not(target_os = "windows"))]
        {
            // Backslashes should be converted to forward slashes
            assert_eq!(normalize_path_separators("a\\b\\c"), "a/b/c");
            assert_eq!(normalize_path_separators("a\\b/c"), "a/b/c");
        }
    }
}
