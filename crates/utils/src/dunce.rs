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

//! Utility functions to handle Windows paths, specifically converting UNC paths to regular paths where possible.
//! This is useful for compatibility with tools that do not support UNC paths (starting with `\\?\`).

#[cfg(any(windows, test))]
use std::ffi::OsStr;

#[cfg(windows)]
use std::path::{Component, Prefix};
use std::{
    borrow::Cow,
    fs, io,
    path::{Path, PathBuf},
};

/// Takes any path, and when possible, converts Windows UNC paths to regular paths.
/// If the path can't be converted, it's returned unmodified.
///
/// On non-Windows this is a no-op.
///
/// `\\?\C:\Windows` will be converted to `C:\Windows`,
/// but `\\?\C:\COM` will be left as-is (due to a reserved filename).
///
/// Use this to pass arbitrary paths to programs that may not be UNC-aware.
///
/// It's generally safe to pass UNC paths to legacy programs, because
/// these paths contain a reserved prefix, so will gracefully fail
/// if used with legacy APIs that don't support UNC.
///
/// This function does not perform any I/O.
///
/// Currently paths with unpaired surrogates aren't converted even if they
/// could be, due to limitations of Rust's `OsStr` API.
///
/// To check if a path remained as UNC, use [`is_simplified()`] or `path.as_os_str().as_encoded_bytes().starts_with(b"\\\\")`.
///
/// # Arguments
///
/// * `path` - The path to simplify.
///
/// # Returns
///
/// A reference to the simplified path if it could be simplified, or the original path otherwise.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use rustfs_utils::simplified;
///
/// #[cfg(windows)]
/// {
///     let unc_path = Path::new(r"\\?\C:\Windows");
///     let simple_path = simplified(unc_path);
///     assert_eq!(simple_path, Path::new(r"C:\Windows"));
/// }
/// ```
#[inline]
#[must_use]
pub fn simplified(path: &Path) -> &Path {
    try_simplified(path).unwrap_or(path)
}

/// Like `std::fs::canonicalize()`, but on Windows it outputs the most
/// compatible form of a path instead of UNC.
///
/// This function first canonicalizes the path (resolving symlinks and absolute path),
/// and then attempts to simplify it using [`simplified()`] on Windows.
///
/// # Arguments
///
/// * `path` - The path to canonicalize.
///
/// # Returns
///
/// The canonicalized and simplified path, or an error if canonicalization fails.
#[inline(always)]
pub fn canonicalize<P: AsRef<Path>>(path: P) -> io::Result<PathBuf> {
    let path = path.as_ref();

    #[cfg(not(windows))]
    {
        fs::canonicalize(path)
    }
    #[cfg(windows)]
    {
        canonicalize_win(path)
    }
}

#[cfg(windows)]
fn canonicalize_win(path: &Path) -> io::Result<PathBuf> {
    let real_path = fs::canonicalize(path)?;
    Ok(try_simplified(&real_path).map(PathBuf::from).unwrap_or(real_path))
}

/// Returns a form of `path` that Win32 file APIs accept beyond `MAX_PATH`.
///
/// `std::fs` adds the `\\?\` prefix internally, but crates that call Win32
/// directly with the caller's path (for example `tempfile`'s `persist`, which
/// uses `MoveFileExW`) fail with `ERROR_PATH_NOT_FOUND` once the path reaches
/// 260 UTF-16 units, unless the process opted into long paths through both the
/// `LongPathsEnabled` policy and an application manifest.
///
/// The path is first made absolute with `GetFullPathNameW` (via
/// [`std::path::absolute`]), so separators, `.`/`..` and trailing dots or spaces
/// are normalized exactly as a non-verbatim Win32 call would see them, and only
/// then prefixed with `\\?\` (or `\\?\UNC\`). Verbatim and device paths are
/// returned unchanged.
///
/// On non-Windows this is a no-op.
pub fn extended_length_path(path: &Path) -> io::Result<Cow<'_, Path>> {
    #[cfg(not(windows))]
    {
        Ok(Cow::Borrowed(path))
    }
    #[cfg(windows)]
    {
        extended_length_path_win(path).map(Cow::Owned)
    }
}

#[cfg(windows)]
fn extended_length_path_win(path: &Path) -> io::Result<PathBuf> {
    use std::{
        ffi::OsString,
        os::windows::ffi::{OsStrExt, OsStringExt},
    };

    let absolute = std::path::absolute(path)?;
    let unc = match absolute.components().next() {
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::Disk(_) => false,
            Prefix::UNC(..) => true,
            // Verbatim and `\\.\` device paths already bypass Win32 normalization.
            _ => return Ok(absolute),
        },
        _ => return Ok(absolute),
    };
    let wide = absolute.as_os_str().encode_wide().collect::<Vec<_>>();
    let (prefix, rest) = if unc {
        // `\\server\share\x` becomes `\\?\UNC\server\share\x`.
        (r"\\?\UNC", &wide[1..])
    } else {
        (r"\\?\", &wide[..])
    };
    let mut extended = prefix.encode_utf16().collect::<Vec<_>>();
    extended.extend_from_slice(rest);
    Ok(PathBuf::from(OsString::from_wide(&extended)))
}

/// Returns `true` if the path is relative or starts with a disk prefix (like "C:").
///
/// This checks if the path is in a "simplified" format. On Windows, this means it
/// is either a relative path, a rooted path without a prefix, or starts with a drive letter (e.g., `C:\`).
/// UNC paths (starting with `\\`) and verbatim paths (starting with `\\?\`) will return `false`.
///
/// On non-Windows platforms, this always returns `true`.
///
/// # Arguments
///
/// * `path` - The path to check.
///
/// # Returns
///
/// `true` if the path is simplified, `false` otherwise.
#[cfg_attr(not(windows), allow(unused))]
#[must_use]
pub fn is_simplified(path: &Path) -> bool {
    #[cfg(windows)]
    if let Some(Component::Prefix(prefix)) = path.components().next() {
        return matches!(prefix.kind(), Prefix::Disk(..));
    }
    true
}

#[doc(hidden)]
pub use self::canonicalize as realpath;

/// Why a single path segment cannot be used as a regular Win32 file name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowsFileNameError {
    /// The segment is empty.
    Empty,
    /// The segment exceeds 255 UTF-16 code units.
    TooLong,
    /// The segment contains one of `< > : " / \ | ? *`.
    ReservedCharacter,
    /// The segment contains a control character (0x00-0x1F).
    ControlCharacter,
    /// The segment ends with `.` or a space, which Win32 path parsing strips.
    TrailingDotOrSpace,
    /// The segment names a DOS device (e.g. `NUL`, `COM1`, `CONIN$`), bare or
    /// with an extension.
    ReservedDeviceName,
}

/// Checks whether `name` can be created *and* addressed again as a single
/// path segment through the Win32 API.
///
/// This is the one rule set for Windows file-name validity: it backs
/// [`simplified()`] and the object-key admission checks in ecstore. The rules
/// apply on every platform so callers can evaluate them without a Windows
/// host; whether to enforce them is the caller's decision.
pub fn check_windows_file_name(name: &str) -> Result<(), WindowsFileNameError> {
    // Windows filename limit is 255 characters (UTF-16 code units)
    // We check byte length first as a fast path optimization.
    if name.len() > 255 && name.encode_utf16().count() > 255 {
        return Err(WindowsFileNameError::TooLong);
    }

    if name.is_empty() {
        return Err(WindowsFileNameError::Empty);
    }

    // Only ASCII subset is checked, and WTF-8/UTF-8 is safe for that
    // Invalid chars: < > : " / \ | ? * and control chars (0-31)
    let byte_str = name.as_bytes();
    if byte_str
        .iter()
        .any(|&c| matches!(c, b'<' | b'>' | b':' | b'"' | b'/' | b'\\' | b'|' | b'?' | b'*'))
    {
        return Err(WindowsFileNameError::ReservedCharacter);
    }
    if byte_str.iter().any(|&c| c <= 31) {
        return Err(WindowsFileNameError::ControlCharacter);
    }
    // Filename can't end with . or space
    if matches!(byte_str.last(), Some(b' ' | b'.')) {
        return Err(WindowsFileNameError::TrailingDotOrSpace);
    }
    if is_reserved_name(name) {
        return Err(WindowsFileNameError::ReservedDeviceName);
    }
    Ok(())
}

/// Checks if the filename is valid for a regular Windows path.
///
/// This checks for:
/// - Length limit (255 chars)
/// - Invalid characters (< > : " / \ | ? * and control chars)
/// - Trailing spaces or dots
#[cfg(any(windows, test))]
fn is_valid_filename(file_name: &OsStr) -> bool {
    // Convert to str first. If not valid UTF-8, it's not a valid "simplified" filename candidate
    // (since we require the whole path to be UTF-8 at the end).
    let s = match file_name.to_str() {
        Some(s) => s,
        None => return false,
    };

    matches!(check_windows_file_name(s), Ok(()) | Err(WindowsFileNameError::ReservedDeviceName))
}

/// Checks if the filename is a reserved DOS device name (e.g., CON, PRN, COM1).
///
/// Reserved names are case-insensitive and apply even if an extension is present (e.g., "CON.txt" is reserved).
#[cfg(any(windows, test))]
fn is_reserved<P: AsRef<OsStr>>(file_name: P) -> bool {
    match file_name.as_ref().to_str() {
        Some(s) => is_reserved_name(s),
        None => false, // Reserved names are all ASCII
    }
}

fn is_reserved_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    // Get the stem (part before the first dot)
    // "CON.txt" -> "CON"
    // "CON" -> "CON"
    let stem = match s.find('.') {
        Some(idx) => &s[..idx],
        None => s,
    };

    // Trim trailing spaces (Windows ignores them, so "CON " is "CON")
    let stem = stem.trim_end();

    if ["CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"]
        .iter()
        .any(|name| stem.eq_ignore_ascii_case(name))
    {
        return true;
    }

    // Check COM1-9, LPT1-9, and the ISO 8859-1 superscript digits Windows
    // also accepts as port numbers (COM¹, LPT³, ...).
    let (Some(base), Some(suffix)) = (stem.get(..3), stem.get(3..)) else {
        return false;
    };
    if !base.eq_ignore_ascii_case("COM") && !base.eq_ignore_ascii_case("LPT") {
        return false;
    }
    let mut digits = suffix.chars();
    match (digits.next(), digits.next()) {
        (Some(digit), None) => matches!(digit, '1'..='9' | '\u{b9}' | '\u{b2}' | '\u{b3}'),
        _ => false,
    }
}

#[cfg(not(windows))]
#[inline]
const fn try_simplified(_path: &Path) -> Option<&Path> {
    None
}

#[cfg(windows)]
fn try_simplified(path: &Path) -> Option<&Path> {
    let mut components = path.components();
    // Check if it starts with \\?\
    match components.next() {
        Some(Component::Prefix(p)) => match p.kind() {
            Prefix::VerbatimDisk(..) => {}
            _ => return None, // Other kinds of UNC paths or regular paths
        },
        _ => return None, // relative or empty
    }

    // Validate all subsequent components
    for component in components {
        match component {
            Component::RootDir => {}
            Component::Normal(file_name) => {
                if !is_valid_filename(file_name) || is_reserved(file_name) {
                    return None;
                }
            }
            _ => return None, // UNC paths take things like ".." literally, which we can't simplify to
        };
    }

    // We use to_str() here because we need to slice the string to remove the prefix.
    // This implicitly requires the path to be valid UTF-8.
    // If it's not, we return None, which is safe (we just don't simplify).
    let s = path.to_str()?;
    // Remove "\\?\" (4 chars)
    let simplified = &s[4..];

    // Check length limit (MAX_PATH is 260)
    if simplified.len() > 260 && simplified.encode_utf16().count() > 260 {
        return None;
    }
    Some(Path::new(simplified))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::path::Path;

    #[test]
    fn test_reserved_names() {
        let reserved = [
            "CON",
            "PRN",
            "AUX",
            "NUL",
            "COM1",
            "COM2",
            "COM3",
            "COM4",
            "COM5",
            "COM6",
            "COM7",
            "COM8",
            "COM9",
            "LPT1",
            "LPT2",
            "LPT3",
            "LPT4",
            "LPT5",
            "LPT6",
            "LPT7",
            "LPT8",
            "LPT9",
            "con",
            "Con",
            "CON.txt",
            "con.txt",
            "CON .txt",
            "con  .txt",
            "CON.tar.gz",
            "con.",
            "con .",
            "COM\u{b9}",
            "com\u{b2}.txt",
            "LPT\u{b3}",
            "CONIN$",
            "conout$",
            "CONIN$.txt",
        ];
        for name in reserved {
            assert!(is_reserved(name), "Should be reserved: {}", name);
        }

        let not_reserved = [
            "CON0",
            "COM0",
            "LPT0",
            "COM10",
            "LPT10",
            "CONsole",
            "PrNter",
            "NuLly",
            "not_con",
            "con_not",
            ".CON",
            "@CON",
            "CON。",
            "foo.con",
            "COM\u{b9}0",
            "COM\u{bd}",
            "CONIN",
            "CONIN$$",
            "中文名",
            "é12",
            "COMé",
        ];
        for name in not_reserved {
            assert!(!is_reserved(name), "Should NOT be reserved: {}", name);
        }
    }

    #[test]
    fn test_valid_filename() {
        let invalid = [
            "",
            ".",
            "..",
            " ",
            " .",
            "foo ",
            "foo.",
            "foo/bar",
            "foo\\bar",
            "foo:bar",
            "foo*bar",
            "foo?bar",
            "foo\"bar",
            "foo<bar",
            "foo>bar",
            "foo|bar",
            "foo\0bar",
            "foo\x1fbar",
        ];
        for name in invalid {
            assert!(!is_valid_filename(OsStr::new(name)), "Should be invalid: {:?}", name);
        }

        let valid = [
            "foo",
            "foo.bar",
            ".foo",
            "foo.bar.baz",
            "foo bar",
            "foo. bar", // space inside is ok
            "中文",
            "😀",
        ];
        for name in valid {
            assert!(is_valid_filename(OsStr::new(name)), "Should be valid: {:?}", name);
        }

        // Test length limit
        let long_name = "a".repeat(256);
        assert!(!is_valid_filename(OsStr::new(&long_name)));
        let ok_name = "a".repeat(255);
        assert!(is_valid_filename(OsStr::new(&ok_name)));
    }

    #[test]
    fn test_check_windows_file_name_reports_the_violated_rule() {
        assert_eq!(check_windows_file_name("file.txt"), Ok(()));
        assert_eq!(check_windows_file_name(""), Err(WindowsFileNameError::Empty));
        assert_eq!(check_windows_file_name(&"a".repeat(256)), Err(WindowsFileNameError::TooLong));
        for name in ["a:b", "a\\b", "a/b", "a*b", "a?b", "a\"b", "a<b", "a>b", "a|b"] {
            assert_eq!(check_windows_file_name(name), Err(WindowsFileNameError::ReservedCharacter), "{name:?}");
        }
        for name in ["a\0b", "a\x01b", "tab\there", "a\x1fb"] {
            assert_eq!(check_windows_file_name(name), Err(WindowsFileNameError::ControlCharacter), "{name:?}");
        }
        for name in ["dir.", "file ", "..", "CON."] {
            assert_eq!(check_windows_file_name(name), Err(WindowsFileNameError::TrailingDotOrSpace), "{name:?}");
        }
        for name in ["NUL", "nul.txt", "COM\u{b9}", "CONOUT$"] {
            assert_eq!(check_windows_file_name(name), Err(WindowsFileNameError::ReservedDeviceName), "{name:?}");
        }
    }

    #[test]
    #[cfg(windows)]
    fn test_simplified_windows() {
        // Basic
        assert_eq!(simplified(Path::new(r"\\?\C:\Windows")), Path::new(r"C:\Windows"));

        // Reserved
        assert_eq!(simplified(Path::new(r"\\?\C:\CON")), Path::new(r"\\?\C:\CON"));
        assert_eq!(simplified(Path::new(r"\\?\C:\aux.txt")), Path::new(r"\\?\C:\aux.txt"));

        // Long paths
        let long_path = format!(r"\\?\C:\{}", "a".repeat(300));
        assert_eq!(simplified(Path::new(&long_path)), Path::new(&long_path));

        let ok_path = format!(r"\\?\C:\{}", "a".repeat(200));
        assert_eq!(simplified(Path::new(&ok_path)), Path::new(&ok_path[4..]));

        // Relative/other prefixes
        assert_eq!(simplified(Path::new(r"C:\Windows")), Path::new(r"C:\Windows"));
        assert_eq!(simplified(Path::new(r"\\server\share")), Path::new(r"\\server\share"));
        assert_eq!(simplified(Path::new(r"\\.\C:\Windows")), Path::new(r"\\.\C:\Windows"));
    }

    #[test]
    #[cfg(windows)]
    fn extended_length_path_prefixes_normalized_windows_paths() {
        assert_eq!(
            &*extended_length_path(Path::new(r"C:/data/./bucket/../bucket/object")).unwrap(),
            Path::new(r"\\?\C:\data\bucket\object")
        );
        assert_eq!(
            &*extended_length_path(Path::new(r"\\server\share\data\object")).unwrap(),
            Path::new(r"\\?\UNC\server\share\data\object")
        );
        for unchanged in [r"\\?\C:\data\object", r"\\?\UNC\server\share\object", r"\\.\C:\data"] {
            assert_eq!(&*extended_length_path(Path::new(unchanged)).unwrap(), Path::new(unchanged));
        }

        let long = format!(r"C:\data\{}\xl.meta.bkp", "a".repeat(300));
        let extended = extended_length_path(Path::new(&long)).unwrap();
        assert_eq!(&*extended, Path::new(&format!(r"\\?\{long}")));
        assert_eq!(simplified(&extended), &*extended, "a long path must stay verbatim");
    }

    #[test]
    #[cfg(not(windows))]
    fn extended_length_path_is_a_noop_off_windows() {
        let path = Path::new("/data/./bucket/object");
        assert!(matches!(extended_length_path(path).unwrap(), Cow::Borrowed(p) if p == path));
    }

    #[test]
    #[cfg(not(windows))]
    fn test_simplified_unix() {
        let p = Path::new("/tmp/foo");
        assert_eq!(simplified(p), p);
    }

    #[test]
    fn test_is_simplified() {
        #[cfg(windows)]
        {
            assert!(is_simplified(Path::new(r"C:\Windows")));
            assert!(is_simplified(Path::new(r"relative\path")));
            assert!(!is_simplified(Path::new(r"\\?\C:\Windows")));
            assert!(!is_simplified(Path::new(r"\\server\share")));
        }
        #[cfg(not(windows))]
        {
            assert!(is_simplified(Path::new("/tmp/foo")));
            assert!(is_simplified(Path::new("relative/path")));
        }
    }
}
