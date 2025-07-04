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

use std::path::{Path, PathBuf};

/// Path processing tool
pub struct PathUtils;

impl PathUtils {
    /// Normalize path
    pub fn normalize(path: &str) -> String {
        let path_buf = PathBuf::from(path);
        path_buf.to_string_lossy().to_string()
    }

    /// Join paths
    pub fn join(base: &str, path: &str) -> String {
        let base_path = PathBuf::from(base);
        let joined = base_path.join(path);
        joined.to_string_lossy().to_string()
    }

    /// Get parent directory
    pub fn parent(path: &str) -> Option<String> {
        let path_buf = PathBuf::from(path);
        path_buf.parent().map(|p| p.to_string_lossy().to_string())
    }

    /// Get filename
    pub fn filename(path: &str) -> Option<String> {
        let path_buf = PathBuf::from(path);
        path_buf.file_name().map(|name| name.to_string_lossy().to_string())
    }

    /// Check if path is absolute
    pub fn is_absolute(path: &str) -> bool {
        Path::new(path).is_absolute()
    }

    /// Check if path exists
    pub fn exists(path: &str) -> bool {
        Path::new(path).exists()
    }

    /// Create directory (if not exists)
    pub fn create_dir_all(path: &str) -> std::io::Result<()> {
        std::fs::create_dir_all(path)
    }

    /// Remove file or directory
    pub fn remove(path: &str) -> std::io::Result<()> {
        if Path::new(path).is_file() {
            std::fs::remove_file(path)
        } else {
            std::fs::remove_dir_all(path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_normalization() {
        assert_eq!(PathUtils::normalize("/path/to/resource"), "/path/to/resource");
        assert_eq!(PathUtils::normalize("path/to/resource"), "path/to/resource");
    }

    #[test]
    fn test_path_joining() {
        assert_eq!(PathUtils::join("/base", "path"), "/base/path");
        assert_eq!(PathUtils::join("base", "path"), "base/path");
    }

    #[test]
    fn test_path_parent() {
        assert_eq!(PathUtils::parent("/path/to/resource"), Some("/path/to".to_string()));
        assert_eq!(PathUtils::parent("/"), None);
    }

    #[test]
    fn test_path_filename() {
        assert_eq!(PathUtils::filename("/path/to/resource"), Some("resource".to_string()));
        assert_eq!(PathUtils::filename("/"), None);
    }

    #[test]
    fn test_path_absolute() {
        assert!(PathUtils::is_absolute("/path/to/resource"));
        assert!(!PathUtils::is_absolute("path/to/resource"));
    }
}
