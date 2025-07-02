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

use std::env;
use std::path::{Path, PathBuf};

/// Get the absolute path to the current project
///
/// This function will try the following method to get the project path:
/// 1. Use the `CARGO_MANIFEST_DIR` environment variable to get the project root directory.
/// 2. Use `std::env::current_exe()` to get the executable file path and deduce the project root directory.
/// 3. Use `std::env::current_dir()` to get the current working directory and try to deduce the project root directory.
///
/// If all methods fail, an error is returned.
///
/// # Returns
/// - `Ok(PathBuf)`: The absolute path of the project that was successfully obtained.
/// - `Err(String)`: Error message for the failed path.
pub fn get_project_root() -> Result<PathBuf, String> {
    // Try to get the project root directory through the CARGO_MANIFEST_DIR environment variable
    if let Ok(manifest_dir) = env::var("CARGO_MANIFEST_DIR") {
        let project_root = Path::new(&manifest_dir).to_path_buf();
        println!("Get the project root directory with CARGO_MANIFEST_DIR:{}", project_root.display());
        return Ok(project_root);
    }

    // Try to deduce the project root directory through the current executable file path
    if let Ok(current_exe) = env::current_exe() {
        let mut project_root = current_exe;
        // Assume that the project root directory is in the parent directory of the parent directory of the executable path (usually target/debug or target/release)
        project_root.pop(); // Remove the executable file name
        project_root.pop(); // Remove target/debug or target/release
        println!("Deduce the project root directory through current_exe:{}", project_root.display());
        return Ok(project_root);
    }

    // Try to deduce the project root directory from the current working directory
    if let Ok(mut current_dir) = env::current_dir() {
        // Assume that the project root directory is in the parent directory of the current working directory
        current_dir.pop();
        println!("Deduce the project root directory through current_dir:{}", current_dir.display());
        return Ok(current_dir);
    }

    // If all methods fail, return an error
    Err("The project root directory cannot be obtained. Please check the running environment and project structure.".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_project_root() {
        match get_project_root() {
            Ok(path) => {
                assert!(path.exists(), "The project root directory does not exist:{}", path.display());
                println!("The test is passed, the project root directory:{}", path.display());
            }
            Err(e) => panic!("Failed to get the project root directory:{e}"),
        }
    }
}
