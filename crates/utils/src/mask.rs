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

/// Mask sensitive information in a string by showing only the first and last characters,
/// replacing the middle characters with "***", and appending the length of the string.
/// If the string is None, return an empty string.
/// If the string has only one character, show that character followed by "***|1".
/// If the string is empty, return an empty string.
///
/// # Examples
/// ```rust no_run
/// use rustfs_utils::mask_sensitive;
/// let masked = mask_sensitive(Some(&"secretpassword".to_string()));
/// assert_eq!(masked, "s***d|14");
/// let masked_empty = mask_sensitive(Some(&"".to_string()));
/// assert_eq!(masked_empty, "");
/// let masked_one_char = mask_sensitive(Some(&"a".to_string()));
/// assert_eq!(masked_one_char, "a***|1");
/// let masked_none = mask_sensitive(None);
/// assert_eq!(masked_none, "");
/// ```
pub fn mask_sensitive(s: Option<&String>) -> String {
    match s {
        None => String::new(),
        Some(s) => {
            let chars: Vec<char> = s.chars().collect();
            let n = chars.len();
            match n {
                0 => String::new(),
                1 => format!("{}***|{}", chars[0], n),
                _ => format!("{}***{}|{}", chars[0], chars[n - 1], n),
            }
        }
    }
}
