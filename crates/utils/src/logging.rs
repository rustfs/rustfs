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

use std::fmt;

#[derive(Clone, Copy)]
pub struct MaskedAccessKey<'a>(pub &'a str);

impl fmt::Display for MaskedAccessKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self.0;
        if value.is_empty() {
            return Ok(());
        }

        let chars: Vec<char> = value.chars().collect();
        match chars.len() {
            0 => Ok(()),
            1..=4 => f.write_str("***"),
            5..=8 => write!(f, "{}***{}", chars[0], chars[chars.len() - 1]),
            len => {
                for ch in &chars[..4] {
                    write!(f, "{ch}")?;
                }
                f.write_str("***")?;
                for ch in &chars[len - 4..] {
                    write!(f, "{ch}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for MaskedAccessKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::MaskedAccessKey;

    #[test]
    fn masks_short_values() {
        assert_eq!(MaskedAccessKey("").to_string(), "");
        assert_eq!(MaskedAccessKey("a").to_string(), "***");
        assert_eq!(MaskedAccessKey("abcd").to_string(), "***");
        assert_eq!(MaskedAccessKey("abcde").to_string(), "a***e");
        assert_eq!(MaskedAccessKey("abcdefgh").to_string(), "a***h");
    }

    #[test]
    fn masks_long_values() {
        assert_eq!(MaskedAccessKey("AKIAIOSFODNN7EXAMPLE").to_string(), "AKIA***MPLE");
        assert_eq!(format!("{:?}", MaskedAccessKey("keystone:user-1234")), "keys***1234");
    }
}
