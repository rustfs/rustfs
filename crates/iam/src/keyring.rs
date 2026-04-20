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

use rustfs_utils::get_env_opt_str;

pub const ENV_IAM_MASTER_KEY: &str = "RUSTFS_IAM_MASTER_KEY";
pub const ENV_IAM_MASTER_KEY_OLD_KEYS: &str = "RUSTFS_IAM_MASTER_KEY_OLD_KEYS";

#[derive(Default, Debug, Clone, PartialEq, Eq)]
struct Keyring {
    current_key: Option<Vec<u8>>,
    decrypt_keys: Vec<Vec<u8>>,
}

fn normalize_key(value: Option<String>) -> Option<String> {
    value.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())
}

fn parse_old_keys(value: Option<String>) -> Vec<String> {
    let Some(value) = normalize_key(value) else {
        return Vec::new();
    };

    value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .collect()
}

fn push_unique_key(keys: &mut Vec<Vec<u8>>, key: Vec<u8>) {
    if !keys.iter().any(|k| k == &key) {
        keys.push(key);
    }
}

fn build_keyring(current_key: Option<String>, old_keys: Option<String>) -> Keyring {
    let mut decrypt_keys = Vec::new();
    let mut current = None;

    if let Some(key) = normalize_key(current_key) {
        let bytes = key.into_bytes();
        current = Some(bytes.clone());
        push_unique_key(&mut decrypt_keys, bytes);
    }

    for key in parse_old_keys(old_keys) {
        push_unique_key(&mut decrypt_keys, key.into_bytes());
    }

    Keyring {
        current_key: current,
        decrypt_keys,
    }
}

fn load_keyring() -> Keyring {
    build_keyring(get_env_opt_str(ENV_IAM_MASTER_KEY), get_env_opt_str(ENV_IAM_MASTER_KEY_OLD_KEYS))
}

pub fn encrypt_key() -> Option<Vec<u8>> {
    load_keyring().current_key
}

pub fn decrypt_keys() -> Vec<Vec<u8>> {
    load_keyring().decrypt_keys
}

pub fn current_key_and_old_keys() -> (Option<Vec<u8>>, Vec<Vec<u8>>) {
    let keyring = load_keyring();
    let current = keyring.current_key.clone();
    let old_keys = if current.is_some() {
        keyring.decrypt_keys.into_iter().skip(1).collect()
    } else {
        keyring.decrypt_keys
    };
    (current, old_keys)
}

#[cfg(test)]
mod tests {
    use super::{build_keyring, parse_old_keys};

    #[test]
    fn test_parse_old_keys_ignores_empty_items() {
        let keys = parse_old_keys(Some(" old-a, ,old-b,, old-c ".to_string()));
        assert_eq!(keys, vec!["old-a".to_string(), "old-b".to_string(), "old-c".to_string()]);
    }

    #[test]
    fn test_build_keyring_includes_current_then_old() {
        let keyring = build_keyring(Some("current-key".to_string()), Some("old-key-1,old-key-2".to_string()));

        assert_eq!(keyring.current_key, Some("current-key".as_bytes().to_vec()));
        assert_eq!(
            keyring.decrypt_keys,
            vec![
                "current-key".as_bytes().to_vec(),
                "old-key-1".as_bytes().to_vec(),
                "old-key-2".as_bytes().to_vec(),
            ]
        );
    }

    #[test]
    fn test_build_keyring_deduplicates_keys() {
        let keyring = build_keyring(Some("k1".to_string()), Some("k1, k2, k2, k3".to_string()));

        assert_eq!(
            keyring.decrypt_keys,
            vec!["k1".as_bytes().to_vec(), "k2".as_bytes().to_vec(), "k3".as_bytes().to_vec(),]
        );
    }

    #[test]
    fn test_build_keyring_uses_old_keys_without_current() {
        let keyring = build_keyring(None, Some("legacy-a,legacy-b".to_string()));

        assert_eq!(keyring.current_key, None);
        assert_eq!(keyring.decrypt_keys, vec!["legacy-a".as_bytes().to_vec(), "legacy-b".as_bytes().to_vec()]);
    }
}
