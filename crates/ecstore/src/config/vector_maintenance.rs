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

//! Strict, restart-required Vector maintenance configuration.

use crate::error::{Error, Result};
use rustfs_config::server_config::{Config, KV, KVS};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY, S3VECTORS_MAINTENANCE_EXECUTION_ACCOUNT, S3VECTORS_MAINTENANCE_SUB_SYS};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, value::RawValue};
use std::{collections::HashSet, sync::LazyLock};

pub(super) static DEFAULT_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.into(),
            value: "off".into(),
            hidden_if_empty: false,
        },
        KV {
            key: S3VECTORS_MAINTENANCE_EXECUTION_ACCOUNT.into(),
            value: String::new(),
            hidden_if_empty: false,
        },
    ])
});

#[derive(Deserialize)]
struct Envelope {
    #[serde(default, deserialize_with = "present_section")]
    s3vectors_maintenance: Option<Box<RawValue>>,
}

// Preserve an explicit null so it cannot acquire the meaning of an absent section.
fn present_section<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Option<Box<RawValue>>, D::Error> {
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Section {
    Wrapped(Wrapper),
    Direct(Target),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Wrapper {
    #[serde(rename = "_")]
    target: Target,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Target {
    Scalars(Scalars),
    Entries(Vec<Entry>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scalars {
    #[serde(default = "disabled")]
    enable: String,
    #[serde(default)]
    execution_account: String,
}

fn disabled() -> String {
    "off".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    key: String,
    value: String,
    #[serde(default, alias = "hiddenIfEmpty")]
    hidden_if_empty: bool,
}

pub(super) fn decode(data: &[u8]) -> Result<Option<KVS>> {
    let envelope: Envelope = serde_json::from_slice(data)?;
    let Some(raw) = envelope.s3vectors_maintenance else {
        return Ok(None);
    };
    let target = match serde_json::from_str::<Section>(raw.get())? {
        Section::Wrapped(wrapper) => wrapper.target,
        Section::Direct(target) => target,
    };
    let values = match target {
        Target::Scalars(values) => KVS(vec![
            KV {
                key: ENABLE_KEY.into(),
                value: values.enable,
                hidden_if_empty: false,
            },
            KV {
                key: S3VECTORS_MAINTENANCE_EXECUTION_ACCOUNT.into(),
                value: values.execution_account,
                hidden_if_empty: false,
            },
        ]),
        Target::Entries(entries) => KVS(entries
            .into_iter()
            .map(|entry| KV {
                key: entry.key,
                value: entry.value,
                hidden_if_empty: entry.hidden_if_empty,
            })
            .collect()),
    };
    normalize(&values).map(Some)
}

fn normalize(values: &KVS) -> Result<KVS> {
    let mut seen = HashSet::new();
    let mut result = DEFAULT_KVS.clone();
    for entry in &values.0 {
        if !matches!(entry.key.as_str(), ENABLE_KEY | S3VECTORS_MAINTENANCE_EXECUTION_ACCOUNT) || !seen.insert(entry.key.as_str())
        {
            return Err(Error::other("invalid or duplicate Vector maintenance configuration key"));
        }
        result.insert(entry.key.clone(), entry.value.clone());
    }
    let enable = result.get(ENABLE_KEY);
    let account = result.get(S3VECTORS_MAINTENANCE_EXECUTION_ACCOUNT);
    if !matches!(enable.as_str(), "on" | "off") {
        return Err(Error::other("Vector maintenance enable must be on or off"));
    }
    if (enable == "on" || !account.is_empty())
        && (!rustfs_policy::auth::is_access_key_valid(&account)
            || account.len() > 65_536
            || account.chars().any(char::is_control)
            || rustfs_policy::auth::contains_reserved_chars(&account)
            || account.ends_with(rustfs_utils::path::GLOBAL_DIR_SUFFIX))
    {
        return Err(Error::other("invalid Vector maintenance execution account"));
    }
    Ok(result)
}

pub(super) fn install(config: &mut Config, values: KVS) {
    config
        .0
        .entry(S3VECTORS_MAINTENANCE_SUB_SYS.into())
        .or_default()
        .insert(DEFAULT_DELIMITER.into(), values);
}

pub(super) fn encode(config: &Config) -> Result<Value> {
    let values = if let Some(targets) = config.0.get(S3VECTORS_MAINTENANCE_SUB_SYS) {
        if targets.len() != 1 || !targets.contains_key(DEFAULT_DELIMITER) {
            return Err(Error::other("Vector maintenance requires exactly the default target"));
        }
        normalize(&targets[DEFAULT_DELIMITER])?
    } else {
        DEFAULT_KVS.clone()
    };
    Ok(serde_json::json!({
        "enable": values.get(ENABLE_KEY),
        "execution_account": values.get(S3VECTORS_MAINTENANCE_EXECUTION_ACCOUNT),
    }))
}

pub(super) fn equivalent(lhs: &Config, rhs: &Config) -> bool {
    matches!((encode(lhs), encode(rhs)), (Ok(lhs), Ok(rhs)) if lhs == rhs)
}
