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

use rustfs_config::server_config::{KV, KVS};
use rustfs_config::{
    DEFAULT_SCANNER_BITROT_CYCLE_SECS, DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS, DEFAULT_SCANNER_CYCLE_MAX_DIRECTORIES,
    DEFAULT_SCANNER_CYCLE_MAX_DURATION_SECS, DEFAULT_SCANNER_CYCLE_MAX_OBJECTS, DEFAULT_SCANNER_IDLE_MODE,
    DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS, DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS, DEFAULT_SCANNER_SPEED,
    DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS, SCANNER_ALERT_EXCESS_FOLDERS, SCANNER_ALERT_EXCESS_VERSION_SIZE,
    SCANNER_ALERT_EXCESS_VERSIONS, SCANNER_BITROT_CYCLE, SCANNER_CACHE_SAVE_TIMEOUT, SCANNER_CYCLE,
    SCANNER_CYCLE_MAX_DIRECTORIES, SCANNER_CYCLE_MAX_DURATION, SCANNER_CYCLE_MAX_OBJECTS, SCANNER_DELAY, SCANNER_IDLE_MODE,
    SCANNER_MAX_CONCURRENT_DISK_SCANS, SCANNER_MAX_CONCURRENT_SET_SCANS, SCANNER_MAX_WAIT, SCANNER_SPEED, SCANNER_START_DELAY,
    SCANNER_YIELD_EVERY_N_OBJECTS,
};
use std::sync::LazyLock;

pub static DEFAULT_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: SCANNER_SPEED.to_owned(),
            value: DEFAULT_SCANNER_SPEED.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_DELAY.to_owned(),
            value: String::new(),
            hidden_if_empty: true,
        },
        KV {
            key: SCANNER_MAX_WAIT.to_owned(),
            value: String::new(),
            hidden_if_empty: true,
        },
        KV {
            key: SCANNER_CYCLE.to_owned(),
            value: String::new(),
            hidden_if_empty: true,
        },
        KV {
            key: SCANNER_START_DELAY.to_owned(),
            value: String::new(),
            hidden_if_empty: true,
        },
        KV {
            key: SCANNER_CYCLE_MAX_DURATION.to_owned(),
            value: DEFAULT_SCANNER_CYCLE_MAX_DURATION_SECS.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_CYCLE_MAX_OBJECTS.to_owned(),
            value: DEFAULT_SCANNER_CYCLE_MAX_OBJECTS.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_CYCLE_MAX_DIRECTORIES.to_owned(),
            value: DEFAULT_SCANNER_CYCLE_MAX_DIRECTORIES.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_BITROT_CYCLE.to_owned(),
            value: DEFAULT_SCANNER_BITROT_CYCLE_SECS.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_IDLE_MODE.to_owned(),
            value: DEFAULT_SCANNER_IDLE_MODE.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_CACHE_SAVE_TIMEOUT.to_owned(),
            value: DEFAULT_SCANNER_CACHE_SAVE_TIMEOUT_SECS.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_MAX_CONCURRENT_SET_SCANS.to_owned(),
            value: DEFAULT_SCANNER_MAX_CONCURRENT_SET_SCANS.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_MAX_CONCURRENT_DISK_SCANS.to_owned(),
            value: DEFAULT_SCANNER_MAX_CONCURRENT_DISK_SCANS.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_YIELD_EVERY_N_OBJECTS.to_owned(),
            value: DEFAULT_SCANNER_YIELD_EVERY_N_OBJECTS.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_ALERT_EXCESS_VERSIONS.to_owned(),
            value: rustfs_config::DEFAULT_SCANNER_ALERT_EXCESS_VERSIONS.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_ALERT_EXCESS_VERSION_SIZE.to_owned(),
            value: rustfs_config::DEFAULT_SCANNER_ALERT_EXCESS_VERSION_SIZE.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: SCANNER_ALERT_EXCESS_FOLDERS.to_owned(),
            value: rustfs_config::DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS.to_string(),
            hidden_if_empty: false,
        },
    ])
});
