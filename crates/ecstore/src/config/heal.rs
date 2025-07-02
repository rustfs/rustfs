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

use crate::error::{Error, Result};
use rustfs_utils::string::parse_bool;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct Config {
    pub bitrot: String,
    pub sleep: Duration,
    pub io_count: usize,
    pub drive_workers: usize,
    pub cache: Duration,
}

impl Config {
    pub fn bitrot_scan_cycle(&self) -> Duration {
        self.cache
    }

    pub fn get_workers(&self) -> usize {
        self.drive_workers
    }

    pub fn update(&mut self, nopts: &Config) {
        self.bitrot = nopts.bitrot.clone();
        self.io_count = nopts.io_count;
        self.sleep = nopts.sleep;
        self.drive_workers = nopts.drive_workers;
    }
}

const RUSTFS_BITROT_CYCLE_IN_MONTHS: u64 = 1;

fn parse_bitrot_config(s: &str) -> Result<Duration> {
    match parse_bool(s) {
        Ok(enabled) => {
            if enabled {
                Ok(Duration::from_secs_f64(0.0))
            } else {
                Ok(Duration::from_secs_f64(-1.0))
            }
        }
        Err(_) => {
            if !s.ends_with("m") {
                return Err(Error::other("unknown format"));
            }

            match s.trim_end_matches('m').parse::<u64>() {
                Ok(months) => {
                    if months < RUSTFS_BITROT_CYCLE_IN_MONTHS {
                        return Err(Error::other(format!("minimum bitrot cycle is {RUSTFS_BITROT_CYCLE_IN_MONTHS} month(s)")));
                    }

                    Ok(Duration::from_secs(months * 30 * 24 * 60))
                }
                Err(err) => Err(Error::other(err)),
            }
        }
    }
}
