use std::time::Duration;

use crate::utils::bool_flag::parse_bool;
use common::error::{Error, Result};

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
                return Err(Error::from_string("unknown format"));
            }

            match s.trim_end_matches('m').parse::<u64>() {
                Ok(months) => {
                    if months < RUSTFS_BITROT_CYCLE_IN_MONTHS {
                        return Err(Error::from_string(format!(
                            "minimum bitrot cycle is {} month(s)",
                            RUSTFS_BITROT_CYCLE_IN_MONTHS
                        )));
                    }

                    Ok(Duration::from_secs(months * 30 * 24 * 60))
                }
                Err(err) => Err(err.into()),
            }
        }
    }
}
