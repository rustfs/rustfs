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

use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::heal::background_heal_ops::{heal_bucket, heal_object};
use crate::heal::heal_commands::{HEAL_DEEP_SCAN, HEAL_NORMAL_SCAN};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use rustfs_utils::path::SLASH_SEPARATOR;
use std::ops::Sub;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

pub const MRF_OPS_QUEUE_SIZE: u64 = 100000;
pub const HEAL_DIR: &str = ".heal";
pub const HEAL_MRFMETA_FORMAT: u64 = 1;
pub const HEAL_MRFMETA_VERSION_V1: u64 = 1;

lazy_static! {
    pub static ref HEAL_MRF_DIR: String =
        format!("{}{}{}{}{}", BUCKET_META_PREFIX, SLASH_SEPARATOR, HEAL_DIR, SLASH_SEPARATOR, "mrf");
    static ref PATTERNS: Vec<Regex> = vec![
        Regex::new(r"^buckets/.*/.metacache/.*").unwrap(),
        Regex::new(r"^tmp/.*").unwrap(),
        Regex::new(r"^multipart/.*").unwrap(),
        Regex::new(r"^tmp-old/.*").unwrap(),
    ];
}

#[derive(Default)]
pub struct PartialOperation {
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub versions: Vec<u8>,
    pub set_index: usize,
    pub pool_index: usize,
    pub queued: DateTime<Utc>,
    pub bitrot_scan: bool,
}

pub struct MRFState {
    tx: Sender<PartialOperation>,
    rx: RwLock<Receiver<PartialOperation>>,
    closed: AtomicBool,
    closing: AtomicBool,
}

impl Default for MRFState {
    fn default() -> Self {
        Self::new()
    }
}

impl MRFState {
    pub fn new() -> MRFState {
        let (tx, rx) = tokio::sync::mpsc::channel(MRF_OPS_QUEUE_SIZE as usize);
        MRFState {
            tx,
            rx: RwLock::new(rx),
            closed: Default::default(),
            closing: Default::default(),
        }
    }

    pub async fn add_partial(&self, op: PartialOperation) {
        if self.closed.load(Ordering::SeqCst) || self.closing.load(Ordering::SeqCst) {
            return;
        }
        let _ = self.tx.send(op).await;
    }

    /// Enhanced heal routine with cancellation support
    ///
    /// This method implements the same healing logic as the original heal_routine,
    /// but adds proper cancellation support via CancellationToken.
    /// The core logic remains identical to maintain compatibility.
    pub async fn heal_routine_with_cancel(&self, cancel_token: CancellationToken) {
        info!("MRF heal routine started with cancellation support");

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!("MRF heal routine received shutdown signal, exiting gracefully");
                    break;
                }
                op_result = async {
                    let mut rx_guard = self.rx.write().await;
                    rx_guard.recv().await
                } => {
                    if let Some(op) = op_result {
                        // Special path filtering (original logic)
                        if op.bucket == RUSTFS_META_BUCKET {
                            for pattern in &*PATTERNS {
                                if pattern.is_match(&op.object) {
                                    continue; // Skip this operation, continue with next
                                }
                            }
                        }

                        // Network reconnection delay (original logic)
                        let now = Utc::now();
                        if now.sub(op.queued).num_seconds() < 1 {
                            tokio::select! {
                                _ = cancel_token.cancelled() => {
                                    info!("MRF heal routine cancelled during reconnection delay");
                                    break;
                                }
                                _ = sleep(Duration::from_secs(1)) => {}
                            }
                        }

                        // Core healing logic (original logic preserved)
                        let scan_mode = if op.bitrot_scan { HEAL_DEEP_SCAN } else { HEAL_NORMAL_SCAN };

                        if op.object.is_empty() {
                            // Heal bucket (original logic)
                            if let Err(err) = heal_bucket(&op.bucket).await {
                                error!("heal bucket failed, bucket: {}, err: {:?}", op.bucket, err);
                            }
                        } else if op.versions.is_empty() {
                            // Heal single object (original logic)
                            if let Err(err) = heal_object(
                                &op.bucket,
                                &op.object,
                                &op.version_id.clone().unwrap_or_default(),
                                scan_mode
                            ).await {
                                error!("heal object failed, bucket: {}, object: {}, err: {:?}", op.bucket, op.object, err);
                            }
                        } else {
                            // Heal multiple versions (original logic)
                            let vers = op.versions.len() / 16;
                            if vers > 0 {
                                for i in 0..vers {
                                    // Check for cancellation before each version
                                    if cancel_token.is_cancelled() {
                                        info!("MRF heal routine cancelled during version processing");
                                        return;
                                    }

                                    let start = i * 16;
                                    let end = start + 16;
                                    if let Err(err) = heal_object(
                                        &op.bucket,
                                        &op.object,
                                        &Uuid::from_slice(&op.versions[start..end]).expect("").to_string(),
                                        scan_mode,
                                    ).await {
                                        error!("heal object failed, bucket: {}, object: {}, err: {:?}", op.bucket, op.object, err);
                                    }
                                }
                            }
                        }
                    } else {
                        info!("MRF heal routine channel closed, exiting");
                        break;
                    }
                }
            }
        }

        info!("MRF heal routine stopped gracefully");
    }
}
