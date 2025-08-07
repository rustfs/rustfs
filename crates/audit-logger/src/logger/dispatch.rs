//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::logger::config::Config;
use crate::logger::entry::{AuditEntry, LogEntry};
use crate::logger::{Loggable, Target, factory};
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct AuditLogger {
    targets: Vec<Arc<dyn Target>>,
}

impl AuditLogger {
    pub fn new(config: &Config) -> Self {
        let targets = factory::create_targets_from_config(config);
        Self { targets }
    }

    pub async fn log(&self, entry: impl Loggable) {
        let boxed_entry: Box<dyn Loggable> = Box::new(entry);
        let entry_arc = Arc::new(boxed_entry);

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        for target in &self.targets {
            let target_clone = target.clone();
            let entry_clone = Arc::clone(&entry_arc);
            let handle = tokio::spawn(async move {
                // 我们需要一个新的 Box，因为 Arc<Box> 不能直接转换为 Box
                let entry_for_send = entry_clone.to_json().unwrap();
                let rehydrated_entry: Box<dyn Loggable> = match serde_json::from_str::<LogEntry>(&entry_for_send) {
                    Ok(log_entry) => Box::new(log_entry),
                    Err(_) => match serde_json::from_str::<AuditEntry>(&entry_for_send) {
                        Ok(audit_entry) => Box::new(audit_entry),
                        Err(_) => {
                            eprintln!("Failed to rehydrate log entry for target {}", target_clone.name());
                            return;
                        }
                    },
                };

                if let Err(e) = target_clone.send(rehydrated_entry).await {
                    eprintln!("Failed to send log to target {}: {}", target_clone.name(), e);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await;
        }
    }

    pub async fn shutdown(&self) {
        println!("Shutting down all logger targets...");
        let mut handles = vec![];
        for target in &self.targets {
            let target = target.clone();
            handles.push(tokio::spawn(async move {
                target.shutdown().await;
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        println!("All logger targets shut down.");
    }
}
