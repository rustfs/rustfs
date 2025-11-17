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

mod error;
pub mod heal;
pub mod scanner;

pub use error::{Error, Result};
pub use heal::{HealManager, HealOptions, HealPriority, HealRequest, HealType, channel::HealChannelProcessor};
pub use scanner::Scanner;
use std::sync::{Arc, OnceLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

// Global cancellation token for AHM services (scanner and other background tasks)
static GLOBAL_AHM_SERVICES_CANCEL_TOKEN: OnceLock<CancellationToken> = OnceLock::new();

/// Initialize the global AHM services cancellation token
pub fn init_ahm_services_cancel_token(cancel_token: CancellationToken) -> Result<()> {
    GLOBAL_AHM_SERVICES_CANCEL_TOKEN
        .set(cancel_token)
        .map_err(|_| Error::Config("AHM services cancel token already initialized".to_string()))
}

/// Get the global AHM services cancellation token
pub fn get_ahm_services_cancel_token() -> Option<&'static CancellationToken> {
    GLOBAL_AHM_SERVICES_CANCEL_TOKEN.get()
}

/// Create and initialize the global AHM services cancellation token
pub fn create_ahm_services_cancel_token() -> CancellationToken {
    let cancel_token = CancellationToken::new();
    init_ahm_services_cancel_token(cancel_token.clone()).expect("AHM services cancel token already initialized");
    cancel_token
}

/// Shutdown all AHM services gracefully
pub fn shutdown_ahm_services() {
    if let Some(cancel_token) = GLOBAL_AHM_SERVICES_CANCEL_TOKEN.get() {
        cancel_token.cancel();
    }
}

/// Global heal manager instance
static GLOBAL_HEAL_MANAGER: OnceLock<Arc<HealManager>> = OnceLock::new();

/// Global heal channel processor instance
static GLOBAL_HEAL_CHANNEL_PROCESSOR: OnceLock<Arc<tokio::sync::Mutex<HealChannelProcessor>>> = OnceLock::new();

/// Initialize and start heal manager with channel processor
pub async fn init_heal_manager(
    storage: Arc<dyn heal::storage::HealStorageAPI>,
    config: Option<heal::manager::HealConfig>,
) -> Result<Arc<HealManager>> {
    // Create heal manager
    let heal_manager = Arc::new(HealManager::new(storage, config));

    // Start heal manager
    heal_manager.start().await?;

    // Store global instance
    GLOBAL_HEAL_MANAGER
        .set(heal_manager.clone())
        .map_err(|_| Error::Config("Heal manager already initialized".to_string()))?;

    // Initialize heal channel
    let channel_receiver = rustfs_common::heal_channel::init_heal_channel();

    // Create channel processor
    let channel_processor = HealChannelProcessor::new(heal_manager.clone());

    // Store channel processor instance first
    GLOBAL_HEAL_CHANNEL_PROCESSOR
        .set(Arc::new(tokio::sync::Mutex::new(channel_processor)))
        .map_err(|_| Error::Config("Heal channel processor already initialized".to_string()))?;

    // Start channel processor in background
    let receiver = channel_receiver;
    tokio::spawn(async move {
        if let Some(processor_guard) = GLOBAL_HEAL_CHANNEL_PROCESSOR.get() {
            let mut processor = processor_guard.lock().await;
            if let Err(e) = processor.start(receiver).await {
                error!("Heal channel processor failed: {}", e);
            }
        }
    });

    info!("Heal manager with channel processor initialized successfully");
    Ok(heal_manager)
}

/// Get global heal manager instance
pub fn get_heal_manager() -> Option<&'static Arc<HealManager>> {
    GLOBAL_HEAL_MANAGER.get()
}

/// Get global heal channel processor instance
pub fn get_heal_channel_processor() -> Option<&'static Arc<tokio::sync::Mutex<HealChannelProcessor>>> {
    GLOBAL_HEAL_CHANNEL_PROCESSOR.get()
}
