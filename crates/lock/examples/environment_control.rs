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

//! Example demonstrating environment variable control of lock system

use rustfs_lock::{LockManager, get_global_lock_manager};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = get_global_lock_manager();

    println!("Lock system status: {}", if manager.is_disabled() { "DISABLED" } else { "ENABLED" });

    match std::env::var("RUSTFS_ENABLE_LOCKS") {
        Ok(value) => println!("RUSTFS_ENABLE_LOCKS set to: {value}"),
        Err(_) => println!("RUSTFS_ENABLE_LOCKS not set (defaults to enabled)"),
    }

    // Test acquiring a lock
    let result = manager.acquire_read_lock("test-bucket", "test-object", "test-owner").await;
    match result {
        Ok(guard) => {
            println!("Lock acquired successfully! Disabled: {}", guard.is_disabled());
        }
        Err(e) => {
            println!("Failed to acquire lock: {e:?}");
        }
    }

    println!("Environment control example completed");
    Ok(())
}
