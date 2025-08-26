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

#![allow(dead_code)]

pub mod config;
pub mod dispatch;
pub mod entry;
pub mod factory;

use async_trait::async_trait;
use std::error::Error;

/// General Log Target Trait
#[async_trait]
pub trait Target: Send + Sync {
    /// Send a single logizable entry
    async fn send(&self, entry: Box<Self>) -> Result<(), Box<dyn Error + Send>>;

    /// Returns the unique name of the target
    fn name(&self) -> &str;

    /// Close target gracefully, ensuring all buffered logs are processed
    async fn shutdown(&self);
}
