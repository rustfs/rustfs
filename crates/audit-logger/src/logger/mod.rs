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

pub mod config;
pub mod dispatch;
pub mod entry;
pub mod factory;
pub mod http_target;

use crate::logger::entry::Loggable;
use async_trait::async_trait;
use std::error::Error;

/// 通用日志目标 Trait
#[async_trait]
pub trait Target: Send + Sync {
    /// 发送单个可日志化条目
    async fn send(&self, entry: Box<Self>) -> Result<(), Box<dyn Error + Send>>;

    /// 返回目标的唯一名称
    fn name(&self) -> &str;

    /// 优雅地关闭目标，确保所有缓冲的日志都被处理
    async fn shutdown(&self);
}
