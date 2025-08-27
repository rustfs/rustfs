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

pub use rustfs_ecstore::config::Config;

pub struct ConfigManager;

impl ConfigManager {
    pub fn load() -> Config {
        // 1. 读取 ENV
        // 2. 读取系统配置文件（如 .rustfs.sys）
        // 3. 合并默认配置
        // 这里只做伪实现，实际应合并多源
        Config::default()
    }
}
