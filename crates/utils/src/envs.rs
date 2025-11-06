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

use std::env;

pub fn get_env_i64(key: &str, default: i64) -> i64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
pub fn get_env_i32(key: &str, default: i32) -> i32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub fn get_env_f32(key: &str, default: f32) -> f32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub fn get_env_i16(key: &str, default: i16) -> i16 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
pub fn get_env_u16(key: &str, default: u16) -> u16 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub fn get_env_u16_opt(key: &str) -> Option<u16> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_usize_opt(key: &str) -> Option<usize> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_i8(key: &str, default: i8) -> i8 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
pub fn get_env_u8(key: &str, default: u8) -> u8 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub fn get_env_opt_i64(key: &str) -> Option<i64> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}
pub fn get_env_opt_i32(key: &str) -> Option<i32> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_opt_f32(key: &str) -> Option<f32> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_opt_i16(key: &str) -> Option<i16> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_opt_i8(key: &str) -> Option<i8> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}
pub fn get_env_opt_u8(key: &str) -> Option<u8> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_opt_opt_i64(key: &str) -> Option<Option<i64>> {
    env::var(key).ok().map(|v| v.parse().ok())
}

pub fn get_env_usize(key: &str, default: usize) -> usize {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
pub fn get_env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
pub fn get_env_u32(key: &str, default: u32) -> u32 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
pub fn get_env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
pub fn get_env_opt_u64(key: &str) -> Option<u64> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_opt_u32(key: &str) -> Option<u32> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_opt_f64(key: &str) -> Option<f64> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_f64(key: &str, default: f64) -> f64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

pub fn get_env_opt_usize(key: &str) -> Option<usize> {
    env::var(key).ok().and_then(|v| v.parse().ok())
}

pub fn get_env_opt_str(key: &str) -> Option<String> {
    env::var(key).ok()
}

pub fn get_env_opt_bool(key: &str) -> Option<bool> {
    env::var(key).ok().and_then(|v| match v.to_lowercase().as_str() {
        "1" | "true" | "yes" => Some(true),
        "0" | "false" | "no" => Some(false),
        _ => None,
    })
}

pub fn get_env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "1" | "true" | "yes" => Some(true),
            "0" | "false" | "no" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}
