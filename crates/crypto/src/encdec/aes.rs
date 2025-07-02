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

pub fn native_aes() -> bool {
    cfg_if::cfg_if! {
        if #[cfg(any(target_arch = "x86", target_arch = "x86_64"))] {
            std::is_x86_feature_detected!("aes") && std::is_x86_feature_detected!("pclmulqdq")
        } else if #[cfg(target_arch = "aarch64")] {
            std::arch::is_aarch64_feature_detected!("aes")
        } else if #[cfg(target_arch = "powerpc64")] {
            false
        } else if #[cfg(target_arch = "s390x")] {
            std::is_s390x_feature_detected!("aes")
                && std::is_s390x_feature_detected!("aescbc")
                && std::is_s390x_feature_detected!("aesctr")
                && (std::is_s390x_feature_detected!("aesgcm") || std::is_s390x_feature_detected!("ghash"))
        } else {
            false
        }
    }
}
