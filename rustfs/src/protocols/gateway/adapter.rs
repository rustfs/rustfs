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

//! Protocol to S3 action adapter

use super::action::S3Action;
use crate::protocols::session::context::Protocol;

/// Check if an operation is supported for the given protocol
pub fn is_operation_supported(protocol: Protocol, action: &S3Action) -> bool {
    match protocol {
        Protocol::Ftps => {
            // FTPS limitations - no native append support
            match action {
                S3Action::PutObject => true, // STOR and APPE both map to PutObject
                _ => true,
            }
        }
        Protocol::Sftp => {
            // SFTP has more capabilities but still constrained by S3 semantics
            true
        }
    }
}
