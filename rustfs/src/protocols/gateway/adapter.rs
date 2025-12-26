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

/// Translate FTP commands to S3 actions
pub fn ftp_command_to_s3_action(command: &str, args: &[&str]) -> Option<S3Action> {
    match command.to_uppercase().as_str() {
        // File operations
        "RETR" => Some(S3Action::GetObject),
        "STOR" => Some(S3Action::PutObject),
        "DELE" => Some(S3Action::DeleteObject),
        "APPE" => Some(S3Action::PutObject), // Append is mapped to PutObject

        // Directory operations
        "LIST" | "NLST" => Some(S3Action::ListBucket),
        "MKD" | "XMKD" => Some(S3Action::PutObject), // Directory creation mapped to PutObject
        "RMD" | "XRMD" => Some(S3Action::DeleteObject), // Directory deletion mapped to DeleteObject

        // Rename operations (mapped to CopyObject + DeleteObject)
        "RNFR" => None, // First part of rename, no action yet
        "RNTO" => Some(S3Action::CopyObject), // Second part of rename

        // Other operations
        "PWD" | "XPWD" => Some(S3Action::ListBucket), // PWD mapped to ListBucket
        "STAT" => Some(S3Action::HeadObject), // STAT mapped to HeadObject
        _ => None,
    }
}

/// Translate SFTP operations to S3 actions
pub fn sftp_operation_to_s3_action(operation: &str, path: &str) -> Option<S3Action> {
    match operation {
        // File operations
        "open" => {
            // Determine if it's read or write based on path and flags (simplified)
            // In practice, this would be determined by the OpenFlags
            Some(S3Action::GetObject) // Default to GetObject for open
        }
        "close" => None, // No S3 action needed
        "read" => Some(S3Action::GetObject),
        "write" => Some(S3Action::PutObject),
        "remove" => Some(S3Action::DeleteObject),
        "rename" => Some(S3Action::CopyObject), // Rename mapped to CopyObject

        // Directory operations
        "list" | "readdir" => Some(S3Action::ListBucket),
        "mkdir" => Some(S3Action::PutObject), // Directory creation mapped to PutObject
        "rmdir" => Some(S3Action::DeleteObject), // Directory deletion mapped to DeleteObject

        // Metadata operations
        "stat" | "lstat" | "fstat" => Some(S3Action::HeadObject),
        _ => None,
    }
}

/// Check if an operation is supported for the given protocol
pub fn is_operation_supported(protocol: Protocol, action: &S3Action) -> bool {
    match protocol {
        Protocol::Ftp | Protocol::Ftps => {
            // FTP/FTPS limitations - no native append support
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

/// Get the S3 resource path for a protocol operation
///
/// MINIO CONSTRAINT: Resource paths MUST follow S3 naming conventions
pub fn get_s3_resource_path(protocol: Protocol, protocol_path: &str) -> String {
    // Convert protocol-specific paths to S3 object keys
    match protocol {
        Protocol::Ftp | Protocol::Ftps | Protocol::Sftp => {
            // Normalize path separators
            protocol_path.replace('\\', "/")
        }
    }
}