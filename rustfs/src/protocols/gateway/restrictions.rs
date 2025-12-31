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

/// Unsupported FTP features list
pub const UNSUPPORTED_FTP_FEATURES: &[&str] = &[
    // Atomic rename operations (must be implemented via CopyObject+DeleteObject)
    "Atomic RNFR/RNTO rename",
    // File append operations (S3 does not support native append)
    "APPE command (file append)",
    // POSIX permission operations (S3 uses ACLs and Policies)
    "chmod command",
    "chown command",
    // Symbolic links (S3 object storage does not support)
    "SYMLINK creation",
    // Hard links (S3 object storage does not support)
    "HARD LINK creation",
    // File locking (S3 does not support filesystem-level locking)
    "File locking mechanism",
    // Direct directory rename (must be implemented via object copy)
    "Directory atomic rename",
];

/// Check if an FTP feature is supported
pub fn is_ftp_feature_supported(feature: &str) -> bool {
    !UNSUPPORTED_FTP_FEATURES.contains(&feature)
}

/// Get S3 equivalent operation for unsupported features
pub fn get_s3_equivalent_operation(unsupported_feature: &str) -> Option<&'static str> {
    match unsupported_feature {
        "Atomic RNFR/RNTO rename" | "SSH_FXP_RENAME atomic rename" | "Directory atomic rename" => {
            Some("Use CopyObject + DeleteObject to implement rename")
        }
        "APPE command (file append)" | "SSH_FXP_OPEN append mode" => Some("Use PutObject to overwrite the entire object"),
        "chmod command"
        | "chown command"
        | "SSH_FXP_SETSTAT permission modification"
        | "SSH_FXP_FSETSTAT permission modification" => Some("Use S3 ACLs or Bucket Policies to manage permissions"),
        "SYMLINK creation" | "SSH_FXP_SYMLINK creation" => Some("S3 object storage does not support symbolic links"),
        "File locking mechanism" | "SSH_FXP_BLOCK file locking" => {
            Some("Use S3 object versioning or conditional writes for concurrency control")
        }
        _ => None,
    }
}
