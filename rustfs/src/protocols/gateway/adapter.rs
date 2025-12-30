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

pub fn is_operation_supported(protocol: Protocol, action: &S3Action) -> bool {
    match protocol {
        Protocol::Ftps => match action {
            // Bucket operations: FTPS cannot create buckets via protocol commands
            S3Action::CreateBucket => false,
            S3Action::DeleteBucket => false,

            // Object operations: All file operations supported
            S3Action::GetObject => true,    // RETR command
            S3Action::PutObject => true,    // STOR and APPE commands both map to PutObject
            S3Action::DeleteObject => true, // DELE command
            S3Action::HeadObject => true,   // SIZE command

            // Multipart operations: FTPS has no native multipart upload support
            S3Action::CreateMultipartUpload => false,
            S3Action::UploadPart => false,
            S3Action::CompleteMultipartUpload => false,
            S3Action::AbortMultipartUpload => false,
            S3Action::ListMultipartUploads => false,
            S3Action::ListParts => false,

            // ACL operations: FTPS has no native ACL support
            S3Action::GetBucketAcl => false,
            S3Action::PutBucketAcl => false,
            S3Action::GetObjectAcl => false,
            S3Action::PutObjectAcl => false,

            // Other operations
            S3Action::CopyObject => false, // No native copy support in FTPS
            S3Action::ListBucket => true,  // LIST command
            S3Action::ListBuckets => true, // LIST at root level
            S3Action::HeadBucket => true,  // Can check if directory exists
        },
        Protocol::Sftp => match action {
            // Bucket operations: SFTP can create/delete buckets via mkdir/rmdir
            S3Action::CreateBucket => true,
            S3Action::DeleteBucket => true,

            // Object operations: All file operations supported
            S3Action::GetObject => true,    // RealPath + Open + Read
            S3Action::PutObject => true,    // Open + Write
            S3Action::DeleteObject => true, // Remove
            S3Action::HeadObject => true,   // Stat/Fstat

            // Multipart operations: SFTP has no native multipart upload support
            S3Action::CreateMultipartUpload => false,
            S3Action::UploadPart => false,
            S3Action::CompleteMultipartUpload => false,
            S3Action::AbortMultipartUpload => false,
            S3Action::ListMultipartUploads => false,
            S3Action::ListParts => false,

            // ACL operations: SFTP has no native ACL support
            S3Action::GetBucketAcl => false,
            S3Action::PutBucketAcl => false,
            S3Action::GetObjectAcl => false,
            S3Action::PutObjectAcl => false,

            // Other operations
            S3Action::CopyObject => false, // No remote copy, only local rename
            S3Action::ListBucket => true,  // Readdir
            S3Action::ListBuckets => true, // Readdir at root
            S3Action::HeadBucket => true,  // Stat on directory
        },
    }
}
