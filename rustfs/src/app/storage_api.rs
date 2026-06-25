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

//! App-local boundary for storage-layer helper APIs used by S3 use cases.

pub(crate) mod access {
    pub(crate) use crate::storage::access::{
        PostObjectRequestMarker, ReqInfo, authorize_request, has_bypass_governance_header, req_info_mut, req_info_ref,
    };
}

pub(crate) mod concurrency {
    pub(crate) use crate::storage::concurrency::{
        ConcurrencyManager, GetObjectGuard, IoQueueStatus, IoStrategy, PutObjectGuard, get_concurrency_aware_buffer_size,
        get_concurrency_manager, get_put_concurrency_aware_buffer_size,
    };
}

pub(crate) mod compression {
    pub(crate) use crate::storage::ecstore_compression::{MIN_DISK_COMPRESSIBLE_SIZE, is_disk_compressible};
}

pub(crate) mod deadlock_detector {
    #[cfg(test)]
    pub(crate) use crate::storage::deadlock_detector::RequestHangDetectionPolicy;
    pub(crate) use crate::storage::deadlock_detector::{DeadlockDetector, get_deadlock_detector};
}

pub(crate) mod ecfs {
    pub(crate) use crate::storage::ecfs::FS;
}

pub(crate) mod head_prefix {
    pub(crate) use crate::storage::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};
}

pub(crate) mod helper {
    pub(crate) use crate::storage::helper::{OperationHelper, spawn_background_with_context};
}

pub(crate) mod io {
    #[cfg(test)]
    pub(crate) use crate::storage::{DecryptReader, EncryptReader, HardLimitReader, boxed_reader};
    pub(crate) use crate::storage::{DynReader, HashReader, WriteEncryption, WritePlan, compression_metadata_value, wrap_reader};
}

pub(crate) mod options {
    pub(crate) use crate::storage::options::{
        copy_dst_opts, copy_src_opts, del_opts, extract_metadata, extract_metadata_from_mime,
        extract_metadata_from_mime_with_object_name, filter_object_metadata, get_complete_multipart_upload_opts,
        get_content_sha256_with_query, get_opts, normalize_content_encoding_for_storage, parse_copy_source_range, put_opts,
        validate_archive_content_encoding,
    };
}

pub(crate) mod request_context {
    pub(crate) use crate::storage::request_context::{RequestContext, spawn_traced};
}

pub(crate) mod sse {
    pub(crate) use crate::storage::sse::{
        EncryptionKeyKind, SSEType, build_ssec_read_headers, encryption_material_to_metadata, extract_ssec_params_from_headers,
        extract_ssekms_context_from_headers, map_get_object_reader_error, mark_encrypted_multipart_metadata,
    };
    pub(crate) use crate::storage::{
        DecryptionRequest, EncryptionRequest, PrepareEncryptionRequest, apply_bucket_default_lock_retention,
        extract_server_side_encryption_from_headers, get_buffer_size_opt_in, sse_decryption, sse_encryption,
        sse_prepare_encryption,
    };
}

pub(crate) mod set_disk {
    pub(crate) use crate::storage::{get_lock_acquire_timeout, is_valid_storage_class};
}

pub(crate) mod timeout_wrapper {
    pub(crate) use crate::storage::timeout_wrapper::{GetObjectTimeoutPolicy, RequestTimeoutWrapper};
}

pub(crate) use crate::storage::{
    RFC1123, StorageDeletedObject, StorageObjectInfo, StorageObjectOptions, StorageObjectToDelete, StoragePutObjReader,
    check_preconditions, get_validated_store, has_replication_rules, parse_object_lock_legal_hold, parse_object_lock_retention,
    parse_part_number_i32_to_usize, process_lambda_configurations, process_queue_configurations, process_topic_configurations,
    remove_object_lock_metadata_for_copy, strip_managed_encryption_metadata, validate_bucket_object_lock_enabled,
    validate_list_object_unordered_with_delimiter, validate_object_key, validate_sse_headers_for_read,
    validate_sse_headers_for_write, validate_ssec_for_read, wrap_response_with_cors,
};
