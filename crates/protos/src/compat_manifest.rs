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

//! Rolling-upgrade compatibility manifest for the internode RPC dual-write
//! payload fields in `node.proto`.
//!
//! Every `xxx` / `xxx_bin` field pair carries the same payload twice: legacy
//! JSON for old readers and msgpack for new ones. If a new node stops
//! producing the JSON side before the fleet-wide fallback count reaches zero,
//! an old node silently decodes an empty payload mid-upgrade. This manifest
//! pins, per message field, which JSON encoder call site must keep existing
//! and under which policy.
//!
//! This is a guard contract surface, not a runtime API. Tests in this crate
//! assert that the manifest exactly covers the `_bin` field pairs declared in
//! `node.proto`; the crates that own the send sites assert their own source
//! against the manifest (`crates/ecstore/src/cluster/rpc/remote_disk.rs` for
//! request sites, `rustfs/src/storage/rpc/node_service/disk.rs` for response
//! sites). Keeping those assertions in the owning crates keeps the dependency
//! direction intact: a contract crate must never read implementation-crate or
//! binary-crate sources (see `docs/architecture/crate-boundaries.md`, enforced
//! by `scripts/check_layer_dependencies.sh`).

/// One `json_field` / `bin_field` dual-write pair on an internode RPC message.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CompatPayloadField {
    pub message: &'static str,
    pub json_field: &'static str,
    pub bin_field: &'static str,
}

/// JSON-side production policy for a request payload field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestJsonPolicy {
    /// May skip the JSON side once `internode_rpc_msgpack_only()` is on.
    MsgpackOnlyEligible,
    /// Must keep dual-writing JSON until the msgpack fallback count is zero.
    AlwaysDualWriteUntilFallbackZero,
}

/// A request-side dual-write send site in `crates/ecstore/src/cluster/rpc/remote_disk.rs`.
#[derive(Clone, Copy, Debug)]
pub struct RequestCompatSendSite {
    pub field: CompatPayloadField,
    /// Exact JSON-encoder statement that must keep existing at the send site.
    pub json_encoder: &'static str,
    pub policy: RequestJsonPolicy,
}

/// A response-side dual-write send site in `rustfs/src/storage/rpc/node_service/disk.rs`.
#[derive(Clone, Copy, Debug)]
pub struct ResponseCompatSendSite {
    pub field: CompatPayloadField,
    /// Exact JSON-encoder statement that must keep existing at the send site.
    pub json_encoder: &'static str,
}

pub const REQUEST_COMPAT_SEND_SITES: &[RequestCompatSendSite] = &[
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "BatchReadVersionRequest",
            json_field: "batch_read_version_req",
            bin_field: "batch_read_version_req_bin",
        },
        json_encoder: "let batch_read_version_req = compat_json(&req)?;",
        policy: RequestJsonPolicy::MsgpackOnlyEligible,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "DeleteVersionRequest",
            json_field: "file_info",
            bin_field: "file_info_bin",
        },
        json_encoder: "let file_info = serde_json::to_string(&fi)?;",
        policy: RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "DeleteVersionRequest",
            json_field: "opts",
            bin_field: "opts_bin",
        },
        json_encoder: "let opts = serde_json::to_string(&opts)?;",
        policy: RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "DeleteVersionsRequest",
            json_field: "opts",
            bin_field: "opts_bin",
        },
        json_encoder: "let opts = match serde_json::to_string(&opts) {",
        policy: RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "DeleteVersionsRequest",
            json_field: "versions",
            bin_field: "versions_bin",
        },
        json_encoder: "versions_str.push(match serde_json::to_string(file_info_versions) {",
        policy: RequestJsonPolicy::AlwaysDualWriteUntilFallbackZero,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "ReadMultipleRequest",
            json_field: "read_multiple_req",
            bin_field: "read_multiple_req_bin",
        },
        json_encoder: "let read_multiple_req = compat_json(&req)?;",
        policy: RequestJsonPolicy::MsgpackOnlyEligible,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "ReadVersionRequest",
            json_field: "opts",
            bin_field: "opts_bin",
        },
        json_encoder: "let encoded_opts = compat_json(opts).and_then(|opts_str| encode_msgpack(opts).map(|opts_bin| (opts_str, opts_bin)));",
        policy: RequestJsonPolicy::MsgpackOnlyEligible,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "RenameDataRequest",
            json_field: "file_info",
            bin_field: "file_info_bin",
        },
        json_encoder: "let file_info = compat_json(&fi)?;",
        policy: RequestJsonPolicy::MsgpackOnlyEligible,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "UpdateMetadataRequest",
            json_field: "file_info",
            bin_field: "file_info_bin",
        },
        json_encoder: "let file_info = compat_json(&fi)?;",
        policy: RequestJsonPolicy::MsgpackOnlyEligible,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "UpdateMetadataRequest",
            json_field: "opts",
            bin_field: "opts_bin",
        },
        json_encoder: "let opts_str = compat_json(&opts)?;",
        policy: RequestJsonPolicy::MsgpackOnlyEligible,
    },
    RequestCompatSendSite {
        field: CompatPayloadField {
            message: "WriteMetadataRequest",
            json_field: "file_info",
            bin_field: "file_info_bin",
        },
        json_encoder: "let file_info = compat_json(&fi)?;",
        policy: RequestJsonPolicy::MsgpackOnlyEligible,
    },
];

pub const RESPONSE_COMPAT_SEND_SITES: &[ResponseCompatSendSite] = &[
    ResponseCompatSendSite {
        field: CompatPayloadField {
            message: "BatchReadVersionResponse",
            json_field: "batch_read_version_resps",
            bin_field: "batch_read_version_resps_bin",
        },
        json_encoder: "compat_response_json(batch_read_version_resp, request_decoded_from_msgpack)",
    },
    ResponseCompatSendSite {
        field: CompatPayloadField {
            message: "ReadMultipleResponse",
            json_field: "read_multiple_resps",
            bin_field: "read_multiple_resps_bin",
        },
        json_encoder: "compat_response_json(read_multiple_resp, false)",
    },
    ResponseCompatSendSite {
        field: CompatPayloadField {
            message: "ReadVersionResponse",
            json_field: "file_info",
            bin_field: "file_info_bin",
        },
        json_encoder: "let file_info_json = compat_response_json(&file_info, request_had_msgpack_payload);",
    },
    ResponseCompatSendSite {
        field: CompatPayloadField {
            message: "ReadXLResponse",
            json_field: "raw_file_info",
            bin_field: "raw_file_info_bin",
        },
        json_encoder: "let raw_file_info_json = compat_response_json(&raw_file_info, false);",
    },
    ResponseCompatSendSite {
        field: CompatPayloadField {
            message: "RenameDataResponse",
            json_field: "rename_data_resp",
            bin_field: "rename_data_resp_bin",
        },
        json_encoder: "let rename_data_resp_json = compat_response_json(rename_data_resp, request_decoded_from_msgpack)",
    },
];

/// Cuts a source file at its trailing `#[cfg(test)] mod tests` module so that
/// send-site assertions only match production code, never the asserting test
/// itself.
pub fn production_source(source: &'static str, file_name: &str) -> &'static str {
    source
        .split("\n#[cfg(test)]\nmod tests")
        .next()
        .unwrap_or_else(|| panic!("{file_name} should contain production source before tests"))
}
