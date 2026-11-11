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

//! Contracts for bucket engines.
//!
//! A bucket engine is a built-in implementation that owns the data plane of the
//! buckets bound to it, beside the erasure-coded object layer (the `object`
//! engine). This crate holds only the contract: names, descriptors and
//! capabilities, the per-bucket binding stored in bucket metadata, the placement
//! view engines use to place data, and the engine trait the server dispatches
//! to. It depends on no storage implementation.
//!
//! See `docs/architecture/bucket-engines.md` for the framework and
//! `docs/architecture/kv-cache-buckets.md` for the first engine.

mod binding;
mod descriptor;
mod engine;
mod environment;
mod error;
mod listing;
mod name;
mod placement;

pub use binding::{BUCKET_ENGINE_CONFIG_FILE, BucketEngineBinding, MAX_BINDING_BYTES, OBJECT_ENGINE};
pub use descriptor::{
    BucketFeature, CustomOperationSpec, EngineCapabilities, EngineDescriptor, EngineObjectOperation, OperationAction,
};
pub use engine::{
    BucketContext, BucketEngine, ByteRange, CustomRequest, CustomResponse, EngineObject, EngineObjectInfo, ObjectWriteMeta,
    RdmaTransfer,
};
pub use environment::{ENGINES_DIR, NodeEnvironment, SYSTEM_DIR, engine_dir};
pub use error::{EngineError, EngineResult};
pub use listing::{ListName, ListPage, ListPageBuilder, ListQuery};
pub use name::{EngineName, NameError, OperationName};
pub use placement::{NodeTargets, PlacementView, TargetId};

/// Version of this contract; bumped on breaking changes to the engine trait or the binding format.
pub const BUCKET_ENGINE_CONTRACT_VERSION: &str = "rustfs.bucket-engine.v2";

/// CreateBucket request header that binds the new bucket to an engine.
pub const CREATE_BUCKET_ENGINE_HEADER: &str = "x-rustfs-bucket-engine";

/// Query parameter that names an engine custom operation on a bucket request.
pub const ENGINE_OPERATION_QUERY: &str = "x-rustfs-engine-op";

/// Prefix of internode operations the engine registry answers itself
/// instead of passing them to an engine's `handle_peer_call`. Engines must
/// not use operation names with this prefix.
pub const FRAMEWORK_OPERATION_PREFIX: &str = "framework-";
/// Framework operation: list the engines this node implements (no bucket).
pub const FRAMEWORK_DESCRIBE_ENGINES: &str = "framework-describe-engines";
/// Framework operation: whether this node's engine holds data for a bucket.
pub const FRAMEWORK_HOLDS_DATA: &str = "framework-holds-data";
/// Framework operation: purge this node's engine data for a bucket.
pub const FRAMEWORK_PURGE: &str = "framework-purge";
