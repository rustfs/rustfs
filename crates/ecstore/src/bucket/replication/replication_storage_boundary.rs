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

use rustfs_filemeta::FileInfo;
use tokio_util::sync::CancellationToken;

use super::replication_error_boundary::Error;
use super::replication_filemeta_boundary::{replication_state_from_filemeta, version_purge_status_from_filemeta};
pub(crate) type ReplicationObjectStore = crate::store::ECStore;
pub(crate) use crate::client::api_get_options::{AdvancedGetOptions, StatObjectOptions};
pub(crate) use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
pub(crate) use crate::storage_api_contracts::list::{
    ListOperations, StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions,
};
pub(crate) use crate::storage_api_contracts::namespace::NamespaceLocking as StorageNamespaceLocking;
pub(crate) use crate::storage_api_contracts::object::{
    DeletedObject, EcstoreObjectOperations, ObjectIO, ObjectOperations, ObjectToDelete,
};
pub(crate) use crate::storage_api_contracts::range::HTTPRangeSpec;
pub(crate) use rustfs_replication::{DeletedObject as ReplicationDeletedObject, ObjectToDelete as ReplicationObjectToDelete};

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
pub(crate) type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

pub trait ReplicationObjectIO:
    ObjectIO<
        Error = Error,
        RangeSpec = HTTPRangeSpec,
        HeaderMap = http::HeaderMap,
        ObjectOptions = ObjectOptions,
        ObjectInfo = ObjectInfo,
        GetObjectReader = GetObjectReader,
        PutObjectReader = PutObjReader,
    > + Send
    + Sync
    + std::fmt::Debug
    + 'static
{
}

impl<T> ReplicationObjectIO for T where
    T: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = http::HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        > + Send
        + Sync
        + std::fmt::Debug
        + 'static
{
}

pub trait ReplicationStorage:
    ObjectIO<
        Error = Error,
        RangeSpec = HTTPRangeSpec,
        HeaderMap = http::HeaderMap,
        ObjectOptions = ObjectOptions,
        ObjectInfo = ObjectInfo,
        GetObjectReader = GetObjectReader,
        PutObjectReader = PutObjReader,
    > + ObjectOperations<
        Error = Error,
        ObjectInfo = ObjectInfo,
        ObjectOptions = ObjectOptions,
        FileInfo = FileInfo,
        ObjectToDelete = ObjectToDelete,
        DeletedObject = DeletedObject,
    > + ListOperations<
        Error = Error,
        ListObjectsV2Info = ListObjectsV2Info,
        ListObjectVersionsInfo = ListObjectVersionsInfo,
        ObjectInfoOrErr = ObjectInfoOrErr,
        WalkOptions = WalkOptions,
        WalkCancellation = CancellationToken,
        WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
    > + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>
{
}

pub(crate) fn deleted_object_for_replication(delete_object: DeletedObject) -> ReplicationDeletedObject {
    ReplicationDeletedObject {
        delete_marker: delete_object.delete_marker,
        delete_marker_version_id: delete_object.delete_marker_version_id,
        object_name: delete_object.object_name,
        version_id: delete_object.version_id,
        delete_marker_mtime: delete_object.delete_marker_mtime,
        replication_state: delete_object.replication_state.as_ref().map(replication_state_from_filemeta),
        found: delete_object.found,
        force_delete: delete_object.force_delete,
    }
}

pub(crate) fn object_to_delete_for_replication(object: &ObjectToDelete) -> ReplicationObjectToDelete {
    ReplicationObjectToDelete {
        object_name: object.object_name.clone(),
        version_id: object.version_id,
        delete_marker_replication_status: object.delete_marker_replication_status.clone(),
        version_purge_status: object.version_purge_status.clone().map(version_purge_status_from_filemeta),
        version_purge_statuses: object.version_purge_statuses.clone(),
        replicate_decision_str: object.replicate_decision_str.clone(),
    }
}

impl<T> ReplicationStorage for T where
    T: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = http::HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        > + ObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        > + ListOperations<
            Error = Error,
            ListObjectsV2Info = ListObjectsV2Info,
            ListObjectVersionsInfo = ListObjectVersionsInfo,
            ObjectInfoOrErr = ObjectInfoOrErr,
            WalkOptions = WalkOptions,
            WalkCancellation = CancellationToken,
            WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        > + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>
{
}
