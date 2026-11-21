// Copyright 2026 RustFS Team
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

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};

use crate::storage_api::vector::{ManagedObjectAccess, ManagedObjectOperation};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::{
    FutureExt, StreamExt,
    stream::{self, BoxStream},
};
use lance_io::object_store::{ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreProvider};
use object_store_lance::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart, path::Path,
};
use tokio::sync::Semaphore;
use url::Url;

use crate::table_catalog::{CatalogMetadataRef, CatalogObjectKey, TableCatalogObject, TableCatalogObjectMetadata};

use super::{LanceManifestError, ManagedDatasetMultipart, ManagedManifestObjects, ManifestIoLimits};

const DATA_READ_CHUNK_BYTES: u64 = 1024 * 1024;
const MULTIPART_CHUNK_BYTES: usize = 5 * 1024 * 1024;

pub(super) struct ManagedObjectStore {
    objects: Arc<dyn ManagedManifestObjects>,
    access: ManagedObjectAccess,
    references: RwLock<HashMap<Path, CatalogMetadataRef>>,
    limits: ManifestIoLimits,
    data_references: Arc<RwLock<HashMap<Path, TableCatalogObjectMetadata>>>,
    read_permits: Arc<Semaphore>,
}

impl fmt::Debug for ManagedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedObjectStore")
            .field("access", &self.access)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ManagedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RustFS managed Lance objects")
    }
}

impl ManagedObjectStore {
    pub(super) fn new(objects: Arc<dyn ManagedManifestObjects>, access: ManagedObjectAccess, limits: ManifestIoLimits) -> Self {
        Self {
            objects,
            access,
            references: RwLock::new(HashMap::new()),
            limits,
            data_references: Arc::new(RwLock::new(HashMap::new())),
            read_permits: Arc::new(Semaphore::new(limits.max_concurrent_reads.get())),
        }
    }

    pub(super) async fn bind_data_reference(
        &self,
        reference: &super::files::ManifestObjectReference,
    ) -> Result<(), LanceManifestError> {
        self.access.check(reference.key.as_str(), ManagedObjectOperation::Head)?;
        let metadata = self.objects.head(&self.access, &reference.key).await?;
        require_revision(metadata.body_revision)?;
        if reference
            .expected_size
            .is_some_and(|expected| expected.get() != metadata.size)
        {
            return Err(LanceManifestError::LengthMismatch {
                expected: reference.expected_size.map_or(0, |size| size.get()),
                actual: metadata.size,
            });
        }
        bind_data_identity(&self.data_references, Path::from(reference.key.as_str()), metadata)
    }

    pub(super) async fn read_data_bytes(&self, key: &CatalogObjectKey, limit: u64) -> Result<Bytes, LanceManifestError> {
        let path = Path::from(key.as_str());
        let reference = self.data_reference(&path)?.ok_or(LanceManifestError::ReferenceMismatch {
            reason: "auxiliary object has no validated manifest reference",
        })?;
        if reference.size > limit {
            return Err(LanceManifestError::ReadLimitExceeded {
                length: reference.size,
                limit,
            });
        }
        self.get_opts(&path, GetOptions::default())
            .await?
            .bytes()
            .await
            .map_err(Into::into)
    }

    fn data_reference(&self, path: &Path) -> Result<Option<TableCatalogObjectMetadata>, LanceManifestError> {
        Ok(self
            .data_references
            .read()
            .map_err(|source| LanceManifestError::Storage {
                operation: "read Dataset object reference",
                source: source.to_string().into(),
            })?
            .get(path)
            .cloned())
    }

    fn data_key(&self, path: &Path) -> Result<CatalogObjectKey, LanceManifestError> {
        self.access.check(path.as_ref(), ManagedObjectOperation::CreateOnly)?;
        let relative = path
            .as_ref()
            .strip_prefix(self.access.allowed_prefix())
            .ok_or(LanceManifestError::ResourceMismatch {
                reason: "Dataset object is outside the bound prefix",
            })?;
        super::files::validate_writable_relative_path(relative)?;
        Ok(CatalogObjectKey::try_new(path.to_string())?)
    }

    async fn get_data(
        &self,
        location: &Path,
        options: GetOptions,
        expected: TableCatalogObjectMetadata,
    ) -> object_store_lance::Result<GetResult> {
        let key = CatalogObjectKey::try_new(location.to_string()).map_err(|source| object_error(source.into()))?;
        self.access
            .check(key.as_str(), ManagedObjectOperation::Head)
            .map_err(|source| object_error(source.into()))?;
        let actual = self.objects.head(&self.access, &key).await.map_err(object_error)?;
        if actual.body_revision != expected.body_revision || actual.size != expected.size || actual.etag != expected.etag {
            return Err(object_error(LanceManifestError::ReferenceMismatch {
                reason: "Dataset object differs from its bound identity",
            }));
        }
        let revision = require_revision(expected.body_revision).map_err(object_error)?;
        let meta = object_meta(location, &actual).map_err(object_error)?;
        options.check_preconditions(&meta)?;
        if options
            .version
            .as_ref()
            .is_some_and(|version| Some(version) != meta.version.as_ref())
        {
            return Err(object_store_lance::Error::Precondition {
                path: location.to_string(),
                source: "Dataset body revision differs".into(),
            });
        }
        let range = options
            .range
            .as_ref()
            .map(|range| range.as_range(meta.size))
            .transpose()
            .map_err(|source| object_store_lance::Error::Generic {
                store: "RustFS managed Lance objects",
                source: Box::new(source),
            })?
            .unwrap_or(0..meta.size);
        let stream_end = if options.head { range.start } else { range.end };
        let objects = self.objects.clone();
        let access = self.access.clone();
        let permits = self.read_permits.clone();
        let expected_etag = expected.etag;
        let stream = stream::try_unfold(range.start, move |start| {
            let objects = objects.clone();
            let access = access.clone();
            let permits = permits.clone();
            let key = key.clone();
            let expected_etag = expected_etag.clone();
            async move {
                if start >= stream_end {
                    return Ok(None);
                }
                let end = start.saturating_add(DATA_READ_CHUNK_BYTES).min(stream_end);
                access
                    .check(key.as_str(), ManagedObjectOperation::Range)
                    .map_err(|source| object_error(source.into()))?;
                let _permit = permits.acquire_owned().await.map_err(|source| {
                    object_error(LanceManifestError::Storage {
                        operation: "acquire Dataset read permit",
                        source: Box::new(source),
                    })
                })?;
                let object = objects
                    .get(&access, &key, Some(start..end), Some(revision))
                    .await
                    .map_err(object_error)?;
                if object.body_revision != Some(revision)
                    || object.etag != expected_etag
                    || object.data.len() as u64 != end - start
                {
                    return Err(object_error(LanceManifestError::ReferenceMismatch {
                        reason: "Dataset range differs from the bound identity or requested length",
                    }));
                }
                Ok(Some((Bytes::from(object.data), end)))
            }
        })
        .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes: Attributes::default(),
            extensions: Default::default(),
        })
    }

    pub(super) fn access(&self) -> &ManagedObjectAccess {
        &self.access
    }

    pub(super) fn bind_reference(&self, reference: &CatalogMetadataRef) -> Result<(), LanceManifestError> {
        self.access
            .check(reference.object_key().as_str(), ManagedObjectOperation::Get)?;
        require_revision(reference.body_revision().map(|revision| revision.as_uuid()))?;
        self.check_length(reference.content_length())?;
        let path = Path::from(reference.object_key().as_str());
        let mut references = self.references.write().map_err(|source| LanceManifestError::Storage {
            operation: "bind manifest reference",
            source: source.to_string().into(),
        })?;
        if references.get(&path).is_some_and(|existing| existing != reference) {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "an immutable path was rebound to different content",
            });
        }
        references.insert(path, reference.clone());
        Ok(())
    }

    fn check_length(&self, length: u64) -> Result<(), LanceManifestError> {
        let limit = self.limits.max_manifest_bytes.get();
        if length > limit {
            return Err(LanceManifestError::ReadLimitExceeded { length, limit });
        }
        Ok(())
    }

    pub(super) async fn create_candidate(
        &self,
        key: &CatalogObjectKey,
        content: Bytes,
    ) -> Result<CatalogMetadataRef, LanceManifestError> {
        self.access.check(key.as_str(), ManagedObjectOperation::CreateOnly)?;
        self.check_length(content.len() as u64)?;
        let created = self.objects.create_only(&self.access, key, content.clone()).await?;
        let revision = require_revision(created.body_revision)?;
        if created.size != content.len() as u64 {
            return Err(LanceManifestError::LengthMismatch {
                expected: content.len() as u64,
                actual: created.size,
            });
        }
        let _permit = self
            .read_permits
            .acquire()
            .await
            .map_err(|source| LanceManifestError::Storage {
                operation: "acquire manifest read permit",
                source: Box::new(source),
            })?;
        self.access.check(key.as_str(), ManagedObjectOperation::Get)?;
        let object = self.objects.get(&self.access, key, None, Some(revision)).await?;
        if object.body_revision != Some(revision) || object.data.as_slice() != content.as_ref() {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "candidate readback differs from its create-only result",
            });
        }
        let reference = CatalogMetadataRef::from_read(key.clone(), &object)?;
        self.bind_reference(&reference)?;
        Ok(reference)
    }

    pub(super) async fn prepare_candidate(
        &self,
        key: &CatalogObjectKey,
        content: Bytes,
    ) -> Result<CatalogMetadataRef, LanceManifestError> {
        self.access.check(key.as_str(), ManagedObjectOperation::CreateOnly)?;
        self.check_length(content.len() as u64)?;
        if let Some(reference) = self.registered_reference(&Path::from(key.as_str()))? {
            let object = self.read_exact(&reference).await?;
            if object.data.as_slice() != content.as_ref() {
                return Err(LanceManifestError::CandidateAlreadyExists {
                    key: key.as_str().to_string(),
                });
            }
            return Ok(reference);
        }
        match self.create_candidate(key, content.clone()).await {
            Ok(reference) => Ok(reference),
            Err(LanceManifestError::CandidateAlreadyExists { .. }) => self.read_existing_candidate(key, &content).await,
            Err(error) => Err(error),
        }
    }

    async fn read_existing_candidate(
        &self,
        key: &CatalogObjectKey,
        content: &Bytes,
    ) -> Result<CatalogMetadataRef, LanceManifestError> {
        self.access.check(key.as_str(), ManagedObjectOperation::Head)?;
        self.access.check(key.as_str(), ManagedObjectOperation::Get)?;
        self.check_length(content.len() as u64)?;
        let _permit = self
            .read_permits
            .acquire()
            .await
            .map_err(|source| LanceManifestError::Storage {
                operation: "acquire candidate reconciliation permit",
                source: Box::new(source),
            })?;
        let metadata = self.objects.head(&self.access, key).await?;
        if metadata.size != content.len() as u64 {
            return Err(LanceManifestError::LengthMismatch {
                expected: content.len() as u64,
                actual: metadata.size,
            });
        }
        let revision = require_revision(metadata.body_revision)?;
        let object = self.objects.get(&self.access, key, None, Some(revision)).await?;
        if object.body_revision != Some(revision) || object.data.as_slice() != content.as_ref() || object.etag != metadata.etag {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "existing candidate differs from the exact attempted content",
            });
        }
        let reference = CatalogMetadataRef::from_read(key.clone(), &object)?;
        self.bind_reference(&reference)?;
        Ok(reference)
    }

    pub(super) async fn read_exact(&self, reference: &CatalogMetadataRef) -> Result<TableCatalogObject, LanceManifestError> {
        self.bind_reference(reference)?;
        let _permit = self
            .read_permits
            .acquire()
            .await
            .map_err(|source| LanceManifestError::Storage {
                operation: "acquire manifest read permit",
                source: Box::new(source),
            })?;
        self.access
            .check(reference.object_key().as_str(), ManagedObjectOperation::Head)?;
        let metadata = self.objects.head(&self.access, reference.object_key()).await?;
        check_metadata(reference, &metadata)?;
        self.check_length(metadata.size)?;
        let revision = require_revision(reference.body_revision().map(|value| value.as_uuid()))?;
        let object = self
            .objects
            .get(&self.access, reference.object_key(), None, Some(revision))
            .await?;
        let actual = CatalogMetadataRef::from_read(reference.object_key().clone(), &object)?;
        if actual.content_length() != reference.content_length() {
            return Err(LanceManifestError::LengthMismatch {
                expected: reference.content_length(),
                actual: actual.content_length(),
            });
        }
        if actual.content_sha256() != reference.content_sha256() {
            return Err(LanceManifestError::DigestMismatch);
        }
        if actual.body_revision() != reference.body_revision() || actual.etag() != reference.etag() {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "body revision or ETag differs from the committed reference",
            });
        }
        Ok(object)
    }

    fn reference(&self, location: &Path) -> Result<CatalogMetadataRef, LanceManifestError> {
        self.registered_reference(location)?
            .ok_or(LanceManifestError::ReferenceMismatch {
                reason: "object path has no Catalog manifest reference",
            })
    }

    fn registered_reference(&self, location: &Path) -> Result<Option<CatalogMetadataRef>, LanceManifestError> {
        let references = self.references.read().map_err(|source| LanceManifestError::Storage {
            operation: "read manifest binding",
            source: source.to_string().into(),
        })?;
        Ok(references.get(location).cloned())
    }
}

fn bind_data_identity(
    references: &RwLock<HashMap<Path, TableCatalogObjectMetadata>>,
    path: Path,
    metadata: TableCatalogObjectMetadata,
) -> Result<(), LanceManifestError> {
    require_revision(metadata.body_revision)?;
    let mut references = references.write().map_err(|source| LanceManifestError::Storage {
        operation: "bind Dataset object identity",
        source: source.to_string().into(),
    })?;
    if references.get(&path).is_some_and(|existing| {
        existing.body_revision != metadata.body_revision || existing.size != metadata.size || existing.etag != metadata.etag
    }) {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "immutable Dataset path was rebound to different content",
        });
    }
    references.insert(path, metadata);
    Ok(())
}

fn put_result(metadata: &TableCatalogObjectMetadata) -> PutResult {
    PutResult {
        e_tag: metadata.etag.clone(),
        version: metadata.body_revision.map(|revision| revision.to_string()),
        extensions: Default::default(),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Dataset multipart failed and abort failed: {abort}")]
struct MultipartCleanupError {
    #[source]
    primary: object_store_lance::Error,
    abort: object_store_lance::Error,
}

async fn abort_failed_upload(upload: &mut dyn MultipartUpload, primary: object_store_lance::Error) -> object_store_lance::Error {
    match upload.abort().await {
        Ok(()) => primary,
        Err(abort) => object_store_lance::Error::Generic {
            store: "RustFS managed Lance objects",
            source: Box::new(MultipartCleanupError { primary, abort }),
        },
    }
}

struct DatasetMultipart {
    upload: Arc<dyn ManagedDatasetMultipart>,
    location: Path,
    references: Arc<RwLock<HashMap<Path, TableCatalogObjectMetadata>>>,
    parts: u16,
    length: u64,
    completed: Option<TableCatalogObjectMetadata>,
    aborted: bool,
}

impl fmt::Debug for DatasetMultipart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatasetMultipart")
            .field("parts", &self.parts)
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl MultipartUpload for DatasetMultipart {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        if self.aborted || self.completed.is_some() || self.parts == 10_000 {
            return futures::future::ready(Err(unsupported("multipart session cannot accept another part"))).boxed();
        }
        let size = payload.content_length() as u64;
        let Some(length) = self.length.checked_add(size) else {
            return futures::future::ready(Err(unsupported("multipart length exceeds the supported range"))).boxed();
        };
        self.parts += 1;
        self.length = length;
        let number = self.parts;
        let upload = self.upload.clone();
        let content = payload.into_iter().collect();
        async move { upload.put_part(number, content, size).await.map_err(object_error) }.boxed()
    }

    async fn complete(&mut self) -> object_store_lance::Result<PutResult> {
        if self.aborted {
            return Err(unsupported("multipart session has been aborted"));
        }
        if let Some(metadata) = &self.completed {
            return Ok(put_result(metadata));
        }
        let metadata = self.upload.complete(self.parts).await.map_err(object_error)?;
        if metadata.size != self.length {
            return Err(object_error(LanceManifestError::LengthMismatch {
                expected: self.length,
                actual: metadata.size,
            }));
        }
        bind_data_identity(&self.references, self.location.clone(), metadata.clone()).map_err(object_error)?;
        self.completed = Some(metadata.clone());
        Ok(put_result(&metadata))
    }

    async fn abort(&mut self) -> object_store_lance::Result<()> {
        if self.completed.is_some() || self.aborted {
            return Ok(());
        }
        self.upload.abort().await.map_err(object_error)?;
        self.aborted = true;
        Ok(())
    }
}

fn require_revision(revision: Option<uuid::Uuid>) -> Result<uuid::Uuid, LanceManifestError> {
    revision
        .filter(|value| !value.is_nil())
        .ok_or(LanceManifestError::ReferenceMismatch {
            reason: "Vector manifest requires a non-nil body revision",
        })
}

fn check_metadata(reference: &CatalogMetadataRef, metadata: &TableCatalogObjectMetadata) -> Result<(), LanceManifestError> {
    if metadata.size != reference.content_length() {
        return Err(LanceManifestError::LengthMismatch {
            expected: reference.content_length(),
            actual: metadata.size,
        });
    }
    if metadata.body_revision != reference.body_revision().map(|value| value.as_uuid())
        || metadata.etag.as_deref() != reference.etag()
    {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "HEAD differs from the committed body identity",
        });
    }
    Ok(())
}

fn object_error(error: LanceManifestError) -> object_store_lance::Error {
    match error {
        LanceManifestError::ObjectNotFound { ref key } => object_store_lance::Error::NotFound {
            path: key.clone(),
            source: Box::new(error),
        },
        LanceManifestError::CandidateAlreadyExists { ref key } => object_store_lance::Error::AlreadyExists {
            path: key.clone(),
            source: Box::new(error),
        },
        other => object_store_lance::Error::Generic {
            store: "RustFS managed Lance objects",
            source: Box::new(other),
        },
    }
}

fn unsupported(operation: &'static str) -> object_store_lance::Error {
    object_store_lance::Error::NotSupported {
        source: operation.into(),
    }
}

fn object_meta(location: &Path, metadata: &TableCatalogObjectMetadata) -> Result<ObjectMeta, LanceManifestError> {
    let last_modified = metadata
        .mod_time
        .map(|value| DateTime::from_timestamp(value.unix_timestamp(), value.nanosecond()))
        .unwrap_or(Some(DateTime::<Utc>::UNIX_EPOCH))
        .ok_or(LanceManifestError::ReferenceMismatch {
            reason: "object timestamp is outside the supported range",
        })?;
    Ok(ObjectMeta {
        location: location.clone(),
        last_modified,
        size: metadata.size,
        e_tag: metadata.etag.clone(),
        version: metadata.body_revision.map(|value| value.to_string()),
    })
}

#[async_trait]
impl ObjectStore for ManagedObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, options: PutOptions) -> object_store_lance::Result<PutResult> {
        if matches!(options.mode, PutMode::Update(_)) || !options.tags.is_empty() || !options.attributes.is_empty() {
            return Err(unsupported("Dataset objects require untagged immutable writes"));
        }
        let key = self.data_key(location).map_err(object_error)?;
        if payload.content_length() > MULTIPART_CHUNK_BYTES {
            let mut upload = self.put_multipart_opts(location, PutMultipartOptions::default()).await?;
            let mut part = Vec::new();
            let mut size = 0;
            for mut bytes in payload {
                while !bytes.is_empty() {
                    let count = (MULTIPART_CHUNK_BYTES - size).min(bytes.len());
                    part.push(bytes.split_to(count));
                    size += count;
                    if size == MULTIPART_CHUNK_BYTES {
                        if let Err(error) = upload.put_part(std::mem::take(&mut part).into_iter().collect()).await {
                            return Err(abort_failed_upload(upload.as_mut(), error).await);
                        }
                        size = 0;
                    }
                }
            }
            if size > 0
                && let Err(error) = upload.put_part(part.into_iter().collect()).await
            {
                return Err(abort_failed_upload(upload.as_mut(), error).await);
            }
            return match upload.complete().await {
                Ok(completed) => Ok(completed),
                Err(error) => Err(abort_failed_upload(upload.as_mut(), error).await),
            };
        }
        let expected_length = payload.content_length() as u64;
        let mut bytes = BytesMut::with_capacity(payload.content_length());
        for part in payload {
            bytes.extend_from_slice(&part);
        }
        let metadata = self
            .objects
            .create_only(&self.access, &key, bytes.freeze())
            .await
            .map_err(object_error)?;
        if metadata.size != expected_length {
            return Err(object_error(LanceManifestError::LengthMismatch {
                expected: expected_length,
                actual: metadata.size,
            }));
        }
        bind_data_identity(&self.data_references, location.clone(), metadata.clone()).map_err(object_error)?;
        Ok(put_result(&metadata))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store_lance::Result<Box<dyn MultipartUpload>> {
        if !options.tags.is_empty() || !options.attributes.is_empty() {
            return Err(unsupported("Dataset multipart requires untagged immutable writes"));
        }
        let key = self.data_key(location).map_err(object_error)?;
        self.access
            .check(key.as_str(), ManagedObjectOperation::CreateMultipart)
            .map_err(|source| object_error(source.into()))?;
        let upload = self
            .objects
            .create_multipart(&self.access, &key)
            .await
            .map_err(object_error)?;
        Ok(Box::new(DatasetMultipart {
            upload,
            location: location.clone(),
            references: self.data_references.clone(),
            parts: 0,
            length: 0,
            completed: None,
            aborted: false,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store_lance::Result<GetResult> {
        let operation = if options.head {
            ManagedObjectOperation::Head
        } else if options.range.is_some() {
            ManagedObjectOperation::Range
        } else {
            ManagedObjectOperation::Get
        };
        self.access
            .check(location.as_ref(), operation)
            .map_err(|source| object_error(source.into()))?;
        if let Some(reference) = self.data_reference(location).map_err(object_error)? {
            return self.get_data(location, options, reference).await;
        }
        let reference = self.reference(location).map_err(object_error)?;
        let object = self.read_exact(&reference).await.map_err(object_error)?;
        let metadata = TableCatalogObjectMetadata {
            body_revision: object.body_revision,
            etag: object.etag,
            mod_time: object.mod_time,
            size: object.data.len() as u64,
        };
        let meta = object_meta(location, &metadata).map_err(object_error)?;
        options.check_preconditions(&meta)?;
        if options.version.is_some() && options.version != meta.version {
            return Err(object_store_lance::Error::Precondition {
                path: location.to_string(),
                source: "requested body revision differs from the Catalog reference".into(),
            });
        }
        let range = options
            .range
            .as_ref()
            .map(|range| range.as_range(meta.size))
            .transpose()
            .map_err(|source| object_store_lance::Error::Generic {
                store: "RustFS managed Lance objects",
                source: Box::new(source),
            })?
            .unwrap_or(0..meta.size);
        let bytes = if options.head {
            Bytes::new()
        } else {
            Bytes::from(object.data).slice(range.start as usize..range.end as usize)
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store_lance::Result<Path>>,
    ) -> BoxStream<'static, object_store_lance::Result<Path>> {
        locations
            .map(|result| {
                result.and_then(|_| Err(unsupported("managed manifest deletion requires Catalog retention authorization")))
            })
            .boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, object_store_lance::Result<ObjectMeta>> {
        stream::once(async { Err(unsupported("manifest discovery is owned by Catalog")) }).boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> object_store_lance::Result<ListResult> {
        Err(unsupported("manifest discovery is owned by Catalog"))
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> object_store_lance::Result<()> {
        Err(unsupported("managed manifest copying is not available"))
    }
}

#[derive(Debug)]
pub(super) struct ManagedStoreProvider {
    pub store: Arc<ManagedObjectStore>,
}

#[async_trait]
impl ObjectStoreProvider for ManagedStoreProvider {
    async fn new_store(&self, url: Url, params: &ObjectStoreParams) -> lance_core::Result<LanceObjectStore> {
        let prefix = self.calculate_object_store_prefix(&url, None)?;
        let mut store = LanceObjectStore::new(
            self.store.clone(),
            url,
            params.resolved_block_size()?,
            None,
            false,
            false,
            self.store.limits.max_concurrent_reads.get(),
            0,
            None,
        );
        store.store_prefix = prefix;
        Ok(store)
    }

    fn calculate_object_store_prefix(&self, url: &Url, _options: Option<&HashMap<String, String>>) -> lance_core::Result<String> {
        let access = self.store.access();
        if url.scheme() != "rustfs-vector" || url.host_str() != Some(access.bucket().storage_bucket_id().as_str()) {
            return Err(lance_core::Error::invalid_input("URL differs from the bound Vector bucket"));
        }
        Ok(format!(
            "rustfs-vector${}${}${}",
            access.bucket().storage_bucket_id().as_str(),
            access.bucket().incarnation().as_uuid(),
            access.table_id().as_str()
        ))
    }
}
