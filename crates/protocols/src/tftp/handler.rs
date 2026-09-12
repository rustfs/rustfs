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

//! TFTP `Handler` implementation backed by the RustFS storage API.

use super::config::TftpConfig;
use super::errors::backend_error_to_tftp;
use super::paths::resolve_object_path;
use super::reader::ObjectReader;
use super::writer::ObjectWriter;
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::{AuthorizationError, S3Action, authorize_operation};
use crate::common::session::SessionContext;
use async_tftp::packet::Error as TftpPacketError;
use async_tftp::server::Handler;
use rustfs_credentials::Credentials;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::warn;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_HANDLER: &str = "tftp_handler";
const EVENT_TFTP_HANDLER_STATE: &str = "tftp_handler_state";

pub struct TftpStorageHandler<S: StorageBackend + Send + Sync + 'static> {
    storage: Arc<S>,
    config: TftpConfig,
    credentials: Credentials,
    session_context: SessionContext,
    transfer_semaphore: Arc<Semaphore>,
}

impl<S: StorageBackend + Send + Sync + 'static> TftpStorageHandler<S> {
    pub fn new(storage: S, config: TftpConfig, credentials: Credentials, session_context: SessionContext) -> Self {
        let permits = config.max_concurrent_transfers;
        Self {
            storage: Arc::new(storage),
            transfer_semaphore: Arc::new(Semaphore::new(permits)),
            config,
            credentials,
            session_context,
        }
    }

    fn backend_timeout(&self) -> Duration {
        Duration::from_secs(self.config.backend_op_timeout_secs)
    }

    fn try_acquire_permit(&self) -> Result<OwnedSemaphorePermit, TftpPacketError> {
        self.transfer_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| TftpPacketError::DiskFull)
    }

    fn effective_max_transfer_bytes(&self, advertised: Option<u64>) -> u64 {
        advertised
            .filter(|size| *size > 0)
            .map(|size| size.min(self.config.max_transfer_bytes))
            .unwrap_or(self.config.max_transfer_bytes)
    }

    fn session_for_client(&self, client: &SocketAddr) -> SessionContext {
        SessionContext::new(self.session_context.principal.clone(), self.session_context.protocol, client.ip())
    }

    async fn authorize(&self, client: &SocketAddr, action: &S3Action, bucket: &str, key: &str) -> Result<(), TftpPacketError> {
        let session = self.session_for_client(client);
        match authorize_operation(&session, action, bucket, Some(key)).await {
            Ok(()) => Ok(()),
            Err(AuthorizationError::AccessDenied) => Err(TftpPacketError::PermissionDenied),
            Err(AuthorizationError::IamUnavailable) => {
                warn!(
                    event = EVENT_TFTP_HANDLER_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_HANDLER,
                    action = action.as_str(),
                    bucket = %bucket,
                    key = %key,
                    result = "iam_unavailable",
                    "tftp handler state changed"
                );
                Err(TftpPacketError::UnknownError)
            }
        }
    }
}

impl<S: StorageBackend + Send + Sync + 'static> Handler for TftpStorageHandler<S> {
    type Reader = ObjectReader<S>;
    type Writer = ObjectWriter<S>;

    async fn read_req_open(&mut self, client: &SocketAddr, path: &Path) -> Result<(Self::Reader, Option<u64>), TftpPacketError> {
        if !self.config.access_mode.allows_read() {
            return Err(TftpPacketError::PermissionDenied);
        }

        let permit = self.try_acquire_permit()?;
        let (bucket, key) = resolve_object_path(path, self.config.default_bucket.as_deref())?;
        self.authorize(client, &S3Action::GetObject, &bucket, &key).await?;

        let head = timeout(self.backend_timeout(), self.storage.head_object(&bucket, &key, &self.credentials))
            .await
            .map_err(|_| TftpPacketError::UnknownError)?
            .map_err(|e| backend_error_to_tftp("head_object", e))?;

        let object_size = head.content_length.unwrap_or(0).max(0) as u64;
        let reader = ObjectReader::new(
            Arc::clone(&self.storage),
            bucket,
            key,
            self.credentials.clone(),
            object_size,
            self.config.read_fetch_bytes,
            self.backend_timeout(),
            permit,
        );

        Ok((reader, Some(object_size)))
    }

    async fn write_req_open(
        &mut self,
        client: &SocketAddr,
        path: &Path,
        size: Option<u64>,
    ) -> Result<Self::Writer, TftpPacketError> {
        if !self.config.access_mode.allows_write() {
            return Err(TftpPacketError::PermissionDenied);
        }

        let permit = self.try_acquire_permit()?;
        let (bucket, key) = resolve_object_path(path, self.config.default_bucket.as_deref())?;
        self.authorize(client, &S3Action::PutObject, &bucket, &key).await?;

        let max_bytes = self.effective_max_transfer_bytes(size);
        let writer = ObjectWriter::new(
            Arc::clone(&self.storage),
            bucket,
            key,
            self.credentials.clone(),
            self.config.read_fetch_bytes.max(super::constants::S3_MIN_PART_SIZE),
            max_bytes,
            self.backend_timeout(),
            permit,
        );

        Ok(writer)
    }
}
