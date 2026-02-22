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

use crate::storage::metadata::mx::StorageManager;
use crate::storage::metadata::types::ChunkInfo;
use bytes::Bytes;
use futures::stream::{self, Stream};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use tokio_util::io::StreamReader;

pub fn new_chunked_reader(
    storage_manager: Arc<dyn StorageManager>,
    chunks: Vec<ChunkInfo>,
) -> StreamReader<Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync>>, Bytes> {
    let stream = stream::unfold(
        (storage_manager, chunks, 0),
        move |(mgr, chunks, idx)| async move {
            if idx >= chunks.len() {
                return None;
            }

            let chunk = &chunks[idx];
            let len = chunks.len();

            // Clone for the spawned task
            let mgr_clone = mgr.clone();
            let hash = chunk.hash.clone();

            // Spawn to ensure the awaited future is Sync (JoinHandle is Sync)
            let handle = tokio::spawn(async move {
                mgr_clone.read_data(&hash).await
            });

            match handle.await {
                Ok(Ok(data)) => Some((Ok(data), (mgr, chunks, idx + 1))),
                Ok(Err(e)) => Some((
                    Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
                    (mgr, chunks, len), // Stop iteration on error
                )),
                Err(e) => Some((
                    Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
                    (mgr, chunks, len), // Stop iteration on error
                )),
            }
        },
    );

    StreamReader::new(Box::pin(stream))
}
