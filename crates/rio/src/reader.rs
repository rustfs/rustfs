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

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

use crate::compress_index::TryGetIndex;
use crate::{EtagResolvable, HashReaderDetector, Reader};

pub struct WarpReader<R> {
    inner: R,
}

impl<R: AsyncRead + Unpin + Send + Sync> WarpReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for WarpReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> HashReaderDetector for WarpReader<R> {}

impl<R: AsyncRead + Unpin + Send + Sync> EtagResolvable for WarpReader<R> {}

impl<R: AsyncRead + Unpin + Send + Sync> TryGetIndex for WarpReader<R> {}

impl<R: AsyncRead + Unpin + Send + Sync> Reader for WarpReader<R> {}
