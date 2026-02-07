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

use std::io::Cursor;
use std::pin::Pin;
use tokio::io::AsyncWrite;

use crate::HttpWriter;

pub enum Writer {
    Cursor(Cursor<Vec<u8>>),
    Http(HttpWriter),
    Other(Box<dyn AsyncWrite + Unpin + Send + Sync>),
}

impl Writer {
    /// Create a Writer::Other from any AsyncWrite + Unpin + Send type.
    pub fn from_tokio_writer<W>(w: W) -> Self
    where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
    {
        Writer::Other(Box::new(w))
    }

    pub fn from_cursor(w: Cursor<Vec<u8>>) -> Self {
        Writer::Cursor(w)
    }

    pub fn from_http(w: HttpWriter) -> Self {
        Writer::Http(w)
    }

    pub fn into_cursor_inner(self) -> Option<Vec<u8>> {
        match self {
            Writer::Cursor(w) => Some(w.into_inner()),
            _ => None,
        }
    }

    pub fn as_cursor(&mut self) -> Option<&mut Cursor<Vec<u8>>> {
        match self {
            Writer::Cursor(w) => Some(w),
            _ => None,
        }
    }
    pub fn as_http(&mut self) -> Option<&mut HttpWriter> {
        match self {
            Writer::Http(w) => Some(w),
            _ => None,
        }
    }

    pub fn into_http(self) -> Option<HttpWriter> {
        match self {
            Writer::Http(w) => Some(w),
            _ => None,
        }
    }

    pub fn into_cursor(self) -> Option<Cursor<Vec<u8>>> {
        match self {
            Writer::Cursor(w) => Some(w),
            _ => None,
        }
    }
}

impl AsyncWrite for Writer {
    fn poll_write(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Writer::Cursor(w) => Pin::new(w).poll_write(cx, buf),
            Writer::Http(w) => Pin::new(w).poll_write(cx, buf),
            Writer::Other(w) => Pin::new(w.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Writer::Cursor(w) => Pin::new(w).poll_flush(cx),
            Writer::Http(w) => Pin::new(w).poll_flush(cx),
            Writer::Other(w) => Pin::new(w.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Writer::Cursor(w) => Pin::new(w).poll_shutdown(cx),
            Writer::Http(w) => Pin::new(w).poll_shutdown(cx),
            Writer::Other(w) => Pin::new(w.as_mut()).poll_shutdown(cx),
        }
    }
}
