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

//! Private ABI for the NVIDIA server SDK.

use cxx::SharedPtr;
use std::io;
use std::os::fd::{AsRawFd, RawFd};

#[cxx::bridge(namespace = "rustfs::cuobject")]
mod ffi {
    #[derive(Clone, Copy, Debug)]
    struct Range {
        offset: usize,
        size: usize,
    }
    #[derive(Clone, Copy, Default)]
    struct Completion {
        handle: usize,
        status: isize,
    }
    struct Operation<'a> {
        key: &'a str,
        token: &'a str,
        address: u64,
        offset: usize,
        length: usize,
        channel: u16,
        handle: usize,
        put: bool,
    }
    struct Outcome {
        count: isize,
        verbs: i32,
        uncertain: bool,
    }

    // SAFETY: native registrations own their server and mapping. Scatter views
    // also own their base. The caller keeps DMA ranges exclusive until completion.
    unsafe extern "C++" {
        include!("native/server.h");
        type Server;
        type Buffer;
        fn header_version() -> u32;
        fn open_server(address: &str, port: u16, channels: i32, depth: u32) -> Result<SharedPtr<Server>>;
        fn register_buffer(server: SharedPtr<Server>, length: usize) -> Result<SharedPtr<Buffer>>;
        fn scatter_list(base: SharedPtr<Buffer>, ranges: &[Range]) -> Result<SharedPtr<Buffer>>;
        fn size(buffer: &Buffer) -> usize;
        fn data(buffer: &Buffer) -> *mut u8;
        fn lease_channel(server: &Server) -> Result<u16>;
        fn release_channel(server: &Server, channel: u16);
        fn completion_fd(server: &Server, channel: u16) -> i32;
        fn drain(server: &Server, fd: i32, events: &mut [Completion]) -> i32;
        unsafe fn transfer(server: &Server, buffer: &Buffer, op: &Operation) -> Outcome;
    }
}

// SAFETY: the SDK supports concurrent transfers on separately leased channels.
// Server and registration metadata stay immutable after construction. Sharing a
// handle gives no safe access to the host bytes.
unsafe impl Send for ffi::Server {}
unsafe impl Sync for ffi::Server {}
unsafe impl Send for ffi::Buffer {}
unsafe impl Sync for ffi::Buffer {}

pub use ffi::{Completion, Operation, Outcome, Range, data, header_version, open_server, register_buffer, scatter_list, size};

pub type ServerHandle = SharedPtr<ffi::Server>;
pub type BufferHandle = SharedPtr<ffi::Buffer>;

pub fn errno(status: isize) -> io::Error {
    match status.checked_neg().and_then(|code| i32::try_from(code).ok()) {
        Some(code) => io::Error::from_raw_os_error(code),
        None => io::Error::other("invalid NVIDIA error status"),
    }
}

pub struct Channel {
    server: SharedPtr<ffi::Server>,
    id: u16,
}

impl Channel {
    pub fn lease(server: &SharedPtr<ffi::Server>) -> io::Result<Self> {
        let id = ffi::lease_channel(server).map_err(io::Error::other)?;
        if id == u16::MAX {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        Ok(Self {
            server: server.clone(),
            id,
        })
    }

    pub fn fd(&self) -> Option<CompletionFd> {
        let fd = ffi::completion_fd(&self.server, self.id);
        (fd >= 0).then(|| CompletionFd {
            fd,
            server: self.server.clone(),
        })
    }

    /// Submits a transfer to the leased channel.
    ///
    /// # Safety
    /// The caller must exclusively own the local range, keep the remote token valid,
    /// and retain the channel and registrations until the SDK confirms completion.
    /// An unknown submission or undelivered completion must preserve those owners.
    pub unsafe fn submit(&mut self, buffer: &ffi::Buffer, mut op: ffi::Operation<'_>) -> ffi::Outcome {
        op.channel = self.id;
        // SAFETY: the caller holds the range and the exclusive channel lease.
        unsafe { ffi::transfer(&self.server, buffer, &op) }
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        ffi::release_channel(&self.server, self.id);
    }
}

pub struct CompletionFd {
    fd: RawFd,
    server: SharedPtr<ffi::Server>,
}

impl AsRawFd for CompletionFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl CompletionFd {
    pub fn drain(&self, events: &mut [ffi::Completion]) -> io::Result<usize> {
        let count = ffi::drain(&self.server, self.fd, events);
        usize::try_from(count).map_err(|_| errno(count as isize))
    }
}
