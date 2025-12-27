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

#[cfg(feature = "tls")]
pub mod certs;
#[cfg(feature = "ip")]
pub mod ip;
#[cfg(feature = "net")]
pub mod net;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "net")]
pub use net::*;

#[cfg(all(feature = "net", feature = "io"))]
pub mod retry;

#[cfg(feature = "io")]
pub mod io;

#[cfg(feature = "hash")]
pub mod hash;

#[cfg(feature = "os")]
pub mod os;

#[cfg(feature = "path")]
pub mod path;

#[cfg(feature = "string")]
pub mod string;

#[cfg(feature = "crypto")]
pub mod crypto;

#[cfg(feature = "compress")]
pub mod compress;

#[cfg(feature = "path")]
pub mod dirs;

#[cfg(feature = "tls")]
pub use certs::*;

#[cfg(feature = "hash")]
pub use hash::*;

#[cfg(feature = "io")]
pub use io::*;

#[cfg(feature = "ip")]
pub use ip::*;

#[cfg(feature = "crypto")]
pub use crypto::*;

#[cfg(feature = "compress")]
pub use compress::*;

#[cfg(feature = "notify")]
mod notify;

#[cfg(feature = "sys")]
pub mod sys;

#[cfg(feature = "sys")]
pub use sys::user_agent::*;

#[cfg(feature = "notify")]
pub use notify::*;

mod envs;
pub use envs::*;

pub mod obj;
