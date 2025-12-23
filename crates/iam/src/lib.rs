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

use crate::error::{Error, Result};
use manager::IamCache;
use rustfs_ecstore::store::ECStore;
use std::sync::{Arc, OnceLock};
use store::object::ObjectStore;
use sys::IamSys;
use tracing::{debug, instrument};

pub mod cache;
pub mod error;
pub mod manager;
pub mod store;
pub mod utils;

pub mod sys;

static IAM_SYS: OnceLock<Arc<IamSys<ObjectStore>>> = OnceLock::new();

#[instrument(skip(ecstore))]
pub async fn init_iam_sys(ecstore: Arc<ECStore>) -> Result<()> {
    debug!("init iam system");
    let s = IamCache::new(ObjectStore::new(ecstore)).await;

    IAM_SYS.get_or_init(move || IamSys::new(s).into());
    Ok(())
}

#[inline]
pub fn get() -> Result<Arc<IamSys<ObjectStore>>> {
    IAM_SYS.get().map(Arc::clone).ok_or(Error::IamSysNotInitialized)
}

pub fn get_global_iam_sys() -> Option<Arc<IamSys<ObjectStore>>> {
    IAM_SYS.get().cloned()
}
