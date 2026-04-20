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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use crate::bucket::lifecycle::bucket_lifecycle_ops::{ExpiryOp, GLOBAL_ExpiryState, TransitionedObject};
use crate::bucket::lifecycle::lifecycle::{self, ObjectOpts};
use crate::global::GLOBAL_TierConfigMgr;
use sha2::{Digest, Sha256};
use std::any::Any;
use std::io::Write;
use uuid::Uuid;
use xxhash_rust::xxh64;

static XXHASH_SEED: u64 = 0;

#[derive(Default)]
#[allow(dead_code)]
struct ObjSweeper {
    object: String,
    bucket: String,
    version_id: Option<Uuid>,
    versioned: bool,
    suspended: bool,
    transition_status: String,
    transition_tier: String,
    transition_version_id: String,
    remote_object: String,
}

#[allow(dead_code)]
impl ObjSweeper {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(bucket: &str, object: &str) -> Result<Self, std::io::Error> {
        Ok(Self {
            object: object.into(),
            bucket: bucket.into(),
            ..Default::default()
        })
    }

    pub fn with_version(&mut self, vid: Option<Uuid>) -> &Self {
        self.version_id = vid.clone();
        self
    }

    pub fn with_versioning(&mut self, versioned: bool, suspended: bool) -> &Self {
        self.versioned = versioned;
        self.suspended = suspended;
        self
    }

    pub fn get_opts(&self) -> lifecycle::ObjectOpts {
        let mut opts = ObjectOpts {
            version_id: self.version_id.clone(),
            versioned: self.versioned,
            version_suspended: self.suspended,
            ..Default::default()
        };
        if self.suspended && self.version_id.is_none_or(|v| v.is_nil()) {
            opts.version_id = None;
        }
        opts
    }

    pub fn set_transition_state(&mut self, info: TransitionedObject) {
        self.transition_tier = info.tier;
        self.transition_status = info.status;
        self.remote_object = info.name;
        self.transition_version_id = info.version_id;
    }

    pub fn should_remove_remote_object(&self) -> Option<Jentry> {
        if self.transition_status != lifecycle::TRANSITION_COMPLETE {
            return None;
        }

        let mut del_tier = false;
        if !self.versioned || self.suspended {
            // 1, 2.a, 2.b
            del_tier = true;
        } else if self.versioned && self.version_id.is_some_and(|v| !v.is_nil()) {
            // 3.a
            del_tier = true;
        }
        if del_tier {
            return Some(Jentry {
                obj_name: self.remote_object.clone(),
                version_id: self.transition_version_id.clone(),
                tier_name: self.transition_tier.clone(),
            });
        }
        None
    }

    pub async fn sweep(&self) {
        let Some(je) = self.should_remove_remote_object() else {
            return;
        };
        let hash = je.op_hash();
        // Grab the sender under a short read lock, then release the lock so we
        // don't hold it across the async send.
        let wrkr = GLOBAL_ExpiryState.read().await.get_worker_ch(hash);
        let Some(wrkr) = wrkr else {
            GLOBAL_ExpiryState.write().await.increment_missed_tier_journal_tasks();
            return;
        };
        if wrkr.send(Some(Box::new(je))).await.is_err() {
            GLOBAL_ExpiryState.write().await.increment_missed_tier_journal_tasks();
        }
    }
}

#[derive(Debug, Clone)]
#[allow(unused_assignments)]
pub struct Jentry {
    pub(crate) obj_name: String,
    pub(crate) version_id: String,
    pub(crate) tier_name: String,
}

impl ExpiryOp for Jentry {
    fn op_hash(&self) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(format!("{}", self.tier_name).as_bytes());
        hasher.update(format!("{}", self.obj_name).as_bytes());
        xxh64::xxh64(hasher.finalize().as_slice(), XXHASH_SEED)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub async fn delete_object_from_remote_tier(obj_name: &str, rv_id: &str, tier_name: &str) -> Result<(), std::io::Error> {
    let mut config_mgr = GLOBAL_TierConfigMgr.write().await;
    let w = match config_mgr.get_driver(tier_name).await {
        Ok(w) => w,
        Err(e) => return Err(std::io::Error::other(e)),
    };
    w.remove(obj_name, rv_id).await
}

pub fn transitioned_delete_journal_entry(
    version_id: Option<Uuid>,
    versioned: bool,
    suspended: bool,
    transitioned: &TransitionedObject,
) -> Option<Jentry> {
    let sweeper = ObjSweeper {
        version_id,
        versioned,
        suspended,
        transition_status: transitioned.status.clone(),
        transition_tier: transitioned.tier.clone(),
        transition_version_id: transitioned.version_id.clone(),
        remote_object: transitioned.name.clone(),
        ..Default::default()
    };

    sweeper.should_remove_remote_object()
}

pub fn transitioned_force_delete_journal_entry(transitioned: &TransitionedObject) -> Option<Jentry> {
    if transitioned.status != lifecycle::TRANSITION_COMPLETE {
        return None;
    }

    Some(Jentry {
        obj_name: transitioned.name.clone(),
        version_id: transitioned.version_id.clone(),
        tier_name: transitioned.tier.clone(),
    })
}

#[cfg(test)]
mod test {}
