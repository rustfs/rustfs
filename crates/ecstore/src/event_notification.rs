#![allow(unused_imports)]
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
#![allow(unused_variables)]

use crate::bucket::metadata::BucketMetadata;
use crate::event::name::EventName;
use crate::event::targetlist::TargetList;
use crate::store::ECStore;
use crate::store_api::ObjectInfo;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct EventNotifier {
    target_list: TargetList,
    //bucket_rules_map: HashMap<String , HashMap<EventName, Rules>>,
}

impl EventNotifier {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            target_list: TargetList::new(),
            //bucket_rules_map: HashMap::new(),
        }))
    }

    fn get_arn_list(&self) -> Vec<String> {
        todo!();
    }

    fn set(&self, bucket: &str, meta: BucketMetadata) {
        todo!();
    }

    fn init_bucket_targets(&self, api: ECStore) -> Result<(), std::io::Error> {
        /*if err := self.target_list.Add(globalNotifyTargetList.Targets()...); err != nil {
          return err
        }
        self.target_list = self.target_list.Init(runtime.GOMAXPROCS(0)) // TODO: make this configurable (y4m4)
        nil*/
        todo!();
    }

    fn send(&self, args: EventArgs) {
        todo!();
    }
}

#[derive(Debug, Default)]
pub struct EventArgs {
    pub event_name: String,
    pub bucket_name: String,
    pub object: ObjectInfo,
    pub req_params: HashMap<String, String>,
    pub resp_elements: HashMap<String, String>,
    pub host: String,
    pub user_agent: String,
}

impl EventArgs {}

pub fn send_event(args: EventArgs) {}
