use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::store_api::ObjectInfo;
use crate::event::name::EventName;
use crate::event::targetlist::TargetList;
use crate::store::ECStore;
use crate::bucket::metadata::BucketMetadata;

pub struct EventNotifier {
    target_list: TargetList,
    //bucket_rules_map: HashMap<String , HashMap<EventName, Rules>>,
}

impl EventNotifier {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            target_list:     TargetList::new(),
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

impl EventArgs {
}

pub fn send_event(args: EventArgs) {
}
