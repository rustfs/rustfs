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

use crate::last_minute::{self};
use std::collections::HashMap;

pub struct ReplicationLatency {
    // Delays for single and multipart PUT requests
    upload_histogram: last_minute::LastMinuteHistogram,
}

impl ReplicationLatency {
    // Merge two ReplicationLatency
    pub fn merge(&mut self, other: &mut ReplicationLatency) -> &ReplicationLatency {
        self.upload_histogram.merge(&other.upload_histogram);
        self
    }

    // Get upload delay (categorized by object size interval)
    pub fn get_upload_latency(&mut self) -> HashMap<String, u64> {
        let mut ret = HashMap::new();
        let avg = self.upload_histogram.get_avg_data();
        for (i, v) in avg.iter().enumerate() {
            let avg_duration = v.avg();
            ret.insert(self.size_tag_to_string(i), avg_duration.as_millis() as u64);
        }
        ret
    }
    pub fn update(&mut self, size: i64, during: std::time::Duration) {
        self.upload_histogram.add(size, during);
    }

    // Simulate the conversion from size tag to string
    fn size_tag_to_string(&self, tag: usize) -> String {
        match tag {
            0 => String::from("Size < 1 KiB"),
            1 => String::from("Size < 1 MiB"),
            2 => String::from("Size < 10 MiB"),
            3 => String::from("Size < 100 MiB"),
            4 => String::from("Size < 1 GiB"),
            _ => String::from("Size > 1 GiB"),
        }
    }
}

// #[derive(Debug, Clone, Default)]
// pub struct ReplicationLastMinute {
//     pub last_minute: LastMinuteLatency,
// }

// impl ReplicationLastMinute {
//     pub fn merge(&mut self, other: ReplicationLastMinute) -> ReplicationLastMinute {
//         let mut nl = ReplicationLastMinute::default();
//         nl.last_minute = self.last_minute.merge(&mut other.last_minute);
//         nl
//     }

//     pub fn add_size(&mut self, n: i64) {
//         let t = SystemTime::now()
//             .duration_since(UNIX_EPOCH)
//             .expect("Time went backwards")
//             .as_secs();
//         self.last_minute.add_all(t - 1, &AccElem { total: t - 1, size: n as u64, n: 1 });
//     }

//     pub fn get_total(&self) -> AccElem {
//         self.last_minute.get_total()
//     }
// }

// impl fmt::Display for ReplicationLastMinute {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         let t = self.last_minute.get_total();
//         write!(f, "ReplicationLastMinute sz= {}, n= {}, dur= {}", t.size, t.n, t.total)
//     }
// }
