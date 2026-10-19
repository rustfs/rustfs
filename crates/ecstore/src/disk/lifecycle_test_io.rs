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

//! Bucket-scoped observations distinguish exact metadata reads from walks.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Default, Clone)]
pub(crate) struct Counts {
    pub walks: usize,
    pub reads: Vec<String>,
}

fn observers() -> &'static Mutex<HashMap<String, Arc<Mutex<Counts>>>> {
    static OBSERVERS: OnceLock<Mutex<HashMap<String, Arc<Mutex<Counts>>>>> = OnceLock::new();
    OBSERVERS.get_or_init(Mutex::default)
}

pub(crate) struct Observer {
    bucket: String,
    counts: Arc<Mutex<Counts>>,
}

impl Observer {
    pub fn new(bucket: &str) -> Self {
        let counts = Arc::new(Mutex::new(Counts::default()));
        assert!(
            observers()
                .lock()
                .expect("I/O observers lock")
                .insert(bucket.to_owned(), counts.clone())
                .is_none(),
            "each test must use its own bucket"
        );
        Self {
            bucket: bucket.to_owned(),
            counts,
        }
    }

    pub fn snapshot(&self) -> Counts {
        self.counts.lock().expect("I/O counts lock").clone()
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        observers().lock().expect("I/O observers lock").remove(&self.bucket);
    }
}

pub(crate) fn observe(bucket: &str, object: Option<&str>) {
    let observer = observers().lock().expect("I/O observers lock").get(bucket).cloned();
    if let Some(observer) = observer {
        let mut counts = observer.lock().expect("I/O counts lock");
        match object {
            Some(object) => counts.reads.push(object.to_owned()),
            None => counts.walks += 1,
        }
    }
}
