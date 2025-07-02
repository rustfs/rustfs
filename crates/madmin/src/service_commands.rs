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

use std::{collections::HashMap, time::Duration};

use hyper::Uri;

use crate::{trace::TraceType, utils::parse_duration};

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ServiceTraceOpts {
    s3: bool,
    internal: bool,
    storage: bool,
    os: bool,
    scanner: bool,
    decommission: bool,
    healing: bool,
    batch_replication: bool,
    batch_key_rotation: bool,
    batch_expire: bool,
    batch_all: bool,
    rebalance: bool,
    replication_resync: bool,
    bootstrap: bool,
    ftp: bool,
    ilm: bool,
    only_errors: bool,
    threshold: Duration,
}

#[allow(dead_code)]
impl ServiceTraceOpts {
    fn trace_types(&self) -> TraceType {
        let mut tt = TraceType::default();
        tt.set_if(self.s3, &TraceType::S3);
        tt.set_if(self.internal, &TraceType::INTERNAL);
        tt.set_if(self.storage, &TraceType::STORAGE);
        tt.set_if(self.os, &TraceType::OS);
        tt.set_if(self.scanner, &TraceType::SCANNER);
        tt.set_if(self.decommission, &TraceType::DECOMMISSION);
        tt.set_if(self.healing, &TraceType::HEALING);

        if self.batch_all {
            tt.set_if(true, &TraceType::BATCH_REPLICATION);
            tt.set_if(true, &TraceType::BATCH_KEY_ROTATION);
            tt.set_if(true, &TraceType::BATCH_EXPIRE);
        } else {
            tt.set_if(self.batch_replication, &TraceType::BATCH_REPLICATION);
            tt.set_if(self.batch_key_rotation, &TraceType::BATCH_KEY_ROTATION);
            tt.set_if(self.batch_expire, &TraceType::BATCH_EXPIRE);
        }

        tt.set_if(self.rebalance, &TraceType::REBALANCE);
        tt.set_if(self.replication_resync, &TraceType::REPLICATION_RESYNC);
        tt.set_if(self.bootstrap, &TraceType::BOOTSTRAP);
        tt.set_if(self.ftp, &TraceType::FTP);
        tt.set_if(self.ilm, &TraceType::ILM);

        tt
    }

    pub fn parse_params(&mut self, uri: &Uri) -> Result<(), String> {
        let query_pairs: HashMap<_, _> = uri
            .query()
            .unwrap_or("")
            .split('&')
            .filter_map(|pair| {
                let mut split = pair.split('=');
                let key = split.next()?.to_string();
                let value = split.next().map(|v| v.to_string()).unwrap_or_else(|| "false".to_string());
                Some((key, value))
            })
            .collect();

        self.s3 = query_pairs.get("s3").is_some_and(|v| v == "true");
        self.os = query_pairs.get("os").is_some_and(|v| v == "true");
        self.scanner = query_pairs.get("scanner").is_some_and(|v| v == "true");
        self.decommission = query_pairs.get("decommission").is_some_and(|v| v == "true");
        self.healing = query_pairs.get("healing").is_some_and(|v| v == "true");
        self.batch_replication = query_pairs.get("batch-replication").is_some_and(|v| v == "true");
        self.batch_key_rotation = query_pairs.get("batch-keyrotation").is_some_and(|v| v == "true");
        self.batch_expire = query_pairs.get("batch-expire").is_some_and(|v| v == "true");
        if query_pairs.get("all").is_some_and(|v| v == "true") {
            self.s3 = true;
            self.internal = true;
            self.storage = true;
            self.os = true;
        }

        self.rebalance = query_pairs.get("rebalance").is_some_and(|v| v == "true");
        self.storage = query_pairs.get("storage").is_some_and(|v| v == "true");
        self.internal = query_pairs.get("internal").is_some_and(|v| v == "true");
        self.only_errors = query_pairs.get("err").is_some_and(|v| v == "true");
        self.replication_resync = query_pairs.get("replication-resync").is_some_and(|v| v == "true");
        self.bootstrap = query_pairs.get("bootstrap").is_some_and(|v| v == "true");
        self.ftp = query_pairs.get("ftp").is_some_and(|v| v == "true");
        self.ilm = query_pairs.get("ilm").is_some_and(|v| v == "true");

        if let Some(threshold) = query_pairs.get("threshold") {
            let duration = parse_duration(threshold)?;
            self.threshold = duration;
        }

        Ok(())
    }
}
