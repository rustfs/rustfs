// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

impl DirectoryCheckpoint {
    pub(crate) async fn wide_storage_fixture(info: &DataUsageCacheInfo) -> [Self; 2] {
        let scope = Scope {
            bucket: info.name.clone(),
            source: info.source.expect("fixture source"),
            identity: info.scan_identity.expect("fixture identity"),
            progress: info.scan_progress.expect("fixture coverage plan"),
            mode: 0,
            prefixes: None,
            watch: None,
        };
        let slot = Saved::default();
        let endpoint = info.scan_directory_source.clone().expect("fixture physical source");
        let physical = Physical {
            endpoint: endpoint.clone(),
            device: 1,
            inode: 2,
            changed: (123, 0),
            modified: SystemTime::UNIX_EPOCH,
        };
        let mut recorder = Recorder::new(slot.clone(), &info.name, scope, endpoint, Some(physical), None);
        recorder.entries_read = 300_000;
        let mut work = DirectoryWork {
            compacted: true,
            ..Default::default()
        };
        for i in 0..300_000 {
            work.names.add(format!("obj-{i:06}"));
            work.names.flush_full().await.expect("record real immutable name page");
        }
        work.names.finish();
        [121_034, 121_035].map(|objects| {
            work.names.seek_after(Some(&format!("obj-{:06}", objects - 1)));
            work.compacted_total.objects = objects;
            work.compacted_total.size = objects * 1024;
            recorder.save(&work);
            slot.lock()
                .expect("fixture slot")
                .take()
                .expect("sealed directory checkpoint")
        })
    }
}
