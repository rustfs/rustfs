// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use rustfs_heal::{HealOptions, HealPriority, HealRequest, HealType};
use rustfs_heal_contracts::heal_channel::HealRequestSource;
use std::time::SystemTime;

#[test]
fn existing_heal_request_literal_remains_constructible() {
    let now = SystemTime::now();
    let request = HealRequest {
        id: "request".to_string(),
        heal_type: HealType::ErasureSet {
            set_disk_id: "pool_0_set_0".to_string(),
            buckets: Vec::new(),
        },
        bucket_incarnation_id: None,
        expected_mrf_bucket_incarnation_id: None,
        options: HealOptions::default(),
        priority: HealPriority::Normal,
        source: HealRequestSource::AutoHeal,
        force_start: false,
        retry_attempts: 0,
        heal_endpoints: vec!["drive-a".to_string()],
        created_at: now,
        enqueued_at: now,
    };
    assert_eq!(request.heal_endpoints, ["drive-a"]);
}
