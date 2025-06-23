use std::sync::atomic::AtomicI64;

use super::targetid::TargetID;

#[derive(Default)]
pub struct TargetList {
    pub current_send_calls: AtomicI64,
    pub total_events: AtomicI64,
    pub events_skipped: AtomicI64,
    pub events_errors_total: AtomicI64,
    //pub targets: HashMap<TargetID, Target>,
    //pub queue:   AsyncEvent,
    //pub targetStats: HashMap<TargetID, TargetStat>,
}

impl TargetList {
    pub fn new() -> TargetList {
        TargetList::default()
    }
}

struct TargetStat {
    current_send_calls: i64,
    total_events: i64,
    failed_events: i64,
}

struct TargetIDResult {
    id: TargetID,
    err: std::io::Error,
}
