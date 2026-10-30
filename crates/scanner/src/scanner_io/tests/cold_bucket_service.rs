// Copyright 2026 RustFS Team
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

use super::*;
use crate::DataUsageBucketService;
use std::path::Path;

const BUCKETS: usize = 17;
const QUANTA: usize = 4;
const ROUND_BOUND: usize = BUCKETS.div_ceil(QUANTA);
const HOT: &str = "bucket-00";
const FAILING: &str = "bucket-01";
const ROUND_REQUEST_ENV: &str = "RUSTFS_COLD_BUCKET_SERVICE_ROUND";
const EPOCH: u64 = 9;

fn inventory() -> Vec<BucketInfo> {
    (0..BUCKETS)
        .map(|index| BucketInfo {
            name: format!("bucket-{index:02}"),
            ..Default::default()
        })
        .collect()
}

fn restore(persisted: Option<DataUsageBucketServiceLedger>, buckets: &[BucketInfo]) -> DataUsageBucketServiceLedger {
    DataUsageBucketServiceLedger::restore(persisted, EPOCH, buckets.iter().map(|bucket| bucket.name.as_str()), None)
}

/// One scheduler lifetime over the production order and queue accounting.
/// `persisted` is decoded from durable bytes; the result is encoded again.
fn serve_round(persisted: &[u8]) -> (Vec<u8>, Vec<String>) {
    let buckets = inventory();
    let decoded = DataUsageCache::unmarshal(persisted).expect("decode saved order");
    let round = Arc::new(ScannerBucketServiceRound::new(restore(decoded.info.scan_bucket_service, &buckets)));
    let dirty = DirtyUsageBuckets::from([(HOT.to_string(), 1)]);
    let mut order = bucket_usage_scan_order(&buckets, &DataUsageCache::default(), &dirty);
    round.order_buckets(&mut order);
    let served = order
        .iter()
        .take(QUANTA)
        .map(|bucket| bucket.name.clone())
        .collect::<Vec<_>>();
    let mut completed = HashSet::new();
    let mut failed = HashSet::new();
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, Default::default());
    for bucket in &served {
        let mut work = BucketWorkGuard::new(Arc::new(AtomicUsize::new(2)), CancellationToken::new());
        work.claim_service(round.clone(), bucket, ctx.clone(), budget.clone());
        drop(work);
        if bucket == FAILING {
            failed.insert(bucket.clone());
        } else {
            completed.insert(bucket.clone());
        }
    }
    let ledger = round.settle(&completed, &failed, &HashSet::new()).expect("served buckets");
    let mut cache = DataUsageCache::default();
    cache.info.scan_bucket_service = Some(ledger);
    (cache.marshal_msg().expect("encode saved order"), served)
}

fn assert_service_bound(rounds: &[Vec<String>]) {
    for bucket in inventory() {
        let mut waited = 0;
        for (index, served) in rounds.iter().enumerate() {
            waited = if served.contains(&bucket.name) { 0 } else { waited + 1 };
            assert!(waited < ROUND_BOUND, "{} waited {waited} rounds at round {index}", bucket.name);
        }
    }
}

#[test]
fn cold_bucket_service_bound_holds_through_the_saved_codec() {
    let mut saved = DataUsageCache::default().marshal_msg().expect("empty cache");
    let mut rounds = Vec::new();
    for _ in 0..(ROUND_BOUND * 4) {
        let (next, served) = serve_round(&saved);
        saved = next;
        rounds.push(served);
    }
    assert_service_bound(&rounds);
    let ledger = DataUsageCache::unmarshal(&saved)
        .expect("decode final order")
        .info
        .scan_bucket_service
        .expect("saved order");
    let failing = ledger.buckets[FAILING];
    assert!(failing.failure_debt >= 4, "every failed turn must stay visible: {failing:?}");
    assert_eq!(failing.last_certified, None, "failure is not certification");
    assert!(failing.service_quanta >= 4, "a failing bucket still spends real turns");
    assert_eq!(ledger.failure_debt(), (1, Some(FAILING)));
    assert_eq!(ledger.buckets[HOT].failure_debt, 0);
    let hot_quanta = ledger.buckets[HOT].service_quanta;
    assert!(
        ledger
            .buckets
            .values()
            .all(|service| service.service_quanta.abs_diff(hot_quanta) <= 1),
        "a hot bucket must not earn more turns than a cold one: {ledger:?}"
    );
}

/// The driver and each round are separate OS processes, so the only carrier
/// between rounds is the saved cache file.
#[test]
fn cold_bucket_service_bound_holds_across_fresh_os_processes() {
    let temp = tempfile::tempdir().expect("fixture directory");
    let order = temp.path().join("order.bin");
    std::fs::write(&order, DataUsageCache::default().marshal_msg().expect("empty cache")).expect("seed order");
    let test = format!(
        "{}::cold_bucket_service_process_round_worker",
        module_path!().split_once("::").expect("crate path").1
    );
    let binary = std::env::current_exe().expect("test binary");
    let listed = std::process::Command::new(&binary)
        .args([test.as_str(), "--exact", "--list"])
        .output()
        .expect("list worker");
    assert!(
        String::from_utf8_lossy(&listed.stdout).contains(&format!("{test}: test")),
        "the restart driver must select a real worker"
    );
    let mut rounds = Vec::new();
    let mut pids = HashSet::new();
    for round in 0..(ROUND_BOUND * 2) {
        let status = std::process::Command::new(&binary)
            .args([test.as_str(), "--exact", "--test-threads=1"])
            .env(ROUND_REQUEST_ENV, &order)
            .stdout(std::process::Stdio::null())
            .status()
            .expect("start worker round");
        assert!(status.success(), "worker round {round} failed");
        let report = std::fs::read_to_string(order.with_extension("served")).expect("worker report");
        let (pid, served) = report.split_once('\n').expect("worker report shape");
        assert!(pids.insert(pid.to_string()), "round {round} reused a process");
        rounds.push(served.lines().map(str::to_owned).collect::<Vec<_>>());
    }
    assert_service_bound(&rounds);
}

#[test]
fn cold_bucket_service_process_round_worker() {
    let Some(order) = std::env::var_os(ROUND_REQUEST_ENV) else {
        let empty = DataUsageCache::default().marshal_msg().expect("empty cache");
        assert_eq!(serve_round(&empty).1.len(), QUANTA);
        return;
    };
    let order = Path::new(&order);
    let (saved, served) = serve_round(&std::fs::read(order).expect("read saved order"));
    std::fs::write(order, saved).expect("save order");
    std::fs::write(order.with_extension("served"), format!("{}\n{}", std::process::id(), served.join("\n")))
        .expect("write worker report");
}

#[test]
fn cold_bucket_service_new_bucket_joins_behind_existing_waiters() {
    let buckets = inventory();
    let mut ledger = restore(None, &buckets);
    ledger.record_service("bucket-00");
    let with_new = buckets
        .iter()
        .cloned()
        .chain([BucketInfo {
            name: "a-new-bucket".to_string(),
            ..Default::default()
        }])
        .collect::<Vec<_>>();
    let ledger = restore(Some(ledger), &with_new);
    let mut order = with_new;
    order.sort_by(|left, right| left.name.cmp(&right.name));
    ScannerBucketServiceRound::new(ledger.clone()).order_buckets(&mut order);
    assert_eq!(order[0].name, "bucket-01", "a stream of new buckets must not displace waiters");
    assert!(
        ledger.buckets["a-new-bucket"].waiting_since > ledger.buckets["bucket-00"].waiting_since,
        "an arrival queues behind the bucket served last"
    );
}

#[test]
fn cold_bucket_service_scope_change_invalidates_the_saved_order() {
    let buckets = inventory();
    let mut ledger = restore(None, &buckets);
    ledger.record_service(FAILING);
    ledger.record_failure(FAILING);

    let moved = DataUsageBucketServiceLedger::restore(
        Some(ledger.clone()),
        EPOCH + 1,
        buckets.iter().map(|bucket| bucket.name.as_str()),
        None,
    );
    assert_eq!(moved.publication_epoch, EPOCH + 1);
    assert_eq!(moved.buckets[FAILING].failure_debt, 0, "another movement epoch starts a new order");

    let mut corrupt = ledger.clone();
    corrupt.next_sequence = 0;
    let rebuilt = restore(Some(corrupt), &buckets);
    assert_eq!(rebuilt.buckets[FAILING].service_quanta, 0, "an impossible wait position is not trusted");

    let remaining = restore(Some(ledger.clone()), &buckets[..1]);
    assert_eq!(remaining.buckets.len(), 1, "a deleted bucket leaves the order");
}

#[test]
fn cold_bucket_service_recreated_bucket_loses_its_old_standing() {
    let buckets = inventory();
    let first = Uuid::from_u128(1);
    let incarnations = HashMap::from([(FAILING.to_string(), first)]);
    let names = || buckets.iter().map(|bucket| bucket.name.as_str());
    let mut ledger = DataUsageBucketServiceLedger::restore(None, EPOCH, names(), Some(&incarnations));
    ledger.record_failure(FAILING);
    let same = DataUsageBucketServiceLedger::restore(Some(ledger.clone()), EPOCH, names(), Some(&incarnations));
    assert_eq!(same.buckets[FAILING].failure_debt, 1);

    let unknown = DataUsageBucketServiceLedger::restore(Some(ledger.clone()), EPOCH, names(), None);
    assert_eq!(
        unknown.buckets[FAILING].incarnation,
        Some(first),
        "a transient identity gap must not erase debt"
    );

    let recreated = HashMap::from([(FAILING.to_string(), Uuid::from_u128(2))]);
    let replaced = DataUsageBucketServiceLedger::restore(Some(ledger), EPOCH, names(), Some(&recreated));
    assert_eq!(replaced.buckets[FAILING].failure_debt, 0, "a recreated bucket does not inherit debt");
    assert_eq!(replaced.buckets[FAILING].incarnation, Some(Uuid::from_u128(2)));
}

#[test]
fn cold_bucket_service_budget_stop_and_resource_veto_add_no_debt() {
    let buckets = inventory();
    let ledger = restore(None, &buckets);
    let before = ledger.buckets["bucket-02"].waiting_since;
    let round = Arc::new(ScannerBucketServiceRound::new(ledger));
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, Default::default());
    let work = || BucketWorkGuard::new(Arc::new(AtomicUsize::new(2)), CancellationToken::new());

    let mut vetoed = work();
    vetoed.claim_service(round.clone(), "bucket-02", ctx.clone(), budget.clone());
    vetoed.mark_requeued();
    drop(vetoed);

    let mut stopped = work();
    stopped.claim_service(round.clone(), "bucket-03", ctx.clone(), budget.clone());
    budget.cancel_for_runtime();
    drop(stopped);

    let names = |names: &[&str]| names.iter().map(|name| name.to_string()).collect::<HashSet<_>>();
    let ledger = round
        .settle(&HashSet::new(), &names(&["bucket-02", "bucket-03"]), &HashSet::new())
        .expect("one bucket was served");
    assert_eq!(ledger.buckets["bucket-02"].waiting_since, before, "a veto hands the turn back");
    assert_eq!(ledger.buckets["bucket-02"].service_quanta, 0);
    assert_eq!(ledger.buckets["bucket-03"].service_quanta, 1, "the budget was really spent");
    assert_eq!(ledger.failure_debt(), (0, None), "budget-cancelled work is progress-only");
}

#[test]
fn cold_bucket_service_order_is_bounded_keeping_the_newest_positions() {
    let mut ledger = DataUsageBucketServiceLedger {
        publication_epoch: EPOCH,
        next_sequence: 5000,
        ..Default::default()
    };
    for sequence in 0..5000_u64 {
        ledger.buckets.insert(
            format!("bucket-{sequence:06}"),
            DataUsageBucketService {
                waiting_since: sequence,
                ..Default::default()
            },
        );
    }
    let bounded = ledger.bounded();
    assert_eq!(bounded.buckets.len(), 4096);
    assert!(bounded.buckets.contains_key("bucket-004999"));
    assert!(!bounded.buckets.contains_key("bucket-000000"));
    assert_eq!(
        restore(Some(bounded.clone()), &[]).publication_epoch,
        EPOCH,
        "a bounded order must pass its own validation"
    );
}

#[test]
fn cold_bucket_service_order_is_an_optional_map_field_for_old_readers() {
    #[derive(serde::Deserialize)]
    struct LegacyInfo {
        name: String,
        next_cycle: u64,
    }
    #[derive(serde::Deserialize)]
    struct LegacyCache {
        info: LegacyInfo,
        #[serde(rename = "cache")]
        _cache: serde::de::IgnoredAny,
    }
    let mut cache = DataUsageCache::default();
    cache.info.name = DATA_USAGE_ROOT.to_string();
    cache.info.next_cycle = 7;
    let absent = cache.marshal_msg().expect("encode cache");
    let field = b"scan_bucket_service";
    assert!(
        !absent.windows(field.len()).any(|window| window == field),
        "an unused order must not change the encoded cache"
    );

    let buckets = inventory();
    let mut ledger = restore(None, &buckets);
    ledger.record_service(FAILING);
    ledger.record_failure(FAILING);
    cache.info.scan_bucket_service = Some(ledger.clone());
    let encoded = cache.marshal_msg().expect("encode order");
    let legacy: LegacyCache = rmp_serde::from_slice(&encoded).expect("old readers must ignore the order");
    assert_eq!((legacy.info.name.as_str(), legacy.info.next_cycle), (DATA_USAGE_ROOT, 7));
    assert_eq!(
        DataUsageCache::unmarshal(&encoded)
            .expect("decode order")
            .info
            .scan_bucket_service,
        Some(ledger)
    );
    assert!(
        DataUsageCache::unmarshal(&absent)
            .expect("decode a cache from an older writer")
            .info
            .scan_bucket_service
            .is_none()
    );
}
