use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustfs_notify::rules::RulesMap;
use rustfs_targets::arn::TargetID;
use starshard::{AsyncShardedHashMap, DEFAULT_SHARDS, SnapshotMode};
use tokio::runtime::Runtime;

fn build_rule_map(target: &TargetID) -> RulesMap {
    let mut rules_map = RulesMap::new();
    rules_map.add_rule_config(&[rustfs_s3_common::EventName::ObjectCreatedPut], "*".to_string(), target.clone());
    rules_map
}

async fn build_map(mode: SnapshotMode, bucket_count: usize) -> AsyncShardedHashMap<String, RulesMap, rustc_hash::FxBuildHasher> {
    let map = AsyncShardedHashMap::with_snapshot_mode(DEFAULT_SHARDS, mode);
    let target = TargetID::new("bench-target".to_string(), "webhook".to_string());

    for i in 0..bucket_count {
        let bucket = format!("bucket-{i}");
        map.insert(bucket, build_rule_map(&target)).await;
    }
    map
}

async fn scan_target_bound(map: &AsyncShardedHashMap<String, RulesMap, rustc_hash::FxBuildHasher>, target_id: &TargetID) -> bool {
    let items = map.iter().await;
    for (_bucket, rules_map) in items {
        if rules_map.contains_target_id(target_id) {
            return true;
        }
    }
    false
}

fn bench_snapshot_mode_scan(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("notify_rule_engine_scan_snapshot_mode");
    // Simulate medium-large config sets where full-map scan cost matters.
    let bucket_sizes = [1_000usize, 10_000usize];

    for bucket_count in bucket_sizes {
        for mode in [SnapshotMode::Clone, SnapshotMode::Cached] {
            let mode_name = match mode {
                SnapshotMode::Clone => "clone",
                SnapshotMode::Cached => "cached",
                SnapshotMode::Cow => "cow",
            };

            let map = rt.block_on(build_map(mode, bucket_count));
            let miss_target = TargetID::new("missing-target".to_string(), "webhook".to_string());

            group.throughput(Throughput::Elements(bucket_count as u64));
            group.bench_with_input(BenchmarkId::new(mode_name, bucket_count), &bucket_count, |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let _ = scan_target_bound(&map, &miss_target).await;
                    });
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_snapshot_mode_scan);
criterion_main!(benches);
