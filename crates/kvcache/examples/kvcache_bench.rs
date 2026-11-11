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

//! Store-core benchmark on local drives.
//!
//! cargo run --release -p rustfs-kvcache --example kvcache_bench -- \
//!     --dirs /mnt/d1,/mnt/d2 --value-bytes 901120 --count 20000 --threads 16 [--direct] [--segment-mib 1024]
//!
//! Phases: write (threads spread over drives), seal, random reads (checksum
//! verified), index lookups, prefix queries, reopen (index rebuild), and
//! dropping every sealed segment (eviction). Each phase prints one line.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rustfs_kvcache::{KvStore, PutOutcome, StoreOptions};

struct Args {
    dirs: Vec<PathBuf>,
    value_bytes: usize,
    count: u64,
    threads: u64,
    direct: bool,
    segment_bytes: u64,
    chain: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        dirs: Vec::new(),
        value_bytes: 901_120,
        count: 10_000,
        threads: 16,
        direct: false,
        segment_bytes: 1 << 30,
        chain: 1_000,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = || iter.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--dirs" => args.dirs = value().split(',').map(PathBuf::from).collect(),
            "--value-bytes" => args.value_bytes = value().parse().expect("--value-bytes"),
            "--count" => args.count = value().parse().expect("--count"),
            "--threads" => args.threads = value().parse().expect("--threads"),
            "--segment-mib" => args.segment_bytes = value().parse::<u64>().expect("--segment-mib") << 20,
            "--chain" => args.chain = value().parse().expect("--chain"),
            "--direct" => args.direct = true,
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(!args.dirs.is_empty(), "--dirs is required");
    args
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn key(i: u64) -> String {
    format!(
        "hicache/kimi-k3/{:064x}",
        (i as u128).wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c834)
    )
}

fn report(phase: &str, items: u64, bytes: u64, started: Instant) {
    let secs = started.elapsed().as_secs_f64();
    println!(
        "{phase:<10} items={items:<10} secs={secs:>8.3} items/s={:>12.0} GB/s={:>7.2}",
        items as f64 / secs,
        bytes as f64 / secs / 1e9
    );
}

fn run_threads(threads: u64, work: impl Fn(u64) + Sync) {
    std::thread::scope(|scope| {
        let work = &work;
        let mut handles = Vec::new();
        for thread in 0..threads {
            handles.push(scope.spawn(move || work(thread)));
        }
        for handle in handles {
            handle.join().expect("worker panicked");
        }
    });
}

fn main() {
    let args = parse_args();
    let options = StoreOptions {
        segment_bytes: args.segment_bytes,
        direct_io: args.direct,
        sync_on_seal: false,
    };
    println!(
        "dirs={:?} value_bytes={} count={} threads={} direct={} segment_mib={}",
        args.dirs,
        args.value_bytes,
        args.count,
        args.threads,
        args.direct,
        args.segment_bytes >> 20
    );
    let (store, _) = KvStore::open(options, args.dirs.clone(), now_ms()).expect("open");
    let store = Arc::new(store);
    let drives = store.drive_count() as u64;
    store.replenish_spares(2).expect("spares");

    // Write: each thread owns keys i ≡ thread (mod threads) and one drive.
    let value: Vec<u8> = (0..args.value_bytes).map(|i| (i % 251) as u8).collect();
    let written = AtomicU64::new(0);
    let started = Instant::now();
    run_threads(args.threads, |thread| {
        let drive = (thread % drives) as u16;
        let mut i = thread;
        while i < args.count {
            if store.put(drive, &key(i), &value, now_ms()).expect("put") == PutOutcome::Written {
                written.fetch_add(1, Ordering::Relaxed);
            }
            i += args.threads;
        }
    });
    let written = written.load(Ordering::Relaxed);
    report("write", written, written * args.value_bytes as u64, started);

    let started = Instant::now();
    store.seal_open_segments(now_ms()).expect("seal");
    report("seal", drives, 0, started);

    // Random reads, checksums verified by the store.
    let reads = args.count.min(20_000);
    let started = Instant::now();
    run_threads(args.threads, |thread| {
        let mut state = thread.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1;
        let mut i = thread;
        while i < reads {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let value = store.get(&key(state % args.count)).expect("get").expect("stored key");
            assert_eq!(value.len(), args.value_bytes);
            i += args.threads;
        }
    });
    report("read", reads, reads * args.value_bytes as u64, started);

    // Index lookups (no I/O), single thread and all threads.
    let lookups = 2_000_000u64;
    let keys: Vec<String> = (0..args.count.min(100_000)).map(key).collect();
    let started = Instant::now();
    let mut hits = 0u64;
    for i in 0..lookups {
        hits += u64::from(store.contains(&keys[(i as usize) % keys.len()]));
    }
    assert_eq!(hits, lookups);
    report("lookup1", lookups, 0, started);
    let started = Instant::now();
    run_threads(args.threads, |thread| {
        let mut i = thread;
        while i < lookups {
            assert!(store.head(&keys[(i as usize) % keys.len()]).is_some());
            i += args.threads;
        }
    });
    report("lookupN", lookups, 0, started);

    // Prefix queries: chains of stored keys ending in a miss.
    let chain: Vec<String> = (0..args.chain as u64)
        .map(|i| key(i % args.count))
        .chain(std::iter::once("missing".to_owned()))
        .collect();
    let queries = 2_000u64;
    let started = Instant::now();
    for _ in 0..queries {
        assert_eq!(store.prefix_query(chain.iter().map(String::as_str)), args.chain);
    }
    report("prefix", queries, 0, started);
    println!(
        "prefix     keys_per_query={} keys/s={:>12.0}",
        args.chain,
        (queries as usize * args.chain) as f64 / started.elapsed().as_secs_f64()
    );

    // Reopen: rebuild the index from footers.
    if Arc::try_unwrap(store).is_err() {
        panic!("no other store references");
    }
    let started = Instant::now();
    let (store, recovery) = KvStore::open(options, args.dirs.clone(), now_ms()).expect("reopen");
    report("rebuild", recovery.entries as u64, 0, started);
    println!(
        "rebuild    segments={} entries={} resealed={} empty_removed={}",
        recovery.segments, recovery.entries, recovery.resealed, recovery.empty_removed
    );

    // Evict everything: drop every sealed segment.
    let started = Instant::now();
    let mut dropped = 0u64;
    let mut segments = 0u64;
    for drive in 0..drives as u16 {
        for segment in store.segments(drive) {
            if segment.sealed {
                dropped += store.drop_segment(drive, segment.id).expect("drop") as u64;
                segments += 1;
            }
        }
    }
    report("drop", segments, segments * args.segment_bytes, started);
    println!("drop       keys_removed={dropped} remaining={}", store.len());
}
