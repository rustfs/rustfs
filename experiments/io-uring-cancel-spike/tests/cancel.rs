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

//! Cancel-safety acceptance tests for backlog#894 Spike 0.
//!
//! In a restricted environment (Docker default seccomp, gVisor) the probe
//! fails with an expected-restriction errno and every test degrades to a
//! skip — the same contract P2's production probe must honor. Run under
//! `--security-opt seccomp=unconfined` (see run-docker.sh) to exercise the
//! real io_uring paths.
#![cfg(target_os = "linux")]

use std::fs::File;
use std::io::Write;
use std::os::fd::FromRawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use io_uring_cancel_spike::UringDriver;

fn driver_or_skip(name: &str) -> Option<UringDriver> {
    match UringDriver::probe_and_start(64) {
        Ok(d) => Some(d),
        Err(e) => {
            assert!(
                e.is_expected_restriction(),
                "probe failed OUTSIDE the expected restriction errno class \
                 (EACCES/EPERM/ENOSYS/EINVAL/EOPNOTSUPP): {e:?}"
            );
            eprintln!("SKIP {name}: restricted environment, graceful degradation path taken ({e:?})");
            None
        }
    }
}

/// Deterministic pseudo-random content so reads are verifiable.
fn make_content(len: usize) -> Vec<u8> {
    let mut state: u64 = 0x2545F4914F6CDD1D;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn temp_file_with(content: &[u8], tag: &str) -> (std::path::PathBuf, Arc<File>) {
    let path = std::env::temp_dir().join(format!("uring-spike-{tag}-{}", std::process::id()));
    std::fs::write(&path, content).expect("write temp file");
    let file = Arc::new(File::open(&path).expect("open temp file"));
    (path, file)
}

/// An OS pipe whose read side never completes until we write — the only
/// deterministic way to hold an op in flight across a future drop.
fn os_pipe() -> (Arc<File>, File) {
    let mut fds = [0i32; 2];
    // SAFETY: fds is a valid out-array; on success both fds are owned here
    // and immediately wrapped in File which takes over closing them.
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    assert_eq!(rc, 0, "pipe(2) failed");
    let read = unsafe { File::from_raw_fd(fds[0]) };
    let write = unsafe { File::from_raw_fd(fds[1]) };
    (Arc::new(read), write)
}

async fn wait_until(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cond()
}

/// Baseline: completed reads return exactly what pread would.
#[tokio::test(flavor = "multi_thread")]
async fn read_matches_std() {
    let Some(driver) = driver_or_skip("read_matches_std") else {
        return;
    };
    const LEN: usize = 8 << 20;
    let content = make_content(LEN);
    let (path, file) = temp_file_with(&content, "correctness");

    for i in 0..64usize {
        let offset = (i * 131_071) % (LEN - 70_000);
        let len = 1 + (i * 8_191) % 65_536;
        let got = driver
            .read_at(Arc::clone(&file), offset as u64, len)
            .await
            .expect("read failed");
        assert_eq!(got, &content[offset..offset + len], "mismatch at offset {offset} len {len}");
    }

    let snap = driver.shutdown();
    assert_eq!(snap.delivered, 64);
    assert_eq!(snap.orphan_reclaimed, 0);
    let _ = std::fs::remove_file(path);
}

/// THE core spike assertion: drop the future while the op is provably still
/// in flight (blocked pipe read, no cancel submitted) and verify the buffer
/// stays owned by the driver until the CQE finally arrives.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_future_buffer_lives_until_cqe() {
    let Some(driver) = driver_or_skip("dropped_future_buffer_lives_until_cqe") else {
        return;
    };
    let (pipe_read, mut pipe_write) = os_pipe();

    let handle = driver.read_current(Arc::clone(&pipe_read), 4096).without_cancel_on_drop();
    assert!(
        wait_until(Duration::from_secs(2), || driver.stats().in_flight == 1).await,
        "read never reached in-flight state"
    );

    drop(handle);

    // No cancel was submitted and the pipe is empty: the op MUST stay in
    // flight and the buffer MUST NOT be reclaimed, no matter how long the
    // future has been gone.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let snap = driver.stats();
    assert_eq!(snap.in_flight, 1, "op vanished without a CQE");
    assert_eq!(snap.orphan_reclaimed, 0, "buffer reclaimed before CQE — UAF window!");

    // Now let the kernel complete the read; the CQE both writes into the
    // driver-owned buffer (safely) and triggers reclamation.
    pipe_write.write_all(&[0xAB; 512]).expect("pipe write");
    assert!(
        wait_until(Duration::from_secs(2), || {
            let s = driver.stats();
            s.in_flight == 0 && s.orphan_reclaimed == 1
        })
        .await,
        "orphaned op was not reclaimed at CQE: {:?}",
        driver.stats()
    );

    driver.shutdown();
}

/// Default drop path: ASYNC_CANCEL accelerates the CQE so the orphaned
/// buffer is reclaimed promptly without any data ever arriving.
#[tokio::test(flavor = "multi_thread")]
async fn async_cancel_accelerates_reclaim() {
    let Some(driver) = driver_or_skip("async_cancel_accelerates_reclaim") else {
        return;
    };
    let (pipe_read, pipe_write) = os_pipe();

    let handle = driver.read_current(Arc::clone(&pipe_read), 4096);
    assert!(
        wait_until(Duration::from_secs(2), || driver.stats().in_flight == 1).await,
        "read never reached in-flight state"
    );

    drop(handle); // submits IORING_OP_ASYNC_CANCEL

    assert!(
        wait_until(Duration::from_secs(2), || {
            let s = driver.stats();
            s.in_flight == 0 && s.orphan_reclaimed == 1
        })
        .await,
        "cancel did not reclaim the orphan: {:?}",
        driver.stats()
    );

    drop(pipe_write);
    driver.shutdown();
}

/// Volume test modeling the EC quorum pattern: many concurrent reads, half
/// the futures dropped immediately. Every op must be accounted for as either
/// delivered or orphan-reclaimed, and survivors must return correct bytes.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_stress_accounts_for_every_buffer() {
    let Some(driver) = driver_or_skip("cancel_stress_accounts_for_every_buffer") else {
        return;
    };
    const LEN: usize = 8 << 20;
    const OPS: usize = 256;
    const READ_LEN: usize = 64 << 10;
    let content = make_content(LEN);
    let (path, file) = temp_file_with(&content, "stress");

    let mut kept = Vec::new();
    for i in 0..OPS {
        let offset = (i * 97_611) % (LEN - READ_LEN);
        let handle = driver.read_at(Arc::clone(&file), offset as u64, READ_LEN);
        if i % 2 == 0 {
            drop(handle); // dropped mid-flight or post-completion — both must be safe
        } else {
            kept.push((offset, handle));
        }
    }

    for (offset, handle) in kept {
        let got = handle.await.expect("kept read failed");
        assert_eq!(got, &content[offset..offset + READ_LEN], "mismatch at offset {offset}");
    }

    let snap = driver.shutdown();
    assert_eq!(snap.submitted, OPS as u64);
    assert_eq!(snap.delivered, (OPS / 2) as u64);
    assert_eq!(
        snap.delivered + snap.orphan_reclaimed,
        OPS as u64,
        "some buffers are unaccounted for: {snap:?}"
    );
    let _ = std::fs::remove_file(path);
}

/// Shutdown with ops still blocked in flight must cancel + drain them before
/// the driver thread exits (and the ring is unmapped).
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_in_flight_ops() {
    let Some(driver) = driver_or_skip("shutdown_drains_in_flight_ops") else {
        return;
    };
    let (pipe_read, pipe_write) = os_pipe();

    let h1 = driver.read_current(Arc::clone(&pipe_read), 1024);
    let h2 = driver.read_current(Arc::clone(&pipe_read), 1024);
    assert!(
        wait_until(Duration::from_secs(2), || driver.stats().in_flight == 2).await,
        "reads never reached in-flight state"
    );

    // shutdown() cancels both, drains to in_flight == 0 (asserted inside),
    // and joins the thread. The held futures then resolve with ECANCELED.
    let snap = driver.shutdown();
    assert_eq!(snap.in_flight, 0);
    assert_eq!(snap.delivered + snap.orphan_reclaimed, snap.submitted);

    for h in [h1, h2] {
        let err = h.await.expect_err("blocked pipe read cannot have succeeded");
        assert_eq!(err.raw_os_error(), Some(libc::ECANCELED), "unexpected error: {err:?}");
    }

    drop(pipe_write);
}
