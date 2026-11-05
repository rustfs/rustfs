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

use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::Duration;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use rustfs_io_core::{BytesPool, BytesPoolConfig};

const WAIT: Duration = Duration::from_secs(5);
const GATE_WAIT: Duration = Duration::from_secs(20);

struct AcquisitionGate {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl Recorder for AcquisitionGate {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        if key.name() == "rustfs_bytes_pool_acquisitions_total" {
            self.entered
                .send(())
                .expect("notify test that acquisition reached the recorder");
            self.release
                .lock()
                .expect("lock acquisition gate")
                .recv_timeout(GATE_WAIT)
                .expect("release the acquisition recorder gate");
        }
        Counter::noop()
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

#[test]
fn returned_buffer_is_counted_while_an_acquisition_recorder_is_busy() {
    let pool = BytesPool::with_config(BytesPoolConfig {
        small_max: 2,
        ..Default::default()
    });
    let held = pool.try_acquire_buffer(8 * 1024).expect("acquire the buffer to return");
    assert_eq!(pool.available_buffers(), 0);

    // This integration test is a dedicated test process: the recorder is local
    // to the worker, while the metrics enablement flag is process-wide.
    rustfs_io_metrics::set_metrics_enabled(true);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker_pool = pool.clone();
    let acquirer = thread::spawn(move || {
        let gate = AcquisitionGate {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        };
        metrics::with_local_recorder(&gate, || {
            worker_pool
                .try_acquire_buffer(8 * 1024)
                .expect("acquire a second same-tier buffer")
        })
    });

    let entered = entered_rx.recv_timeout(WAIT);
    if entered.is_err() {
        let _ = release_tx.send(());
        let _ = acquirer.join();
        panic!("acquisition did not reach the recorder gate: {entered:?}");
    }

    let (dropped_tx, dropped_rx) = mpsc::channel();
    let returner = thread::spawn(move || {
        drop(held);
        let _ = dropped_tx.send(());
    });
    let returned_while_blocked = dropped_rx.recv_timeout(WAIT);

    // Always release before joining so a blocking Drop cannot deadlock the test.
    let _ = release_tx.send(());
    let returned = returner.join();
    let acquired = acquirer.join();
    rustfs_io_metrics::set_metrics_enabled(false);

    assert!(returned_while_blocked.is_ok(), "returning a buffer must not wait on the recorder");
    returned.expect("return worker should finish");
    let acquired = acquired.expect("acquisition worker should finish");
    let after_return = pool.available_buffers();
    let reused = pool.try_acquire_buffer(8 * 1024).expect("reuse the returned buffer");
    let after_reuse = pool.available_buffers();
    drop(reused);
    drop(acquired);
    let after_both_return = pool.available_buffers();
    assert_eq!(
        (after_return, after_reuse, after_both_return),
        (1, 0, 2),
        "returned buffers must be counted, and reuse must not underflow the available gauge"
    );
}
