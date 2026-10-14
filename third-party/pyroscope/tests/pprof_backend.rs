#[cfg(feature = "backend-pprof-rs")]
mod tests {
    use pyroscope::backend::{pprof_backend, BackendConfig, PprofConfig};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn next_size(hasher: &mut DefaultHasher) -> usize {
        Instant::now().hash(hasher);
        (hasher.finish() as usize) % (256 * 1024) + 1
    }

    // NOTE: This test is not reliable — it only exercises the initialize/report
    // lifecycle against a live workload and makes no assertions on the output.
    // No failures have been observed so far; kept as a smoke test for the future.
    #[test]
    fn test_pprof_backend_alloc_loop() {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();

        // Start the alloc loop slightly before the profiler initializes
        let alloc_thread = std::thread::spawn(move || {
            let mut hasher = DefaultHasher::new();
            while !stop_thread.load(Ordering::Relaxed) {
                // Vary allocation size between 1 B and 256 KiB
                let size = next_size(&mut hasher);
                let v: Vec<u8> = vec![0u8; size];
                drop(v);
            }
        });

        // Brief pause so the alloc thread is running before profiling starts
        std::thread::sleep(Duration::from_millis(50));

        let mut backend = pprof_backend(PprofConfig::default(), BackendConfig::default())
            .initialize()
            .expect("failed to initialize pprof backend");

        std::thread::sleep(Duration::from_secs(5));

        let reports = backend.report().expect("failed to dump report");
        drop(reports);

        stop.store(true, Ordering::Relaxed);
        alloc_thread.join().expect("alloc thread panicked");
    }
}
