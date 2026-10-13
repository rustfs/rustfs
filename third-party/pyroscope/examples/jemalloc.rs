//! Jemalloc memory profiling example.
//!
//! This example demonstrates how to use the jemalloc backend for memory profiling.
//! It requires jemalloc to be configured as the global allocator with profiling enabled.
//!
//! # Requirements
//!
//! Add these dependencies to your Cargo.toml:
//!
//! ```toml
//! [dependencies]
//! pyroscope = { version = "2.0.0", features = ["backend-jemalloc"] }
//! tikv-jemallocator = { version = "0.6", features = ["profiling"] }
//! ```
//!
//! # Running
//!
//! ```sh
//! # Enable jemalloc profiling via environment variable
//! _RJEM_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:19 \
//!     cargo run --example jemalloc --features backend-jemalloc
//! ```

use pyroscope::backend::jemalloc::jemalloc_backend;
use pyroscope::pyroscope::PyroscopeAgentBuilder;
use std::time::Duration;

// Configure jemalloc as the global allocator.
// Profiling must also be enabled at runtime via MALLOC_CONF or _RJEM_MALLOC_CONF.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let agent = PyroscopeAgentBuilder::new(
        "http://localhost:4040",
        "example.jemalloc",
        100,
        "pyroscope-rs",
        env!("CARGO_PKG_VERSION"),
        jemalloc_backend(),
    )
    .tags(vec![("env", "dev")])
    .build()?;

    let agent_running = agent.start()?;

    // Simulate heavy allocations for 30 seconds.
    let start = std::time::Instant::now();
    let mut iteration = 0u64;
    while start.elapsed() < Duration::from_secs(30) {
        // Allocate vectors of varying sizes (1KB to 1MB).
        for i in 0..100 {
            let size = 1024 * (1 + (iteration as usize + i) % 1024);
            let v: Vec<u8> = vec![0u8; size];
            std::hint::black_box(&v);
        }
        iteration += 1;
    }
    eprintln!("Completed {iteration} iterations");

    let agent_ready = agent_running.stop()?;
    agent_ready.shutdown();

    Ok(())
}
