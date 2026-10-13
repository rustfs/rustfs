//! Rust integration for [Pyroscope](https://grafana.com/oss/pyroscope/).
//!
//! # Quick Start
//!
//! ## Add the Pyroscope Library and the pprof-rs backend to Cargo.toml
//!
//! ```toml
//! [dependencies]
//! pyroscope = { version = "2.0.0", features = ["backend-pprof-rs"] }
//! ```
//!
//! ## Configure a Pyroscope Agent
//!
//! ```no_run
//! use pyroscope::pyroscope::PyroscopeAgentBuilder;
//! use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let agent = PyroscopeAgentBuilder::new(
//!     "http://localhost:4040",
//!     "myapp",
//!     100, // sample rate
//!     "pyroscope-rs",
//!     env!("CARGO_PKG_VERSION"),
//!     pprof_backend(PprofConfig::default(), BackendConfig::default()),
//! )
//! .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Start/Stop profiling
//!
//! To start profiling code and sending data.
//!
//! ```ignore
//! let agent_running = agent.start()?;
//! ```
//!
//! To stop profiling code. You can restart the profiling at a later point.
//!
//! ```ignore
//! let agent_ready = agent_running.stop()?;
//! ```
//!
//! Before you drop the variable, make sure to shutdown the agent.
//!
//! ```ignore
//! agent_ready.shutdown();
//! ```

extern crate core;

// Re-exports structs
pub use crate::pyroscope::PyroscopeAgent;
pub use error::{PyroscopeError, Result};

pub mod backend;
pub mod encode;
pub mod error;
pub mod pyroscope;
pub mod session;
pub mod timer;

mod utils;
pub use utils::ThreadId;
pub mod ffikit;
