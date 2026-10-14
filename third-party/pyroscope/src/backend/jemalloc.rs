use crate::backend::{
    Backend, BackendImpl, BackendUninitialized, ReportBatch, ReportData, ThreadTag,
};
use crate::error::{PyroscopeError, Result};

const LOG_TAG: &str = "Pyroscope::Jemalloc";

/// Create a jemalloc memory profiling backend.
///
/// # Example
///
/// ```no_run
/// use pyroscope::pyroscope::PyroscopeAgentBuilder;
/// use pyroscope::backend::jemalloc::jemalloc_backend;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let agent = PyroscopeAgentBuilder::new(
///     "http://localhost:4040", "my-app", 100,
///     "pyroscope-rs", env!("CARGO_PKG_VERSION"),
///     jemalloc_backend(),
/// )
/// .build()?;
/// agent.start()?;
/// # Ok(())
/// # }
/// ```
pub fn jemalloc_backend() -> BackendImpl<BackendUninitialized> {
    BackendImpl::new(Box::new(Jemalloc))
}

#[derive(Debug)]
struct Jemalloc;

impl Backend for Jemalloc {
    fn initialize(&mut self) -> Result<()> {
        let prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().ok_or_else(|| {
            PyroscopeError::new(
                "jemalloc: PROF_CTL not available. Ensure jemalloc is configured with prof:true",
            )
        })?;
        let guard = prof_ctl.try_lock().map_err(|_| {
            PyroscopeError::new(
                "jemalloc: failed to acquire PROF_CTL lock during initialization. \
                 This is unexpected at startup; ensure no other code holds the lock.",
            )
        })?;
        if !guard.activated() {
            return Err(PyroscopeError::new(
                "jemalloc: profiling is not activated. Ensure malloc_conf includes prof:true,prof_active:true",
            ));
        }

        log::info!(target: LOG_TAG, "Jemalloc profiling backend initialized");

        Ok(())
    }

    fn shutdown(self: Box<Self>) -> Result<()> {
        log::trace!(target: LOG_TAG, "Shutting down jemalloc backend");
        Ok(())
    }

    fn report(&mut self) -> Result<ReportBatch> {
        let prof_ctl = jemalloc_pprof::PROF_CTL
            .as_ref()
            .ok_or_else(|| PyroscopeError::new("jemalloc: PROF_CTL not available"))?;
        let mut guard = prof_ctl.try_lock().map_err(|_| {
            PyroscopeError::new("jemalloc: failed to acquire PROF_CTL lock for report")
        })?;
        let pprof_data = guard
            .dump_pprof()
            .map_err(|e| PyroscopeError::new(&format!("jemalloc: dump_pprof failed: {e}")))?;

        Ok(ReportBatch {
            profile_type: "memory".into(),
            data: ReportData::RawPprof(pprof_data),
        })
    }

    fn add_tag(&self, _tag: ThreadTag) -> Result<()> {
        Ok(())
    }

    fn remove_tag(&self, _tag: ThreadTag) -> Result<()> {
        Ok(())
    }
}
