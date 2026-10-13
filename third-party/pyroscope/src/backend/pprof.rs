use crate::backend::pprofrs::{ProfilerGuard, ProfilerGuardBuilder};
use crate::backend::{
    Backend, BackendConfig, BackendImpl, BackendUninitialized, Report, ReportBatch, ReportData,
    StackBuffer, StackFrame, StackTrace, ThreadTag, ThreadTagsSet,
};
use crate::error::{PyroscopeError, Result};
use std::{
    collections::HashMap,
    ffi::OsStr,
    ops::Deref,
    sync::{Arc, Mutex},
};

const LOG_TAG: &str = "Pyroscope::Pprofrs";

pub fn pprof_backend(
    config: PprofConfig,
    backend_config: BackendConfig,
) -> BackendImpl<BackendUninitialized> {
    BackendImpl::new(Box::new(Pprof::new(config, backend_config)))
}

#[derive(Debug)]
pub struct PprofConfig {
    pub sample_rate: u32,
}

impl Default for PprofConfig {
    fn default() -> Self {
        PprofConfig { sample_rate: 100 }
    }
}

#[derive(Default)]
pub struct Pprof<'a> {
    buffer: Arc<Mutex<StackBuffer>>,
    config: PprofConfig,
    backend_config: BackendConfig,
    guard: Arc<Mutex<Option<ProfilerGuard<'a>>>>,
    ruleset: ThreadTagsSet,
}

impl std::fmt::Debug for Pprof<'_> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(fmt, "Pprof Backend")
    }
}

impl<'a> Pprof<'a> {
    pub fn new(config: PprofConfig, backend_config: BackendConfig) -> Self {
        Pprof {
            buffer: Arc::new(Mutex::new(StackBuffer::default())),
            config,
            backend_config,
            guard: Arc::new(Mutex::new(None)),
            ruleset: ThreadTagsSet::default(),
        }
    }
}

impl Backend for Pprof<'_> {
    fn shutdown(self: Box<Self>) -> Result<()> {
        log::trace!(target: LOG_TAG, "Shutting down sampler thread");
        Ok(())
    }

    fn initialize(&mut self) -> Result<()> {
        let profiler = ProfilerGuardBuilder::default()
            .frequency(self.config.sample_rate as i32)
            .build()
            .map_err(|e| PyroscopeError::new(e.to_string().as_str()))?;

        *self.guard.lock()? = Some(profiler);

        Ok(())
    }

    fn report(&mut self) -> Result<ReportBatch> {
        self.dump_report()?;

        let buffer = self.buffer.clone();

        let report: StackBuffer = buffer.lock()?.deref().to_owned();

        let reports: Vec<Report> = report.into();

        buffer.lock()?.clear();

        Ok(ReportBatch {
            profile_type: "process_cpu".into(),
            data: ReportData::Reports(reports),
        })
    }

    fn add_tag(&self, tag: ThreadTag) -> Result<()> {
        if self.guard.lock()?.as_ref().is_some() {
            self.dump_report()?;
        }

        self.ruleset.add(tag)?;

        Ok(())
    }

    fn remove_tag(&self, tag: ThreadTag) -> Result<()> {
        if self.guard.lock()?.as_ref().is_some() {
            self.dump_report()?;
        }

        self.ruleset.remove(tag)?;

        Ok(())
    }
}

impl Pprof<'_> {
    /// Workaround for pprof-rs to interrupt the profiler.
    pub fn dump_report(&self) -> Result<()> {
        let report = self
            .guard
            .lock()?
            .as_ref()
            .ok_or_else(|| PyroscopeError::new("pprof-rs: ProfilerGuard report error"))?
            .report()
            .build_and_clear(true)
            .map_err(|e| PyroscopeError::new(e.to_string().as_str()))?;

        let stack_buffer = Into::<StackBuffer>::into(Into::<StackBufferWrapper>::into((
            report,
            &self.backend_config,
        )));

        let data: HashMap<StackTrace, usize> = stack_buffer
            .data
            .iter()
            .map(|(stacktrace, ss)| {
                let stacktrace = stacktrace.to_owned().add_tag_rules(&self.ruleset);
                (stacktrace, ss.to_owned())
            })
            .collect();

        let buffer = self.buffer.clone();

        for (stacktrace, count) in data {
            buffer.lock()?.record_with_count(stacktrace, count)?;
        }

        Ok(())
    }
}

struct StackBufferWrapper(StackBuffer);

impl From<StackBufferWrapper> for StackBuffer {
    fn from(stackbuffer: StackBufferWrapper) -> Self {
        stackbuffer.0
    }
}

impl From<(crate::backend::pprofrs::Report, &BackendConfig)> for StackBufferWrapper {
    fn from(arg: (crate::backend::pprofrs::Report, &BackendConfig)) -> Self {
        let (report, config) = arg;
        let buffer_data: HashMap<StackTrace, usize> = report
            .data
            .iter()
            .map(|(key, value)| {
                (
                    Into::<StackTraceWrapper>::into((key.to_owned(), config)).into(),
                    value.to_owned() as usize,
                )
            })
            .collect();
        StackBufferWrapper(StackBuffer::new(buffer_data))
    }
}

struct StackTraceWrapper(StackTrace);

impl From<StackTraceWrapper> for StackTrace {
    fn from(stack_trace: StackTraceWrapper) -> Self {
        stack_trace.0
    }
}

impl From<(crate::backend::pprofrs::Frames, &BackendConfig)> for StackTraceWrapper {
    fn from(arg: (crate::backend::pprofrs::Frames, &BackendConfig)) -> Self {
        let (frames, config) = arg;
        let thread_id = frames.thread_id as libc::pthread_t;
        StackTraceWrapper(StackTrace::new(
            config,
            None,
            Some(thread_id.into()),
            Some(frames.thread_name),
            frames
                .frames
                .concat()
                .iter()
                .map(|frame| Into::<StackFrameWrapper>::into(frame.to_owned()).into())
                .collect(),
        ))
    }
}

struct StackFrameWrapper(StackFrame);

impl From<StackFrameWrapper> for StackFrame {
    fn from(stack_frame: StackFrameWrapper) -> Self {
        stack_frame.0
    }
}

impl From<crate::backend::pprofrs::Symbol> for StackFrameWrapper {
    fn from(symbol: crate::backend::pprofrs::Symbol) -> Self {
        StackFrameWrapper(StackFrame::new(
            None,
            Some(symbol.name()),
            Some(
                symbol
                    .filename
                    .clone()
                    .unwrap_or_default()
                    .file_name()
                    .unwrap_or_else(|| OsStr::new(""))
                    .to_str()
                    .unwrap_or("")
                    .to_string(),
            ),
            Some(
                symbol
                    .filename
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or("")
                    .to_string(),
            ),
            None,
            symbol.lineno,
        ))
    }
}
