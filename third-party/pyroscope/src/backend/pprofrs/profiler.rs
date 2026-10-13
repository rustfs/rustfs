// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryInto;
use std::os::raw::c_int;
use std::time::SystemTime;

use nix::sys::signal;
use once_cell::sync::Lazy;
use smallvec::SmallVec;
use spin::RwLock;

use crate::backend::pprofrs::backtrace::{Trace, TraceImpl};
use crate::backend::pprofrs::collector::Collector;
use crate::backend::pprofrs::error::{Error, Result};
use crate::backend::pprofrs::frames::UnresolvedFrames;
use crate::backend::pprofrs::report::ReportBuilder;
use crate::backend::pprofrs::timer::Timer;
use crate::backend::pprofrs::{MAX_DEPTH, MAX_THREAD_NAME};

pub(crate) static PROFILER: Lazy<RwLock<Result<Profiler>>> =
    Lazy::new(|| RwLock::new(Profiler::new()));

pub struct Profiler {
    pub(crate) data: Collector<UnresolvedFrames>,
    sample_counter: i32,

    running: bool,

    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    blocklist_segments: Vec<(usize, usize)>,
}

#[derive(Clone)]
pub struct ProfilerGuardBuilder {
    frequency: c_int,

    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    blocklist_segments: Vec<(usize, usize)>,
}

impl Default for ProfilerGuardBuilder {
    fn default() -> ProfilerGuardBuilder {
        ProfilerGuardBuilder {
            frequency: 99,

            #[cfg(any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64"
            ))]
            blocklist_segments: Vec::new(),
        }
    }
}

impl ProfilerGuardBuilder {
    pub fn frequency(self, frequency: c_int) -> Self {
        Self { frequency, ..self }
    }

    pub fn build(self) -> Result<ProfilerGuard<'static>> {
        trigger_lazy();

        match PROFILER.write().as_mut() {
            Err(err) => {
                log::error!("Error in creating profiler: {}", err);
                Err(Error::Creating)
            }
            Ok(profiler) => {
                #[cfg(any(
                    target_arch = "x86_64",
                    target_arch = "aarch64",
                    target_arch = "riscv64",
                    target_arch = "loongarch64"
                ))]
                {
                    profiler.blocklist_segments = self.blocklist_segments;
                }

                match profiler.start() {
                    Ok(()) => Ok(ProfilerGuard::<'static> {
                        profiler: &PROFILER,
                        timer: Some(Timer::new(self.frequency)),
                    }),
                    Err(err) => Err(err),
                }
            }
        }
    }
}

/// RAII structure used to stop profiling when dropped. It is the only interface to access profiler.
pub struct ProfilerGuard<'a> {
    profiler: &'a RwLock<Result<Profiler>>,
    timer: Option<Timer>,
}

fn trigger_lazy() {
    let _ = backtrace::Backtrace::new();
    let _profiler = PROFILER.read();
    TraceImpl::init();
}

impl ProfilerGuard<'_> {
    /// Generate a report
    pub fn report(&self) -> ReportBuilder<'_> {
        ReportBuilder::new(self.profiler)
    }
}

impl<'a> Drop for ProfilerGuard<'a> {
    fn drop(&mut self) {
        drop(self.timer.take());

        match self.profiler.write().as_mut() {
            Err(_) => {}
            Ok(profiler) => match profiler.stop() {
                Ok(()) => {}
                Err(err) => log::error!("error while stopping profiler {}", err),
            },
        }
    }
}

fn write_thread_name_fallback(current_thread: libc::pthread_t, name: &mut [libc::c_char]) {
    let mut len = 0;
    let mut base = 1;

    while current_thread as u128 > base && len < MAX_THREAD_NAME {
        base *= 10;
        len += 1;
    }

    let mut index = 0;
    while index < len && base > 1 {
        base /= 10;

        name[index] = match (48 + (current_thread as u128 / base) % 10).try_into() {
            Ok(digit) => digit,
            Err(_) => {
                log::error!("fail to convert thread_id to string");
                0
            }
        };

        index += 1;
    }
}

#[cfg(not(all(any(target_os = "linux", target_os = "macos"), target_env = "gnu")))]
fn write_thread_name(current_thread: libc::pthread_t, name: &mut [libc::c_char]) {
    write_thread_name_fallback(current_thread, name);
}

#[cfg(all(any(target_os = "linux", target_os = "macos"), target_env = "gnu"))]
fn write_thread_name(current_thread: libc::pthread_t, name: &mut [libc::c_char]) {
    let name_ptr = name as *mut [libc::c_char] as *mut libc::c_char;
    let ret = unsafe { libc::pthread_getname_np(current_thread, name_ptr, MAX_THREAD_NAME) };

    if ret != 0 {
        write_thread_name_fallback(current_thread, name);
    }
}

struct ErrnoProtector(libc::c_int);

impl ErrnoProtector {
    fn new() -> Self {
        unsafe {
            #[cfg(target_os = "android")]
            {
                let errno = *libc::__errno();
                Self(errno)
            }
            #[cfg(target_os = "linux")]
            {
                let errno = *libc::__errno_location();
                Self(errno)
            }
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            {
                let errno = *libc::__error();
                Self(errno)
            }
        }
    }
}

impl Drop for ErrnoProtector {
    fn drop(&mut self) {
        unsafe {
            #[cfg(target_os = "android")]
            {
                *libc::__errno() = self.0;
            }
            #[cfg(target_os = "linux")]
            {
                *libc::__errno_location() = self.0;
            }
            #[cfg(any(target_os = "macos", target_os = "freebsd"))]
            {
                *libc::__error() = self.0;
            }
        }
    }
}

#[no_mangle]
#[cfg_attr(
    not(all(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))),
    allow(unused_variables)
)]
#[allow(clippy::unnecessary_cast)]
extern "C" fn perf_signal_handler(
    _signal: c_int,
    _siginfo: *mut libc::siginfo_t,
    ucontext: *mut libc::c_void,
) {
    let _errno = ErrnoProtector::new();

    if let Some(mut guard) = PROFILER.try_write() {
        if let Ok(profiler) = guard.as_mut() {
            #[cfg(any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64"
            ))]
            if !ucontext.is_null() {
                let ucontext: *mut libc::ucontext_t = ucontext as *mut libc::ucontext_t;

                #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
                let addr =
                    unsafe { (*ucontext).uc_mcontext.gregs[libc::REG_RIP as usize] as usize };

                #[cfg(all(target_arch = "x86_64", target_os = "freebsd"))]
                let addr = unsafe { (*ucontext).uc_mcontext.mc_rip as usize };

                #[cfg(all(target_arch = "x86_64", target_os = "macos"))]
                let addr = unsafe {
                    let mcontext = (*ucontext).uc_mcontext;
                    if mcontext.is_null() {
                        0
                    } else {
                        (*mcontext).__ss.__rip as usize
                    }
                };

                #[cfg(all(
                    target_arch = "aarch64",
                    any(target_os = "android", target_os = "linux")
                ))]
                let addr = unsafe { (*ucontext).uc_mcontext.pc as usize };

                #[cfg(all(target_arch = "aarch64", target_os = "freebsd"))]
                let addr = unsafe { (*ucontext).mc_gpregs.gp_elr as usize };

                #[cfg(all(target_arch = "aarch64", target_os = "macos"))]
                let addr = unsafe {
                    let mcontext = (*ucontext).uc_mcontext;
                    if mcontext.is_null() {
                        0
                    } else {
                        (*mcontext).__ss.__pc as usize
                    }
                };

                #[cfg(all(target_arch = "riscv64", target_os = "linux"))]
                let addr = unsafe { (*ucontext).uc_mcontext.__gregs[libc::REG_PC] as usize };

                #[cfg(all(target_arch = "loongarch64", target_os = "linux"))]
                let addr = unsafe { (*ucontext).uc_mcontext.__pc as usize };

                if profiler.is_blocklisted(addr) {
                    return;
                }
            }

            let mut bt: SmallVec<[<TraceImpl as Trace>::Frame; MAX_DEPTH]> =
                SmallVec::with_capacity(MAX_DEPTH);
            let mut index = 0;

            let sample_timestamp: SystemTime = SystemTime::now();
            TraceImpl::trace(ucontext, |frame| {
                if index < MAX_DEPTH {
                    bt.push(frame.clone());
                    index += 1;
                    true
                } else {
                    false
                }
            });

            let current_thread = unsafe { libc::pthread_self() };
            let mut name = [0; MAX_THREAD_NAME];
            let name_ptr = &mut name as *mut [libc::c_char] as *mut libc::c_char;

            write_thread_name(current_thread, &mut name);

            let name = unsafe { std::ffi::CStr::from_ptr(name_ptr) };
            profiler.sample(bt, name.to_bytes(), current_thread as u64, sample_timestamp);
        }
    }
}

impl Profiler {
    fn new() -> Result<Self> {
        Ok(Profiler {
            data: Collector::new()?,
            sample_counter: 0,
            running: false,

            #[cfg(any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64"
            ))]
            blocklist_segments: Vec::new(),
        })
    }

    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    fn is_blocklisted(&self, addr: usize) -> bool {
        for libs in &self.blocklist_segments {
            if addr > libs.0 && addr < libs.1 {
                return true;
            }
        }
        false
    }
}

impl Profiler {
    pub fn start(&mut self) -> Result<()> {
        log::info!("starting cpu profiler");
        if self.running {
            Err(Error::Running)
        } else {
            self.register_signal_handler()?;
            self.running = true;

            Ok(())
        }
    }

    fn init(&mut self) -> Result<()> {
        self.sample_counter = 0;
        self.data = Collector::new()?;
        self.running = false;

        Ok(())
    }

    /// Clear the sample data collector without stopping profiling.
    /// Signal handler and timer remain active — only the accumulated
    /// samples are discarded.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotRunning`] if the profiler is not currently active.
    /// Returns an I/O error if truncating the overflow backing file fails.
    ///
    /// NOTE: pyroscope patch — added to support periodic report collection
    /// without recreating the ProfilerGuard. See https://github.com/grafana/pyroscope-rs/issues/399
    pub fn clear(&mut self) -> Result<()> {
        if self.running {
            self.sample_counter = 0;
            self.data.clear()?;
            Ok(())
        } else {
            Err(Error::NotRunning)
        }
    }

    pub fn stop(&mut self) -> Result<()> {
        log::info!("stopping cpu profiler");
        if self.running {
            self.unregister_signal_handler()?;
            self.init()?;

            Ok(())
        } else {
            Err(Error::NotRunning)
        }
    }

    fn register_signal_handler(&mut self) -> Result<()> {
        let handler = signal::SigHandler::SigAction(perf_signal_handler);
        // SA_RESTART will only restart a syscall when it's safe to do so,
        // e.g. when it's a blocking read(2) or write(2). See man 7 signal.
        let flags = signal::SaFlags::SA_SIGINFO | signal::SaFlags::SA_RESTART;
        let sigaction = signal::SigAction::new(handler, flags, signal::SigSet::empty());
        _ = unsafe { signal::sigaction(signal::SIGPROF, &sigaction) }?;
        Ok(())
    }

    // On macOS 26 ARM64 the kernel passes the handler address directly to
    // _sigtramp even when the disposition is SIG_IGN (value 1). _sigtramp
    // branches to it unconditionally, so address 0x1 is not instruction-aligned
    // → SIGBUS (PC alignment fault). A real function pointer is always a valid
    // aligned address.
    // https://github.com/grafana/pyroscope-pprof-rs/pull/40/changes
    #[cfg(target_os = "macos")]
    fn unregister_signal_handler(&mut self) -> Result<()> {
        extern "C" fn noop(_: c_int, _: *mut libc::siginfo_t, _: *mut libc::c_void) {}
        let noop = signal::SigAction::new(
            signal::SigHandler::SigAction(noop),
            signal::SaFlags::SA_SIGINFO | signal::SaFlags::SA_RESTART,
            signal::SigSet::empty(),
        );
        unsafe { signal::sigaction(signal::SIGPROF, &noop) }?;
        Ok(())
    }

    // Never restore SIG_DFL: SIGPROF's default action terminates the process,
    // so a pending signal delivered between unregister and re-register would be
    // fatal. SIG_IGN safely discards those signals on non-macOS platforms.
    // See https://github.com/tikv/pprof-rs/issues/288
    //     https://github.com/grafana/pprof-rs/pull/8
    #[cfg(not(target_os = "macos"))]
    fn unregister_signal_handler(&mut self) -> Result<()> {
        let ignore = signal::SigAction::new(
            signal::SigHandler::SigIgn,
            signal::SaFlags::empty(),
            signal::SigSet::empty(),
        );
        unsafe { signal::sigaction(signal::SIGPROF, &ignore) }?;
        Ok(())
    }

    // This function has to be AS-safe
    pub fn sample(
        &mut self,
        backtrace: SmallVec<[<TraceImpl as Trace>::Frame; MAX_DEPTH]>,
        thread_name: &[u8],
        thread_id: u64,
        sample_timestamp: SystemTime,
    ) {
        let frames = UnresolvedFrames::new(backtrace, thread_name, thread_id, sample_timestamp);
        self.sample_counter += 1;

        if let Ok(()) = self.data.add(frames, 1) {}
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    struct AllocDetector {
        should_count_alloc: std::sync::atomic::AtomicBool,
        alloc_count: std::sync::atomic::AtomicUsize,
    }

    unsafe impl std::alloc::GlobalAlloc for AllocDetector {
        unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
            if self
                .should_count_alloc
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                self.alloc_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }

            unsafe { std::alloc::System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
            unsafe { std::alloc::System.dealloc(ptr, layout) }
        }
    }
    impl AllocDetector {
        fn enable_count_alloc(&self) {
            self.should_count_alloc
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn disable_count_alloc(&self) {
            self.should_count_alloc
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }

        fn alloc_count(&self) -> usize {
            self.alloc_count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[global_allocator]
    static ALLOC: AllocDetector = AllocDetector {
        should_count_alloc: std::sync::atomic::AtomicBool::new(false),
        alloc_count: std::sync::atomic::AtomicUsize::new(0),
    };

    #[test]
    fn test_no_alloc_during_unwind() {
        // This test cannot run parallelly because it requires the global allocator to
        // record the allocation count.

        trigger_lazy();
        PROFILER.write().as_mut().unwrap().start().unwrap();
        let timer = Timer::new(999);
        let start = std::time::Instant::now();
        ALLOC.enable_count_alloc();

        // alloc something to make sure the ALLOC works fine.
        let _alloc = Box::new(1usize);
        // busy loop for a while to trigger some samples
        while start.elapsed().as_millis() < 500 {
            std::hint::black_box(());
        }
        ALLOC.disable_count_alloc();

        assert_eq!(ALLOC.alloc_count(), 1);

        drop(timer);
        PROFILER.write().as_mut().unwrap().stop().unwrap();
    }
}
