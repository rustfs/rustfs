use crate::backend::pprofrs::addr_validate::Pipes;
use framehop::{
    CacheNative, MustNotAllocateDuringUnwind, UnwindRegsNative, Unwinder, UnwinderNative,
};
use libc::{c_void, ucontext_t};
use once_cell::sync::Lazy;
use spin::RwLock;

mod shlib;

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
fn get_regs_from_context(ucontext: *mut c_void) -> Option<(UnwindRegsNative, u64)> {
    let ucontext: *mut ucontext_t = ucontext as *mut ucontext_t;
    if ucontext.is_null() {
        return None;
    }

    let thread_state = unsafe {
        let mcontext = (*ucontext).uc_mcontext;
        if mcontext.is_null() {
            return None;
        } else {
            (*mcontext).__ss
        }
    };

    Some((
        UnwindRegsNative::new(thread_state.__lr, thread_state.__sp, thread_state.__fp),
        thread_state.__pc,
    ))
}

#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
fn get_regs_from_context(ucontext: *mut c_void) -> Option<(UnwindRegsNative, u64)> {
    let ucontext: *mut ucontext_t = ucontext as *mut ucontext_t;
    if ucontext.is_null() {
        return None;
    }

    let thread_state = unsafe {
        let mcontext = (*ucontext).uc_mcontext;
        if mcontext.is_null() {
            return None;
        } else {
            (*mcontext).__ss
        }
    };

    Some((
        UnwindRegsNative::new(thread_state.__rip, thread_state.__rsp, thread_state.__rbp),
        thread_state.__rip,
    ))
}

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
fn get_regs_from_context(ucontext: *mut c_void) -> Option<(UnwindRegsNative, u64)> {
    let ucontext: *mut ucontext_t = ucontext as *mut ucontext_t;
    if ucontext.is_null() {
        return None;
    }

    let regs = unsafe { &(*ucontext).uc_mcontext.regs };
    let sp = unsafe { (*ucontext).uc_mcontext.sp };
    Some((UnwindRegsNative::new(regs[30], sp, regs[29]), regs[30]))
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn get_regs_from_context(ucontext: *mut c_void) -> Option<(UnwindRegsNative, u64)> {
    let ucontext: *mut ucontext_t = ucontext as *mut ucontext_t;
    if ucontext.is_null() {
        return None;
    }
    let regs = unsafe { &(*ucontext).uc_mcontext.gregs };

    Some((
        UnwindRegsNative::new(
            regs[libc::REG_RIP as usize] as u64,
            regs[libc::REG_RSP as usize] as u64,
            regs[libc::REG_RBP as usize] as u64,
        ),
        regs[libc::REG_RIP as usize] as u64,
    ))
}

struct FramehopUnwinder {
    unwinder: UnwinderNative<Vec<u8>, MustNotAllocateDuringUnwind>,
    cache: CacheNative<MustNotAllocateDuringUnwind>,
    pipes: Pipes,
}

impl FramehopUnwinder {
    pub fn new() -> Self {
        let mut unwinder = UnwinderNative::new();
        for obj in shlib::get_objects() {
            unwinder.add_module(obj.clone());
        }
        let cache = CacheNative::default();
        let pipes = Pipes::default();
        FramehopUnwinder {
            unwinder,
            cache,
            pipes,
        }
    }

    pub fn iter_frames<F: FnMut(&Frame) -> bool>(&mut self, ctx: *mut c_void, mut cb: F) {
        let (regs, pc) = match get_regs_from_context(ctx) {
            Some(fp) => fp,
            None => return,
        };

        let mut closure = |addr| read_stack(&mut self.pipes, addr);
        let mut iter = self
            .unwinder
            .iter_frames(pc, regs, &mut self.cache, &mut closure);
        while let Ok(Some(frame)) = iter.next() {
            if !cb(&Frame {
                ip: frame.address() as usize,
            }) {
                break;
            }
        }
    }
}

fn read_stack(pipes: &mut Pipes, addr: u64) -> Result<u64, ()> {
    let aligned_addr = addr & !0b111;
    if crate::backend::pprofrs::addr_validate::validate(pipes, aligned_addr as _) {
        Ok(unsafe { (aligned_addr as *const u64).read() })
    } else {
        Err(())
    }
}

static UNWINDER: Lazy<RwLock<FramehopUnwinder>> =
    Lazy::new(|| RwLock::new(FramehopUnwinder::new()));
#[derive(Clone, Debug)]
pub struct Frame {
    pub ip: usize,
}

extern "C" {
    fn _Unwind_FindEnclosingFunction(pc: *mut c_void) -> *mut c_void;

}

impl super::Frame for Frame {
    type S = backtrace::Symbol;
    fn ip(&self) -> usize {
        self.ip
    }

    fn symbol_address(&self) -> *mut c_void {
        if cfg!(target_os = "macos") || cfg!(target_os = "ios") {
            self.ip as *mut c_void
        } else {
            unsafe { _Unwind_FindEnclosingFunction(self.ip as *mut c_void) }
        }
    }

    fn resolve_symbol<F: FnMut(&Self::S)>(&self, cb: F) {
        backtrace::resolve(self.ip as *mut c_void, cb);
    }
}

pub struct Trace;

impl super::Trace for Trace {
    type Frame = Frame;

    fn init() {
        let _ = UNWINDER.read();
    }

    fn trace<F: FnMut(&Self::Frame) -> bool>(ctx: *mut c_void, cb: F)
    where
        Self: Sized,
    {
        // For Linux, this `try_write` should always succeed, because `SIGPROF` will never be delivered to
        // another thread while the signal handler is running. However, I'm not sure about other OSes, so
        // we use `try_write` to be safe instead of using `static mut` and `unsafe` directly.
        match UNWINDER.try_write() {
            None => (),
            Some(mut unwinder) => {
                unwinder.iter_frames(ctx, cb);
            }
        }
    }
}
