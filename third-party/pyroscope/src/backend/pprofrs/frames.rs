// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::SystemTime;

use smallvec::SmallVec;
use symbolic_demangle::demangle;

use crate::backend::pprofrs::backtrace::{Frame, Trace, TraceImpl};
use crate::backend::pprofrs::{MAX_DEPTH, MAX_THREAD_NAME};

// #[cfg(not(feature = "perfmaps"))]
fn resolve_in_perfmap(_ip: usize) -> Option<Symbol> {
    None
}

#[derive(Clone)]
pub struct UnresolvedFrames {
    pub frames: SmallVec<[<TraceImpl as Trace>::Frame; MAX_DEPTH]>,
    pub thread_name: [u8; MAX_THREAD_NAME],
    pub thread_name_length: usize,
    pub thread_id: u64,
    pub sample_timestamp: SystemTime,
}

impl Default for UnresolvedFrames {
    fn default() -> Self {
        let frames = SmallVec::with_capacity(MAX_DEPTH);
        Self {
            frames,
            thread_name: [0; MAX_THREAD_NAME],
            thread_name_length: 0,
            thread_id: 0,
            sample_timestamp: SystemTime::now(),
        }
    }
}

impl Debug for UnresolvedFrames {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        self.frames.fmt(f)
    }
}

impl UnresolvedFrames {
    pub fn new(
        frames: SmallVec<[<TraceImpl as Trace>::Frame; MAX_DEPTH]>,
        tn: &[u8],
        thread_id: u64,
        sample_timestamp: SystemTime,
    ) -> Self {
        let thread_name_length = tn.len();
        let mut thread_name = [0; MAX_THREAD_NAME];
        thread_name[0..thread_name_length].clone_from_slice(tn);

        Self {
            frames,
            thread_name,
            thread_name_length,
            thread_id,
            sample_timestamp,
        }
    }
}

impl PartialEq for UnresolvedFrames {
    fn eq(&self, other: &Self) -> bool {
        let (frames1, frames2) = (&self.frames, &other.frames);
        if self.thread_id != other.thread_id || frames1.len() != frames2.len() {
            false
        } else {
            Iterator::zip(frames1.iter(), frames2.iter())
                .all(|(s1, s2)| s1.symbol_address() == s2.symbol_address())
        }
    }
}

impl Eq for UnresolvedFrames {}

impl Hash for UnresolvedFrames {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.frames
            .iter()
            .for_each(|frame| frame.symbol_address().hash(state));
        self.thread_id.hash(state);
    }
}

/// Symbol is a representation of a function symbol. It contains name and addr of it. If built with
/// debug message, it can also provide line number and filename. The name in it is not demangled.
#[derive(Debug, Clone)]
pub struct Symbol {
    /// This name is raw name of a symbol (which hasn't been demangled).
    pub name: Option<Vec<u8>>,

    /// Line number of this symbol. If compiled with debug message, you can get it.
    pub lineno: Option<u32>,

    /// Filename of this symbol. If compiled with debug message, you can get it.
    pub filename: Option<PathBuf>,
}

impl Symbol {
    pub fn raw_name(&self) -> &[u8] {
        self.name.as_deref().unwrap_or(b"Unknown")
    }

    pub fn name(&self) -> String {
        demangle(&String::from_utf8_lossy(self.raw_name())).into_owned()
    }
}

unsafe impl Send for Symbol {}

impl<T> From<&T> for Symbol
where
    T: crate::backend::pprofrs::backtrace::Symbol,
{
    fn from(symbol: &T) -> Self {
        Symbol {
            name: symbol.name(),
            lineno: symbol.lineno(),
            filename: symbol.filename(),
        }
    }
}

impl Display for Symbol {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(&self.name())
    }
}

impl PartialEq for Symbol {
    fn eq(&self, other: &Self) -> bool {
        self.raw_name() == other.raw_name()
    }
}

impl Hash for Symbol {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.raw_name().hash(state)
    }
}

/// A representation of a backtrace. `thread_name` and `thread_id` was got from `pthread_getname_np`
/// and `pthread_self`. frames is a vector of symbols.
#[derive(Clone, PartialEq, Hash)]
pub struct Frames {
    pub frames: Vec<Vec<Symbol>>,
    pub thread_name: String,
    pub thread_id: u64,
    pub sample_timestamp: SystemTime,
}

impl From<UnresolvedFrames> for Frames {
    fn from(frames: UnresolvedFrames) -> Self {
        let mut fs = Vec::new();

        let mut frame_iter = frames.frames.iter();

        while let Some(frame) = frame_iter.next() {
            let mut symbols: Vec<Symbol> = Vec::new();

            if let Some(perfmap_symbol) = resolve_in_perfmap(frame.ip()) {
                symbols.push(perfmap_symbol);
            } else {
                frame.resolve_symbol(|symbol| {
                    let symbol = Symbol::from(symbol);
                    symbols.push(symbol);
                });
            }

            if symbols.iter().any(|symbol| {
                // macOS prepends an underscore even with `#[no_mangle]`
                matches!(
                    &*symbol.name(),
                    "perf_signal_handler" | "_perf_signal_handler"
                )
            }) {
                // ignore frame itself and its next one
                frame_iter.next();
                continue;
            }

            if !symbols.is_empty() {
                fs.push(symbols);
            }
        }

        Self {
            frames: fs,
            thread_name: String::from_utf8_lossy(&frames.thread_name[0..frames.thread_name_length])
                .into_owned(),
            thread_id: frames.thread_id,
            sample_timestamp: frames.sample_timestamp,
        }
    }
}

impl Eq for Frames {}

impl Debug for Frames {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        for frame in self.frames.iter() {
            write!(f, "FRAME: ")?;
            for symbol in frame.iter() {
                write!(f, "{symbol} -> ")?;
            }
        }
        write!(f, "THREAD: ")?;
        if !self.thread_name.is_empty() {
            write!(f, "{}", self.thread_name)
        } else {
            write!(f, "ThreadId({})", self.thread_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demangle_rust() {
        let symbol = Symbol {
            name: Some(b"_ZN3foo3barE".to_vec()),
            lineno: None,
            filename: None,
        };

        assert_eq!(&symbol.name(), "foo::bar")
    }
}
