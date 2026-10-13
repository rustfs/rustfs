// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use spin::RwLock;

use crate::backend::pprofrs::frames::Frames;
use crate::backend::pprofrs::profiler::Profiler;

use crate::backend::pprofrs::{Error, Result};

/// The final presentation of a report which is actually an `HashMap` from `Frames` to isize (count).
pub struct Report {
    /// Key is a backtrace captured by profiler and value is count of it.
    pub data: HashMap<Frames, isize>,
}

type FramesPostProcessor = Box<dyn Fn(&mut Frames)>;

/// A builder of `Report` and `UnresolvedReport`. It builds report from a running `Profiler`.
pub struct ReportBuilder<'a> {
    frames_post_processor: Option<FramesPostProcessor>,
    profiler: &'a RwLock<Result<Profiler>>,
}

impl<'a> ReportBuilder<'a> {
    pub(crate) fn new(profiler: &'a RwLock<Result<Profiler>>) -> Self {
        Self {
            frames_post_processor: None,
            profiler,
        }
    }

    /// Build a `Report`. If `clear` is true, atomically clears the
    /// profiler's sample data under the same write lock.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Creating`] if the profiler lock is poisoned.
    /// Returns an I/O error if reading the overflow backing file or clearing the collector fails.
    ///
    /// NOTE: pyroscope patch — added to support periodic report collection
    /// without recreating the ProfilerGuard. See https://github.com/grafana/pyroscope-rs/issues/399
    pub fn build_and_clear(&self, clear: bool) -> Result<Report> {
        let mut hash_map = HashMap::new();

        match self.profiler.write().as_mut() {
            Err(err) => {
                log::error!("Error in creating profiler: {}", err);
                Err(Error::Creating)
            }
            Ok(profiler) => {
                profiler.data.try_iter()?.for_each(|entry| {
                    let count = entry.count;
                    if count > 0 {
                        let mut key = Frames::from(entry.item.clone());
                        if let Some(processor) = &self.frames_post_processor {
                            processor(&mut key);
                        }

                        match hash_map.get_mut(&key) {
                            Some(value) => {
                                *value += count;
                            }
                            None => {
                                match hash_map.insert(key, count) {
                                    None => {}
                                    Some(_) => {
                                        unreachable!();
                                    }
                                };
                            }
                        }
                    }
                });

                if clear {
                    profiler.clear()?;
                }

                Ok(Report { data: hash_map })
            }
        }
    }
}

/// This will generate Report in a human-readable format:
///
/// ```shell
/// FRAME: pprof::profiler::perf_signal_handler::h7b995c4ab2e66493 -> FRAME: Unknown -> FRAME: {func1} ->
/// FRAME: {func2} -> FRAME: {func3} ->  THREAD: {thread_name} {count}
/// ```
impl Debug for Report {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        for (key, val) in self.data.iter() {
            write!(f, "{key:?} {val}")?;
            writeln!(f)?;
        }

        Ok(())
    }
}
