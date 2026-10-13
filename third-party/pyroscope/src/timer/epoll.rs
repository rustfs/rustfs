use super::TimerSignal;
use crate::{
    utils::{check_err, get_time_range},
    PyroscopeError, Result,
};

use std::sync::{
    mpsc::{channel, Sender},
    Arc, Mutex,
};
use std::{
    thread::{self, JoinHandle},
    time::Duration,
};

const LOG_TAG: &str = "Pyroscope::Timer";

/// A thread that sends a notification every 10th second
///
/// Timer will send an event to attached listeners (mpsc::Sender) every 10th
/// second (...10, ...20, ...)
///
/// The Timer thread will run continously until all Senders are dropped.
/// The Timer thread will be joined when all Senders are dropped.

#[derive(Debug)]
pub struct Timer {
    /// A vector to store listeners
    txs: Arc<Mutex<Vec<Sender<TimerSignal>>>>,

    /// Thread handle
    pub handle: Option<JoinHandle<Result<()>>>,
}

impl Timer {
    /// Initialize Timer and run a thread to send events to attached listeners
    /// `interval` is the cadence; snapshots are aligned to and fired once per
    /// interval window (rounded down to whole seconds)
    pub fn initialize(interval: Duration) -> Result<Self> {
        log::info!(target: LOG_TAG, "Initializing Timer");

        let cycle = Duration::from_secs(interval.as_secs().max(1));

        let txs = Arc::new(Mutex::new(Vec::new()));

        // Add a dummy tx so the below thread does not terminate early
        let (tx, _rx) = channel();
        txs.lock()?.push(tx);

        let timer_fd = Timer::set_timerfd(cycle)?;
        let epoll_fd = Timer::create_epollfd(timer_fd)?;

        let handle = Some({
            let txs = txs.clone();
            thread::spawn(move || {
                loop {
                    // Exit thread if there are no listeners
                    if txs.lock()?.is_empty() {
                        // Close file descriptors
                        unsafe { libc::close(timer_fd) };
                        unsafe { libc::close(epoll_fd) };

                        log::info!(target: LOG_TAG, "Timer thread terminated");

                        return Ok::<_, PyroscopeError>(());
                    }

                    // Fire @ 10th sec
                    let res = Timer::epoll_wait(timer_fd, epoll_fd);
                    if matches!(&res, Err(PyroscopeError::Io(err)) if err.kind() == std::io::ErrorKind::Interrupted)
                    {
                        continue;
                    }
                    res?;

                    // Get the current time range
                    let from = TimerSignal::NextSnapshot(get_time_range(0, cycle)?.from);

                    log::trace!(target: LOG_TAG, "Timer fired @ {from}");

                    // Iterate through Senders
                    txs.lock()?.iter().for_each(|tx| {
                        // Send event to attached Sender
                        match tx.send(from) {
                            Ok(_) => {
                                log::trace!(target: LOG_TAG, "Sent event to listener @ {:?}", &tx)
                            }
                            Err(_e) => {} // There could be a less confusing message, or this
                                          // refactored to avoid a first sender
                                          //log::warn!(
                                          //target: LOG_TAG,
                                          //"Failed to send event to listener @ {:?} - {}",
                                          //&tx,
                                          //e
                                          //),
                        }
                    });
                }
            })
        });

        Ok(Self { handle, txs })
    }

    /// create and set a timer file descriptor
    fn set_timerfd(cycle: Duration) -> Result<libc::c_int> {
        // Set the timer to use the system time.
        let clockid: libc::clockid_t = libc::CLOCK_REALTIME;
        // Non-blocking file descriptor
        let clock_flags: libc::c_int = libc::TFD_NONBLOCK;

        // Create timer fd
        let tfd = timerfd_create(clockid, clock_flags)?;

        // Get the next event time
        let first_fire = get_time_range(0, cycle)?.until;

        // new_value sets the Timer
        let mut new_value = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: cycle.as_secs() as libc::c_long,
                tv_nsec: cycle.subsec_nanos() as libc::c_long,
            },
            it_value: libc::timespec {
                tv_sec: first_fire as libc::c_long,
                tv_nsec: 0,
            },
        };

        // Empty itimerspec object
        let mut old_value = libc::itimerspec {
            it_interval: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
        };

        let set_flags = libc::TFD_TIMER_ABSTIME;

        // Set the timer
        timerfd_settime(tfd, set_flags, &mut new_value, &mut old_value)?;

        // Return file descriptor
        Ok(tfd)
    }

    /// Create a new epoll file descriptor and add the timer to its interests
    fn create_epollfd(timer_fd: libc::c_int) -> Result<libc::c_int> {
        // create a new epoll fd
        let epoll_fd = epoll_create1(0)?;

        // event to pull
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: 1,
        };

        let epoll_flags = libc::EPOLL_CTL_ADD;

        // add event to the epoll
        epoll_ctl(epoll_fd, epoll_flags, timer_fd, &mut event)?;

        // return epoll fd
        Ok(epoll_fd)
    }

    /// Wait for an event on the epoll file descriptor
    fn epoll_wait(timer_fd: libc::c_int, epoll_fd: libc::c_int) -> Result<()> {
        // vector to store events
        let mut events = Vec::with_capacity(1);

        // wait for the timer to fire an event. This is function will block.
        unsafe {
            epoll_wait(epoll_fd, events.as_mut_ptr(), 1, -1)?;
        }

        // read the value from the timerfd. This is required to re-arm the timer.
        let mut buffer: u64 = 0;
        let bufptr: *mut _ = &mut buffer;
        unsafe {
            read(timer_fd, bufptr as *mut libc::c_void, 8)?;
        }

        Ok(())
    }

    /// Attach an mpsc::Sender to Timer
    ///
    /// Timer will dispatch an event with the timestamp of the current instant,
    /// every 10th second to all attached senders
    pub fn attach_listener(&mut self, tx: Sender<TimerSignal>) -> Result<()> {
        // Push Sender to a Vector of Sender(s)
        let txs = Arc::clone(&self.txs);
        txs.lock()?.push(tx);

        Ok(())
    }

    /// Clear the listeners (txs) from Timer. This will shutdown the Timer thread
    pub fn drop_listeners(&mut self) -> Result<()> {
        let txs = Arc::clone(&self.txs);
        txs.lock()?.clear();

        Ok(())
    }
}

// Wrapper for libc functions.
//
// Error wrapper for some libc functions used by the library. This only does
// Error (-1 return) wrapping. Alternatively, the nix crate could be used
// instead of expanding this wrappers (if more functions and types are used
// from libc)

// libc::timerfd wrapper
pub fn timerfd_create(clockid: libc::clockid_t, clock_flags: libc::c_int) -> Result<i32> {
    check_err(unsafe { libc::timerfd_create(clockid, clock_flags) })
}

/// libc::timerfd_settime wrapper
pub fn timerfd_settime(
    timer_fd: i32,
    set_flags: libc::c_int,
    new_value: &mut libc::itimerspec,
    old_value: &mut libc::itimerspec,
) -> Result<()> {
    check_err(unsafe { libc::timerfd_settime(timer_fd, set_flags, new_value, old_value) })?;
    Ok(())
}

/// libc::epoll_create1 wrapper
pub fn epoll_create1(epoll_flags: libc::c_int) -> Result<i32> {
    check_err(unsafe { libc::epoll_create1(epoll_flags) })
}

/// libc::epoll_ctl wrapper
pub fn epoll_ctl(
    epoll_fd: i32,
    epoll_flags: libc::c_int,
    timer_fd: i32,
    event: &mut libc::epoll_event,
) -> Result<()> {
    check_err(unsafe { libc::epoll_ctl(epoll_fd, epoll_flags, timer_fd, event) })?;
    Ok(())
}

/// libc::epoll_wait wrapper
///
/// # Safety
/// This function is a wrapper for libc::epoll_wait.
pub unsafe fn epoll_wait(
    epoll_fd: i32,
    events: *mut libc::epoll_event,
    maxevents: libc::c_int,
    timeout: libc::c_int,
) -> Result<()> {
    check_err(libc::epoll_wait(epoll_fd, events, maxevents, timeout))?;
    Ok(())
}

/// libc::read wrapper
///
/// # Safety
/// This function is a wrapper for libc::read.
pub unsafe fn read(timer_fd: i32, bufptr: *mut libc::c_void, count: libc::size_t) -> Result<()> {
    check_err(libc::read(timer_fd, bufptr, count))?;
    Ok(())
}
