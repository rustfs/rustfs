use super::TimerSignal;
use crate::{
    utils::{check_err, get_time_range},
    Result,
};

use std::sync::{
    mpsc::{channel, Receiver, Sender},
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

#[derive(Debug, Default)]
pub struct Timer {
    /// A vector to store listeners (mpsc::Sender)
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

        // Add Default tx
        let (tx, _rx): (Sender<TimerSignal>, Receiver<TimerSignal>) = channel();
        txs.lock()?.push(tx);

        let kqueue = kqueue()?;

        let handle = Some({
            let txs = txs.clone();
            thread::spawn(move || {
                // Wait for initial expiration
                let initial_event = Timer::register_initial_expiration(kqueue, cycle)?;
                Timer::wait_event(kqueue, [initial_event].as_mut_ptr())?;

                // Register loop event
                let loop_event = Timer::register_loop_expiration(kqueue, cycle)?;

                // Loop every cycle
                loop {
                    // Exit thread if there are no listeners
                    if txs.lock()?.is_empty() {
                        // TODO: should close file descriptors?
                        log::info!(target: LOG_TAG, "Timer thread terminated");
                        return Ok(());
                    }

                    // Get current time
                    let from = TimerSignal::NextSnapshot(get_time_range(0, cycle)?.from);

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

                    // Wait 10s
                    Timer::wait_event(kqueue, [loop_event].as_mut_ptr())?;
                }
            })
        });

        Ok(Self { handle, txs })
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

    /// Wait for the timer event
    fn wait_event(kqueue: i32, events: *mut libc::kevent) -> Result<()> {
        kevent(kqueue, [].as_mut_ptr(), 0, events, 1, std::ptr::null())?;
        Ok(())
    }

    /// Register an initial expiration event
    fn register_initial_expiration(kqueue: i32, cycle: Duration) -> Result<libc::kevent> {
        // Get first event time
        let first_fire = get_time_range(0, cycle)?.until;

        let initial_event = libc::kevent {
            ident: 1,
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
            fflags: libc::NOTE_ABSOLUTE | libc::NOTE_SECONDS,
            data: first_fire as isize,
            udata: std::ptr::null_mut::<libc::c_void>(),
        };

        // add first event
        kevent(
            kqueue,
            [initial_event].as_ptr(),
            1,
            [].as_mut_ptr(),
            0,
            std::ptr::null(),
        )?;

        Ok(initial_event)
    }

    /// Register a loop expiration event
    fn register_loop_expiration(kqueue: i32, duration: Duration) -> Result<libc::kevent> {
        let loop_event = libc::kevent {
            ident: 1,
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ENABLE,
            fflags: 0,
            data: duration.as_millis() as isize,
            udata: std::ptr::null_mut::<libc::c_void>(),
        };

        // add loop event
        kevent(
            kqueue,
            [loop_event].as_ptr(),
            1,
            [].as_mut_ptr(),
            0,
            std::ptr::null(),
        )?;

        Ok(loop_event)
    }
}

/// libc::kqueue wrapper
fn kqueue() -> Result<i32> {
    check_err(unsafe { libc::kqueue() })
}

/// libc::kevent wrapper
fn kevent(
    kqueue: i32,
    change: *const libc::kevent,
    c_count: libc::c_int,
    events: *mut libc::kevent,
    e_count: libc::c_int,
    timeout: *const libc::timespec,
) -> Result<()> {
    check_err(unsafe { libc::kevent(kqueue, change, c_count, events, e_count, timeout) })?;
    Ok(())
}
