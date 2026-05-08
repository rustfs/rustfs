// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Per-session liveness watchdog.
//!
//! Detects sessions that are silent at the SFTP handler layer while
//! the underlying TCP connection is in CLOSE_WAIT, and cancels them
//! so the server does not accumulate orphaned per-session resources
//! (handle table entries, in-flight multipart uploads, read caches).
//!
//! The watchdog runs one tokio task per session. Every
//! WEDGE_WATCHDOG_TICK_SECS it inspects the session's last-activity
//! stamp and the kernel TCP state for the connection. A wedged
//! session shows two coincident signals: silence past
//! WEDGE_FAST_KILL_SILENCE_SECS, and a TCP state of CLOSE_WAIT
//! (peer FIN'd, application has not closed). A healthy idle session
//! shows ESTABLISHED. Two consecutive positive ticks are required
//! before the watchdog cancels.
//!
//! The TCP-state probe lives in lifecycle::probe_tcp_state and reads
//! /proc/net/tcp[6] to look up the row matching the session's local
//! and peer addresses. CLOSE_WAIT is unambiguous, so a slow S3
//! backend operation that pipelines into a still-ESTABLISHED socket
//! cannot be misdiagnosed as a wedge.
//!
//! Platform-conditional detection latency. On Linux the procfs probe
//! gives a fast-kill window of WEDGE_FAST_KILL_SILENCE_SECS plus one
//! tick (approximately 45 s) from the moment a session enters
//! CLOSE_WAIT. On macOS, Windows, and other non-Linux targets the
//! /proc/net/tcp files are unavailable, the read returns Err, the
//! probe returns None, and the watchdog falls back to
//! WEDGE_FALLBACK_KILL_SILENCE_SECS (approximately 30 minutes).
//! Server-side resource accumulation is bounded in both cases. The
//! recommended deployment platform is Linux.
//!
//! On cancel the watchdog calls shutdown(Both) on the duplicated
//! socket so russh's inner select unwedges via EOF propagation,
//! then signals the shared CancellationToken so the outer session
//! task drops the RunningSession.

use super::constants::limits::{WEDGE_FALLBACK_KILL_SILENCE_SECS, WEDGE_FAST_KILL_SILENCE_SECS, WEDGE_WATCHDOG_TICK_SECS};
use super::lifecycle::{SessionDiag, TcpState, probe_tcp_state};
use socket2::Socket;
use std::net::Shutdown;
#[cfg(unix)]
use std::os::fd::AsFd;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

/// Reason a watchdog cancelled its session. Surfaced in the warn log
/// the watchdog emits at cancel time so operators can correlate the
/// cancel with the upstream client behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WedgeReason {
    /// Two consecutive ticks observed silence past the fast threshold
    /// AND a TCP state of CLOSE_WAIT (peer FIN'd, application has not
    /// drained the SSH stream).
    TcpStateCloseWaitConfirmed,
    /// Silence past WEDGE_FALLBACK_KILL_SILENCE_SECS regardless of
    /// the TCP_STATE probe result. Backstop for the case where the
    /// probe itself fails (closed dup, missing /proc, kernel without
    /// procfs entries) or where the wedge surfaces in a state other
    /// than CLOSE_WAIT.
    FallbackSilence,
}

impl WedgeReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::TcpStateCloseWaitConfirmed => "tcp_state_close_wait_confirmed",
            Self::FallbackSilence => "fallback_silence",
        }
    }
}

/// Duplicate the TcpStream's underlying socket via the safe AsFd path
/// and wrap the result in a socket2::Socket. The dup exists solely so
/// the watchdog can call shutdown(Both) on the wedged session without
/// racing russh for the original fd. Returns None when the dup fails.
/// Callers should treat None as "no watchdog this session, accept-loop
/// continues".
#[cfg(unix)]
pub(super) fn dup_socket(stream: &TcpStream) -> Option<Socket> {
    let cloned = stream.as_fd().try_clone_to_owned().ok()?;
    Some(Socket::from(cloned))
}

/// Non-Unix stub: AsFd on TcpStream is Unix-only. Returns None so the
/// caller falls back to WEDGE_FALLBACK_KILL_SILENCE_SECS.
#[cfg(not(unix))]
pub(super) fn dup_socket(_stream: &TcpStream) -> Option<Socket> {
    None
}

/// Spawn a per-session watchdog tick task.
///
/// The task owns the duplicated socket (closed on task end via
/// Socket::Drop) and a clone of the session's CancellationToken.
/// The task exits when it cancels the session itself or when the
/// outer session task cancels the token after a clean session end.
pub(super) fn spawn_for_session(session_diag: Arc<SessionDiag>, socket: Socket, cancel_token: CancellationToken) {
    tokio::spawn(async move {
        let session_id = session_diag.session_id;
        let local = session_diag.local;
        let peer = session_diag.peer;
        let mut tick = tokio::time::interval(Duration::from_secs(WEDGE_WATCHDOG_TICK_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick fires immediately; skip it so the watchdog never
        // makes a decision before one full silence window has elapsed.
        tick.tick().await;
        let mut wedge_suspected = false;
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                _ = tick.tick() => {
                    let silence_secs = silence_secs(&session_diag);
                    let probe = probe_tcp_state(local, peer);
                    let outcome = evaluate(silence_secs, probe, wedge_suspected);
                    match outcome {
                        Decision::Quiet => {
                            wedge_suspected = false;
                        }
                        Decision::SuspectedFirstTick => {
                            wedge_suspected = true;
                        }
                        Decision::Cancel(reason) => {
                            tracing::warn!(
                                target: "rustfs_protocols::sftp::watchdog",
                                session_id,
                                peer = %peer,
                                silence_secs,
                                reason = reason.as_str(),
                                "wedge watchdog cancelling session: russh select! parked outside its arms",
                            );
                            cancel_token.cancel();
                            break;
                        }
                    }
                }
            }
        }
        // Shut down the duplicated socket on every exit path. The
        // cancellation could come from this watchdog's own kill
        // decision, from the session task after a clean session end,
        // or from the listener-wide shutdown cascade. In the wedge
        // and shutdown-cascade cases the russh inner task is parked
        // at chan.send(...).await on a backpressured mpsc and only
        // unblocks when its read or write socket fails. shutdown
        // here makes the next I/O on the original fd return EOF,
        // which propagates through russh-sftp and drops the mpsc
        // receiver. In the clean-end case russh has already returned
        // and dropped its half of the fd; this call sends a final
        // FIN on the still-open dup, which the peer's stack
        // tolerates.
        let _ = socket.shutdown(Shutdown::Both);
    });
}

#[derive(Debug, PartialEq, Eq)]
enum Decision {
    /// No wedge signal this tick; reset any suspected state.
    Quiet,
    /// First tick to observe silence past the fast threshold AND a
    /// non-healthy probe result. Hold suspected state for one more
    /// tick before deciding.
    SuspectedFirstTick,
    /// Cancel the session for the given reason.
    Cancel(WedgeReason),
}

fn silence_secs(session_diag: &SessionDiag) -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let last_ms = session_diag.last_activity_ms.load(Ordering::Relaxed);
    now_ms.saturating_sub(last_ms) / 1000
}

/// Pure decision function. Takes the silence count, the TCP-state
/// probe outcome (Some(state) for a known kernel TCP state, None for
/// probe failure), and the previous tick's suspected flag. Returns
/// the action the watchdog should take.
///
/// CLOSE_WAIT is the unambiguous wedge signature: peer FIN'd and the
/// application has not closed. Other states (ESTABLISHED, FIN_WAIT_*,
/// transient close-handshake states) are treated as not-wedge.
///
/// Probe failures (None) are treated as wedge-suspect rather than
/// healthy: a session whose probe has failed and which has been silent
/// past the fast threshold is at minimum not coming back, and the
/// fallback silence threshold is the absolute backstop.
fn evaluate(silence_secs: u64, probe: Option<TcpState>, wedge_suspected: bool) -> Decision {
    if silence_secs >= WEDGE_FALLBACK_KILL_SILENCE_SECS {
        return Decision::Cancel(WedgeReason::FallbackSilence);
    }
    if silence_secs < WEDGE_FAST_KILL_SILENCE_SECS {
        return Decision::Quiet;
    }
    let wedge_signal = match probe {
        Some(TcpState::CloseWait) => true,
        Some(TcpState::Established) | Some(TcpState::Other(_)) => false,
        None => true,
    };
    if !wedge_signal {
        return Decision::Quiet;
    }
    if wedge_suspected {
        Decision::Cancel(WedgeReason::TcpStateCloseWaitConfirmed)
    } else {
        Decision::SuspectedFirstTick
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_below_fast_threshold_is_quiet() {
        let decision = evaluate(WEDGE_FAST_KILL_SILENCE_SECS - 1, Some(TcpState::Established), false);
        assert_eq!(decision, Decision::Quiet);
    }

    #[test]
    fn silence_above_fast_with_established_is_quiet() {
        let decision = evaluate(WEDGE_FAST_KILL_SILENCE_SECS, Some(TcpState::Established), false);
        assert_eq!(decision, Decision::Quiet);
    }

    #[test]
    fn silence_above_fast_with_transient_close_state_is_quiet() {
        // FIN_WAIT_2 (0x05): the connection is in a clean close
        // handshake initiated by the local side. Not a wedge.
        let decision = evaluate(WEDGE_FAST_KILL_SILENCE_SECS, Some(TcpState::Other(0x05)), false);
        assert_eq!(decision, Decision::Quiet);
    }

    #[test]
    fn silence_above_fast_with_close_wait_first_tick_is_suspected() {
        let decision = evaluate(WEDGE_FAST_KILL_SILENCE_SECS, Some(TcpState::CloseWait), false);
        assert_eq!(decision, Decision::SuspectedFirstTick);
    }

    #[test]
    fn silence_above_fast_with_close_wait_second_tick_cancels() {
        let decision = evaluate(WEDGE_FAST_KILL_SILENCE_SECS, Some(TcpState::CloseWait), true);
        assert_eq!(decision, Decision::Cancel(WedgeReason::TcpStateCloseWaitConfirmed));
    }

    #[test]
    fn probe_failed_silence_above_fast_first_tick_is_suspected() {
        let decision = evaluate(WEDGE_FAST_KILL_SILENCE_SECS, None, false);
        assert_eq!(decision, Decision::SuspectedFirstTick);
    }

    #[test]
    fn probe_failed_silence_above_fast_second_tick_cancels() {
        let decision = evaluate(WEDGE_FAST_KILL_SILENCE_SECS, None, true);
        assert_eq!(decision, Decision::Cancel(WedgeReason::TcpStateCloseWaitConfirmed));
    }

    #[test]
    fn silence_above_fallback_cancels_regardless_of_probe() {
        let decision = evaluate(WEDGE_FALLBACK_KILL_SILENCE_SECS, Some(TcpState::Established), false);
        assert_eq!(decision, Decision::Cancel(WedgeReason::FallbackSilence));
    }
}
