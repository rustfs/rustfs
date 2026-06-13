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

//! Silence-only liveness backstop for non-Linux targets.
//!
//! Non-Linux targets cannot read /proc/net/tcp, so the CLOSE_WAIT
//! probe used by the Linux watchdog is unavailable. fallback_watchdog runs
//! one tokio task per session that cancels the session once it has
//! been silent at the SFTP handler layer at or past
//! WEDGE_FALLBACK_KILL_SILENCE_SECS. There is no socket introspection
//! and no fast-kill path. A session silent below that threshold keeps
//! running, so healthy idle sessions are left alone.

use super::constants::limits::{WEDGE_FALLBACK_KILL_SILENCE_SECS, WEDGE_WATCHDOG_TICK_SECS};
use super::lifecycle::SessionDiag;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SFTP_WATCHDOG: &str = "sftp_watchdog";
const EVENT_SFTP_WATCHDOG_STATE: &str = "sftp_watchdog_state";

/// Spawn a per-session silence backstop tick task.
///
/// The task holds a clone of the per-session CancellationToken and an
/// Arc to the SessionDiag it reads the activity stamp from. It exits
/// when it cancels the session itself or when the outer session task
/// cancels the token after a clean session end.
pub(super) fn spawn_for_session(session_diag: Arc<SessionDiag>, cancel_token: CancellationToken) {
    tokio::spawn(async move {
        let session_id = session_diag.session_id;
        let peer = session_diag.peer;
        let mut tick = tokio::time::interval(Duration::from_secs(WEDGE_WATCHDOG_TICK_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick fires immediately. Skip it so the backstop never
        // makes a decision before one full silence window has elapsed.
        tick.tick().await;
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break,
                _ = tick.tick() => {
                    let silence_secs = silence_secs(&session_diag);
                    if fallback_threshold_reached(silence_secs) {
                        tracing::warn!(
                            event = EVENT_SFTP_WATCHDOG_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_SFTP_WATCHDOG,
                            session_id,
                            peer = %peer,
                            silence_secs,
                            reason = "fallback_silence",
                            "sftp watchdog state changed",
                        );
                        cancel_token.cancel();
                        break;
                    }
                }
            }
        }
    });
}

/// Seconds since the session last stamped activity at the SFTP handler
/// layer.
fn silence_secs(session_diag: &SessionDiag) -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let last_ms = session_diag.last_activity_ms.load(Ordering::Relaxed);
    now_ms.saturating_sub(last_ms) / 1000
}

/// True once silence has reached WEDGE_FALLBACK_KILL_SILENCE_SECS, the
/// point at which the fallback backstop cancels the session.
fn fallback_threshold_reached(silence_secs: u64) -> bool {
    silence_secs >= WEDGE_FALLBACK_KILL_SILENCE_SECS
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn loopback() -> SocketAddr {
        "127.0.0.1:2222".parse().expect("static loopback address parses")
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after the unix epoch")
            .as_millis() as u64
    }

    #[test]
    fn threshold_reached_at_and_above_fallback() {
        assert!(fallback_threshold_reached(WEDGE_FALLBACK_KILL_SILENCE_SECS));
        assert!(fallback_threshold_reached(WEDGE_FALLBACK_KILL_SILENCE_SECS + 1));
    }

    #[test]
    fn threshold_not_reached_below_fallback() {
        assert!(!fallback_threshold_reached(WEDGE_FALLBACK_KILL_SILENCE_SECS - 1));
        assert!(!fallback_threshold_reached(0));
    }

    #[tokio::test(start_paused = true)]
    async fn cancels_session_silent_at_or_past_fallback() {
        let session_diag = Arc::new(SessionDiag::new(loopback(), loopback()));
        // Stamp activity far enough in the past that the first decision
        // tick already sees silence at or past the fallback threshold.
        // silence_secs reads the wall clock, which the paused tokio
        // timer does not move, so the stamp stays stale while virtual
        // time auto-advances the tick interval.
        let stale_ms = now_ms() - (WEDGE_FALLBACK_KILL_SILENCE_SECS + 60) * 1000;
        session_diag.last_activity_ms.store(stale_ms, Ordering::Relaxed);
        let cancel_token = CancellationToken::new();

        spawn_for_session(session_diag, cancel_token.clone());

        tokio::time::timeout(Duration::from_secs(WEDGE_WATCHDOG_TICK_SECS * 3), cancel_token.cancelled())
            .await
            .expect("a session silent at or past the fallback threshold must be cancelled");
        assert!(cancel_token.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_token_short_circuits_cleanly() {
        let session_diag = Arc::new(SessionDiag::new(loopback(), loopback()));
        session_diag.last_activity_ms.store(now_ms(), Ordering::Relaxed);
        let observer = Arc::downgrade(&session_diag);
        let cancel_token = CancellationToken::new();
        // Cancel before the task observes its first decision tick. The
        // cancelled arm must win and the task must drop its Arc on the
        // way out rather than waiting a full tick interval.
        cancel_token.cancel();

        spawn_for_session(session_diag, cancel_token.clone());

        // Sleep rather than yield between checks so the runtime parks
        // and its time driver fires the interval's immediate first
        // tick. A busy yield loop would starve that timer and the task
        // would stay parked before it reaches the cancelled arm.
        for _ in 0..200 {
            if observer.strong_count() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("task must release its SessionDiag after a clean cancel");
    }

    #[test]
    fn silence_secs_grows_from_a_stale_stamp() {
        let session_diag = SessionDiag::new(loopback(), loopback());
        session_diag.last_activity_ms.store(now_ms() - 5_000, Ordering::Relaxed);
        // Lower bound only. The exact value depends on wall-clock elapsed
        // during the test, which a contended runner can stretch past a
        // fixed upper bound.
        assert!(silence_secs(&session_diag) >= 5);
    }
}
