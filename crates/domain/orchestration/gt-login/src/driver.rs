//! Sync driver that wraps a [`Pty`] adapter, ingests output, and surfaces [`LoginEvent`]s
//! via a caller-supplied callback. The `gt-web` task (`hq-fe-auth.2`) drives this from
//! a tokio `spawn_blocking` thread; the domain stays sync so it can be exercised
//! end-to-end by `#[test]` without a runtime.
//!
//! `hq-fe-auth.5` adds optional per-phase deadlines via [`LoginDriver::run_with_timeouts`].
//! The driver spawns a watchdog thread that calls the child's [`PtyKiller`] when the
//! deadline expires, so a hung `claude /login` surfaces as
//! [`LoginFailure::Timeout`] (added in `.4`) without leaking a zombie process.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::events::{LoginEvent, LoginFailure};
use crate::pty::{Pty, PtyChild, PtyKiller};
use crate::state::extract_url;

/// Terminal outcome of [`LoginDriver::run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginOutcome {
    /// Token accepted; the CLI exited 0.
    Complete { account: String },
    /// Anything that did not reach `Complete`.
    Failed { reason: LoginFailure },
}

/// Per-phase deadlines for [`LoginDriver::run_with_timeouts`] — `hq-fe-auth.5`.
///
/// `url_phase` bounds how long the driver waits for `claude /login` to print the OAuth
/// URL after spawn; if exceeded, the watchdog kills the child and the outcome is
/// [`LoginFailure::Timeout`] with `phase: "url"`.
///
/// `token_phase` bounds how long the driver waits for the caller's `token_source`
/// closure to return after the URL is surfaced. If exceeded, the watchdog kills the
/// child and the outcome is `Timeout { phase: "token" }`. The caller's closure is
/// expected to honour the same deadline (typically via `Receiver::recv_timeout`) so it
/// returns `None` instead of blocking forever.
#[derive(Debug, Clone, Copy)]
pub struct LoginTimeouts {
    pub url_phase: Duration,
    pub token_phase: Duration,
}

impl LoginTimeouts {
    /// Build with explicit values. Either field can be set to `Duration::MAX` to keep
    /// that phase unbounded (matches the legacy [`LoginDriver::run`] behaviour).
    pub fn new(url_phase: Duration, token_phase: Duration) -> Self {
        Self {
            url_phase,
            token_phase,
        }
    }

    /// Sentinel used internally when the legacy `run` entry is called: both phases
    /// bounded by `Duration::MAX`, which the watchdog treats as "no deadline".
    fn unbounded() -> Self {
        Self {
            url_phase: Duration::MAX,
            token_phase: Duration::MAX,
        }
    }
}

impl Default for LoginTimeouts {
    /// 120 s URL phase / 300 s token phase. Picked so a typical operator paste-back
    /// round trip (open browser, copy code) fits comfortably while a hung CLI is reaped
    /// within two minutes.
    fn default() -> Self {
        Self {
            url_phase: Duration::from_secs(120),
            token_phase: Duration::from_secs(300),
        }
    }
}

/// What the driver needs from the caller, plumbed in through [`LoginDriver::run`]:
///
/// - the spawn-port adapter ([`Pty`])
/// - the executable + args (`("claude", &["/login"])` in prod)
/// - the account label to echo back on `Complete`
/// - a token producer: the driver blocks on this after surfacing `UrlReady`. Returning
///   `None` cancels the flow (mapped to [`LoginFailure::Cancelled`] unless the
///   token-phase watchdog fired first, in which case it becomes
///   [`LoginFailure::Timeout`]).
/// - an event sink that takes each [`LoginEvent`] as it happens.
pub struct LoginDriver<P: Pty + ?Sized> {
    pty: Arc<P>,
}

impl<P: Pty + ?Sized> LoginDriver<P> {
    pub fn new(pty: Arc<P>) -> Self {
        Self { pty }
    }

    /// Run the login flow without per-phase deadlines. Equivalent to
    /// [`LoginDriver::run_with_timeouts`] with both phases bounded by `Duration::MAX` —
    /// kept as the stable entry so existing callers remain unchanged while opting in to
    /// timeouts via the new method.
    pub fn run(
        &self,
        program: &str,
        args: &[&str],
        account: &str,
        token_source: impl FnMut(&str) -> Option<String>,
        on_event: impl FnMut(LoginEvent),
    ) -> LoginOutcome {
        self.run_with_timeouts(
            program,
            args,
            account,
            LoginTimeouts::unbounded(),
            token_source,
            on_event,
        )
    }

    /// Run the login flow with per-phase deadlines. The driver:
    ///
    /// 1. Spawns the child.
    /// 2. Starts a watchdog thread holding a clone of the child's [`PtyKiller`]. The
    ///    watchdog sleeps until the URL-phase deadline, then kills the child unless the
    ///    driver already signalled "URL extracted".
    /// 3. Reads from the PTY until [`extract_url`] succeeds or `read_chunk` returns `0`.
    ///    On EOF, joining the watchdog returns whether it fired — that distinguishes
    ///    "watchdog kill → Ok(0)" (Timeout) from a genuine CLI EOF (UrlMissing).
    /// 4. On URL surfaced: cancels the URL-phase watchdog, starts a fresh watchdog for
    ///    the token-phase deadline, then calls `token_source`. The caller's closure is
    ///    expected to honour the same deadline (return `None` once it expires).
    ///    Joining the watchdog after `token_source` returns deterministically resolves
    ///    the race between caller-timeout (`recv_timeout`) and watchdog-fire.
    /// 5. Writes token, waits for child exit, emits `Complete` / `TokenRejected`.
    ///
    /// Blocks the current thread; callers on async edges put this on `spawn_blocking`.
    pub fn run_with_timeouts(
        &self,
        program: &str,
        args: &[&str],
        account: &str,
        timeouts: LoginTimeouts,
        mut token_source: impl FnMut(&str) -> Option<String>,
        mut on_event: impl FnMut(LoginEvent),
    ) -> LoginOutcome {
        let mut child = match self.pty.spawn(program, args) {
            Ok(c) => c,
            Err(e) => {
                let reason = LoginFailure::Spawn {
                    message: e.to_string(),
                };
                on_event(LoginEvent::Failed {
                    reason: reason.clone(),
                });
                return LoginOutcome::Failed { reason };
            }
        };

        on_event(LoginEvent::Started);

        // ---------------- Phase 1: URL extraction (with watchdog).
        let url_watchdog = Watchdog::start(child.killer(), timeouts.url_phase);
        let mut output_buf = String::new();
        let mut chunk = [0u8; 4096];

        let url_for_prompt: String = loop {
            let n = match child.read_chunk(&mut chunk) {
                Ok(n) => n,
                Err(e) => {
                    let _ = url_watchdog.cancel_and_join();
                    return fail_io(&mut on_event, &mut *child, e);
                }
            };
            if n == 0 {
                // EOF before URL: either the CLI exited prematurely (UrlMissing) or the
                // watchdog killed it (Timeout). `cancel_and_join` joins the watchdog
                // thread first then reads `fired`, so the flag is finalised before we
                // read it.
                let timed_out = url_watchdog.cancel_and_join();
                let reason = if timed_out {
                    LoginFailure::Timeout {
                        phase: "url".to_string(),
                    }
                } else {
                    LoginFailure::UrlMissing
                };
                on_event(LoginEvent::Failed {
                    reason: reason.clone(),
                });
                child.kill();
                return LoginOutcome::Failed { reason };
            }
            output_buf.push_str(&String::from_utf8_lossy(&chunk[..n]));
            if let Some(url) = extract_url(&output_buf) {
                on_event(LoginEvent::UrlReady { url: url.clone() });
                break url;
            }
        };
        let _ = url_watchdog.cancel_and_join();

        // ---------------- Phase 2: token (with separate watchdog).
        //
        // Disambiguating `None` from `token_source` (Timeout vs Cancelled) is *clock*-based,
        // not watchdog-based: the caller's closure (`recv_timeout(token_phase)`) and the
        // watchdog share the same deadline, so racing the watchdog's `fired` flag against the
        // closure's return is nondeterministic — whichever 1s timer trips first wins, which
        // flaked as `Cancelled` when the closure returned a hair before the watchdog fired.
        // Instead we measure how long the closure actually blocked: a genuine operator cancel
        // drops the token sender *before* the deadline (elapsed < token_phase → Cancelled),
        // while hitting the deadline means elapsed >= token_phase → Timeout. The watchdog is
        // still started so a hung child is killed and its kill is OR-ed in as a backstop.
        let token_watchdog = Watchdog::start(child.killer(), timeouts.token_phase);
        let token_started = Instant::now();
        let token_result = token_source(&url_for_prompt);
        let token_elapsed = token_started.elapsed();
        let watchdog_fired = token_watchdog.cancel_and_join();
        let token_timed_out = watchdog_fired
            || (timeouts.token_phase != Duration::MAX && token_elapsed >= timeouts.token_phase);

        let token = match token_result {
            Some(t) => t,
            None => {
                let reason = if token_timed_out {
                    LoginFailure::Timeout {
                        phase: "token".to_string(),
                    }
                } else {
                    LoginFailure::Cancelled
                };
                on_event(LoginEvent::Failed {
                    reason: reason.clone(),
                });
                child.kill();
                return LoginOutcome::Failed { reason };
            }
        };

        // ---------------- Phase 3: write token + wait for exit.
        let mut payload = token.into_bytes();
        payload.push(b'\n');
        if let Err(e) = child.write_all(&payload) {
            return fail_io(&mut on_event, &mut *child, e);
        }

        let status = match child.wait() {
            Ok(s) => s,
            Err(e) => return fail_io(&mut on_event, &mut *child, e),
        };
        if status == 0 {
            on_event(LoginEvent::Complete {
                account: account.to_string(),
            });
            LoginOutcome::Complete {
                account: account.to_string(),
            }
        } else {
            let reason = LoginFailure::TokenRejected { status };
            on_event(LoginEvent::Failed {
                reason: reason.clone(),
            });
            LoginOutcome::Failed { reason }
        }
    }
}

/// Caller-facing handle alias (the spawn-side wrapper in `gt-web` holds this driver
/// behind an `Arc` keyed by account). Kept as a thin re-export so the API surface stays
/// stable across `.1` / `.2` / `.5`.
pub type LoginHandle<P> = LoginDriver<P>;

/// Single-shot watchdog: sleeps until `deadline` (or until the driver cancels it via
/// `cancel_and_join`), then calls `killer.kill()`. The `fired` flag lets the driver tell
/// "watchdog killed the child" apart from "child closed cleanly".
struct Watchdog {
    handle: Option<JoinHandle<()>>,
    fired: Arc<AtomicBool>,
    cancel: Arc<(Mutex<bool>, Condvar)>,
}

impl Watchdog {
    fn start(killer: Box<dyn PtyKiller>, deadline: Duration) -> Self {
        let fired = Arc::new(AtomicBool::new(false));
        let cancel = Arc::new((Mutex::new(false), Condvar::new()));
        // Duration::MAX = "no deadline" — skip the thread entirely.
        if deadline == Duration::MAX {
            return Self {
                handle: None,
                fired,
                cancel,
            };
        }
        let fired_w = fired.clone();
        let cancel_w = cancel.clone();
        let handle = std::thread::spawn(move || {
            let (lock, cv) = &*cancel_w;
            let mut guard = lock.lock().expect("watchdog lock");
            let start = Instant::now();
            let mut remaining = deadline;
            loop {
                if *guard {
                    return;
                }
                let res = cv.wait_timeout(guard, remaining).expect("watchdog wait");
                guard = res.0;
                if *guard {
                    return;
                }
                if res.1.timed_out() {
                    fired_w.store(true, Ordering::SeqCst);
                    killer.kill();
                    return;
                }
                let elapsed = start.elapsed();
                if elapsed >= deadline {
                    fired_w.store(true, Ordering::SeqCst);
                    killer.kill();
                    return;
                }
                remaining = deadline - elapsed;
            }
        });
        Self {
            handle: Some(handle),
            fired,
            cancel,
        }
    }

    /// Wake the watchdog thread (if any), join it, and return whether it fired before
    /// the cancel signal arrived. Joining first guarantees the `fired` atomic has its
    /// final value when read — important when the caller's own deadline (e.g. a
    /// `Receiver::recv_timeout`) races the watchdog at the same wallclock instant.
    /// Phase boundaries happen at most twice per login, so the per-boundary
    /// `Mutex::lock` + `notify_one` + thread-join cost is negligible.
    fn cancel_and_join(mut self) -> bool {
        let (lock, cv) = &*self.cancel;
        if let Ok(mut g) = lock.lock() {
            *g = true;
            cv.notify_all();
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        self.fired.load(Ordering::SeqCst)
    }
}

fn fail_io(
    on_event: &mut impl FnMut(LoginEvent),
    child: &mut dyn PtyChild,
    err: std::io::Error,
) -> LoginOutcome {
    let reason = LoginFailure::Io {
        message: err.to_string(),
    };
    on_event(LoginEvent::Failed {
        reason: reason.clone(),
    });
    child.kill();
    LoginOutcome::Failed { reason }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pty::FakePty;

    fn collect_events() -> (impl FnMut(LoginEvent), std::sync::Arc<std::sync::Mutex<Vec<LoginEvent>>>)
    {
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let cloned = log.clone();
        let sink = move |e: LoginEvent| {
            cloned.lock().expect("event log").push(e);
        };
        (sink, log)
    }

    #[test]
    fn happy_path_emits_started_url_ready_complete() {
        let pty = Arc::new(FakePty::scripted(
            vec![b"Open https://console.anthropic.com/oauth?state=xyz to continue.\n".to_vec()],
            0,
        ));
        let driver = LoginDriver::new(pty.clone());
        let (sink, log) = collect_events();
        let outcome = driver.run(
            "claude",
            &["/login"],
            "primary",
            |_url| Some("TOK-123".to_string()),
            sink,
        );

        assert_eq!(
            outcome,
            LoginOutcome::Complete {
                account: "primary".into()
            }
        );
        let log = log.lock().expect("event log").clone();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0], LoginEvent::Started);
        assert_eq!(
            log[1],
            LoginEvent::UrlReady {
                url: "https://console.anthropic.com/oauth?state=xyz".into()
            }
        );
        assert_eq!(
            log[2],
            LoginEvent::Complete {
                account: "primary".into()
            }
        );
        assert_eq!(pty.take_writes(), vec![b"TOK-123\n".to_vec()]);
    }

    #[test]
    fn url_split_across_chunks_is_extracted() {
        let pty = Arc::new(FakePty::scripted(
            vec![
                b"Open https://console.anthropic".to_vec(),
                b".com/oauth?x=1\n".to_vec(),
            ],
            0,
        ));
        let driver = LoginDriver::new(pty);
        let (sink, log) = collect_events();
        let outcome = driver.run("claude", &["/login"], "alt", |_| Some("T".into()), sink);
        assert!(matches!(outcome, LoginOutcome::Complete { .. }));
        let log = log.lock().expect("event log").clone();
        assert!(matches!(
            log[1],
            LoginEvent::UrlReady { ref url } if url == "https://console.anthropic.com/oauth?x=1"
        ));
    }

    #[test]
    fn eof_before_url_is_url_missing() {
        let pty = Arc::new(FakePty::scripted(
            vec![b"banner with no url\n".to_vec()],
            0,
        ));
        let driver = LoginDriver::new(pty);
        let (sink, log) = collect_events();
        let outcome = driver.run("claude", &["/login"], "a", |_| Some("T".into()), sink);
        assert_eq!(
            outcome,
            LoginOutcome::Failed {
                reason: LoginFailure::UrlMissing
            }
        );
        let log = log.lock().expect("event log").clone();
        assert!(matches!(log.last(), Some(LoginEvent::Failed { .. })));
    }

    #[test]
    fn caller_cancel_propagates() {
        let pty = Arc::new(FakePty::scripted(
            vec![b"https://console.anthropic.com/x\n".to_vec()],
            0,
        ));
        let driver = LoginDriver::new(pty);
        let (sink, _log) = collect_events();
        let outcome = driver.run("claude", &["/login"], "a", |_| None, sink);
        assert_eq!(
            outcome,
            LoginOutcome::Failed {
                reason: LoginFailure::Cancelled
            }
        );
    }

    #[test]
    fn nonzero_exit_is_token_rejected() {
        let pty = Arc::new(FakePty::scripted(
            vec![b"https://console.anthropic.com/x\n".to_vec()],
            17,
        ));
        let driver = LoginDriver::new(pty);
        let (sink, _log) = collect_events();
        let outcome = driver.run("claude", &["/login"], "a", |_| Some("BAD".into()), sink);
        assert_eq!(
            outcome,
            LoginOutcome::Failed {
                reason: LoginFailure::TokenRejected { status: 17 }
            }
        );
    }

    // -----------------------------------------------------------------------
    // hq-fe-auth.5 — per-phase timeouts.

    #[test]
    fn url_phase_timeout_maps_to_failure_timeout_url() {
        // FakePty::blocking never emits a URL; the watchdog must kill the child after
        // url_phase elapses and the driver must report `Timeout{phase:"url"}` instead of
        // `UrlMissing`.
        let pty = Arc::new(FakePty::blocking(Vec::new(), 0));
        let driver = LoginDriver::new(pty);
        let (sink, log) = collect_events();
        let outcome = driver.run_with_timeouts(
            "claude",
            &["/login"],
            "acct-a",
            LoginTimeouts::new(Duration::from_millis(80), Duration::from_secs(60)),
            |_| Some("T".into()),
            sink,
        );
        assert_eq!(
            outcome,
            LoginOutcome::Failed {
                reason: LoginFailure::Timeout {
                    phase: "url".to_string(),
                }
            }
        );
        let log = log.lock().expect("event log").clone();
        assert!(matches!(
            log.last(),
            Some(LoginEvent::Failed {
                reason: LoginFailure::Timeout { .. }
            })
        ));
    }

    #[test]
    fn token_phase_timeout_distinguishes_from_cancel() {
        // URL extracted immediately; `token_source` blocks past the deadline, then
        // returns None. The watchdog kills the child in the meantime, and the driver
        // maps the (None + watchdog-fired) combo to Timeout{phase:"token"}.
        let pty = Arc::new(FakePty::scripted(
            vec![b"https://console.anthropic.com/x\n".to_vec()],
            0,
        ));
        let driver = LoginDriver::new(pty);
        let (sink, _log) = collect_events();
        let outcome = driver.run_with_timeouts(
            "claude",
            &["/login"],
            "acct-a",
            LoginTimeouts::new(Duration::from_secs(60), Duration::from_millis(60)),
            |_| {
                std::thread::sleep(Duration::from_millis(150));
                None
            },
            sink,
        );
        assert_eq!(
            outcome,
            LoginOutcome::Failed {
                reason: LoginFailure::Timeout {
                    phase: "token".to_string(),
                }
            }
        );
    }

    #[test]
    fn url_phase_completes_before_watchdog_does_not_time_out() {
        let pty = Arc::new(FakePty::scripted(
            vec![b"https://console.anthropic.com/y\n".to_vec()],
            0,
        ));
        let driver = LoginDriver::new(pty);
        let (sink, _log) = collect_events();
        let outcome = driver.run_with_timeouts(
            "claude",
            &["/login"],
            "acct-a",
            LoginTimeouts::new(Duration::from_secs(60), Duration::from_secs(60)),
            |_| Some("OK".into()),
            sink,
        );
        assert_eq!(
            outcome,
            LoginOutcome::Complete {
                account: "acct-a".into()
            }
        );
    }
}
