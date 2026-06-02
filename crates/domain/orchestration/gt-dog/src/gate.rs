//! Gate evaluator (`hq-mod-dogs.3`).
//!
//! A Dog claims a Plugin and must decide **whether it may fire right now**. That decision is
//! the Plugin's [`Gate`](gt_plugin::descriptor::Gate): one of five
//! [`GateType`](gt_plugin::descriptor::GateType)s, each reading a different field of the gate
//! frontmatter:
//!
//! - **Cooldown** — `duration`: open once `now - last_fire >= duration`.
//! - **Cron** — `schedule`: open when a scheduled tick falls in `(last_fire, now]`.
//! - **Condition** — `check`: open when a predicate over current state holds.
//! - **Event** — `on`: open when the subscribed event kind has fired since the last run. The
//!   subscription itself is `gt-plugin`'s cross-module relay (`hq-mod-events.4`); wiring it to
//!   a running pool is the per-workspace pool's job (`hq-mod-dogs.8`), so here the *fact* of a
//!   fire arrives through [`GateContext::event_fired`].
//! - **Manual** — caller-triggered: never auto-opens (a no-op for the scheduler).
//!
//! [`evaluate`] is a **pure function** (non-negotiable #2: no clock or I/O in the replay
//! core). Everything time- or world-dependent — the current instant, the last fire, the
//! condition predicate's truth, whether the event fired — is supplied by the edge through
//! [`GateContext`]. The Gate descriptor comes straight from `gt-plugin` (ported up,
//! `hq-core-port.11`) rather than being re-mirrored here.

use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use cron::Schedule;

use gt_plugin::descriptor::{Gate, GateType};

/// Whether a [`Gate`] permits its Dog to fire now, and if not, why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GateDecision {
    /// The gate is open — the Dog may run.
    Open,
    /// The gate is closed; `reason` is a human-facing explanation (for logs / digests).
    Closed {
        /// Why the gate did not open.
        reason: String,
    },
}

impl GateDecision {
    /// `true` when the gate is [`Open`](GateDecision::Open).
    pub fn is_open(&self) -> bool {
        matches!(self, GateDecision::Open)
    }
}

/// Reason a gate could not be evaluated — a malformed or incomplete descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GateError {
    /// The gate's `type` requires a field the frontmatter did not populate (e.g. a `cooldown`
    /// gate with no `duration`).
    MissingField {
        /// The gate type that requires the field.
        kind: GateType,
        /// The missing frontmatter field name.
        field: &'static str,
    },
    /// A `cooldown` `duration` was not a positive `<N><s|m|h|d>` value.
    BadDuration(String),
    /// A `cron` `schedule` failed to parse.
    BadCron(String),
}

impl std::fmt::Display for GateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GateError::MissingField { kind, field } => {
                write!(f, "{kind:?} gate is missing required field `{field}`")
            }
            GateError::BadDuration(s) => {
                write!(f, "invalid cooldown duration {s:?} (expected e.g. `30s`, `5m`, `2h`, `1d`)")
            }
            GateError::BadCron(s) => write!(f, "invalid cron schedule: {s}"),
        }
    }
}

impl std::error::Error for GateError {}

/// The world-facts the edge supplies so [`evaluate`] stays pure (NN#2).
pub struct GateContext<'a> {
    /// The instant the gate is evaluated against (injected, never read from a clock here).
    pub now: DateTime<Utc>,
    /// When this Dog's claim last fired, if ever. Drives Cooldown and Cron.
    pub last_fire: Option<DateTime<Utc>>,
    /// Resolves a Condition gate's `check` expression to a truth value over current state.
    pub condition: &'a dyn Fn(&str) -> bool,
    /// Whether an Event gate's subscribed `on` kind has fired since `last_fire`.
    pub event_fired: bool,
}

/// Decide whether `gate` is open given the world-facts in `ctx`.
pub fn evaluate(gate: &Gate, ctx: &GateContext<'_>) -> Result<GateDecision, GateError> {
    match gate.kind {
        GateType::Manual => Ok(GateDecision::Closed {
            reason: "manual gate is caller-triggered".to_string(),
        }),

        GateType::Cooldown => {
            let raw = gate.duration.as_deref().ok_or(GateError::MissingField {
                kind: GateType::Cooldown,
                field: "duration",
            })?;
            let cooldown = parse_duration(raw)?;
            match ctx.last_fire {
                // Never fired → immediately due.
                None => Ok(GateDecision::Open),
                Some(last) => {
                    let elapsed = ctx.now - last;
                    if elapsed >= cooldown {
                        Ok(GateDecision::Open)
                    } else {
                        let remaining = (cooldown - elapsed).num_seconds();
                        Ok(GateDecision::Closed {
                            reason: format!("cooldown: {remaining}s remaining"),
                        })
                    }
                }
            }
        }

        GateType::Cron => {
            let raw = gate.schedule.as_deref().ok_or(GateError::MissingField {
                kind: GateType::Cron,
                field: "schedule",
            })?;
            let schedule = Schedule::from_str(raw).map_err(|e| GateError::BadCron(e.to_string()))?;
            // Open when the next tick after the last fire has already arrived. With no prior
            // fire we look from the epoch, so the most recent due tick counts.
            let anchor = ctx.last_fire.unwrap_or(DateTime::<Utc>::MIN_UTC);
            match schedule.after(&anchor).next() {
                Some(tick) if tick <= ctx.now => Ok(GateDecision::Open),
                _ => Ok(GateDecision::Closed {
                    reason: "cron: no tick due".to_string(),
                }),
            }
        }

        GateType::Condition => {
            let check = gate.check.as_deref().ok_or(GateError::MissingField {
                kind: GateType::Condition,
                field: "check",
            })?;
            if (ctx.condition)(check) {
                Ok(GateDecision::Open)
            } else {
                Ok(GateDecision::Closed {
                    reason: format!("condition {check:?} is false"),
                })
            }
        }

        GateType::Event => {
            // `on` must be declared even though the fire signal arrives via the context: a
            // missing `on` is a malformed gate, not a closed one.
            let _on = gate.on.as_deref().ok_or(GateError::MissingField {
                kind: GateType::Event,
                field: "on",
            })?;
            if ctx.event_fired {
                Ok(GateDecision::Open)
            } else {
                Ok(GateDecision::Closed {
                    reason: "event: subscribed kind has not fired since last run".to_string(),
                })
            }
        }
    }
}

/// Parse a positive `<N><unit>` duration (`s`, `m`, `h`, `d`) into a [`chrono::Duration`].
fn parse_duration(raw: &str) -> Result<Duration, GateError> {
    let s = raw.trim();
    let bad = || GateError::BadDuration(raw.to_string());
    let (digits, unit) = s.split_at(s.len().checked_sub(1).ok_or_else(bad)?);
    let n: i64 = digits.parse().map_err(|_| bad())?;
    if n <= 0 {
        return Err(bad());
    }
    let dur = match unit {
        "s" => Duration::seconds(n),
        "m" => Duration::minutes(n),
        "h" => Duration::hours(n),
        "d" => Duration::days(n),
        _ => return Err(bad()),
    };
    Ok(dur)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    fn always_false(_: &str) -> bool {
        false
    }

    /// A gate of `kind` with no populated fields, for the field-presence error paths.
    fn gate(kind: GateType) -> Gate {
        Gate {
            kind,
            ..Gate::default()
        }
    }

    fn ctx_at(now: i64, last_fire: Option<i64>) -> GateContext<'static> {
        GateContext {
            now: ts(now),
            last_fire: last_fire.map(ts),
            condition: &always_false,
            event_fired: false,
        }
    }

    #[test]
    fn manual_never_auto_opens() {
        let g = gate(GateType::Manual);
        assert!(matches!(
            evaluate(&g, &ctx_at(1000, Some(0))).unwrap(),
            GateDecision::Closed { .. }
        ));
    }

    #[test]
    fn cooldown_open_when_never_fired() {
        let g = Gate {
            kind: GateType::Cooldown,
            duration: Some("30s".into()),
            ..Gate::default()
        };
        assert_eq!(evaluate(&g, &ctx_at(1000, None)).unwrap(), GateDecision::Open);
    }

    #[test]
    fn cooldown_closed_within_window_open_after() {
        let g = Gate {
            kind: GateType::Cooldown,
            duration: Some("1m".into()),
            ..Gate::default()
        };
        // 30s after last fire: still cooling down.
        let within = evaluate(&g, &ctx_at(1030, Some(1000))).unwrap();
        assert!(matches!(within, GateDecision::Closed { .. }), "{within:?}");
        // 60s after: due.
        assert_eq!(evaluate(&g, &ctx_at(1060, Some(1000))).unwrap(), GateDecision::Open);
    }

    #[test]
    fn cooldown_missing_duration_errors() {
        let err = evaluate(&gate(GateType::Cooldown), &ctx_at(0, None)).unwrap_err();
        assert_eq!(
            err,
            GateError::MissingField {
                kind: GateType::Cooldown,
                field: "duration"
            }
        );
    }

    #[test]
    fn cooldown_bad_duration_errors() {
        for bad in ["", "30x", "abc", "-5s", "0s"] {
            let g = Gate {
                kind: GateType::Cooldown,
                duration: Some(bad.into()),
                ..Gate::default()
            };
            assert!(
                matches!(evaluate(&g, &ctx_at(0, None)), Err(GateError::BadDuration(_))),
                "expected BadDuration for {bad:?}"
            );
        }
    }

    #[test]
    fn cron_due_when_tick_passed() {
        // Every hour at minute/second 0 (6-field cron: sec min hour dom mon dow).
        let g = Gate {
            kind: GateType::Cron,
            schedule: Some("0 0 * * * *".into()),
            ..Gate::default()
        };
        // last fire at epoch; now is 02:00:00 UTC → at least one hourly tick (01:00:00) is due.
        let now = 2 * 3600;
        assert_eq!(
            evaluate(&g, &ctx_at(now, Some(1))).unwrap(),
            GateDecision::Open
        );
    }

    #[test]
    fn cron_not_due_within_interval() {
        let g = Gate {
            kind: GateType::Cron,
            schedule: Some("0 0 * * * *".into()),
            ..Gate::default()
        };
        // last fire at 01:00:00; now 01:30:00 → next tick (02:00:00) not yet due.
        let last = 3600;
        let now = 3600 + 1800;
        assert!(matches!(
            evaluate(&g, &ctx_at(now, Some(last))).unwrap(),
            GateDecision::Closed { .. }
        ));
    }

    #[test]
    fn cron_bad_schedule_errors() {
        let g = Gate {
            kind: GateType::Cron,
            schedule: Some("not a cron".into()),
            ..Gate::default()
        };
        assert!(matches!(
            evaluate(&g, &ctx_at(0, None)),
            Err(GateError::BadCron(_))
        ));
    }

    #[test]
    fn condition_open_when_predicate_true_closed_when_false() {
        let g = Gate {
            kind: GateType::Condition,
            check: Some("queue_depth > 0".into()),
            ..Gate::default()
        };
        let truthy = |_: &str| true;
        let open_ctx = GateContext {
            now: ts(0),
            last_fire: None,
            condition: &truthy,
            event_fired: false,
        };
        assert_eq!(evaluate(&g, &open_ctx).unwrap(), GateDecision::Open);
        // default ctx predicate is always_false → closed.
        assert!(matches!(
            evaluate(&g, &ctx_at(0, None)).unwrap(),
            GateDecision::Closed { .. }
        ));
    }

    #[test]
    fn condition_missing_check_errors() {
        let err = evaluate(&gate(GateType::Condition), &ctx_at(0, None)).unwrap_err();
        assert_eq!(
            err,
            GateError::MissingField {
                kind: GateType::Condition,
                field: "check"
            }
        );
    }

    #[test]
    fn event_open_only_when_fired() {
        let g = Gate {
            kind: GateType::Event,
            on: Some("bead.created.v1".into()),
            ..Gate::default()
        };
        // not fired → closed.
        assert!(matches!(
            evaluate(&g, &ctx_at(0, None)).unwrap(),
            GateDecision::Closed { .. }
        ));
        // fired → open.
        let fired = GateContext {
            now: ts(0),
            last_fire: None,
            condition: &always_false,
            event_fired: true,
        };
        assert_eq!(evaluate(&g, &fired).unwrap(), GateDecision::Open);
    }

    #[test]
    fn event_missing_on_errors() {
        let err = evaluate(&gate(GateType::Event), &ctx_at(0, None)).unwrap_err();
        assert_eq!(
            err,
            GateError::MissingField {
                kind: GateType::Event,
                field: "on"
            }
        );
    }
}
