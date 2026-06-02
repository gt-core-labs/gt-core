//! Quota actor: sole owner of the `AccountRegistry` (per-account/session rate, EWMA and the
//! per-window prediction flag). The clock enters as data on every message from the edge
//! (`now_secs`); the actor never reads it. The prediction is delegated to the pure
//! [`crate::expectations::predict`].
//!
//! The actor emits events to the relay (`mpsc<Envelope<QuotaEvent>>`); the composition root
//! drains them into the sync bus and persists them (audit + Postgres via outbox).

use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use gt_events::{AppError, Command, Envelope};

use crate::commands::QuotaCommand;
use crate::cost::ModelWeights;
use crate::events::QuotaEvent;
use crate::expectations::predict;
use crate::state::{Account, AccountRegistry, AccountWindow};

pub enum QuotaMsg {
    /// Initialize / replace an account's window (read by an edge probe).
    UpsertAccount {
        account: Account,
    },
    /// Drop an account from the registry (symmetric to `UpsertAccount`; an edge op, no event).
    /// Replies `true` if the id existed and was removed, `false` otherwise.
    RemoveAccount {
        account_id: String,
        reply: oneshot::Sender<bool>,
    },
    /// A model response: feeds the account consumption and the per-session rate.
    Sample {
        account: String,
        session: String,
        model: String,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_creation: u64,
        now_secs: u64,
    },
    /// Authoritative snapshot of the provider headers — reconciles the local counter.
    Probe {
        account: String,
        remaining: u64,
        resets_at_secs: u64,
        now_secs: u64,
    },
    /// Explicit window reset (the probe detects it when `resets_at` crossed `now`).
    ResetWindow {
        account: String,
        started_at_secs: u64,
        resets_at_secs: u64,
    },
    /// Edge tick: re-evaluate the predictor for every account with a live window (uses the
    /// supplied `now_secs`).
    Tick {
        now_secs: u64,
        threshold_secs: u64,
    },
    /// Reactive state observed at the provider.
    Limited {
        account: String,
        now_secs: u64,
    },
    Blocked {
        account: String,
        until_secs: Option<u64>,
        now_secs: u64,
    },
    Rotated {
        from_account: String,
        to_account: String,
        now_secs: u64,
    },
    /// Diagnostics: (live accounts, predictions emitted).
    Snapshot(oneshot::Sender<(usize, usize)>),
    /// Read-only snapshot of every registered account. The edge needs this to pick a healthy
    /// rotation target when the predictive chain fires (`docs/features/token-tracking-prediction.md`).
    AccountsSnapshot(oneshot::Sender<Vec<Account>>),
    /// "Ask without doing": run `validate` against the current registry, no mutation/emit.
    Validate {
        cmd: QuotaCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
    /// Apply the command: mutate the registry and emit the produced event to the relay.
    Exec {
        cmd: QuotaCommand,
        reply: oneshot::Sender<Result<(), AppError>>,
    },
}

#[derive(Clone)]
pub struct QuotaHandle {
    tx: mpsc::Sender<QuotaMsg>,
}

impl QuotaHandle {
    pub async fn upsert_account(&self, account: Account) {
        let _ = self.tx.send(QuotaMsg::UpsertAccount { account }).await;
    }

    /// Symmetric to [`Self::upsert_account`]. Returns `true` if an account with `id` was
    /// removed, `false` otherwise (including when the actor has shut down).
    pub async fn remove_account(&self, account_id: impl Into<String>) -> bool {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(QuotaMsg::RemoveAccount {
                account_id: account_id.into(),
                reply,
            })
            .await
            .is_err()
        {
            return false;
        }
        rx.await.unwrap_or(false)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn sample(
        &self,
        account: impl Into<String>,
        session: impl Into<String>,
        model: impl Into<String>,
        input: u64,
        output: u64,
        cache_read: u64,
        cache_creation: u64,
        now_secs: u64,
    ) {
        let _ = self
            .tx
            .send(QuotaMsg::Sample {
                account: account.into(),
                session: session.into(),
                model: model.into(),
                input,
                output,
                cache_read,
                cache_creation,
                now_secs,
            })
            .await;
    }

    pub async fn probe(
        &self,
        account: impl Into<String>,
        remaining: u64,
        resets_at_secs: u64,
        now_secs: u64,
    ) {
        let _ = self
            .tx
            .send(QuotaMsg::Probe {
                account: account.into(),
                remaining,
                resets_at_secs,
                now_secs,
            })
            .await;
    }

    pub async fn reset_window(
        &self,
        account: impl Into<String>,
        started_at_secs: u64,
        resets_at_secs: u64,
    ) {
        let _ = self
            .tx
            .send(QuotaMsg::ResetWindow {
                account: account.into(),
                started_at_secs,
                resets_at_secs,
            })
            .await;
    }

    pub async fn tick(&self, now_secs: u64, threshold_secs: u64) {
        let _ = self
            .tx
            .send(QuotaMsg::Tick {
                now_secs,
                threshold_secs,
            })
            .await;
    }

    pub async fn limited(&self, account: impl Into<String>, now_secs: u64) {
        let _ = self
            .tx
            .send(QuotaMsg::Limited {
                account: account.into(),
                now_secs,
            })
            .await;
    }

    pub async fn blocked(
        &self,
        account: impl Into<String>,
        until_secs: Option<u64>,
        now_secs: u64,
    ) {
        let _ = self
            .tx
            .send(QuotaMsg::Blocked {
                account: account.into(),
                until_secs,
                now_secs,
            })
            .await;
    }

    pub async fn rotated(
        &self,
        from_account: impl Into<String>,
        to_account: impl Into<String>,
        now_secs: u64,
    ) {
        let _ = self
            .tx
            .send(QuotaMsg::Rotated {
                from_account: from_account.into(),
                to_account: to_account.into(),
                now_secs,
            })
            .await;
    }

    pub async fn snapshot(&self) -> (usize, usize) {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(QuotaMsg::Snapshot(reply)).await.is_err() {
            return (0, 0);
        }
        rx.await.unwrap_or((0, 0))
    }

    /// Owned snapshot of every account in the registry. Used by the real `Effects` adapter to
    /// pick a healthy rotation target on `BlockPredicted` / `AccountLimited`.
    pub async fn accounts(&self) -> Vec<Account> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(QuotaMsg::AccountsSnapshot(reply)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// "Ask without doing": run `validate` against the current registry snapshot.
    /// The answer is a snapshot; the actor revalidates on `exec`.
    pub async fn validate(&self, cmd: QuotaCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(QuotaMsg::Validate { cmd, reply })
            .await
            .map_err(|_| AppError::Other("quota actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("quota actor dropped reply".into()))?
    }

    /// Apply the command. The actor re-validates inside the same tick and emits the produced
    /// `QuotaEvent` to the relay.
    pub async fn exec(&self, cmd: QuotaCommand) -> Result<(), AppError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(QuotaMsg::Exec { cmd, reply })
            .await
            .map_err(|_| AppError::Other("quota actor gone".into()))?;
        rx.await
            .map_err(|_| AppError::Other("quota actor dropped reply".into()))?
    }
}

/// Spawn the actor. `weights` defines the per-model normalization (see `cost.rs`);
/// `events` is the relay to the bus.
pub fn spawn(
    events: mpsc::Sender<Envelope<QuotaEvent>>,
    weights: HashMap<String, ModelWeights>,
) -> QuotaHandle {
    spawn_hydrated(events, weights, AccountRegistry::default(), 0)
}

/// Boot hydration (hq-8iur.1): same as [`spawn`] but seeds the actor with a pre-built
/// [`AccountRegistry`] and the count of `BlockPredicted` events emitted in the prior run.
/// The composition root passes both from the reducer rebuilt by `replay_gt`. `weights` are
/// applied on top so per-model normalization config can change across restarts independently
/// of the durable registry.
pub fn spawn_hydrated(
    events: mpsc::Sender<Envelope<QuotaEvent>>,
    weights: HashMap<String, ModelWeights>,
    initial: AccountRegistry,
    predictions_seen: usize,
) -> QuotaHandle {
    let (tx, mut rx) = mpsc::channel::<QuotaMsg>(64);

    tokio::spawn(async move {
        let mut registry = initial;
        registry.set_weights(weights);
        let mut predictions_emitted: usize = predictions_seen;

        while let Some(msg) = rx.recv().await {
            match msg {
                QuotaMsg::UpsertAccount { account } => {
                    registry.upsert_account(account);
                }
                QuotaMsg::RemoveAccount { account_id, reply } => {
                    let removed = registry.remove_account(&account_id);
                    let _ = reply.send(removed);
                }
                QuotaMsg::Sample {
                    account,
                    session,
                    model,
                    input,
                    output,
                    cache_read,
                    cache_creation,
                    now_secs,
                } => {
                    registry.apply_sample(
                        &account,
                        &session,
                        &model,
                        input,
                        output,
                        cache_read,
                        cache_creation,
                        now_secs,
                    );
                    let _ = events
                        .send(Envelope::root(QuotaEvent::TokensSampled {
                            account,
                            session,
                            model,
                            input,
                            output,
                            cache_read,
                            cache_creation,
                            now_secs,
                        }))
                        .await;
                }
                QuotaMsg::Probe {
                    account,
                    remaining,
                    resets_at_secs,
                    now_secs,
                } => {
                    registry.apply_probe(&account, remaining, resets_at_secs);
                    let _ = events
                        .send(Envelope::root(QuotaEvent::UsageProbed {
                            account,
                            remaining,
                            resets_at_secs,
                            now_secs,
                        }))
                        .await;
                }
                QuotaMsg::ResetWindow {
                    account,
                    started_at_secs,
                    resets_at_secs,
                } => {
                    if let Some(acc) = registry.get_mut(&account) {
                        if let Some(w) = acc.window.as_mut() {
                            w.started_at_secs = started_at_secs;
                            w.resets_at_secs = resets_at_secs;
                            w.consumed = 0.0;
                        }
                    }
                    registry.clear_window_prediction(&account);
                    let _ = events
                        .send(Envelope::root(QuotaEvent::WindowReset {
                            account,
                            started_at_secs,
                            resets_at_secs,
                        }))
                        .await;
                }
                QuotaMsg::Tick {
                    now_secs,
                    threshold_secs,
                } => {
                    // Stable snapshot of accounts-with-window to avoid re-borrowing the
                    // registry during iteration.
                    let candidates: Vec<(String, AccountWindow, f64)> = registry
                        .accounts()
                        .filter_map(|a| {
                            let w = a.window.clone()?;
                            let rate = registry.rate(&a.id).map(|e| e.value).unwrap_or(0.0);
                            Some((a.id.clone(), w, rate))
                        })
                        .collect();
                    for (id, window, rate) in candidates {
                        let started_at = window.started_at_secs;
                        let p = predict(&window, now_secs, rate, threshold_secs);
                        if p.should_predict_block {
                            if registry.mark_predicted(&id, started_at) {
                                if let Some(eta) = p.eta_to_block_secs {
                                    predictions_emitted += 1;
                                    let _ = events
                                        .send(Envelope::root(QuotaEvent::BlockPredicted {
                                            account: id,
                                            eta_to_block_secs: eta,
                                            consumed: p.consumed,
                                            limit: window.limit,
                                            rate_per_min: p.rate_per_min,
                                            now_secs,
                                        }))
                                        .await;
                                }
                            }
                        }
                    }
                }
                QuotaMsg::Limited { account, now_secs } => {
                    if let Some(a) = registry.get_mut(&account) {
                        a.status = crate::state::AccountQuotaStatus::Limited;
                    }
                    let _ = events
                        .send(Envelope::root(QuotaEvent::AccountLimited { account, now_secs }))
                        .await;
                }
                QuotaMsg::Blocked {
                    account,
                    until_secs,
                    now_secs,
                } => {
                    if let Some(a) = registry.get_mut(&account) {
                        a.status = crate::state::AccountQuotaStatus::Blocked;
                    }
                    let _ = events
                        .send(Envelope::root(QuotaEvent::Blocked {
                            account,
                            until_secs,
                            now_secs,
                        }))
                        .await;
                }
                QuotaMsg::Rotated {
                    from_account,
                    to_account,
                    now_secs,
                } => {
                    registry.apply_rotation(&from_account);
                    let _ = events
                        .send(Envelope::root(QuotaEvent::Rotated {
                            from_account,
                            to_account,
                            now_secs,
                        }))
                        .await;
                }
                QuotaMsg::Snapshot(reply) => {
                    let _ = reply.send((registry.accounts().count(), predictions_emitted));
                }
                QuotaMsg::AccountsSnapshot(reply) => {
                    let _ = reply.send(registry.accounts().cloned().collect());
                }
                QuotaMsg::Validate { cmd, reply } => {
                    let _ = reply.send(cmd.validate(&registry));
                }
                QuotaMsg::Exec { cmd, reply } => {
                    // execute() re-validates first → no TOCTOU within the actor tick. On
                    // success it returns the event to emit (emit-on-apply).
                    match cmd.execute(&mut registry) {
                        Ok(event) => {
                            let _ = events.send(Envelope::root(event)).await;
                            let _ = reply.send(Ok(()));
                        }
                        Err(e) => {
                            let _ = reply.send(Err(e));
                        }
                    }
                }
            }
        }
    });

    QuotaHandle { tx }
}
