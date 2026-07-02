//! `NotificationKind` — the CLOSED set of operator-notification kinds (gtcore-7a707a).
//!
//! `kind` on the `notifications` row was validated ad-hoc as an inline string slice
//! (`["decision", "info", "alert"].contains(..)`) at each write site, and the audit
//! found free-form values (`warning`, `escalation`, `merge-failure`) reaching the
//! writers — which the DB CHECK then rejected opaquely, so a caller saw a bare
//! `500`/insert error and retried under a *different* kind (the `warning→alert`,
//! `escalation→alert` double-sends in evidence (b)).
//!
//! Typing it as a closed enum — the same rationale as
//! [`IssueType`](gt_issues::taxonomy::IssueType) / [`Dispatch`](gt_issues::taxonomy::Dispatch)
//! — rejects an out-of-set value at the frontier with an EXPLICIT, listing error
//! instead of a silent DB failure, and gives the writers one shared parser.

use serde::{Deserialize, Serialize};

/// The kind of an operator notification. A closed set: the value is validated at
/// the write frontier, mirroring the `notifications.kind` DB CHECK.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NotificationKind {
    /// The caller asks the operator to decide something (the bell's "needs you" lane).
    Decision,
    /// A purely informational heads-up.
    Info,
    /// A failure / capacity fault the operator should see promptly.
    Alert,
}

impl NotificationKind {
    /// Every variant, in the canonical order — used to render the allowed-values
    /// list in a validation error and to keep the DB CHECK in lock-step.
    pub const ALL: &'static [NotificationKind] = &[Self::Decision, Self::Info, Self::Alert];

    /// Parse a wire/SQL token into the typed kind. `None` for any other string so an
    /// out-of-set value is rejected at the frontier instead of silently reaching the
    /// DB CHECK (mirrors [`Dispatch::parse`](gt_issues::taxonomy::Dispatch::parse)).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "decision" => Some(Self::Decision),
            "info" => Some(Self::Info),
            "alert" => Some(Self::Alert),
            _ => None,
        }
    }

    /// The lowercase wire/store form (the `notifications.kind` column value).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Decision => "decision",
            Self::Info => "info",
            Self::Alert => "alert",
        }
    }

    /// The comma-separated allowed values, for a validation error message.
    pub fn allowed() -> String {
        Self::ALL
            .iter()
            .map(|k| k.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The standard "kind must be one of: …" validation message for a rejected value.
    pub fn reject_message() -> String {
        format!("kind must be one of: {}", Self::allowed())
    }
}

impl Default for NotificationKind {
    /// `decision` — the same default the write surfaces have always stamped.
    fn default() -> Self {
        Self::Decision
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trips_every_variant() {
        for &k in NotificationKind::ALL {
            assert_eq!(NotificationKind::parse(k.as_str()), Some(k));
        }
    }

    #[test]
    fn parse_rejects_the_free_form_values_the_audit_found() {
        // Exactly the out-of-set kinds evidence (c) reported.
        for bad in ["warning", "escalation", "merge-failure", "", "Alert", "ALERT", "banana"] {
            assert_eq!(NotificationKind::parse(bad), None, "{bad} must be rejected");
        }
    }

    #[test]
    fn allowed_lists_every_variant_and_matches_the_db_check() {
        assert_eq!(NotificationKind::allowed(), "decision, info, alert");
        assert_eq!(NotificationKind::reject_message(), "kind must be one of: decision, info, alert");
    }

    #[test]
    fn default_is_decision() {
        assert_eq!(NotificationKind::default(), NotificationKind::Decision);
        assert_eq!(NotificationKind::default().as_str(), "decision");
    }

    #[test]
    fn serde_uses_the_lowercase_wire_form() {
        assert_eq!(
            serde_json::to_string(&NotificationKind::Alert).unwrap(),
            "\"alert\""
        );
        let parsed: NotificationKind = serde_json::from_str("\"info\"").unwrap();
        assert_eq!(parsed, NotificationKind::Info);
    }
}
