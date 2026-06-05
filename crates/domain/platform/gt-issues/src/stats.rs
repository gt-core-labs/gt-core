//! Transport-free aggregation for the issues statistics surface (`hq-web-extras.12`).
//!
//! `gt-web`'s statistics view (`gt-web.5`) needs counts + progress + lead-time roll-ups *without*
//! pulling every tracker row across the wire. This module computes those aggregates over a slice
//! of [`IssueRow`]s — the same rows the cheap snapshot already serves — grouped by one or more
//! dimensions. It holds **no** I/O: the REST handler in [`crate::http`] fetches the rows and the
//! pure functions here fold them, so the aggregation is exhaustively unit-testable without a store
//! or an HTTP server (docs/03 Rule 4: domain logic stays transport-free).
//!
//! ## Dimensions ([`GroupDim`])
//!
//! - `epic`     — `external_ref` (the epic linkage; `""` for orphan beads).
//! - `rig`      — derived from the bead-id prefix ↔ `rig.issue_prefix` (see [`rig_of`]).
//! - `status`   — the lifecycle column (`open`/`working`/`closed`/…).
//! - `domain`   — each taxonomy domain in `domain_json` (a row with N domains lands in N buckets).
//! - `assignee` — the `assignee` column (`""` = unassigned); the per-user view.
//! - `owner`    — the `owner` column (`""` = unowned); the per-user view.
//!
//! Multiple dimensions compose: `group_by=assignee,rig` yields one bucket per
//! `(assignee, rig)` pair, which is exactly the `Usuario -> Rigs` tree the FE paints inside a
//! workspace. A row that fans out on a multi-valued dimension (`domain`) contributes to one
//! bucket per value of that dimension, per the cross-product with the scalar dimensions.
//!
//! ## Per-bucket aggregate ([`StatBucket`])
//!
//! `counts {open, working, closed, other, total}` + `progress_pct` (`closed/total`, 0..=100) +
//! `lead_time` ([`LeadTime`], from `created_at`/`closed_at` on **closed** rows). Counts are
//! exact against a raw count of the same slice — that is the acceptance bar.

use serde::Serialize;

use gt_store_dolt::IssueRow;

/// One grouping dimension the `?group_by=` querystring accepts. Parsed case-insensitively from a
/// comma-separated list; unknown tokens are a client error (surfaced as `422` by the handler).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum GroupDim {
    /// `external_ref` — the epic linkage.
    Epic,
    /// Bead-id prefix ↔ `rig.issue_prefix` (see [`rig_of`]).
    Rig,
    /// Lifecycle `status`.
    Status,
    /// Each taxonomy domain in `domain_json` (multi-valued: fans the row out).
    Domain,
    /// `assignee` (`""` = unassigned).
    Assignee,
    /// `owner` (`""` = unowned).
    Owner,
}

impl GroupDim {
    /// Parse a single dimension token, case-insensitively. `Err` carries the offending token so
    /// the handler can return a precise `422` message.
    pub fn parse(token: &str) -> Result<Self, String> {
        match token.trim().to_ascii_lowercase().as_str() {
            "epic" => Ok(Self::Epic),
            "rig" => Ok(Self::Rig),
            "status" => Ok(Self::Status),
            "domain" => Ok(Self::Domain),
            "assignee" => Ok(Self::Assignee),
            "owner" => Ok(Self::Owner),
            other => Err(format!(
                "unknown group_by dimension {other:?}; expected one of \
                 epic|rig|status|domain|assignee|owner"
            )),
        }
    }

    /// Parse a comma-separated `group_by` list into the ordered dimension vector. Empty/missing
    /// list ⇒ `Err` (the handler requires at least one dimension). Duplicate dimensions are
    /// de-duplicated preserving first-seen order so `group_by=rig,rig` is harmless.
    pub fn parse_list(raw: &str) -> Result<Vec<Self>, String> {
        let mut dims: Vec<Self> = Vec::new();
        for tok in raw.split(',').map(str::trim).filter(|t| !t.is_empty()) {
            let dim = Self::parse(tok)?;
            if !dims.contains(&dim) {
                dims.push(dim);
            }
        }
        if dims.is_empty() {
            return Err("group_by must name at least one of \
                        epic|rig|status|domain|assignee|owner"
                .to_string());
        }
        Ok(dims)
    }

    /// The label this dimension carries in a bucket `key`. Stable snake_case so the FE can type
    /// the key object.
    fn field(self) -> &'static str {
        match self {
            Self::Epic => "epic",
            Self::Rig => "rig",
            Self::Status => "status",
            Self::Domain => "domain",
            Self::Assignee => "assignee",
            Self::Owner => "owner",
        }
    }

    /// The value(s) a row contributes for this dimension. Scalar dimensions yield exactly one
    /// value; `domain` yields one per taxonomy entry (or `[""]` when the row has none, so an
    /// undomained bead still counts once). The canonical "missing" value is `""` across the board,
    /// matching how the store represents unassigned/unowned/orphan.
    fn values_of(self, row: &IssueRow) -> Vec<String> {
        match self {
            Self::Epic => vec![row.external_ref.clone().unwrap_or_default()],
            Self::Rig => vec![rig_of(&row.id)],
            Self::Status => vec![row.status.clone()],
            Self::Assignee => vec![row.assignee.clone().unwrap_or_default()],
            Self::Owner => vec![row.owner.clone().unwrap_or_default()],
            Self::Domain => {
                let parsed = parse_domains(&row.domain_json);
                if parsed.is_empty() {
                    vec![String::new()]
                } else {
                    parsed
                }
            }
        }
    }
}

/// Derive the **rig** bucket from a bead id. The rig owns a beads `issue_prefix`
/// (`rig.issue_prefix`, see `gt-rig`), and every bead it owns is named `<prefix>...` — by
/// convention `<prefix>.<n>` (e.g. `hq-web-extras.12`) or `<prefix>-<sub>...`. The store does not
/// carry the rig table, and the bead spec asks the prefix be *derived from the id*, so we take the
/// leading hyphen-delimited token (`hq` for every `hq-*` id) as the rig namespace key. This is the
/// coarsest stable derivation that needs no rig-table join; a finer prefix↔rig resolution (when a
/// single namespace splits into several rigs) is a follow-up that would pass the rig prefix set in.
///
/// An id with no `-` (or empty) yields the id stripped of its `.<n>` suffix, so a degenerate id
/// still groups deterministically rather than into a single empty bucket.
pub fn rig_of(id: &str) -> String {
    match id.split_once('-') {
        Some((head, _)) if !head.is_empty() => head.to_string(),
        // No hyphen: fall back to the id without its trailing `.<n>` numeric suffix.
        _ => id.split('.').next().unwrap_or(id).to_string(),
    }
}

/// Parse the `domain_json` raw-JSON-array string into its domain strings. A malformed or empty
/// array yields an empty vec (the row then counts under the `""` domain bucket), never an error —
/// stats must never 500 on one odd row.
fn parse_domains(domain_json: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(domain_json).unwrap_or_default()
}

/// Lead-time summary for the **closed** rows in a bucket, in whole seconds between `created_at` and
/// `closed_at`. `count` is how many closed rows had both timestamps parse (the sample size); the
/// stats are `None` when that sample is empty so the FE renders "n/a" rather than a misleading `0`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct LeadTime {
    /// Number of closed rows that contributed (both timestamps present + parseable).
    pub count: u64,
    /// Mean lead time in seconds, rounded to the nearest second; `None` when `count == 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_secs: Option<i64>,
    /// Median lead time in seconds; `None` when `count == 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub median_secs: Option<i64>,
    /// Minimum lead time in seconds; `None` when `count == 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_secs: Option<i64>,
    /// Maximum lead time in seconds; `None` when `count == 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_secs: Option<i64>,
}

impl LeadTime {
    /// Fold a sample of per-row lead times (seconds) into the summary.
    fn from_samples(mut samples: Vec<i64>) -> Self {
        if samples.is_empty() {
            return Self { count: 0, mean_secs: None, median_secs: None, min_secs: None, max_secs: None };
        }
        samples.sort_unstable();
        let count = samples.len() as u64;
        let sum: i128 = samples.iter().map(|&s| s as i128).sum();
        let mean = (sum / count as i128) as i64;
        let median = {
            let n = samples.len();
            if n % 2 == 1 {
                samples[n / 2]
            } else {
                // Mean of the two central elements (i128 to avoid overflow on large gaps).
                ((samples[n / 2 - 1] as i128 + samples[n / 2] as i128) / 2) as i64
            }
        };
        Self {
            count,
            mean_secs: Some(mean),
            median_secs: Some(median),
            min_secs: Some(samples[0]),
            max_secs: Some(samples[count as usize - 1]),
        }
    }
}

/// One aggregated bucket: the `key` names the group (one entry per requested dimension), and the
/// body carries the counts, progress, and lead-time roll-up over the rows that fell into it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct StatBucket {
    /// The group key: `{ "<dim>": "<value>", ... }`, one entry per requested dimension in the
    /// requested order. For `group_by=assignee,rig` this is `{"assignee":"alice","rig":"hq"}`.
    pub key: std::collections::BTreeMap<String, String>,
    /// Count of rows with `status == "open"`.
    pub open: u64,
    /// Count of rows with `status == "working"`.
    pub working: u64,
    /// Count of rows with `status == "closed"`.
    pub closed: u64,
    /// Count of rows whose status is none of the above (defensive; usually 0).
    pub other: u64,
    /// Total rows in the bucket (`open + working + closed + other`).
    pub total: u64,
    /// Progress as `closed / total * 100`, rounded to one decimal (0.0..=100.0). `0.0` when the
    /// bucket is empty (cannot happen — empty buckets aren't emitted — but defined for safety).
    pub progress_pct: f64,
    /// Lead-time summary over the bucket's closed rows.
    pub lead_time: LeadTime,
}

/// The full statistics response: which dimensions were grouped on, the buckets, and a `totals`
/// roll-up across **all** rows (ungrouped) so the FE has a denominator without re-summing.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "axum", derive(utoipa::ToSchema))]
pub struct StatsResponse {
    /// The dimensions requested, in order (echoes `?group_by=`).
    pub group_by: Vec<GroupDim>,
    /// One bucket per distinct group key, sorted by key for a stable render order.
    pub buckets: Vec<StatBucket>,
    /// Counts/progress/lead-time over every row, independent of grouping. Its `key` is empty.
    pub totals: StatBucket,
}

/// In-progress accumulator folded per bucket, finalised into a [`StatBucket`].
#[derive(Default)]
struct Acc {
    open: u64,
    working: u64,
    closed: u64,
    other: u64,
    lead_samples: Vec<i64>,
}

impl Acc {
    fn observe(&mut self, row: &IssueRow) {
        match row.status.as_str() {
            "open" => self.open += 1,
            "working" => self.working += 1,
            "closed" => {
                self.closed += 1;
                if let Some(secs) = lead_secs(row) {
                    self.lead_samples.push(secs);
                }
            }
            _ => self.other += 1,
        }
    }

    fn finish(self, key: std::collections::BTreeMap<String, String>) -> StatBucket {
        let total = self.open + self.working + self.closed + self.other;
        let progress_pct = if total == 0 {
            0.0
        } else {
            // One-decimal rounding so 1/3 reads 33.3, not a long float the FE has to trim.
            ((self.closed as f64 / total as f64) * 1000.0).round() / 10.0
        };
        StatBucket {
            key,
            open: self.open,
            working: self.working,
            closed: self.closed,
            other: self.other,
            total,
            progress_pct,
            lead_time: LeadTime::from_samples(self.lead_samples),
        }
    }
}

/// Whole-seconds lead time of a closed row: `closed_at - created_at`, or `None` when either
/// timestamp is missing or unparseable, or the result is negative (clock skew / bad data). RFC-3339
/// is the stored format; we parse leniently and never error a whole request over one bad row.
fn lead_secs(row: &IssueRow) -> Option<i64> {
    let created = row.created_at.as_deref().and_then(parse_ts)?;
    let closed = row.closed_at.as_deref().and_then(parse_ts)?;
    let delta = closed - created;
    (delta >= 0).then_some(delta)
}

/// Parse an RFC-3339 timestamp to a Unix-second count. Accepts the `Z` and offset forms the store
/// emits. Returns `None` on anything it cannot read so the caller skips the sample. Mirrors
/// `gt-feed`'s `parse_epoch_secs` (the workspace's `time`-based timestamp parsing).
fn parse_ts(s: &str) -> Option<i64> {
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;
    OffsetDateTime::parse(s.trim(), &Rfc3339).ok().map(|t| t.unix_timestamp())
}

/// Compute the statistics response over `rows`, grouped by `dims` (already parsed + de-duplicated).
///
/// The cross-product of each row's per-dimension values forms the bucket key set the row
/// contributes to: scalar dimensions yield one value, the multi-valued `domain` dimension fans the
/// row across its domains. `totals` folds every row once, ungrouped, regardless of fan-out, so the
/// denominator stays the true row count.
pub fn aggregate(rows: &[IssueRow], dims: &[GroupDim]) -> StatsResponse {
    use std::collections::BTreeMap;

    let mut buckets: BTreeMap<Vec<(String, String)>, Acc> = BTreeMap::new();
    let mut totals = Acc::default();

    for row in rows {
        // Ungrouped totals: one observation per row, never fanned out.
        totals.observe(row);

        // Build the per-dimension value lists, then the cartesian product of keys this row hits.
        let per_dim: Vec<(GroupDim, Vec<String>)> =
            dims.iter().map(|&d| (d, d.values_of(row))).collect();
        for key in cartesian(&per_dim) {
            buckets.entry(key).or_default().observe(row);
        }
    }

    let buckets = buckets
        .into_iter()
        .map(|(k, acc)| {
            let key = k.into_iter().collect::<BTreeMap<String, String>>();
            acc.finish(key)
        })
        .collect();

    StatsResponse {
        group_by: dims.to_vec(),
        buckets,
        totals: totals.finish(BTreeMap::new()),
    }
}

/// Cartesian product of the per-dimension `(field, [values])` lists into a sorted-by-field key for
/// each combination. With the multi-valued `domain` dimension this fans a row across every
/// combination; with only scalar dimensions it yields a single key. Each produced key is a vec of
/// `(field, value)` pairs ordered by field (so it round-trips into a `BTreeMap`).
fn cartesian(per_dim: &[(GroupDim, Vec<String>)]) -> Vec<Vec<(String, String)>> {
    let mut acc: Vec<Vec<(String, String)>> = vec![Vec::new()];
    for (dim, values) in per_dim {
        let mut next = Vec::with_capacity(acc.len() * values.len().max(1));
        for prefix in &acc {
            for v in values {
                let mut k = prefix.clone();
                k.push((dim.field().to_string(), v.clone()));
                next.push(k);
            }
        }
        acc = next;
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, status: &str, created: Option<&str>, closed: Option<&str>) -> IssueRow {
        IssueRow {
            id: id.to_string(),
            title: "t".into(),
            status: status.into(),
            priority: 1,
            issue_type: "task".into(),
            assignee: None,
            owner: None,
            created_at: created.map(String::from),
            updated_at: None,
            closed_at: closed.map(String::from),
            external_ref: None,
            spec_id: None,
            domain_json: "[]".into(),
            surface_json: "[]".into(),
            depends_on_json: "[]".into(),
            role_scope: None,
            version: 0,
            phase: "P1".into(),
            delivered_sha: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
        }
    }

    #[test]
    fn parse_list_dedups_and_rejects_unknown_and_empty() {
        assert_eq!(GroupDim::parse_list("rig").unwrap(), vec![GroupDim::Rig]);
        assert_eq!(
            GroupDim::parse_list("Assignee, RIG ,assignee").unwrap(),
            vec![GroupDim::Assignee, GroupDim::Rig]
        );
        assert!(GroupDim::parse_list("").is_err());
        assert!(GroupDim::parse_list("  ,, ").is_err());
        assert!(GroupDim::parse_list("epic,bogus").is_err());
    }

    #[test]
    fn rig_derives_leading_token() {
        assert_eq!(rig_of("hq-web-extras.12"), "hq");
        assert_eq!(rig_of("hq-auth.3"), "hq");
        assert_eq!(rig_of("tobx-thing.1"), "tobx");
        // No hyphen: strip the .N suffix.
        assert_eq!(rig_of("solo.4"), "solo");
        assert_eq!(rig_of("solo"), "solo");
    }

    #[test]
    fn counts_match_raw_count_by_status() {
        let rows = vec![
            row("hq-a.1", "open", None, None),
            row("hq-a.2", "working", None, None),
            row("hq-a.3", "closed", Some("2026-01-01T00:00:00Z"), Some("2026-01-02T00:00:00Z")),
            row("hq-a.4", "closed", Some("2026-01-01T00:00:00Z"), Some("2026-01-03T00:00:00Z")),
        ];
        let resp = aggregate(&rows, &[GroupDim::Status]);
        // One bucket per distinct status.
        let by: std::collections::HashMap<_, _> = resp
            .buckets
            .iter()
            .map(|b| (b.key.get("status").unwrap().clone(), b))
            .collect();
        assert_eq!(by["open"].total, 1);
        assert_eq!(by["working"].total, 1);
        assert_eq!(by["closed"].total, 2);
        assert_eq!(by["closed"].closed, 2);
        // Totals roll-up matches the raw count.
        assert_eq!(resp.totals.total, 4);
        assert_eq!(resp.totals.closed, 2);
        assert_eq!(resp.totals.open, 1);
    }

    #[test]
    fn progress_pct_is_closed_over_total() {
        let rows = vec![
            row("hq-a.1", "open", None, None),
            row("hq-a.2", "closed", Some("2026-01-01T00:00:00Z"), Some("2026-01-02T00:00:00Z")),
            row("hq-a.3", "closed", Some("2026-01-01T00:00:00Z"), Some("2026-01-02T00:00:00Z")),
        ];
        let resp = aggregate(&rows, &[GroupDim::Rig]);
        assert_eq!(resp.buckets.len(), 1);
        // 2 of 3 closed => 66.7%.
        assert_eq!(resp.buckets[0].progress_pct, 66.7);
        assert_eq!(resp.totals.progress_pct, 66.7);
    }

    #[test]
    fn lead_time_stats_over_closed_rows() {
        // Deltas: 1 day (86400) and 3 days (259200).
        let rows = vec![
            row("hq-a.1", "closed", Some("2026-01-01T00:00:00Z"), Some("2026-01-02T00:00:00Z")),
            row("hq-a.2", "closed", Some("2026-01-01T00:00:00Z"), Some("2026-01-04T00:00:00Z")),
            // open row contributes no lead sample.
            row("hq-a.3", "open", Some("2026-01-01T00:00:00Z"), None),
        ];
        let resp = aggregate(&rows, &[GroupDim::Rig]);
        let lt = &resp.buckets[0].lead_time;
        assert_eq!(lt.count, 2);
        assert_eq!(lt.min_secs, Some(86_400));
        assert_eq!(lt.max_secs, Some(259_200));
        assert_eq!(lt.mean_secs, Some((86_400 + 259_200) / 2));
        assert_eq!(lt.median_secs, Some((86_400 + 259_200) / 2));
    }

    #[test]
    fn lead_time_empty_when_no_closed_rows() {
        let rows = vec![row("hq-a.1", "open", Some("2026-01-01T00:00:00Z"), None)];
        let resp = aggregate(&rows, &[GroupDim::Rig]);
        let lt = &resp.buckets[0].lead_time;
        assert_eq!(lt.count, 0);
        assert_eq!(lt.mean_secs, None);
        assert_eq!(lt.median_secs, None);
    }

    #[test]
    fn negative_or_unparseable_lead_is_skipped() {
        let rows = vec![
            // closed before created (skew) — skipped.
            row("hq-a.1", "closed", Some("2026-01-05T00:00:00Z"), Some("2026-01-01T00:00:00Z")),
            // unparseable — skipped.
            row("hq-a.2", "closed", Some("not-a-date"), Some("also-bad")),
        ];
        let resp = aggregate(&rows, &[GroupDim::Rig]);
        let b = &resp.buckets[0];
        // Both still count as closed rows...
        assert_eq!(b.closed, 2);
        // ...but neither contributes a lead-time sample.
        assert_eq!(b.lead_time.count, 0);
    }

    #[test]
    fn multi_group_by_forms_cross_product_buckets() {
        let mut r1 = row("hq-a.1", "closed", Some("2026-01-01T00:00:00Z"), Some("2026-01-02T00:00:00Z"));
        r1.assignee = Some("alice".into());
        let mut r2 = row("tobx-b.1", "open", None, None);
        r2.assignee = Some("alice".into());
        let mut r3 = row("hq-c.1", "open", None, None);
        r3.assignee = Some("bob".into());
        let resp = aggregate(&[r1, r2, r3], &[GroupDim::Assignee, GroupDim::Rig]);
        // (alice,hq) (alice,tobx) (bob,hq) => 3 buckets.
        assert_eq!(resp.buckets.len(), 3);
        let find = |a: &str, rg: &str| {
            resp.buckets
                .iter()
                .find(|b| b.key.get("assignee").map(String::as_str) == Some(a)
                    && b.key.get("rig").map(String::as_str) == Some(rg))
                .unwrap()
        };
        assert_eq!(find("alice", "hq").total, 1);
        assert_eq!(find("alice", "hq").closed, 1);
        assert_eq!(find("alice", "tobx").total, 1);
        assert_eq!(find("bob", "hq").total, 1);
        // Totals never fan out: 3 rows, 3 total.
        assert_eq!(resp.totals.total, 3);
    }

    #[test]
    fn domain_dimension_fans_multivalued_row() {
        let mut r = row("hq-a.1", "open", None, None);
        r.domain_json = r#"["platform.auth","platform.documents"]"#.into();
        let undomained = row("hq-b.1", "open", None, None); // domain_json "[]"
        let resp = aggregate(&[r, undomained], &[GroupDim::Domain]);
        // auth, documents, and the "" bucket for the undomained row.
        assert_eq!(resp.buckets.len(), 3);
        let total: u64 = resp.buckets.iter().map(|b| b.total).sum();
        // The multivalued row is counted in 2 buckets => 3 bucket-rows total across 2 source rows.
        assert_eq!(total, 3);
        // Totals fold each source row once.
        assert_eq!(resp.totals.total, 2);
    }

    #[test]
    fn assignee_owner_missing_maps_to_empty_string() {
        let rows = vec![row("hq-a.1", "open", None, None)];
        let resp = aggregate(&rows, &[GroupDim::Assignee]);
        assert_eq!(resp.buckets[0].key.get("assignee").map(String::as_str), Some(""));
    }
}
