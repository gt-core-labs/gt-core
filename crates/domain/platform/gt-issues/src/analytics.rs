//! The Kanban analytics projection (hq-1cd840, epic hq-56b5ee).
//!
//! Plane-style summary over the SAME rows `board.list` and the report engine
//! read (no new store, read-only): the operator's four headline KPIs —
//! AVANCE / ERRORES / PENDIENTES / RETRASOS — plus the chart series (burndown,
//! work distribution, created-vs-resolved). Pure functions over [`IssueRow`]
//! (+ a reopen count the caller derives from the audit trail), so the numbers
//! reconcile with the board and the report by construction.

use std::collections::BTreeMap;

use serde::Serialize;

use gt_store_dolt::IssueRow;

/// The defect `issue_type`s ERRORES counts (the ADR's pinned definition: the
/// defect type plus audit-derived reopens, which ride in separately).
const DEFECT_TYPES: &[&str] = &["bug", "fix", "defect"];

/// One key→count pair of a distribution.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Slice {
    /// The bucket key (assignee, status, `P{n}`, module id…); `""` = none.
    pub key: String,
    /// Row count.
    pub count: i64,
}

/// One day of a time series (`YYYY-MM-DD`).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DayPoint {
    /// The day.
    pub date: String,
    /// Created that day.
    pub created: i64,
    /// Closed that day.
    pub closed: i64,
    /// Open scope remaining at end of day (burndown line).
    pub remaining: i64,
    /// Estimated hours remaining at end of day (hours burndown; unestimated
    /// cards weigh 0).
    pub remaining_hours: f64,
}

/// AVANCE — progress.
#[derive(Debug, Clone, Serialize)]
pub struct Avance {
    /// Closed rows.
    pub closed: i64,
    /// All rows in scope.
    pub total: i64,
    /// `closed / total` in percent (0 when the scope is empty).
    pub pct: f64,
    /// Per-module progress (key = module epic id, `""` = sin módulo).
    pub by_module: Vec<ModuleProgress>,
}

/// One module's progress bar.
#[derive(Debug, Clone, Serialize)]
pub struct ModuleProgress {
    /// The module epic id (`""` = sin módulo).
    pub module: String,
    /// Closed rows in the module.
    pub closed: i64,
    /// All the module's rows.
    pub total: i64,
}

/// ERRORES — defects.
#[derive(Debug, Clone, Serialize)]
pub struct Errores {
    /// Live + closed defect-type beads (`bug`/`fix`/`defect`).
    pub defects: i64,
    /// Reopened transitions observed in the audit trail (caller-supplied).
    pub reopens: i64,
    /// `defects + reopens` — the headline number.
    pub total: i64,
    /// Defects per module.
    pub by_module: Vec<Slice>,
    /// Defects per Nivel (`P0`/`P1`/`P2`).
    pub by_nivel: Vec<Slice>,
}

/// PENDIENTES — the open backlog.
#[derive(Debug, Clone, Serialize)]
pub struct Pendientes {
    /// `status = open`.
    pub open: i64,
    /// `status = working`.
    pub working: i64,
    /// `open + working` — the headline number.
    pub total: i64,
    /// Pending per assignee (`""` = sin asignar).
    pub by_assignee: Vec<Slice>,
    /// Pending per priority (`P0`…).
    pub by_priority: Vec<Slice>,
    /// Pending per module.
    pub by_module: Vec<Slice>,
}

/// HORAS — the planning estimates (`estimated_hours`, mockup "Horas Est.",
/// hq-62130a). Sums over the same task rows the other KPIs read; cards without
/// an estimate contribute 0 and are surfaced via `sin_estimar`.
#[derive(Debug, Clone, Serialize)]
pub struct Horas {
    /// Sum of `estimated_hours` over every task in scope.
    pub estimadas: f64,
    /// Sum over `status = closed` tasks (work delivered, in plan hours).
    pub completadas: f64,
    /// Sum over open + working tasks (plan hours still outstanding).
    pub pendientes: f64,
    /// Tasks with no estimate — the planning-coverage gap.
    pub sin_estimar: i64,
    /// Per-module hours (key = module epic id, `""` = sin módulo).
    pub by_module: Vec<ModuleHours>,
}

/// One module's estimated/completed hours.
#[derive(Debug, Clone, Serialize)]
pub struct ModuleHours {
    /// The module epic id (`""` = sin módulo).
    pub module: String,
    /// Sum of `estimated_hours` over the module's tasks.
    pub estimadas: f64,
    /// Sum over the module's closed tasks.
    pub completadas: f64,
}

/// RETRASOS — overdue + at-risk.
#[derive(Debug, Clone, Serialize)]
pub struct Retrasos {
    /// `due_date < today AND status != closed` — the headline number.
    pub overdue: i64,
    /// Due within the at-risk window (and not yet overdue/closed).
    pub at_risk: i64,
    /// The configured window in days (echoed).
    pub at_risk_days: i64,
    /// Days-late distribution buckets: `1-7`, `8-30`, `>30`.
    pub days_late: Vec<Slice>,
    /// The overdue card ids (worst first), capped at 50 for the drill-down.
    pub overdue_ids: Vec<String>,
}

/// The full `analytics.summary` payload.
#[derive(Debug, Clone, Serialize)]
pub struct AnalyticsSummary {
    /// Scope echo.
    pub rig: String,
    /// Scope echo.
    pub workspace: String,
    /// `today` used for the date math (`YYYY-MM-DD`), echoed for reproducibility.
    pub today: String,
    pub avance: Avance,
    pub errores: Errores,
    pub pendientes: Pendientes,
    pub retrasos: Retrasos,
    pub horas: Horas,
    /// Work distribution by status (whole scope).
    pub by_status: Vec<Slice>,
    /// Created-vs-resolved + burndown, last `days` days, oldest first.
    pub series: Vec<DayPoint>,
}

fn day_of(ts: &Option<String>) -> Option<String> {
    ts.as_deref().map(|t| t.get(..10).unwrap_or(t).to_string())
}

fn slices(map: BTreeMap<String, i64>) -> Vec<Slice> {
    let mut v: Vec<Slice> = map.into_iter().map(|(key, count)| Slice { key, count }).collect();
    v.sort_by(|a, b| b.count.cmp(&a.count).then(a.key.cmp(&b.key)));
    v
}

/// Days between two `YYYY-MM-DD` strings (`b - a`), via a simple
/// days-since-epoch conversion (proleptic Gregorian; fine for tracker dates).
fn days_between(a: &str, b: &str) -> Option<i64> {
    Some(epoch_days(b)? - epoch_days(a)?)
}

fn epoch_days(date: &str) -> Option<i64> {
    let mut it = date.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    // Howard Hinnant's days_from_civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe - 719468)
}

fn date_of_epoch_days(z: i64) -> String {
    // Inverse of days_from_civil.
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// Build the summary. `rows` = the (rig, workspace) scope exactly as
/// `board.list`/the report read it (epics included — they title modules and are
/// excluded from task KPIs, mirroring the report projection). `parent_map` maps
/// child issue id → parent epic id (from `issue_relations` `child_of` rows).
/// `reopens` is the audit-derived reopened-transition count the caller supplies.
/// `today` is `YYYY-MM-DD` (server clock at the edge); `at_risk_days` the
/// configurable window; `series_days` the chart span.
pub fn summarize(
    rig: &str,
    workspace: &str,
    rows: &[IssueRow],
    reopens: i64,
    today: &str,
    at_risk_days: i64,
    series_days: i64,
    parent_map: &std::collections::HashMap<String, String>,
) -> AnalyticsSummary {
    let tasks: Vec<&IssueRow> = rows.iter().filter(|r| r.issue_type != "epic").collect();

    // AVANCE
    let total = tasks.len() as i64;
    let closed = tasks.iter().filter(|r| r.status == "closed").count() as i64;
    let mut module_acc: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for t in &tasks {
        let m = parent_map.get(&t.id).cloned().unwrap_or_default();
        let e = module_acc.entry(m).or_default();
        e.1 += 1;
        if t.status == "closed" {
            e.0 += 1;
        }
    }
    let avance = Avance {
        closed,
        total,
        pct: if total == 0 { 0.0 } else { (closed as f64 / total as f64) * 100.0 },
        by_module: module_acc
            .into_iter()
            .map(|(module, (closed, total))| ModuleProgress { module, closed, total })
            .collect(),
    };

    // ERRORES
    let defects: Vec<&&IssueRow> =
        tasks.iter().filter(|r| DEFECT_TYPES.contains(&r.issue_type.as_str())).collect();
    let mut err_mod: BTreeMap<String, i64> = BTreeMap::new();
    let mut err_nivel: BTreeMap<String, i64> = BTreeMap::new();
    for d in &defects {
        *err_mod.entry(parent_map.get(&d.id).cloned().unwrap_or_default()).or_default() += 1;
        *err_nivel.entry(format!("P{}", d.priority)).or_default() += 1;
    }
    let errores = Errores {
        defects: defects.len() as i64,
        reopens,
        total: defects.len() as i64 + reopens,
        by_module: slices(err_mod),
        by_nivel: slices(err_nivel),
    };

    // PENDIENTES
    let open = tasks.iter().filter(|r| r.status == "open").count() as i64;
    let working = tasks.iter().filter(|r| r.status == "working").count() as i64;
    let mut p_assignee: BTreeMap<String, i64> = BTreeMap::new();
    let mut p_priority: BTreeMap<String, i64> = BTreeMap::new();
    let mut p_module: BTreeMap<String, i64> = BTreeMap::new();
    for t in tasks.iter().filter(|r| r.status != "closed") {
        *p_assignee.entry(t.assignee.clone().unwrap_or_default()).or_default() += 1;
        *p_priority.entry(format!("P{}", t.priority)).or_default() += 1;
        *p_module.entry(parent_map.get(&t.id).cloned().unwrap_or_default()).or_default() += 1;
    }
    let pendientes = Pendientes {
        open,
        working,
        total: open + working,
        by_assignee: slices(p_assignee),
        by_priority: slices(p_priority),
        by_module: slices(p_module),
    };

    // RETRASOS
    let mut overdue: Vec<(i64, String)> = Vec::new(); // (days late, id)
    let mut at_risk = 0i64;
    let mut late_buckets: BTreeMap<String, i64> = BTreeMap::new();
    for t in tasks.iter().filter(|r| r.status != "closed") {
        let Some(due) = &t.due_date else { continue };
        let Some(diff) = days_between(due.as_str(), today) else { continue };
        if diff > 0 {
            overdue.push((diff, t.id.clone()));
            let bucket = if diff <= 7 { "1-7" } else if diff <= 30 { "8-30" } else { ">30" };
            *late_buckets.entry(bucket.to_string()).or_default() += 1;
        } else if -diff <= at_risk_days {
            at_risk += 1;
        }
    }
    overdue.sort_by(|a, b| b.0.cmp(&a.0));
    let retrasos = Retrasos {
        overdue: overdue.len() as i64,
        at_risk,
        at_risk_days,
        days_late: slices(late_buckets),
        overdue_ids: overdue.into_iter().take(50).map(|(_, id)| id).collect(),
    };

    // HORAS
    let mut estimadas = 0.0f64;
    let mut completadas = 0.0f64;
    let mut h_pendientes = 0.0f64;
    let mut sin_estimar = 0i64;
    let mut h_module: BTreeMap<String, (f64, f64)> = BTreeMap::new(); // (estimadas, completadas)
    for t in &tasks {
        let Some(h) = t.estimated_hours else {
            sin_estimar += 1;
            continue;
        };
        estimadas += h;
        let e = h_module.entry(parent_map.get(&t.id).cloned().unwrap_or_default()).or_default();
        e.0 += h;
        if t.status == "closed" {
            completadas += h;
            e.1 += h;
        } else {
            h_pendientes += h;
        }
    }
    let horas = Horas {
        estimadas,
        completadas,
        pendientes: h_pendientes,
        sin_estimar,
        by_module: h_module
            .into_iter()
            .map(|(module, (estimadas, completadas))| ModuleHours { module, estimadas, completadas })
            .collect(),
    };

    // Distribution by status (whole scope).
    let mut by_status: BTreeMap<String, i64> = BTreeMap::new();
    for t in &tasks {
        *by_status.entry(t.status.clone()).or_default() += 1;
    }

    // Series: created-vs-resolved per day + burndown remaining, last N days.
    let today_days = epoch_days(today).unwrap_or(0);
    let start = today_days - series_days + 1;
    let mut created_by_day: BTreeMap<i64, i64> = BTreeMap::new();
    let mut closed_by_day: BTreeMap<i64, i64> = BTreeMap::new();
    let mut created_h_by_day: BTreeMap<i64, f64> = BTreeMap::new();
    let mut closed_h_by_day: BTreeMap<i64, f64> = BTreeMap::new();
    let mut created_before = 0i64;
    let mut closed_before = 0i64;
    let mut created_h_before = 0.0f64;
    let mut closed_h_before = 0.0f64;
    for t in &tasks {
        let h = t.estimated_hours.unwrap_or(0.0);
        if let Some(c) = day_of(&t.created_at).and_then(|d| epoch_days(&d)) {
            if c < start {
                created_before += 1;
                created_h_before += h;
            } else if c <= today_days {
                *created_by_day.entry(c).or_default() += 1;
                *created_h_by_day.entry(c).or_default() += h;
            }
        }
        if let Some(c) = day_of(&t.closed_at).and_then(|d| epoch_days(&d)) {
            if c < start {
                closed_before += 1;
                closed_h_before += h;
            } else if c <= today_days {
                *closed_by_day.entry(c).or_default() += 1;
                *closed_h_by_day.entry(c).or_default() += h;
            }
        }
    }
    let mut remaining = created_before - closed_before;
    let mut remaining_hours = created_h_before - closed_h_before;
    let mut series = Vec::with_capacity(series_days.max(0) as usize);
    for day in start..=today_days {
        let created = created_by_day.get(&day).copied().unwrap_or(0);
        let closed = closed_by_day.get(&day).copied().unwrap_or(0);
        remaining += created - closed;
        remaining_hours +=
            created_h_by_day.get(&day).copied().unwrap_or(0.0) - closed_h_by_day.get(&day).copied().unwrap_or(0.0);
        series.push(DayPoint {
            date: date_of_epoch_days(day),
            created,
            closed,
            remaining,
            remaining_hours,
        });
    }

    AnalyticsSummary {
        rig: rig.to_string(),
        workspace: workspace.to_string(),
        today: today.to_string(),
        avance,
        errores,
        pendientes,
        retrasos,
        horas,
        by_status: slices(by_status),
        series,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::*;

    fn row(
        id: &str,
        ty: &str,
        status: &str,
        prio: i32,
        assignee: Option<&str>,
        created: &str,
        closed: Option<&str>,
        due: Option<&str>,
        hours: Option<f64>,
    ) -> IssueRow {
        let mut v: IssueRow = serde_json::from_value(serde_json::json!({
            "id": id, "title": id, "status": status, "priority": prio,
            "issue_type": ty, "assignee": assignee, "owner": null,
            "created_at": format!("{created}T10:00:00Z"),
            "updated_at": null,
            "closed_at": closed.map(|c| format!("{c}T10:00:00Z")),
            "spec_id": null, "role_scope": null,
        }))
        .expect("row");
        v.due_date = due.map(str::to_string);
        v.estimated_hours = hours;
        v
    }

    fn sample() -> (Vec<IssueRow>, HashMap<String, String>) {
        let rows = vec![
            row("hq-mod", "epic", "open", 1, None, "2026-06-01", None, None, None),
            row("hq-1", "task", "closed", 0, Some("ana"), "2026-06-01", Some("2026-06-05"), None, Some(5.0)),
            row("hq-2", "task", "working", 1, Some("ana"), "2026-06-02", None, Some("2026-06-01"), Some(3.0)),
            row("hq-3", "bug", "open", 0, Some("bob"), "2026-06-08", None, Some("2026-06-12"), None),
            row("hq-4", "task", "open", 2, None, "2026-06-09", None, Some("2026-07-30"), Some(2.0)),
        ];
        let mut parent_map = HashMap::new();
        parent_map.insert("hq-1".to_string(), "hq-mod".to_string());
        parent_map.insert("hq-2".to_string(), "hq-mod".to_string());
        parent_map.insert("hq-3".to_string(), "hq-mod".to_string());
        (rows, parent_map)
    }

    #[test]
    fn kpis_match_their_pinned_definitions() {
        let (sample_rows, parent_map) = sample();
        let s = summarize("hq", "default", &sample_rows, 2, "2026-06-10", 3, 14, &parent_map);
        // AVANCE: 1 closed of 4 tasks (the epic is never a task row).
        assert_eq!((s.avance.closed, s.avance.total), (1, 4));
        assert!((s.avance.pct - 25.0).abs() < 1e-9);
        // ERRORES: 1 bug + 2 audit reopens.
        assert_eq!((s.errores.defects, s.errores.reopens, s.errores.total), (1, 2, 3));
        assert_eq!(s.errores.by_nivel[0], Slice { key: "P0".into(), count: 1 });
        // PENDIENTES: 2 open + 1 working.
        assert_eq!((s.pendientes.open, s.pendientes.working, s.pendientes.total), (2, 1, 3));
        // RETRASOS: hq-2 due 06-01 (9 days late) overdue; hq-3 due 06-12 within
        // the 3-day window = at risk; hq-4 due 07-30 = neither.
        assert_eq!((s.retrasos.overdue, s.retrasos.at_risk), (1, 1));
        assert_eq!(s.retrasos.overdue_ids, vec!["hq-2".to_string()]);
        assert_eq!(s.retrasos.days_late, vec![Slice { key: "8-30".into(), count: 1 }]);
        // HORAS: 5 (closed) + 3 (working) + 2 (open) estimated; hq-3 unestimated.
        assert!((s.horas.estimadas - 10.0).abs() < 1e-9);
        assert!((s.horas.completadas - 5.0).abs() < 1e-9);
        assert!((s.horas.pendientes - 5.0).abs() < 1e-9);
        assert_eq!(s.horas.sin_estimar, 1);
        // by_module: hq-mod has 8h estimated / 5h completed; "" has 2h / 0h.
        let m: Vec<(&str, f64, f64)> = s
            .horas
            .by_module
            .iter()
            .map(|m| (m.module.as_str(), m.estimadas, m.completadas))
            .collect();
        assert_eq!(m, vec![("", 2.0, 0.0), ("hq-mod", 8.0, 5.0)]);
    }

    #[test]
    fn series_tracks_created_closed_and_burndown_remaining() {
        let (sample_rows, parent_map) = sample();
        let s = summarize("hq", "default", &sample_rows, 0, "2026-06-10", 3, 10, &parent_map);
        assert_eq!(s.series.len(), 10);
        assert_eq!(s.series.first().unwrap().date, "2026-06-01");
        // 06-01: hq-1 created (the epic is excluded) → remaining 1 (5h).
        assert_eq!(
            s.series[0],
            DayPoint { date: "2026-06-01".into(), created: 1, closed: 0, remaining: 1, remaining_hours: 5.0 }
        );
        // 06-05: hq-1 closes → remaining drops back to 1 (hq-2 arrived 06-02),
        // hours drop to hq-2's 3h.
        let d5 = &s.series[4];
        assert_eq!((d5.closed, d5.remaining), (1, 1));
        assert!((d5.remaining_hours - 3.0).abs() < 1e-9);
        // End of window: 4 created, 1 closed → 3 remaining (3h + 0h + 2h).
        assert_eq!(s.series.last().unwrap().remaining, 3);
        assert!((s.series.last().unwrap().remaining_hours - 5.0).abs() < 1e-9);
    }

    #[test]
    fn closed_rows_with_due_dates_never_count_as_overdue() {
        let rows = vec![row(
            "hq-x", "task", "closed", 1, None, "2026-06-01", Some("2026-06-02"), Some("2026-05-01"), None,
        )];
        let s = summarize("hq", "default", &rows, 0, "2026-06-10", 7, 7, &HashMap::new());
        assert_eq!(s.retrasos.overdue, 0);
    }

    #[test]
    fn date_math_round_trips() {
        for d in ["2026-06-10", "2024-02-29", "2026-01-01", "2025-12-31"] {
            assert_eq!(date_of_epoch_days(epoch_days(d).unwrap()), d);
        }
        assert_eq!(days_between("2026-06-01", "2026-06-10"), Some(9));
        assert_eq!(days_between("2026-06-10", "2026-06-01"), Some(-9));
    }
}
