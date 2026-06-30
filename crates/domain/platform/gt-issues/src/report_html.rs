//! HTML render of the scheduled report digest (hq-562e0b, epic hq-efc379).
//!
//! Email body for the System report scheduler: the planning bitácora
//! ([`OperatorReport`], same projection `report.generate` serializes to
//! csv/xlsx) plus the analytics KPIs ([`AnalyticsSummary`], same numbers
//! `analytics.summary` returns) — reconciliation by construction, both render
//! from the structs the existing engines already build.
//!
//! Email-client constraints: ONE standalone document, inline styles only (no
//! `<link>`/`<script>`/external CSS — Gmail strips them), table layout, same
//! navy aesthetic as the xlsx report.

use pulldown_cmark::{html::push_html, Event, Options, Parser, Tag, TagEnd};

use crate::analytics::AnalyticsSummary;
use crate::report::{OperatorReport, ReportComment, ReportSection};

/// Minimal HTML escaping for text nodes/attributes.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

/// Render a markdown string to inline-safe HTML for email clients.
/// GFM enabled (tables, strikethrough, task lists). Returns an empty string for
/// empty input so callers can use it in format strings without conditional guards.
fn md_to_html(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    let opts = Options::ENABLE_TABLES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS;
    // Strip raw HTML events so operator notes cannot inject arbitrary tags.
    let safe = Parser::new_ext(s, opts).filter(|e| {
        !matches!(
            e,
            Event::Html(_)
                | Event::InlineHtml(_)
                | Event::Start(Tag::HtmlBlock)
                | Event::End(TagEnd::HtmlBlock)
        )
    });
    let mut out = String::with_capacity(s.len() + 64);
    push_html(&mut out, safe);
    out
}

/// Render a bead/epic's comments as an inline block (gtcore-01bcf2): one entry
/// per comment showing `[autor · YYYY-MM-DD]` header and the body rendered as
/// markdown HTML. Empty input → empty string. `compact=true` drops the top
/// divider for the section-header variant.
fn render_comments(comments: &[ReportComment], compact: bool) -> String {
    if comments.is_empty() {
        return String::new();
    }
    let items: String = comments
        .iter()
        .map(|c| {
            format!(
                "<div style=\"font-size:11px;color:#555;margin-top:4px;\">\
                 <span style=\"color:{NAVY};font-weight:bold;\">[{autor}]</span>\
                 <span style=\"color:#888;font-size:10px;margin-left:4px;\">{fecha}</span>\
                 <div style=\"margin-top:2px;\">{body}</div></div>",
                autor = esc(&c.author),
                fecha = esc(&c.fecha),
                body = md_to_html(&c.body),
            )
        })
        .collect();
    let wrap = if compact {
        "margin-top:2px;"
    } else {
        "margin-top:4px;border-top:1px dashed #c9c9c9;padding-top:3px;"
    };
    format!(
        "<div style=\"{wrap}\"><div style=\"font-size:10px;color:#888;\
         text-transform:uppercase;letter-spacing:.5px;\">Comentarios</div>{items}</div>"
    )
}

fn fmt_horas(h: Option<f64>) -> String {
    h.map(|v| format!("{v:.1}")).unwrap_or_default()
}

/// The epic's own metadata as a small subline under the module band
/// (gtcore-7bf261): estado chip · est. epic Nh · responsable · inicio→fin.
/// Each part is dropped when the epic carries nothing for it; an epic with no
/// metadata at all (or the "Sin módulo" tail) yields an empty string.
fn epic_meta_line(section: &ReportSection) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !section.epic_estado.is_empty() {
        parts.push(format!(
            "<span style=\"color:{};font-weight:bold;\">{}</span>",
            estado_color(&section.epic_estado),
            esc(&section.epic_estado),
        ));
    }
    if let Some(h) = section.epic_horas {
        parts.push(format!("est. epic {h:.1} h"));
    }
    if !section.epic_responsable.is_empty() {
        parts.push(esc(&section.epic_responsable));
    }
    match (section.epic_inicio.is_empty(), section.epic_fin.is_empty()) {
        (false, false) => parts.push(format!("{}→{}", esc(&section.epic_inicio), esc(&section.epic_fin))),
        (false, true) => parts.push(esc(&section.epic_inicio)),
        (true, false) => parts.push(format!("→{}", esc(&section.epic_fin))),
        (true, true) => {}
    }
    if parts.is_empty() {
        return String::new();
    }
    format!(
        "<div style=\"font-size:11px;font-weight:normal;color:#5b6b8c;margin-top:2px;\">{}</div>",
        parts.join(" · ")
    )
}

/// Estado chip colors, mirroring the xlsx vocabulary.
fn estado_color(estado: &str) -> &'static str {
    match estado {
        "Completado" => "#1e7e34",
        "En curso" => "#b8860b",
        _ => "#6c757d",
    }
}

const NAVY: &str = "#1f3864";
const HEADER_BG: &str = "#2e5395";
const ROW_ALT: &str = "#f2f6fc";
const TD: &str = "padding:6px 8px;border:1px solid #d9e2f3;font-size:13px;vertical-align:top;";
/// Short-column cells stay on one line (the operator asked for nowrap) so the
/// narrow columns get their own width and stop bunching up; only Tarea/Notas
/// wrap, since they hold long text + comments.
const NOWRAP: &str = "white-space:nowrap;";

/// Per-module palette `(row_tint, band_tint, icon)`: each module group paints
/// its task rows with a tenuous tint, its separator band with a slightly
/// stronger shade, and carries its own icon — all cycling by module index so
/// adjacent modules are visually distinct (operator request: "una fila por
/// submódulo + colores tenues por módulo + iconos variados").
const MODULE_TINTS: &[(&str, &str, &str)] = &[
    ("#f2f6fc", "#dbe6f6", "🏢"), // blue
    ("#f1f8f1", "#dcefdc", "🧩"), // green
    ("#fdf6ee", "#f8e6d2", "⚙️"), // orange
    ("#f7f2fb", "#e7dbf3", "📊"), // purple
    ("#fbf2f3", "#f6dcdf", "🗄️"), // pink
    ("#eef9f8", "#d4efec", "🗂️"), // teal
];

/// One KPI card cell.
fn kpi(label: &str, value: String, detail: String) -> String {
    format!(
        "<td style=\"padding:10px 14px;border:1px solid #d9e2f3;background:{ROW_ALT};\
         text-align:center;\"><div style=\"font-size:11px;color:{NAVY};text-transform:uppercase;\
         letter-spacing:.5px;\">{label}</div><div style=\"font-size:22px;font-weight:bold;\
         color:{NAVY};\">{value}</div><div style=\"font-size:11px;color:#6c757d;\">{detail}\
         </div></td>"
    )
}

/// Render the digest: `(bitácora, kpis, fecha)` → standalone HTML document.
/// Pure — no IO, no clock (the caller passes `fecha`, normally
/// `summary.today`).
pub fn render_digest(report: &OperatorReport, summary: &AnalyticsSummary, fecha: &str) -> String {
    let mut html = String::with_capacity(16 * 1024);
    html.push_str(
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\"></head>\
         <body style=\"margin:0;padding:16px;background:#ffffff;\
         font-family:Arial,Helvetica,sans-serif;color:#212529;\">",
    );

    // Title bar (xlsx merged-title parity).
    let total_tasks: usize = report.sections.iter().map(|s| s.rows.len()).sum();
    html.push_str(&format!(
        "<table style=\"border-collapse:collapse;width:100%;\"><tr>\
         <td style=\"background:{NAVY};color:#ffffff;padding:12px 16px;font-size:18px;\
         font-weight:bold;\">Reporte de planning — {rig} / {ws}\
         <span style=\"font-size:13px;font-weight:normal;margin-left:12px;\
         opacity:.8;\">{total_tasks} tareas</span>\
         <span style=\"float:right;font-weight:normal;font-size:13px;\">{fecha}</span>\
         </td></tr></table>",
        rig = esc(&report.rig),
        ws = esc(&report.workspace),
        fecha = esc(fecha),
    ));

    // KPI strip — the 5 analytics headlines.
    html.push_str("<table style=\"border-collapse:collapse;width:100%;margin-top:12px;\"><tr>");
    html.push_str(&kpi(
        "Avance",
        format!("{:.0}%", summary.avance.pct),
        format!("{} / {} cerradas", summary.avance.closed, summary.avance.total),
    ));
    html.push_str(&kpi(
        "Errores",
        summary.errores.total.to_string(),
        format!("{} defectos, {} reaperturas", summary.errores.defects, summary.errores.reopens),
    ));
    html.push_str(&kpi(
        "Pendientes",
        summary.pendientes.total.to_string(),
        format!("{} abiertas, {} en curso", summary.pendientes.open, summary.pendientes.working),
    ));
    html.push_str(&kpi(
        "Retrasos",
        summary.retrasos.overdue.to_string(),
        format!("{} en riesgo ({}d)", summary.retrasos.at_risk, summary.retrasos.at_risk_days),
    ));
    html.push_str(&kpi(
        "Horas",
        format!("{:.1}", summary.horas.estimadas),
        format!(
            "{:.1} hechas / {:.1} pendientes",
            summary.horas.completadas, summary.horas.pendientes
        ),
    ));
    html.push_str("</tr></table>");

    // Bitácora: ONE flat table — every task row carries the complete column set
    // (Modulo first, CSV/Excel-style), no per-section bands. Rows stay grouped by
    // module because the sections are emitted in order.
    html.push_str("<table style=\"border-collapse:collapse;width:100%;margin-top:16px;\">");
    html.push_str(&format!(
        "<tr>{}</tr>",
        ["Modulo", "Tarea", "Proceso", "Nivel", "Horas Est.", "Estado", "Responsable",
         "Fecha Inicio", "Fecha Fin", "Notas"]
            .map(|h| format!(
                "<th style=\"{TD}{NOWRAP}background:{HEADER_BG};color:#ffffff;text-align:left;\">{h}</th>"
            ))
            .join("")
    ));
    for (mi, section) in report.sections.iter().enumerate() {
        let (row_tint, band_tint, icon) = MODULE_TINTS[mi % MODULE_TINTS.len()];
        // Group separator row per module (epic) — a full-width band with the
        // module title, its own icon, and its hours subtotal.
        html.push_str(&format!(
            "<tr><td colspan=\"10\" style=\"{TD}background:{band_tint};color:{NAVY};\
             font-weight:bold;\">{icon} {title}\
             <span style=\"float:right;font-weight:normal;color:#5b6b8c;\">Σ hijos {horas:.1} h</span>\
             {meta}</td></tr>",
            title = esc(&section.module_title),
            horas = section.horas,
            meta = epic_meta_line(section),
        ));
        // Epic-level comments (rare): a full-width row under the band.
        if !section.comentarios.is_empty() {
            html.push_str(&format!(
                "<tr><td colspan=\"10\" style=\"{TD}background:{row_tint};\">{}</td></tr>",
                render_comments(&section.comentarios, true),
            ));
        }
        for row in &section.rows {
            // Subtle per-module tint on every row of the group.
            let bg = format!("background:{row_tint};");
            html.push_str(&format!(
                "<tr><td style=\"{TD}{bg}{NOWRAP}\">{modulo}</td>\
                 <td style=\"{TD}{bg}\">{tarea}<div style=\"font-size:11px;color:#6c757d;\">\
                 {id}</div></td><td style=\"{TD}{bg}{NOWRAP}\">{proceso}</td>\
                 <td style=\"{TD}{bg}{NOWRAP}\">{nivel}</td>\
                 <td style=\"{TD}{bg}{NOWRAP}text-align:right;\">{horas}</td>\
                 <td style=\"{TD}{bg}{NOWRAP}color:{estado_color};font-weight:bold;\">{estado}</td>\
                 <td style=\"{TD}{bg}{NOWRAP}\">{resp}</td>\
                 <td style=\"{TD}{bg}{NOWRAP}\">{ini}</td>\
                 <td style=\"{TD}{bg}{NOWRAP}\">{fin}</td>\
                 <td style=\"{TD}{bg}\">{notas}{comentarios}</td></tr>",
                modulo = esc(&section.module_title),
                tarea = esc(&row.tarea),
                id = esc(&row.id),
                proceso = esc(&row.proceso),
                nivel = esc(&row.nivel),
                horas = fmt_horas(row.horas),
                estado_color = estado_color(&row.estado),
                estado = esc(&row.estado),
                resp = esc(&row.responsable),
                ini = esc(&row.fecha_inicio),
                fin = esc(&row.fecha_fin),
                notas = md_to_html(&row.notas),
                comentarios = render_comments(&row.comentarios, false),
            ));
        }
    }
    html.push_str("</table>");

    // TOTAL HORAS footer (xlsx parity).
    html.push_str(&format!(
        "<table style=\"border-collapse:collapse;width:100%;margin-top:12px;\">\
         <tr><td style=\"background:{NAVY};color:#ffffff;padding:10px 16px;font-weight:bold;\">\
         TOTAL HORAS<span style=\"float:right;\">{total:.1}</span></td></tr></table>",
        total = report.total_horas,
    ));

    html.push_str("</body></html>");
    html
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::summarize;
    use crate::report::build_report;
    use gt_store_dolt::IssueRow;

    fn row(id: &str, ty: &str, title: &str, status: &str, eref: Option<&str>) -> IssueRow {
        let mut v: IssueRow = serde_json::from_value(serde_json::json!({
            "id": id, "title": title, "status": status, "priority": 1,
            "issue_type": ty, "assignee": "ana", "owner": null,
            "created_at": null, "updated_at": null, "closed_at": null,
            "external_ref": eref, "spec_id": null, "role_scope": null,
        }))
        .expect("row");
        v.estimated_hours = Some(2.0);
        v.start_date = Some("2026-06-01".into());
        v.due_date = Some("2026-06-30".into());
        v.notes = Some("nota <x>".into());
        v
    }

    #[test]
    fn digest_contains_every_row_and_the_five_kpis() {
        let rows = vec![
            row("hq-e1", "epic", "Modulo Uno", "open", None),
            row("hq-1", "task", "Tarea A", "closed", Some("hq-e1")),
            row("hq-2", "task", "Tarea B", "working", Some("hq-e1")),
            row("hq-3", "bug", "Suelta", "open", None),
        ];
        let mut parent_map = std::collections::HashMap::new();
        parent_map.insert("hq-1".to_string(), "hq-e1".to_string());
        parent_map.insert("hq-2".to_string(), "hq-e1".to_string());
        // A comment on a bead (renders in its Notas cell) and one on the epic
        // (renders in the section header) — gtcore-01bcf2.
        let mut comments = std::collections::HashMap::new();
        comments.insert(
            "hq-1".to_string(),
            vec![crate::report::ReportComment {
                author: "ana".into(),
                fecha: "2026-06-10".into(),
                body: "revisar <edge> case".into(),
            }],
        );
        comments.insert(
            "hq-e1".to_string(),
            vec![crate::report::ReportComment {
                author: "leo".into(),
                fecha: "2026-06-11".into(),
                body: "modulo bloqueado".into(),
            }],
        );
        let report = build_report("hq", "default", &rows, &parent_map, &comments);
        let summary = summarize("hq", "default", &rows, 0, "2026-06-12", 7, 30, &parent_map);
        let html = render_digest(&report, &summary, "2026-06-12");

        // Standalone, no external resources.
        assert!(html.starts_with("<!DOCTYPE html>"));
        assert!(!html.contains("<link") && !html.contains("<script"));
        // Every task row present; epic titles section, not row.
        for needle in ["Tarea A", "Tarea B", "Suelta", "Modulo Uno", "Sin módulo"] {
            assert!(html.contains(needle), "missing {needle}");
        }
        // The five KPIs.
        for kpi in ["Avance", "Errores", "Pendientes", "Retrasos", "Horas"] {
            assert!(html.contains(kpi), "missing KPI {kpi}");
        }
        // Flat table: the Modulo column is a real header (not a section band).
        assert!(html.contains(">Modulo</th>"), "missing flat Modulo column header");
        // Per-module separator band + tenuous module tint on the rows.
        assert!(html.contains("🏢 Modulo Uno"), "missing module separator band");
        assert!(html.contains("background:#dbe6f6"), "missing module band tint");
        assert!(html.contains("background:#f2f6fc"), "missing module row tint");
        // The band differentiates the children rollup from the epic's own
        // metadata subline (gtcore-7bf261): estado · est. epic Nh · resp · dates.
        assert!(html.contains("Σ hijos"), "missing children-rollup label");
        assert!(html.contains("est. epic 2.0 h"), "missing epic's own estimate");
        assert!(html.contains("Pendiente"), "missing epic estado");
        assert!(html.contains("2026-06-01→2026-06-30"), "missing epic date span");
        // Reconciles with the summary by construction.
        assert!(html.contains(&format!("{:.0}%", summary.avance.pct)));
        assert!(html.contains("3 tareas"), "missing task count in header");
        assert!(html.contains("TOTAL HORAS"));
        assert!(html.contains(&format!("{:.1}", report.total_horas)));
        // Safe-HTML: raw HTML tags in notes are stripped by the safe markdown
        // renderer (not escaped, just dropped) — the text "nota" survives but
        // the `<x>` injection does not appear in the output in any form.
        assert!(html.contains("nota"), "nota text must appear");
        assert!(!html.contains("<x>"), "raw <x> tag must not pass through");
        // Comments render with [autor] date + markdown body (gtcore-01bcf2).
        assert!(html.contains("Comentarios"), "missing comments block");
        assert!(html.contains("[ana]"), "missing bead comment author");
        assert!(html.contains("2026-06-10"), "bead comment must show fecha");
        // body goes through md_to_html — raw markdown angle-brackets in the
        // body text are rendered as HTML, not escaped as &lt;/&gt;.
        assert!(html.contains("revisar"), "bead comment body missing");
        assert!(html.contains("[leo]"), "missing epic comment author");
        assert!(html.contains("2026-06-11"), "epic comment must show fecha");
        assert!(html.contains("modulo bloqueado"), "missing epic comment body");
    }

    /// True when `html` contains a `[YYYY-MM-DD …]` bracket — the timestamp
    /// prefix the digest used to put in front of folded comments. The render
    /// test (gtcore-abc0ae AC) asserts this is absent from the Notas fold.
    pub(crate) fn has_dated_comment_bracket(html: &str) -> bool {
        // Scan for "[dddd-dd-dd" (4-2-2 digits with dashes, right after a `[`).
        let bytes = html.as_bytes();
        let is_digit = |i: usize| bytes.get(i).is_some_and(u8::is_ascii_digit);
        for (i, &b) in bytes.iter().enumerate() {
            if b != b'[' {
                continue;
            }
            let d = i + 1;
            if is_digit(d)
                && is_digit(d + 1)
                && is_digit(d + 2)
                && is_digit(d + 3)
                && bytes.get(d + 4) == Some(&b'-')
                && is_digit(d + 5)
                && is_digit(d + 6)
                && bytes.get(d + 7) == Some(&b'-')
                && is_digit(d + 8)
                && is_digit(d + 9)
            {
                return true;
            }
        }
        false
    }
}
