//! The operator report projection (hq-fc7d6a, epic hq-56b5ee).
//!
//! Renders the tracker mockup: per-module sections with columns Modulo / Tarea
//! / Proceso / Nivel / Horas Est. / Estado / Responsable / Fecha Inicio /
//! Fecha Fin / Notas, and a TOTAL HORAS footer. PURE projection over the same
//! rows `board.list` reads (ADR: no second card storage) — a module IS an epic
//! (D5, epic-per-module via `external_ref`): epic rows title the sections and
//! are not task rows themselves; cards without an epic collect under "Sin
//! módulo".
//!
//! Two serializers: CSV (text, attachable as an `md`-class document) and XLSX
//! (`rust_xlsxwriter`, binary — the blob store keeps the bytes). Delivery
//! (document attach + optional outbox email) is the composition handler's job.

use std::collections::BTreeMap;
use std::collections::HashMap;

use serde::Serialize;

use gt_store_dolt::IssueRow;

/// One comment on a bead/epic, projected for the report (gtcore-01bcf2).
///
/// The comments themselves live in a SEPARATE store (Postgres `comments`, keyed
/// by bead id); the composition layer loads them and maps each to this plain
/// struct so this domain crate stays free of the PG/clock dependency. `fecha` is
/// the pre-formatted `YYYY-MM-DD` creation date.
#[derive(Debug, Clone, Serialize)]
pub struct ReportComment {
    /// Authoring actor (server-injected at creation).
    pub author: String,
    /// Creation date, pre-formatted `YYYY-MM-DD`.
    pub fecha: String,
    /// The comment body (rendered/escaped by the consumer).
    pub body: String,
}

/// One task row of the report, mockup column order.
#[derive(Debug, Clone, Serialize)]
pub struct ReportRow {
    /// The bead id (xlsx Módulo column reference; not a CSV column).
    pub id: String,
    /// Tarea — the card title.
    pub tarea: String,
    /// Proceso — the card's `issue_type` (task/spike/…).
    pub proceso: String,
    /// Nivel — the priority (0 = highest), rendered `P0`/`P1`/`P2`.
    pub nivel: String,
    /// Horas Est. — `estimated_hours`, blank when unplanned.
    pub horas: Option<f64>,
    /// Estado — the status in operator vocabulary.
    pub estado: String,
    /// Responsable — the assignee, blank when unassigned.
    pub responsable: String,
    /// Fecha Inicio (`YYYY-MM-DD`).
    pub fecha_inicio: String,
    /// Fecha Fin (`YYYY-MM-DD`).
    pub fecha_fin: String,
    /// Notas — the card's free-form notes.
    pub notas: String,
    /// Comentarios — the bead's threaded comments (gtcore-01bcf2). Empty when
    /// the board carries none; rendered inside the Notas column by the digest.
    pub comentarios: Vec<ReportComment>,
}

/// One module section: the epic the cards hang on (ADR D5).
#[derive(Debug, Clone, Serialize)]
pub struct ReportSection {
    /// The epic bead id backing the module (empty for the no-module tail).
    pub module_id: String,
    /// Modulo — the epic title (or "Sin módulo").
    pub module_title: String,
    /// The section's task rows.
    pub rows: Vec<ReportRow>,
    /// The CHILDREN rollup: sum of the section's task-row Horas Est. Distinct
    /// from [`Self::epic_horas`] (the epic's OWN estimate) — gtcore-6621af keeps
    /// both so neither signal is lost.
    pub horas: f64,
    /// Comentarios on the module epic itself (gtcore-01bcf2). The epic is a
    /// section header, not a row, so its comments render in the section header.
    /// Empty for the "Sin módulo" tail (no backing epic).
    pub comentarios: Vec<ReportComment>,
    /// Estado of the epic itself, operator vocabulary (gtcore-6621af). Empty for
    /// the "Sin módulo" tail or an epic outside the row set.
    pub epic_estado: String,
    /// The epic's OWN Horas Est. (`estimated_hours`) — its planning estimate,
    /// not the children rollup ([`Self::horas`]). `None` when unplanned or no
    /// backing epic.
    pub epic_horas: Option<f64>,
    /// Responsable of the epic itself (its assignee). Empty when unassigned or
    /// no backing epic.
    pub epic_responsable: String,
    /// Fecha Inicio of the epic itself (`YYYY-MM-DD`). Empty when unset.
    pub epic_inicio: String,
    /// Fecha Fin of the epic itself (`YYYY-MM-DD`). Empty when unset.
    pub epic_fin: String,
    /// Notas of the epic itself. Empty when none or no backing epic.
    pub epic_notas: String,
}

/// The whole operator report.
#[derive(Debug, Clone, Serialize)]
pub struct OperatorReport {
    /// Rig half of the board scope key.
    pub rig: String,
    /// Workspace half.
    pub workspace: String,
    /// Per-module sections, module title order; "Sin módulo" last.
    pub sections: Vec<ReportSection>,
    /// TOTAL HORAS — the grand Horas Est. sum.
    pub total_horas: f64,
}

/// The operator vocabulary for the status column.
fn estado_label(status: &str) -> &'static str {
    match status {
        "open" => "Pendiente",
        "working" => "En curso",
        "closed" => "Completado",
        _ => "Pendiente",
    }
}

/// The operator vocabulary for the Nivel column (mockup: Alto/Medio/Bajo).
fn nivel_label(priority: i32) -> &'static str {
    match priority {
        0 => "Alto",
        1 => "Medio",
        _ => "Bajo",
    }
}

/// Build the report from one (rig, workspace)'s tracker rows — the same rows
/// `board.list` projects. Epics title the module sections (D5) and are not
/// task rows; everything else groups under its parent epic from `parent_map`
/// (`issue_relations` `child_of` rows, keyed by child id).
///
/// `comments` is keyed by bead/epic id (gtcore-01bcf2): a task row's comments
/// attach to its [`ReportRow`], an epic's to the [`ReportSection`] it titles.
/// Pass an empty map when comments are not loaded (xlsx/csv paths).
pub fn build_report(
    rig: &str,
    workspace: &str,
    rows: &[IssueRow],
    parent_map: &HashMap<String, String>,
    comments: &HashMap<String, Vec<ReportComment>>,
) -> OperatorReport {
    // Module epics present in the row set, keyed by id. The section header
    // draws its title AND its own metadata (estado/horas/responsable/fechas/
    // notas) from the epic row — the epic is a data source, not just a label
    // (gtcore-6621af).
    let mut epics: BTreeMap<&str, &IssueRow> = BTreeMap::new();
    for row in rows {
        if row.issue_type == "epic" {
            epics.insert(row.id.as_str(), row);
        }
    }

    // Bucket task rows per module epic.
    let mut buckets: BTreeMap<String, Vec<ReportRow>> = BTreeMap::new();
    for row in rows {
        if row.issue_type == "epic" {
            continue;
        }
        let module = parent_map.get(&row.id).cloned().unwrap_or_default();
        buckets.entry(module).or_default().push(ReportRow {
            id: row.id.clone(),
            tarea: row.title.clone(),
            proceso: row.issue_type.clone(),
            nivel: nivel_label(row.priority).to_string(),
            horas: row.estimated_hours,
            estado: estado_label(&row.status).to_string(),
            responsable: row.assignee.clone().unwrap_or_default(),
            fecha_inicio: row.start_date.clone().unwrap_or_default(),
            fecha_fin: row.due_date.clone().unwrap_or_default(),
            notas: row.notes.clone().unwrap_or_default(),
            comentarios: comments.get(&row.id).cloned().unwrap_or_default(),
        });
    }

    let mut sections: Vec<ReportSection> = Vec::new();
    let mut tail: Option<ReportSection> = None;
    for (module_id, rows) in buckets {
        let horas: f64 = rows.iter().filter_map(|r| r.horas).sum();
        // Epic-level comments hang on the module epic id (empty for "Sin módulo").
        let comentarios = comments.get(&module_id).cloned().unwrap_or_default();
        // The backing epic row (absent for "Sin módulo" or an epic filtered out
        // of the row set — gtcore-f46a56 loads it on the per-epic export path).
        let epic = epics.get(module_id.as_str()).copied();
        let section = ReportSection {
            module_title: if module_id.is_empty() {
                "Sin módulo".to_string()
            } else {
                epic.map(|e| e.title.clone())
                    // The epic may live outside the filtered row set; its id
                    // still names the module.
                    .unwrap_or_else(|| module_id.clone())
            },
            epic_estado: epic.map(|e| estado_label(&e.status).to_string()).unwrap_or_default(),
            epic_horas: epic.and_then(|e| e.estimated_hours),
            epic_responsable: epic.and_then(|e| e.assignee.clone()).unwrap_or_default(),
            epic_inicio: epic.and_then(|e| e.start_date.clone()).unwrap_or_default(),
            epic_fin: epic.and_then(|e| e.due_date.clone()).unwrap_or_default(),
            epic_notas: epic.and_then(|e| e.notes.clone()).unwrap_or_default(),
            module_id,
            rows,
            horas,
            comentarios,
        };
        if section.module_id.is_empty() {
            tail = Some(section);
        } else {
            sections.push(section);
        }
    }
    sections.sort_by(|a, b| a.module_title.cmp(&b.module_title));
    if let Some(t) = tail {
        sections.push(t); // "Sin módulo" always last
    }

    let total_horas = sections.iter().map(|s| s.horas).sum();
    OperatorReport {
        rig: rig.to_string(),
        workspace: workspace.to_string(),
        sections,
        total_horas,
    }
}

/// Escape one CSV field (RFC 4180: quote when it carries `,`/`"`/newline).
fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Serialize the report as CSV, mockup column order, one header row, the
/// module on every row (a flat file has no merged section headers), and the
/// TOTAL HORAS footer.
pub fn to_csv(report: &OperatorReport) -> String {
    let mut out = String::from(
        "Modulo,Tarea,Proceso,Nivel,Horas Est.,Estado,Responsable,Fecha Inicio,Fecha Fin,Notas\n",
    );
    for section in &report.sections {
        // Module header row carrying the epic's OWN metadata (gtcore-7bf261):
        // the flat CSV has no bands, so the epic's estado/horas/responsable/
        // dates/notas ride on a `(epic)` row ahead of its task rows. Skipped for
        // the "Sin módulo" tail (no backing epic).
        if !section.module_id.is_empty() {
            let epic_cols = [
                csv_field(&section.module_title),
                csv_field("(epic)"),
                csv_field("epic"),
                String::new(),
                section.epic_horas.map(|h| h.to_string()).unwrap_or_default(),
                csv_field(&section.epic_estado),
                csv_field(&section.epic_responsable),
                section.epic_inicio.clone(),
                section.epic_fin.clone(),
                csv_field(&section.epic_notas),
            ];
            out.push_str(&epic_cols.join(","));
            out.push('\n');
        }
        for row in &section.rows {
            let cols = [
                csv_field(&section.module_title),
                csv_field(&row.tarea),
                csv_field(&row.proceso),
                csv_field(&row.nivel),
                row.horas.map(|h| h.to_string()).unwrap_or_default(),
                csv_field(&row.estado),
                csv_field(&row.responsable),
                row.fecha_inicio.clone(),
                row.fecha_fin.clone(),
                csv_field(&row.notas),
            ];
            out.push_str(&cols.join(","));
            out.push('\n');
        }
    }
    out.push_str(&format!("TOTAL HORAS,,,,{},,,,,\n", report.total_horas));
    out
}

/// Serialize the report as XLSX in the operator mockup aesthetic (hq-039316):
/// merged navy title bar, navy header row (white bold), light-blue module
/// section bands, color-coded Nivel (Alto rojo / Medio ámbar / Bajo verde) and
/// Estado (Completado verde / Pendiente naranja / En curso azul), bordered data
/// grid, and a navy TOTAL HORAS footer.
pub fn to_xlsx(report: &OperatorReport) -> Result<Vec<u8>, String> {
    use rust_xlsxwriter::{Format, FormatAlign, FormatBorder, Workbook};

    const NAVY: &str = "1F3864";
    const SECTION_BG: &str = "D9E1F2";

    let mut wb = Workbook::new();
    let sheet = wb.add_worksheet();
    sheet.set_name("Tracker").map_err(|e| e.to_string())?;

    // Column widths follow the mockup proportions.
    for (col, w) in [
        (0u16, 18.0), (1, 42.0), (2, 34.0), (3, 9.0), (4, 10.0),
        (5, 13.0), (6, 16.0), (7, 12.0), (8, 12.0), (9, 60.0),
    ] {
        sheet.set_column_width(col, w).map_err(|e| e.to_string())?;
    }

    let title_fmt = Format::new()
        .set_bold()
        .set_font_size(13)
        .set_font_color("FFFFFF")
        .set_background_color(NAVY)
        .set_align(FormatAlign::Center)
        .set_align(FormatAlign::VerticalCenter);
    let header_fmt = Format::new()
        .set_bold()
        .set_font_color("FFFFFF")
        .set_background_color(NAVY)
        .set_align(FormatAlign::Center)
        .set_align(FormatAlign::VerticalCenter)
        .set_border(FormatBorder::Thin)
        .set_border_color(NAVY);
    let section_fmt = Format::new()
        .set_bold()
        .set_font_color(NAVY)
        .set_background_color(SECTION_BG)
        .set_border(FormatBorder::Thin)
        .set_border_color("B4C6E7");
    let cell = Format::new()
        .set_border(FormatBorder::Thin)
        .set_border_color("BFBFBF")
        .set_align(FormatAlign::VerticalCenter);
    let cell_wrap = cell.clone().set_text_wrap().set_align(FormatAlign::Top);
    let cell_center = cell.clone().set_align(FormatAlign::Center);
    let cell_id = cell.clone().set_font_size(9).set_font_color("595959");
    // Nivel chips (mockup: Alto rosa, Bajo verde; Medio ámbar).
    let nivel_alto = cell_center.clone().set_bold().set_background_color("F8CBCC").set_font_color("9C0006");
    let nivel_medio = cell_center.clone().set_bold().set_background_color("FFE599").set_font_color("9C6500");
    let nivel_bajo = cell_center.clone().set_bold().set_background_color("C6EFCE").set_font_color("006100");
    // Estado chips (mockup: Completado verde, Pendiente naranja; En curso azul).
    let estado_done = cell_center.clone().set_bold().set_background_color("C6EFCE").set_font_color("006100");
    let estado_pending = cell_center.clone().set_bold().set_background_color("FBE2B4").set_font_color("9C6500");
    let estado_working = cell_center.clone().set_bold().set_background_color("DDEBF7").set_font_color("1F4E78");
    let total_fmt = Format::new()
        .set_bold()
        .set_font_color("FFFFFF")
        .set_background_color(NAVY)
        .set_align(FormatAlign::Center);
    let subtotal_fmt = Format::new()
        .set_bold()
        .set_font_color(NAVY)
        .set_background_color(SECTION_BG)
        .set_border(FormatBorder::Thin)
        .set_border_color("B4C6E7");

    const LAST_COL: u16 = 9;

    // Title bar.
    sheet.set_row_height(0, 26).map_err(|e| e.to_string())?;
    sheet
        .merge_range(0, 0, 0, LAST_COL, &format!("Tracker {} / {}", report.rig, report.workspace), &title_fmt)
        .map_err(|e| e.to_string())?;

    // Header row.
    let headers = [
        "Modulo", "Tarea", "Proceso", "Nivel", "Horas Est.", "Estado", "Responsable",
        "Fecha Inicio", "Fecha Fin", "Notas",
    ];
    sheet.set_row_height(1, 20).map_err(|e| e.to_string())?;
    for (col, h) in headers.iter().enumerate() {
        sheet
            .write_with_format(1, col as u16, *h, &header_fmt)
            .map_err(|e| e.to_string())?;
    }
    sheet.set_freeze_panes(2, 0).map_err(|e| e.to_string())?;

    let mut r: u32 = 2;
    for section in &report.sections {
        // Section band: module title left + the epic's OWN estimate as a title
        // suffix; col4 stays the CHILDREN rollup subtotal (mockup parity). The
        // remaining band cells, blank before, now carry the epic's own metadata
        // (gtcore-7bf261) aligned with the Estado/Responsable/Inicio/Fin/Notas
        // columns. col4 (rollup) and the title's "est. epic" stay differentiated.
        let band_title = match section.epic_horas {
            Some(h) => format!("{} · est. epic {h:.1} h", section.module_title),
            None => section.module_title.clone(),
        };
        sheet
            .merge_range(r, 0, r, 3, &band_title, &section_fmt)
            .map_err(|e| e.to_string())?;
        sheet
            .write_with_format(r, 4, section.horas, &subtotal_fmt)
            .map_err(|e| e.to_string())?;
        sheet
            .write_with_format(r, 5, section.epic_estado.as_str(), &section_fmt)
            .map_err(|e| e.to_string())?;
        sheet
            .write_with_format(r, 6, section.epic_responsable.as_str(), &section_fmt)
            .map_err(|e| e.to_string())?;
        sheet
            .write_with_format(r, 7, section.epic_inicio.as_str(), &section_fmt)
            .map_err(|e| e.to_string())?;
        sheet
            .write_with_format(r, 8, section.epic_fin.as_str(), &section_fmt)
            .map_err(|e| e.to_string())?;
        sheet
            .write_with_format(r, 9, section.epic_notas.as_str(), &section_fmt)
            .map_err(|e| e.to_string())?;
        r += 1;
        for row in &section.rows {
            // Fixed height: wrapped Tarea/Notas clip at ~2 lines instead of
            // blowing the row up — the operator expands a row when needed.
            sheet.set_row_height(r, 30).map_err(|e| e.to_string())?;
            sheet.write_with_format(r, 0, row.id.as_str(), &cell_id).map_err(|e| e.to_string())?;
            sheet.write_with_format(r, 1, row.tarea.as_str(), &cell_wrap).map_err(|e| e.to_string())?;
            sheet.write_with_format(r, 2, row.proceso.as_str(), &cell_wrap).map_err(|e| e.to_string())?;
            let nivel_fmt = match row.nivel.as_str() {
                "Alto" => &nivel_alto,
                "Medio" => &nivel_medio,
                _ => &nivel_bajo,
            };
            sheet.write_with_format(r, 3, row.nivel.as_str(), nivel_fmt).map_err(|e| e.to_string())?;
            match row.horas {
                Some(h) => sheet.write_with_format(r, 4, h, &cell_center),
                None => sheet.write_with_format(r, 4, "", &cell_center),
            }
            .map_err(|e| e.to_string())?;
            let estado_fmt = match row.estado.as_str() {
                "Completado" => &estado_done,
                "En curso" => &estado_working,
                _ => &estado_pending,
            };
            sheet.write_with_format(r, 5, row.estado.as_str(), estado_fmt).map_err(|e| e.to_string())?;
            sheet.write_with_format(r, 6, row.responsable.as_str(), &cell_center).map_err(|e| e.to_string())?;
            sheet.write_with_format(r, 7, row.fecha_inicio.as_str(), &cell_center).map_err(|e| e.to_string())?;
            sheet.write_with_format(r, 8, row.fecha_fin.as_str(), &cell_center).map_err(|e| e.to_string())?;
            sheet.write_with_format(r, 9, row.notas.as_str(), &cell_wrap).map_err(|e| e.to_string())?;
            r += 1;
        }
    }

    // TOTAL HORAS footer, navy like the mockup.
    sheet
        .merge_range(r, 0, r, 3, "TOTAL HORAS ESTIMADAS", &total_fmt)
        .map_err(|e| e.to_string())?;
    sheet
        .write_with_format(r, 4, report.total_horas, &total_fmt)
        .map_err(|e| e.to_string())?;
    for col in 5..=LAST_COL {
        sheet.write_with_format(r, col, "", &total_fmt).map_err(|e| e.to_string())?;
    }

    wb.save_to_buffer().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::*;

    fn row(id: &str, ty: &str, title: &str, horas: Option<f64>, status: &str) -> IssueRow {
        let mut v: IssueRow = serde_json::from_value(serde_json::json!({
            "id": id, "title": title, "status": status, "priority": 1,
            "issue_type": ty, "assignee": "ana", "owner": null,
            "created_at": null, "updated_at": null, "closed_at": null,
            "spec_id": null, "role_scope": null,
        }))
        .expect("row");
        v.estimated_hours = horas;
        v.start_date = Some("2026-06-01".into());
        v.due_date = Some("2026-06-15".into());
        v
    }

    fn sample() -> OperatorReport {
        let rows = vec![
            // The epic carries its OWN estimate (20.0) — distinct from the 12.5
            // its children roll up (gtcore-6621af).
            row("hq-mod-a", "epic", "Módulo Auth", Some(20.0), "working"),
            row("hq-1", "task", "Login form", Some(8.0), "working"),
            row("hq-2", "task", "Sesiones, con \"comillas\"", Some(4.5), "closed"),
            row("hq-3", "spike", "Suelto", Some(2.0), "open"),
        ];
        let mut parent_map = HashMap::new();
        parent_map.insert("hq-1".to_string(), "hq-mod-a".to_string());
        parent_map.insert("hq-2".to_string(), "hq-mod-a".to_string());
        build_report("hq", "default", &rows, &parent_map, &HashMap::new())
    }

    #[test]
    fn groups_per_module_epic_with_totals_and_no_module_tail() {
        let report = sample();
        assert_eq!(report.sections.len(), 2);
        assert_eq!(report.sections[0].module_title, "Módulo Auth");
        assert_eq!(report.sections[0].rows.len(), 2);
        assert_eq!(report.sections[0].horas, 12.5);
        // Epic itself is a section, never a task row.
        assert!(report.sections[0].rows.iter().all(|r| r.tarea != "Módulo Auth"));
        // No-module tail last.
        assert_eq!(report.sections[1].module_title, "Sin módulo");
        assert_eq!(report.total_horas, 14.5);
        // Estado uses operator vocabulary.
        assert_eq!(report.sections[0].rows[0].estado, "En curso");

        // The epic's OWN metadata rides on the section (gtcore-6621af), distinct
        // from the children rollup: epic_horas is the epic's estimate (20.0),
        // `horas` is the children sum (12.5).
        let auth = &report.sections[0];
        assert_eq!(auth.epic_estado, "En curso");
        assert_eq!(auth.epic_horas, Some(20.0));
        assert_eq!(auth.horas, 12.5);
        assert_eq!(auth.epic_responsable, "ana");
        assert_eq!(auth.epic_inicio, "2026-06-01");
        assert_eq!(auth.epic_fin, "2026-06-15");

        // The "Sin módulo" tail has no backing epic → epic fields stay empty.
        let tail = &report.sections[1];
        assert!(tail.epic_estado.is_empty());
        assert_eq!(tail.epic_horas, None);
        assert!(tail.epic_responsable.is_empty());
        assert!(tail.epic_inicio.is_empty());
        assert!(tail.epic_fin.is_empty());
    }

    /// The contract gtcore-f46a56's handler fix relies on: when the epic row is
    /// ABSENT from the set (the `report.generate --epic` parent_id filter returns
    /// only children), the module falls back to the raw id and the epic metadata
    /// is empty. Injecting the epic row (as the handler now does) is what restores
    /// the title + metadata — proven by `sample()` above, where the epic IS present.
    #[test]
    fn epic_row_absent_falls_back_to_raw_id_and_empty_meta() {
        // Only children — no `hq-mod-a` epic row in the set.
        let rows = vec![
            row("hq-1", "task", "Login form", Some(8.0), "working"),
            row("hq-2", "task", "Sesiones", Some(4.5), "closed"),
        ];
        let mut parent_map = HashMap::new();
        parent_map.insert("hq-1".to_string(), "hq-mod-a".to_string());
        parent_map.insert("hq-2".to_string(), "hq-mod-a".to_string());
        let report = build_report("hq", "default", &rows, &parent_map, &HashMap::new());

        assert_eq!(report.sections.len(), 1);
        let s = &report.sections[0];
        // Raw id as the module title (the bug the handler fix avoids).
        assert_eq!(s.module_title, "hq-mod-a");
        // No epic row ⇒ no epic metadata, but the children rollup still holds.
        assert!(s.epic_estado.is_empty());
        assert_eq!(s.epic_horas, None);
        assert!(s.epic_responsable.is_empty());
        assert_eq!(s.horas, 12.5);
    }

    #[test]
    fn csv_carries_header_rows_and_total_footer() {
        let csv = to_csv(&sample());
        let lines: Vec<&str> = csv.lines().collect();
        assert!(lines[0].starts_with("Modulo,Tarea,Proceso,Nivel,Horas Est."));
        // header + 1 epic row (Módulo Auth) + 3 task rows + footer.
        assert_eq!(lines.len(), 6);
        assert!(lines.last().unwrap().starts_with("TOTAL HORAS,,,,14.5"));
        // RFC 4180 quoting for the embedded quotes/comma.
        assert!(csv.contains("\"Sesiones, con \"\"comillas\"\"\""));
        // The epic's OWN metadata rides on a `(epic)` header row (gtcore-7bf261):
        // its own estimate (20), estado, responsable and dates — distinct from
        // the per-task rows. The "Sin módulo" tail emits no such row.
        assert!(csv.contains("Módulo Auth,(epic),epic,,20,En curso,ana,2026-06-01,2026-06-15,"));
        assert!(!csv.contains("Sin módulo,(epic)"));
    }

    #[test]
    fn xlsx_serializes_to_a_zip_container() {
        let bytes = to_xlsx(&sample()).expect("xlsx");
        // XLSX is a ZIP: PK magic.
        assert_eq!(&bytes[..2], b"PK");
        assert!(bytes.len() > 500);
    }

    /// True when `s` carries a `YYYY-MM-DD` date or an `HH:MM` clock time —
    /// the redundant timestamp the tracker must NOT fold into the Notas cell
    /// (gtcore-abc0ae).
    fn has_timestamp(s: &str) -> bool {
        let b = s.as_bytes();
        let digit = |i: usize| b.get(i).is_some_and(u8::is_ascii_digit);
        for i in 0..b.len() {
            // YYYY-MM-DD
            if digit(i)
                && digit(i + 1)
                && digit(i + 2)
                && digit(i + 3)
                && b.get(i + 4) == Some(&b'-')
                && digit(i + 5)
                && digit(i + 6)
                && b.get(i + 7) == Some(&b'-')
                && digit(i + 8)
                && digit(i + 9)
            {
                return true;
            }
            // HH:MM
            if digit(i) && digit(i + 1) && b.get(i + 2) == Some(&b':') && digit(i + 3) && digit(i + 4)
            {
                return true;
            }
        }
        false
    }

    /// The tracker xlsx/csv Notas cell must not carry a redundant date/time
    /// (gtcore-abc0ae AC). The dates already live in Fecha Inicio/Fin; the
    /// `closed … (by …)` breadcrumb that lands in `notes` carries no timestamp,
    /// and the projection adds none.
    #[test]
    fn csv_notas_cell_has_no_redundant_timestamp() {
        let mut closed = row("hq-2", "task", "Sesiones", Some(4.5), "closed");
        // The breadcrumb the close handler appends to `notes` (handlers.rs):
        // attribution only, never a date/time.
        closed.notes = Some("closed @ deadbeefcafe (by ana)".into());
        let rows = vec![
            row("hq-mod-a", "epic", "Módulo Auth", None, "open"),
            closed,
        ];
        let mut parent_map = HashMap::new();
        parent_map.insert("hq-2".to_string(), "hq-mod-a".to_string());
        let report = build_report("hq", "default", &rows, &parent_map, &HashMap::new());
        let csv = to_csv(&report);

        // The breadcrumb body survives (presentation-only change)…
        assert!(csv.contains("closed @ deadbeefcafe (by ana)"), "breadcrumb body dropped");
        // …but no Notas cell carries a YYYY-MM-DD or HH:MM timestamp. The only
        // dates in the file are the dedicated Fecha Inicio/Fin columns.
        for line in csv.lines() {
            if line.starts_with("Modulo,") || line.starts_with("TOTAL HORAS") {
                continue;
            }
            let notas = line.rsplit(',').next().unwrap_or("");
            assert!(
                !has_timestamp(notas),
                "Notas cell carries a redundant timestamp: {notas:?}"
            );
        }
    }
}
