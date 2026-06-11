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

use serde::Serialize;

use gt_store_dolt::IssueRow;

/// One task row of the report, mockup column order.
#[derive(Debug, Clone, Serialize)]
pub struct ReportRow {
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
    /// The section's Horas Est. subtotal.
    pub horas: f64,
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
        "closed" => "Hecho",
        _ => "Pendiente",
    }
}

/// Build the report from one (rig, workspace)'s tracker rows — the same rows
/// `board.list` projects. Epics title the module sections (D5) and are not
/// task rows; everything else groups under its `external_ref`.
pub fn build_report(rig: &str, workspace: &str, rows: &[IssueRow]) -> OperatorReport {
    // Module titles: the epics present in the row set.
    let mut titles: BTreeMap<&str, &str> = BTreeMap::new();
    for row in rows {
        if row.issue_type == "epic" {
            titles.insert(row.id.as_str(), row.title.as_str());
        }
    }

    // Bucket task rows per module epic.
    let mut buckets: BTreeMap<String, Vec<ReportRow>> = BTreeMap::new();
    for row in rows {
        if row.issue_type == "epic" {
            continue;
        }
        let module = row.external_ref.clone().unwrap_or_default();
        buckets.entry(module).or_default().push(ReportRow {
            tarea: row.title.clone(),
            proceso: row.issue_type.clone(),
            nivel: format!("P{}", row.priority),
            horas: row.estimated_hours,
            estado: estado_label(&row.status).to_string(),
            responsable: row.assignee.clone().unwrap_or_default(),
            fecha_inicio: row.start_date.clone().unwrap_or_default(),
            fecha_fin: row.due_date.clone().unwrap_or_default(),
            notas: row.notes.clone().unwrap_or_default(),
        });
    }

    let mut sections: Vec<ReportSection> = Vec::new();
    let mut tail: Option<ReportSection> = None;
    for (module_id, rows) in buckets {
        let horas: f64 = rows.iter().filter_map(|r| r.horas).sum();
        let section = ReportSection {
            module_title: if module_id.is_empty() {
                "Sin módulo".to_string()
            } else {
                titles
                    .get(module_id.as_str())
                    .map(|t| t.to_string())
                    // The epic may live outside the filtered row set; its id
                    // still names the module.
                    .unwrap_or_else(|| module_id.clone())
            },
            module_id,
            rows,
            horas,
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

/// Serialize the report as XLSX: one sheet, bold module section headers,
/// the mockup columns, per-section subtotals, and the TOTAL HORAS footer.
pub fn to_xlsx(report: &OperatorReport) -> Result<Vec<u8>, String> {
    use rust_xlsxwriter::{Format, Workbook};

    let mut wb = Workbook::new();
    let sheet = wb.add_worksheet();
    sheet.set_name("Tracker").map_err(|e| e.to_string())?;

    let bold = Format::new().set_bold();
    let header = Format::new().set_bold().set_background_color("D9E1F2");

    let headers = [
        "Modulo", "Tarea", "Proceso", "Nivel", "Horas Est.", "Estado", "Responsable",
        "Fecha Inicio", "Fecha Fin", "Notas",
    ];
    for (col, h) in headers.iter().enumerate() {
        sheet
            .write_with_format(0, col as u16, *h, &header)
            .map_err(|e| e.to_string())?;
    }

    let mut r: u32 = 1;
    for section in &report.sections {
        // Section header row: the module title spanning the first column.
        sheet
            .write_with_format(r, 0, section.module_title.as_str(), &bold)
            .map_err(|e| e.to_string())?;
        r += 1;
        for row in &section.rows {
            sheet.write(r, 1, row.tarea.as_str()).map_err(|e| e.to_string())?;
            sheet.write(r, 2, row.proceso.as_str()).map_err(|e| e.to_string())?;
            sheet.write(r, 3, row.nivel.as_str()).map_err(|e| e.to_string())?;
            if let Some(h) = row.horas {
                sheet.write(r, 4, h).map_err(|e| e.to_string())?;
            }
            sheet.write(r, 5, row.estado.as_str()).map_err(|e| e.to_string())?;
            sheet.write(r, 6, row.responsable.as_str()).map_err(|e| e.to_string())?;
            sheet.write(r, 7, row.fecha_inicio.as_str()).map_err(|e| e.to_string())?;
            sheet.write(r, 8, row.fecha_fin.as_str()).map_err(|e| e.to_string())?;
            sheet.write(r, 9, row.notas.as_str()).map_err(|e| e.to_string())?;
            r += 1;
        }
        // Section subtotal.
        sheet
            .write_with_format(r, 0, "Subtotal", &bold)
            .map_err(|e| e.to_string())?;
        sheet
            .write_with_format(r, 4, section.horas, &bold)
            .map_err(|e| e.to_string())?;
        r += 2; // blank spacer row between sections
    }
    sheet
        .write_with_format(r, 0, "TOTAL HORAS", &bold)
        .map_err(|e| e.to_string())?;
    sheet
        .write_with_format(r, 4, report.total_horas, &bold)
        .map_err(|e| e.to_string())?;

    wb.save_to_buffer().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, ty: &str, title: &str, eref: Option<&str>, horas: Option<f64>, status: &str) -> IssueRow {
        let mut v: IssueRow = serde_json::from_value(serde_json::json!({
            "id": id, "title": title, "status": status, "priority": 1,
            "issue_type": ty, "assignee": "ana", "owner": null,
            "created_at": null, "updated_at": null, "closed_at": null,
            "external_ref": eref, "spec_id": null, "role_scope": null,
        }))
        .expect("row");
        v.estimated_hours = horas;
        v.start_date = Some("2026-06-01".into());
        v.due_date = Some("2026-06-15".into());
        v
    }

    fn sample() -> OperatorReport {
        let rows = vec![
            row("hq-mod-a", "epic", "Módulo Auth", None, None, "open"),
            row("hq-1", "task", "Login form", Some("hq-mod-a"), Some(8.0), "working"),
            row("hq-2", "task", "Sesiones, con \"comillas\"", Some("hq-mod-a"), Some(4.5), "closed"),
            row("hq-3", "spike", "Suelto", None, Some(2.0), "open"),
        ];
        build_report("hq", "default", &rows)
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
    }

    #[test]
    fn csv_carries_header_rows_and_total_footer() {
        let csv = to_csv(&sample());
        let lines: Vec<&str> = csv.lines().collect();
        assert!(lines[0].starts_with("Modulo,Tarea,Proceso,Nivel,Horas Est."));
        // 3 task rows + header + footer.
        assert_eq!(lines.len(), 5);
        assert!(lines.last().unwrap().starts_with("TOTAL HORAS,,,,14.5"));
        // RFC 4180 quoting for the embedded quotes/comma.
        assert!(csv.contains("\"Sesiones, con \"\"comillas\"\"\""));
    }

    #[test]
    fn xlsx_serializes_to_a_zip_container() {
        let bytes = to_xlsx(&sample()).expect("xlsx");
        // XLSX is a ZIP: PK magic.
        assert_eq!(&bytes[..2], b"PK");
        assert!(bytes.len() > 500);
    }
}
