//! Machine-readable run records (`chaos/results/sweep-main.jsonl`) + the generated markdown
//! summary (`chaos/results/summary.md`). Every record carries `isolation: "read committed"`
//! (Amendment §A4).

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::json;

/// One run of (crash_point × schedule) or a scripted schedule.
#[derive(Debug, Clone)]
pub struct RunRecord {
    pub branch: String,
    pub crash_point: String,
    pub schedule: String,
    /// Whether the armed process aborted under its driving workload (closure/reachability).
    pub aborted: bool,
    /// Empty == all five invariants held.
    pub violations: Vec<String>,
    pub note: String,
}

impl RunRecord {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    fn to_json(&self) -> serde_json::Value {
        json!({
            "branch": self.branch,
            "crash_point": self.crash_point,
            "schedule": self.schedule,
            "isolation": "read committed",
            "aborted": self.aborted,
            "passed": self.passed(),
            "violations": self.violations,
            "note": self.note,
        })
    }
}

/// Append the run records as JSON Lines.
pub fn write_jsonl(path: &Path, records: &[RunRecord]) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).with_context(|| format!("mkdir {}", dir.display()))?;
    }
    let mut f = fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
    for r in records {
        writeln!(f, "{}", r.to_json()).context("write jsonl line")?;
    }
    Ok(())
}

/// Generate the human-readable summary: the headline line + a per-crash-point pass grid.
pub fn write_summary(path: &Path, records: &[RunRecord], closure: &ClosureReport) -> Result<()> {
    let total = records.len();
    let passed = records.iter().filter(|r| r.passed()).count();
    let conservation_violations = records
        .iter()
        .flat_map(|r| &r.violations)
        .filter(|v| v.starts_with("conservation"))
        .count();
    let send_violations = records
        .iter()
        .flat_map(|r| &r.violations)
        .filter(|v| v.starts_with("send-count"))
        .count();

    // Crash-point → schedule → pass grid (only the four enumerated schedule columns; scripted
    // §A2/§A3 + seeded runs are listed separately below).
    let cols = ["Single", "DuplicateStream", "ConcurrentConsumers", "RestartRedelivery"];
    let mut grid: BTreeMap<String, BTreeMap<String, bool>> = BTreeMap::new();
    for r in records {
        if cols.contains(&r.schedule.as_str()) {
            grid.entry(r.crash_point.clone())
                .or_default()
                .insert(r.schedule.clone(), r.passed());
        }
    }

    let mut out = String::new();
    out.push_str("# Phase 2 — Exhaustive crash-point sweep (main)\n\n");
    out.push_str(&format!(
        "**Headline:** a process crash at every statement boundary in the pipeline, under every \
         redelivery schedule — **{conservation_violations} conservation violations, \
         {send_violations} re-sends** across {total} runs.\n\n"
    ));
    out.push_str(&format!(
        "- Runs: **{passed}/{total} passed** (all five invariants held).\n"
    ));
    out.push_str("- Isolation: **READ COMMITTED** (recorded in every record — Amendment §A4).\n");
    out.push_str(&format!(
        "- Registry closure: **{}/{} crash points aborted** under their driving workload{}.\n\n",
        closure.aborted,
        closure.total,
        if closure.unreached.is_empty() {
            " (every variant has a live, reachable fire-site)".to_string()
        } else {
            format!(" — UNREACHED: {}", closure.unreached.join(", "))
        }
    ));

    out.push_str("## Per-crash-point pass grid\n\n");
    out.push_str("| Crash point | Single | DuplicateStream | ConcurrentConsumers | RestartRedelivery |\n");
    out.push_str("|---|---|---|---|---|\n");
    for (cp, scheds) in &grid {
        out.push_str(&format!("| `{cp}` "));
        for col in cols {
            let cell = match scheds.get(col) {
                Some(true) => "✓",
                Some(false) => "✗",
                None => "—",
            };
            out.push_str(&format!("| {cell} "));
        }
        out.push_str("|\n");
    }

    // Scripted (non-grid) records.
    let scripted: Vec<&RunRecord> = records
        .iter()
        .filter(|r| !cols.contains(&r.schedule.as_str()))
        .collect();
    if !scripted.is_empty() {
        out.push_str("\n## Scripted schedules (§A2 / §A3) + seeded sweep\n\n");
        out.push_str("| Name | Crash point | Passed | Note |\n|---|---|---|---|\n");
        for r in scripted {
            out.push_str(&format!(
                "| `{}` | `{}` | {} | {} |\n",
                r.schedule,
                r.crash_point,
                if r.passed() { "✓" } else { "✗" },
                r.note
            ));
        }
    }

    // Any failures, spelled out.
    let failures: Vec<&RunRecord> = records.iter().filter(|r| !r.passed()).collect();
    if !failures.is_empty() {
        out.push_str("\n## Failures\n\n");
        for r in failures {
            out.push_str(&format!(
                "- `{}` × `{}`: {}\n",
                r.crash_point,
                r.schedule,
                r.violations.join("; ")
            ));
        }
    }

    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).ok();
    }
    fs::write(path, out).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

/// Registry-closure / reachability tally (§4.3): which crash points aborted under their workload.
#[derive(Debug, Default)]
pub struct ClosureReport {
    pub total: usize,
    pub aborted: usize,
    pub unreached: Vec<String>,
}
