use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags};
use serde::Serialize;

use crate::dashboard;

#[derive(Debug, Serialize)]
struct Window {
    count: u64,
    first_checked_at_unix: Option<i64>,
    last_checked_at_unix: Option<i64>,
    observed_span_seconds: Option<i64>,
}

impl Window {
    fn new(count: u64, first: Option<i64>, last: Option<i64>) -> Self {
        Self {
            count,
            first_checked_at_unix: first,
            last_checked_at_unix: last,
            observed_span_seconds: first.zip(last).map(|(start, end)| end - start),
        }
    }

    fn include(&mut self, other: &Self) {
        self.count += other.count;
        if let Some(first) = other.first_checked_at_unix {
            self.first_checked_at_unix = Some(
                self.first_checked_at_unix
                    .map_or(first, |current| current.min(first)),
            );
        }
        if let Some(last) = other.last_checked_at_unix {
            self.last_checked_at_unix = Some(
                self.last_checked_at_unix
                    .map_or(last, |current| current.max(last)),
            );
        }
        self.observed_span_seconds = self
            .first_checked_at_unix
            .zip(self.last_checked_at_unix)
            .map(|(start, end)| end - start);
    }
}

#[derive(Serialize)]
struct InspectionDay {
    date_utc: String,
    complete: u64,
    partial: u64,
    completions: Window,
}

#[derive(Serialize)]
struct TimingReport {
    generated_at_unix: u64,
    valid_parsed_cells: u64,
    estimated_unique_valid_smiles: u64,
    cached_probes: Window,
    found_probes: u64,
    saved_inspections: Window,
    inspections_by_utc_day: Vec<InspectionDay>,
    note: &'static str,
}

fn report(connection: &Connection) -> Result<TimingReport, Box<dyn Error>> {
    let (count, first, last, found) = connection.query_row(
        "SELECT count(*), min(checked_at), max(checked_at),
                coalesce(sum(CASE WHEN json_extract(result_json, '$.status') = 'found' THEN 1 ELSE 0 END), 0)
         FROM probe_cache",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let mut valid_parsed_cells = 0_u64;
    let mut saved_inspections = Window::new(0, None, None);
    let mut inspections_by_utc_day = Vec::new();
    let mut statement = connection.prepare(
        "SELECT date(json_extract(detail_json, '$.checked_at_unix'), 'unixepoch'),
                count(*), min(json_extract(detail_json, '$.checked_at_unix')),
                max(json_extract(detail_json, '$.checked_at_unix')),
                sum(CASE WHEN json_extract(detail_json, '$.status') = 'complete' THEN 1 ELSE 0 END),
                sum(CASE WHEN json_extract(detail_json, '$.status') = 'partial' THEN 1 ELSE 0 END),
                sum(CASE WHEN json_type(detail_json, '$.valid_smiles') = 'integer'
                         THEN json_extract(detail_json, '$.valid_smiles') ELSE 0 END),
                sum(CASE WHEN json_type(detail_json, '$.valid_smiles') = 'integer'
                         THEN 1 ELSE 0 END)
         FROM inspection_results
         GROUP BY 1 ORDER BY 1",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let date_utc: String = row.get(0)?;
        let completions = Window::new(row.get(1)?, row.get(2)?, row.get(3)?);
        let complete: u64 = row.get(4)?;
        let partial: u64 = row.get(5)?;
        if complete + partial != completions.count || row.get::<_, u64>(7)? != completions.count {
            return Err(
                format!("inspection status or valid cell count missing on {date_utc}").into(),
            );
        }
        valid_parsed_cells += row.get::<_, u64>(6)?;
        saved_inspections.include(&completions);
        inspections_by_utc_day.push(InspectionDay {
            date_utc,
            complete,
            partial,
            completions,
        });
    }
    let estimated_unique_valid_smiles =
        dashboard::estimated_unique(connection, saved_inspections.count)?;
    Ok(TimingReport {
        generated_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        valid_parsed_cells,
        estimated_unique_valid_smiles,
        cached_probes: Window::new(count, first, last),
        found_probes: found,
        saved_inspections,
        inspections_by_utc_day,
        note: "Observed spans run from first to last saved completion, not measured runtimes. The cache overwrites earlier records, omits metadata-only skips, and stores no crawl start or per-file scan duration. UTC days are not run boundaries.",
    })
}

const START: &str = "<!-- cache-timings:start -->";
const END: &str = "<!-- cache-timings:end -->";

fn count(value: u64) -> String {
    let mut digits = value.to_string();
    let mut index = digits.len();
    while index > 3 {
        index -= 3;
        digits.insert(index, ',');
    }
    digits
}

fn duration(seconds: Option<i64>) -> String {
    let Some(seconds) = seconds else {
        return "—".into();
    };
    let hours = seconds / 3600;
    let minutes = seconds % 3600 / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn utc(connection: &Connection, seconds: i64) -> Result<String, Box<dyn Error>> {
    Ok(connection.query_row(
        "SELECT strftime('%Y-%m-%d %H:%M:%S', ?1, 'unixepoch')",
        [seconds],
        |row| row.get(0),
    )?)
}

fn range(connection: &Connection, window: &Window) -> Result<String, Box<dyn Error>> {
    match (window.first_checked_at_unix, window.last_checked_at_unix) {
        (Some(first), Some(last)) => Ok(format!(
            "{}–{} UTC",
            utc(connection, first)?,
            utc(connection, last)?
        )),
        _ => Ok("No saved completions".into()),
    }
}

fn readme_table(connection: &Connection, metrics: &TimingReport) -> Result<String, Box<dyn Error>> {
    let mut table = String::new();
    writeln!(
        table,
        "Local cache report generated {} UTC; the published dashboard may differ. These are **observed completion spans, not runtimes or forecasts**.",
        utc(connection, metrics.generated_at_unix.try_into()?)?
    )?;
    writeln!(table)?;
    writeln!(table, "| Metric | Observed value | Scope |")?;
    writeln!(table, "| --- | ---: | --- |")?;
    let complete: u64 = metrics
        .inspections_by_utc_day
        .iter()
        .map(|day| day.complete)
        .sum();
    let partial: u64 = metrics
        .inspections_by_utc_day
        .iter()
        .map(|day| day.partial)
        .sum();
    writeln!(
        table,
        "| Inspected datasets | {} | {} complete; {} partial |",
        count(metrics.saved_inspections.count),
        count(complete),
        count(partial)
    )?;
    writeln!(
        table,
        "| Valid parsed cells | {} | Exact sum across inspected files |",
        count(metrics.valid_parsed_cells)
    )?;
    writeln!(
        table,
        "| Estimated unique valid strings | ≈{} | HyperLogLog across inspected files |",
        count(metrics.estimated_unique_valid_smiles)
    )?;
    writeln!(
        table,
        "| Cached discovery probes | {} | {} `found`; metadata-only skips are not cached |",
        count(metrics.cached_probes.count),
        count(metrics.found_probes)
    )?;
    writeln!(
        table,
        "| Discovery completion window | {} | {} |",
        duration(metrics.cached_probes.observed_span_seconds),
        range(connection, &metrics.cached_probes)?
    )?;
    for day in &metrics.inspections_by_utc_day {
        writeln!(
            table,
            "| Inspections on {} | {} | {} files; {} |",
            day.date_utc,
            duration(day.completions.observed_span_seconds),
            count(day.completions.count),
            range(connection, &day.completions)?
        )?;
    }
    Ok(table)
}

fn updated_readme(source: &str, table: &str) -> Result<String, Box<dyn Error>> {
    let (before, rest) = source
        .split_once(START)
        .ok_or("README is missing timing start marker")?;
    let (prior, after) = rest
        .split_once(END)
        .ok_or("README is missing timing end marker")?;
    if prior.contains(START) || after.contains(START) || after.contains(END) {
        return Err("README has duplicate timing markers".into());
    }
    Ok(format!("{before}{START}\n{table}{END}{after}"))
}

pub fn export(cache_path: &Path, output: &Path) -> Result<(), Box<dyn Error>> {
    let mut connection = Connection::open_with_flags(cache_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let transaction = connection.transaction()?;
    let metrics = report(&transaction)?;
    let table = readme_table(&transaction, &metrics)?;
    transaction.commit()?;
    let readme = Path::new("README.md");
    let updated = updated_readme(&fs::read_to_string(readme)?, &table)?;
    fs::write(output, serde_json::to_vec_pretty(&metrics)?)?;
    fs::write(readme, updated)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use streams_of_whiskey::hll::{FORMAT, Sketch};

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE probe_cache (checked_at INTEGER, result_json TEXT);
                 CREATE TABLE inspection_results (dataset_id TEXT, detail_json TEXT);
                 CREATE TABLE inspection_sketches (dataset_id TEXT, registers BLOB);
                 CREATE TABLE cache_metadata (key TEXT, value TEXT);",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO cache_metadata VALUES ('sketch_format', ?1)",
                [FORMAT],
            )
            .unwrap();
        connection
    }

    #[test]
    fn reports_saved_completion_windows_without_treating_utc_days_as_runs() {
        let connection = connection();
        connection
            .execute_batch(
                "INSERT INTO probe_cache VALUES (86390, '{\"status\":\"found\"}'),
                                                (86410, '{\"status\":\"skipped\"}');
                 INSERT INTO inspection_results VALUES
                   ('one', '{\"status\":\"complete\",\"checked_at_unix\":86395,\"valid_smiles\":7}'),
                   ('two', '{\"status\":\"partial\",\"checked_at_unix\":86412,\"valid_smiles\":5}');",
            )
            .unwrap();
        for id in ["one", "two"] {
            connection
                .execute(
                    "INSERT INTO inspection_sketches VALUES (?1, ?2)",
                    rusqlite::params![id, Sketch::new().as_bytes()],
                )
                .unwrap();
        }
        let metrics = report(&connection).unwrap();
        assert_eq!(metrics.cached_probes.count, 2);
        assert_eq!(metrics.cached_probes.observed_span_seconds, Some(20));
        assert_eq!(metrics.valid_parsed_cells, 12);
        assert_eq!(metrics.estimated_unique_valid_smiles, 0);
        assert_eq!(metrics.found_probes, 1);
        assert_eq!(metrics.saved_inspections.observed_span_seconds, Some(17));
        assert_eq!(metrics.inspections_by_utc_day.len(), 2);
        assert_eq!(metrics.inspections_by_utc_day[0].date_utc, "1970-01-01");
        assert_eq!(metrics.inspections_by_utc_day[0].complete, 1);
        assert_eq!(metrics.inspections_by_utc_day[1].date_utc, "1970-01-02");
        assert_eq!(metrics.inspections_by_utc_day[1].partial, 1);
        assert_eq!(
            metrics.inspections_by_utc_day[1]
                .completions
                .observed_span_seconds,
            Some(0)
        );
        let table = readme_table(&connection, &metrics).unwrap();
        assert!(table.contains("| Inspected datasets | 2 | 1 complete; 1 partial |"));
        assert!(table.contains("| Valid parsed cells | 12 |"));
        assert!(table.contains("| Inspections on 1970-01-02 | 0s | 1 files;"));
    }

    #[test]
    fn empty_cache_has_no_observed_duration() {
        let connection = connection();
        let metrics = report(&connection).unwrap();
        assert_eq!(metrics.cached_probes.count, 0);
        assert_eq!(metrics.found_probes, 0);
        assert_eq!(metrics.cached_probes.observed_span_seconds, None);
        assert_eq!(metrics.saved_inspections.count, 0);
        assert_eq!(metrics.saved_inspections.observed_span_seconds, None);
        assert!(metrics.inspections_by_utc_day.is_empty());
    }

    #[test]
    fn refreshes_only_marked_readme_section() {
        let source = format!("Intro\n{START}\nold figures\n{END}\nOther notes\n");
        let updated = updated_readme(&source, "new figures\n").unwrap();
        assert_eq!(
            updated,
            format!("Intro\n{START}\nnew figures\n{END}\nOther notes\n")
        );
        assert_eq!(
            updated_readme(&updated, "latest figures\n").unwrap(),
            format!("Intro\n{START}\nlatest figures\n{END}\nOther notes\n")
        );
        assert!(updated_readme("Intro\nOther notes\n", "figures\n").is_err());
    }
}
