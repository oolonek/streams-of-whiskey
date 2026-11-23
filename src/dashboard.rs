use std::error::Error;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use streams_of_whiskey::hll::{FORMAT, Sketch};

#[derive(Deserialize, Serialize)]
struct Dataset {
    dataset: String,
    version: String,
    dataset_url: String,
    file: String,
    file_url: String,
    format: String,
    axis: String,
    found_column: String,
    rows_scanned: u64,
    valid_smiles: u64,
    invalid_smiles: u64,
    empty_cells: u64,
    non_string_cells: u64,
    oversized_cells: u64,
    valid_of_parsed_percent: Option<String>,
    status: String,
    checked_at_unix: u64,
}

#[derive(Serialize)]
struct Snapshot {
    generated_at_unix: u64,
    estimated_unique_valid_smiles: u64,
    datasets: Vec<Dataset>,
}

/// Export only the display fields; the crawl cache and the strings themselves stay local.
pub fn export(cache_path: &Path, output: &Path) -> Result<(), Box<dyn Error>> {
    let mut connection = Connection::open_with_flags(cache_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let transaction = connection.transaction()?;
    let legacy: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'inspected_smiles')",
        [],
        |row| row.get(0),
    )?;
    if legacy {
        return Err("legacy exact-SMILES storage detected; stop inspections and run --migrate-hll before exporting".into());
    }
    let format: String = transaction.query_row(
        "SELECT value FROM cache_metadata WHERE key = 'sketch_format'",
        [],
        |row| row.get(0),
    )?;
    if format != FORMAT {
        return Err(format!("unsupported sketch format {format:?}; expected {FORMAT}").into());
    }
    let datasets = {
        let mut statement = transaction
            .prepare("SELECT detail_json FROM inspection_results ORDER BY dataset_id")?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .map(|result| -> Result<Dataset, Box<dyn Error>> {
                let dataset: Dataset = serde_json::from_str(&result?)?;
                if !matches!(dataset.status.as_str(), "complete" | "partial") {
                    return Err(format!("non-completed inspection: {}", dataset.dataset).into());
                }
                Ok(dataset)
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut merged = Sketch::new();
    let mut read_sketches = 0_usize;
    {
        let mut statement = transaction.prepare(
            "SELECT r.dataset_id, s.registers
             FROM inspection_results r LEFT JOIN inspection_sketches s USING (dataset_id)
             ORDER BY r.dataset_id",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            match row.get_ref(1)? {
                ValueRef::Blob(bytes) => merged.merge_bytes(bytes)?,
                ValueRef::Null => {
                    let id = row.get_ref(0)?.as_str()?;
                    return Err(format!("missing HLL sketch for {id}; run --migrate-hll").into());
                }
                _ => return Err("saved HLL sketch is not a BLOB".into()),
            }
            read_sketches += 1;
        }
    }
    let sketch_count: u64 =
        transaction.query_row("SELECT COUNT(*) FROM inspection_sketches", [], |row| {
            row.get(0)
        })?;
    if read_sketches != datasets.len() || sketch_count != datasets.len() as u64 {
        return Err("inspection sketches do not match saved inspections; run --migrate-hll".into());
    }
    transaction.commit()?;
    let snapshot = Snapshot {
        generated_at_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        estimated_unique_valid_smiles: merged.estimate(),
        datasets,
    };
    // Serialize before replacing a previously published snapshot.
    fs::write(output, serde_json::to_vec(&snapshot)?)?;
    Ok(())
}
