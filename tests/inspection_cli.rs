use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use streams_of_whiskey::hll::{FORMAT, Sketch};

#[test]
fn resume_without_tsv_keeps_existing_report_and_uses_cached_inspection() {
    let directory = std::env::temp_dir().join(format!(
        "streams-of-whiskey-resume-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    let cache_path = directory.join("cache.sqlite");
    let report_path = directory.join("found-details.tsv");
    fs::write(&report_path, "previous run's report\n").unwrap();
    seed_completed_inspection(&cache_path);

    let output = Command::new(env!("CARGO_BIN_EXE_streams-of-whiskey"))
        .args(["--inspect-found", "--cache"])
        .arg(&cache_path)
        .current_dir(&directory)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(&report_path).unwrap(),
        "previous run's report\n"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("reusing completed inspection"));

    fs::remove_file(&report_path).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_streams-of-whiskey"))
        .args(["--inspect-found", "--cache"])
        .arg(&cache_path)
        .current_dir(&directory)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !report_path.exists(),
        "inspection unexpectedly created a TSV"
    );

    fs::remove_dir_all(directory).unwrap();
}

fn seed_completed_inspection(path: &Path) {
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE probe_cache (
            dataset_id TEXT PRIMARY KEY, revision TEXT,
            result_json TEXT NOT NULL, checked_at INTEGER NOT NULL
         );
         CREATE TABLE inspection_results (
            dataset_id TEXT PRIMARY KEY, revision TEXT NOT NULL,
            file TEXT NOT NULL, axis_request TEXT NOT NULL,
            found_row INTEGER NOT NULL, found_column TEXT NOT NULL,
            detail_json TEXT NOT NULL
         );
         CREATE TABLE inspection_sketches (
            dataset_id TEXT PRIMARY KEY, registers BLOB NOT NULL
         );
         CREATE TABLE cache_metadata (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
         );",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO cache_metadata (key, value) VALUES ('sketch_format', ?1)",
            [FORMAT],
        )
        .unwrap();
    connection.execute(
        "INSERT INTO probe_cache VALUES (?1, ?2, ?3, 1)",
        params!["org/data|direct:v3", "sha-pinned", r#"{"dataset":"org/data","status":"found","location":{"file":"train.csv","row":0,"column":"SMILES"}}"#],
    ).unwrap();
    let detail = serde_json::json!({
        "dataset": "org/data", "version": "sha-pinned", "version_source": "cached_sha",
        "dataset_url": "https://huggingface.co/datasets/org/data/tree/sha-pinned",
        "file": "train.csv",
        "file_url": "https://huggingface.co/datasets/org/data/resolve/sha-pinned/train.csv",
        "format": "csv", "scope": "pinpointed_file", "axis": "auto",
        "found_row": 0, "found_column": "SMILES", "file_bytes": 12,
        "rows_scanned": 1, "cells_scanned": 1,
        "valid_smiles": 1, "invalid_smiles": 0, "empty_cells": 0,
        "non_string_cells": 0, "oversized_cells": 0,
        "valid_of_parsed_percent": "100.00", "status": "complete",
        "error": "", "checked_at_unix": 1
    });
    connection
        .execute(
            "INSERT INTO inspection_results VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)",
            params![
                "org/data",
                "sha-pinned",
                "train.csv",
                "auto",
                "SMILES",
                detail.to_string()
            ],
        )
        .unwrap();
    let mut sketch = Sketch::new();
    sketch.insert("CCO");
    connection
        .execute(
            "INSERT INTO inspection_sketches VALUES (?1, ?2)",
            params!["org/data", sketch.as_bytes()],
        )
        .unwrap();
}
