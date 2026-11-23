use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};

use crate::hll::{FORMAT, Sketch};

const TTL_SECONDS: i64 = 24 * 60 * 60;

pub struct Cache {
    connection: Connection,
}

pub struct CachedFound {
    pub revision: Option<String>,
    pub result_json: String,
}
pub struct CachedInspection {
    pub revision: String,
    pub file: String,
    pub axis_request: String,
    pub found_row: u64,
    pub found_column: String,
    pub detail_json: String,
}

impl Cache {
    pub fn open(path: &Path) -> Result<Self, rusqlite::Error> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS probe_cache (
                 dataset_id TEXT PRIMARY KEY,
                 revision TEXT,
                 result_json TEXT NOT NULL,
                 checked_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS inspection_results (
                 dataset_id TEXT PRIMARY KEY,
                 revision TEXT NOT NULL,
                 file TEXT NOT NULL,
                 axis_request TEXT NOT NULL,
                 found_row INTEGER NOT NULL,
                 found_column TEXT NOT NULL,
                 detail_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS cache_metadata (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS inspection_sketches (
                 dataset_id TEXT PRIMARY KEY,
                 registers BLOB NOT NULL
             );",
        )?;
        let format: Option<String> = connection
            .query_row(
                "SELECT value FROM cache_metadata WHERE key = 'sketch_format'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        match format {
            Some(value) if value != FORMAT => {
                return Err(format_error(&format!(
                    "unsupported sketch format {value:?}; expected {FORMAT:?}"
                )));
            }
            None => {
                let sketch_count: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM inspection_sketches",
                    [],
                    |row| row.get(0),
                )?;
                if sketch_count != 0 {
                    return Err(format_error("sketches exist without hash/format metadata"));
                }
                connection.execute(
                    "INSERT INTO cache_metadata (key, value) VALUES ('sketch_format', ?1)",
                    [FORMAT],
                )?;
            }
            Some(_) => {}
        }
        Ok(Self { connection })
    }

    /// A legacy table must be migrated explicitly, even if it is empty.
    pub fn migration_needed(&self) -> Result<bool, rusqlite::Error> {
        self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master
                           WHERE type = 'table' AND name = 'inspected_smiles')",
            [],
            |row| row.get(0),
        )
    }

    pub fn get(&self, id: &str, revision: Option<&str>) -> Result<Option<String>, rusqlite::Error> {
        let cutoff = unix_now() - TTL_SECONDS;
        self.connection
            .query_row(
                "SELECT result_json FROM probe_cache
                 WHERE dataset_id = ?1 AND checked_at > ?2
                   AND (?3 IS NULL OR revision = ?3)",
                params![id, cutoff, revision],
                |row| row.get(0),
            )
            .optional()
    }

    pub fn put(&self, id: &str, revision: Option<&str>, json: &str) -> Result<(), rusqlite::Error> {
        self.connection.execute(
            "INSERT INTO probe_cache (dataset_id, revision, result_json, checked_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(dataset_id) DO UPDATE SET
                 revision = excluded.revision,
                 result_json = excluded.result_json,
                 checked_at = excluded.checked_at",
            params![id, revision, json, unix_now()],
        )?;
        Ok(())
    }

    /// Return pinned file-level matches, newest first. Expiration affects
    /// probe reuse, not explicit inspection of previously discovered matches.
    pub fn found(&self) -> Result<Vec<CachedFound>, rusqlite::Error> {
        let mut statement = self.connection.prepare(
            "SELECT revision, result_json FROM probe_cache
             WHERE json_extract(result_json, '$.status') = 'found'
               AND json_extract(result_json, '$.location.file') IS NOT NULL
             ORDER BY checked_at DESC",
        )?;
        statement
            .query_map([], |row| {
                Ok(CachedFound {
                    revision: row.get(0)?,
                    result_json: row.get(1)?,
                })
            })?
            .collect()
    }
    pub fn inspection(&self, id: &str) -> Result<Option<CachedInspection>, rusqlite::Error> {
        self.connection
            .query_row(
                "SELECT revision, file, axis_request, found_row, found_column, detail_json
                 FROM inspection_results JOIN inspection_sketches USING (dataset_id)
                 WHERE dataset_id = ?1",
                [id],
                |row| {
                    Ok(CachedInspection {
                        revision: row.get(0)?,
                        file: row.get(1)?,
                        axis_request: row.get(2)?,
                        found_row: row.get(3)?,
                        found_column: row.get(4)?,
                        detail_json: row.get(5)?,
                    })
                },
            )
            .optional()
    }

    pub fn replace_inspection(
        &self,
        id: &str,
        revision: &str,
        file: &str,
        axis_request: &str,
        found_row: u64,
        found_column: &str,
        detail_json: &str,
        sketch: &Sketch,
    ) -> Result<(), rusqlite::Error> {
        if self.migration_needed()? {
            return Err(format_error(
                "legacy inspected_smiles table exists; run --migrate-hll before saving inspections",
            ));
        }
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        // Both representations change together; a failed write leaves the
        // previous completed inspection available for report replay.
        let result = (|| {
            self.connection.execute(
                "INSERT INTO inspection_results
                 (dataset_id, revision, file, axis_request, found_row, found_column, detail_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(dataset_id) DO UPDATE SET
                     revision = excluded.revision, file = excluded.file,
                     axis_request = excluded.axis_request, found_row = excluded.found_row,
                     found_column = excluded.found_column, detail_json = excluded.detail_json",
                params![
                    id,
                    revision,
                    file,
                    axis_request,
                    found_row,
                    found_column,
                    detail_json
                ],
            )?;
            self.connection.execute(
                "INSERT INTO inspection_sketches (dataset_id, registers) VALUES (?1, ?2)
                 ON CONFLICT(dataset_id) DO UPDATE SET registers = excluded.registers",
                params![id, sketch.as_bytes()],
            )?;
            self.connection.execute_batch("COMMIT")
        })();
        if result.is_err() {
            let _ = self.connection.execute_batch("ROLLBACK");
        }
        result
    }
}

fn format_error(message: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::other(message.to_owned())),
    )
}

/// Explicitly replace the legacy membership table with per-dataset sketches.
/// All reads, writes, verification and the DROP happen within one transaction:
/// any error (or process interruption) preserves the legacy table for a retry.
pub fn migrate_hll(path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Err(format!("cache does not exist: {}", path.display()).into());
    }
    let mut cache = Cache::open(path)?;
    if !cache.migration_needed()? {
        eprintln!("No legacy SMILES table to migrate in {}", path.display());
        return Ok(0);
    }
    let tx = cache
        .connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    eprintln!(
        "Validating legacy membership in {} before conversion...",
        path.display()
    );
    let orphans: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM inspected_smiles s
             LEFT JOIN inspection_results r ON r.dataset_id = s.dataset_id
             WHERE r.dataset_id IS NULL
         )",
        [],
        |row| row.get(0),
    )?;
    if orphans {
        return Err(
            "legacy SMILES contain a dataset without a saved inspection; migration rolled back"
                .into(),
        );
    }
    let sketch_orphans: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM inspection_sketches s
             LEFT JOIN inspection_results r ON r.dataset_id = s.dataset_id
             WHERE r.dataset_id IS NULL
         )",
        [],
        |row| row.get(0),
    )?;
    if sketch_orphans {
        return Err("sketch exists without a saved inspection; migration rolled back".into());
    }
    eprintln!(
        "Migrating legacy SMILES in {} (the old table is retained until commit)...",
        path.display()
    );
    let mut converted = 0;
    let mut scanned = 0_u64;
    {
        let mut inspections = tx.prepare(
            "SELECT dataset_id, detail_json FROM inspection_results ORDER BY dataset_id",
        )?;
        let mut members =
            tx.prepare("SELECT smiles FROM inspected_smiles WHERE dataset_id = ?1")?;
        let mut prior =
            tx.prepare("SELECT registers FROM inspection_sketches WHERE dataset_id = ?1")?;
        let mut insert =
            tx.prepare("INSERT INTO inspection_sketches (dataset_id, registers) VALUES (?1, ?2)")?;
        let rows = inspections.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (id, detail_json) = row?;
            let existing: Option<Vec<u8>> = prior.query_row([&id], |row| row.get(0)).optional()?;
            if let Some(bytes) = &existing {
                Sketch::from_bytes(bytes)
                    .map_err(|error| format!("invalid existing sketch for {id}: {error}"))?;
            }
            let mut sketch = Sketch::new();
            let mut membership = 0_u64;
            let mut legacy_rows = members.query([&id])?;
            while let Some(row) = legacy_rows.next()? {
                sketch.insert(row.get_ref(0)?.as_str()?);
                membership += 1;
                scanned += 1;
                if scanned % 1_000_000 == 0 {
                    eprintln!("  {converted} datasets converted, {scanned} legacy rows scanned");
                }
            }
            if existing.is_some() && membership != 0 {
                return Err(format!(
                    "dataset {id} has both legacy membership and a sketch; migration rolled back"
                )
                .into());
            }
            if existing.is_none() {
                let detail: serde_json::Value = serde_json::from_str(&detail_json)
                    .map_err(|error| format!("invalid saved inspection for {id}: {error}"))?;
                let valid = detail
                    .get("valid_smiles")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        format!("saved inspection for {id} has no exact valid_smiles count")
                    })?;
                if (valid == 0 && membership != 0)
                    || (valid != 0 && membership == 0)
                    || membership > valid
                {
                    return Err(format!("missing or inconsistent legacy membership for {id}: {membership} distinct strings vs {valid} valid cells; migration rolled back").into());
                }
                insert.execute(params![id, sketch.as_bytes()])?;
                converted += 1;
                if converted % 100 == 0 {
                    eprintln!("  {converted} datasets converted, {scanned} legacy rows scanned");
                }
            }
        }
    }
    let missing: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM inspection_results r
             LEFT JOIN inspection_sketches s ON r.dataset_id = s.dataset_id
             WHERE s.dataset_id IS NULL
         )",
        [],
        |row| row.get(0),
    )?;
    if missing {
        return Err("not all saved inspections have sketches; migration rolled back".into());
    }
    tx.execute_batch("DROP TABLE inspected_smiles")?;
    tx.commit()?;
    eprintln!(
        "Migrated {converted} inspections ({scanned} legacy rows); legacy table removed. VACUUM reclaims disk space now; retry with --compact-cache if compaction fails."
    );
    Ok(converted)
}

/// Rewrite the SQLite file to reclaim the space released by migration.
/// Safe to retry separately if the migration committed but compaction failed.
pub fn compact(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let connection =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let busy: i64 =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    if busy != 0 {
        return Err(
            "WAL checkpoint is busy; retry --compact-cache when other readers close".into(),
        );
    }
    eprintln!("Compacting {} with SQLite VACUUM...", path.display());
    connection.execute_batch("VACUUM")?;
    let busy: i64 =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    if busy != 0 {
        return Err("VACUUM finished but the WAL checkpoint is busy; retry --compact-cache".into());
    }
    eprintln!("SQLite compaction complete for {}", path.display());
    Ok(())
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_revision_must_match_but_unknown_revision_uses_fresh_entry() {
        let cache = Cache::open(Path::new(":memory:")).unwrap();
        let result = r#"{"dataset":"org/data","status":"found"}"#;
        cache.put("org/data", Some("sha-a"), result).unwrap();

        assert_eq!(
            cache.get("org/data", Some("sha-a")).unwrap().as_deref(),
            Some(result)
        );
        assert_eq!(cache.get("org/data", Some("sha-b")).unwrap(), None);
        assert_eq!(
            cache.get("org/data", None).unwrap().as_deref(),
            Some(result)
        );

        cache.put("org/data", Some("sha-b"), "updated").unwrap();
        assert_eq!(cache.get("org/data", Some("sha-a")).unwrap(), None);
        assert_eq!(
            cache.get("org/data", Some("sha-b")).unwrap().as_deref(),
            Some("updated")
        );
        cache.put("org/data", None, "no revision").unwrap();
        assert_eq!(cache.get("org/data", Some("sha-b")).unwrap(), None);
    }

    #[test]
    fn stale_entry_is_not_retrieved_even_when_revision_matches() {
        let cache = Cache::open(Path::new(":memory:")).unwrap();
        cache.put("org/data", Some("sha-a"), "completed").unwrap();
        cache
            .connection
            .execute(
                "UPDATE probe_cache SET checked_at = ?1 WHERE dataset_id = ?2",
                params![unix_now() - TTL_SECONDS - 60, "org/data"],
            )
            .unwrap();

        assert_eq!(cache.get("org/data", Some("sha-a")).unwrap(), None);
        assert_eq!(cache.get("org/data", None).unwrap(), None);
        cache.put("org/data", Some("sha-a"), "refreshed").unwrap();
        assert_eq!(
            cache.get("org/data", Some("sha-a")).unwrap().as_deref(),
            Some("refreshed")
        );
    }

    #[test]
    fn inspection_selects_only_file_level_matches_with_pinned_revision() {
        let cache = Cache::open(Path::new(":memory:")).unwrap();
        cache
            .put(
                "org/chem|direct:v3|thorough=false|rows=1|files=1",
                Some("sha-pinned"),
                r#"{"dataset":"org/chem","status":"found","location":{"file":"train.csv","row":0,"column":"SMILES"}}"#,
            )
            .unwrap();
        cache
            .put(
                "org/other|direct:v3|thorough=false|rows=1|files=1",
                Some("sha-other"),
                r#"{"dataset":"org/other","status":"skipped"}"#,
            )
            .unwrap();
        cache
            .put(
                "org/legacy|direct:v1",
                None,
                r#"{"dataset":"org/legacy","status":"found","location":{"config":"default","split":"train"}}"#,
            )
            .unwrap();
        let rows = cache.found().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].revision.as_deref(), Some("sha-pinned"));
        assert!(rows[0].result_json.contains("train.csv"));
    }
    #[test]
    fn completed_result_survives_reopening_database() {
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT_DB: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "streams-of-whiskey-cache-{}-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_DB.fetch_add(1, Ordering::Relaxed),
        ));
        let result = r#"{"dataset":"org/data","has_smiles":true,"status":"found"}"#;
        {
            let cache = Cache::open(&path).unwrap();
            cache.put("org/data", Some("sha-a"), result).unwrap();
        }
        let reopened = Cache::open(&path).unwrap();
        assert_eq!(
            reopened.get("org/data", Some("sha-a")).unwrap().as_deref(),
            Some(result)
        );
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn inspection_replacement_merges_overlap_and_preserves_old_data_until_commit() {
        let cache = Cache::open(Path::new(":memory:")).unwrap();
        assert!(!cache.migration_needed().unwrap());
        let sketch = |strings: &[&str]| {
            let mut sketch = Sketch::new();
            for string in strings {
                sketch.insert(string);
            }
            sketch
        };
        cache
            .replace_inspection(
                "org/one",
                "sha-a",
                "train.csv",
                "column",
                0,
                "smiles",
                "first",
                &sketch(&["CCO", "C(C)O", "CCO"]),
            )
            .unwrap();
        cache
            .replace_inspection(
                "org/two",
                "sha-b",
                "data.parquet",
                "auto",
                4,
                "smiles",
                "second",
                &sketch(&["CCO", "N"]),
            )
            .unwrap();
        let estimate = || {
            let mut union = Sketch::new();
            let mut statement = cache
                .connection
                .prepare("SELECT registers FROM inspection_sketches")
                .unwrap();
            for blob in statement
                .query_map([], |row| row.get::<_, Vec<u8>>(0))
                .unwrap()
            {
                union.merge(&Sketch::from_bytes(&blob.unwrap()).unwrap());
            }
            union.estimate()
        };
        assert_eq!(estimate(), 3);
        let interrupted = sketch(&["O"]);
        assert_eq!(estimate(), 3);
        assert_eq!(
            cache.inspection("org/one").unwrap().unwrap().detail_json,
            "first"
        );

        cache
            .replace_inspection(
                "org/one",
                "sha-new",
                "new.tsv",
                "row",
                7,
                "x",
                "replaced",
                &interrupted,
            )
            .unwrap();
        assert_eq!(estimate(), 3);
        let first = cache.inspection("org/one").unwrap().unwrap();
        assert_eq!(
            (first.revision.as_str(), first.file.as_str()),
            ("sha-new", "new.tsv")
        );
        assert_eq!(first.detail_json, "replaced");
        let bytes: Vec<u8> = cache
            .connection
            .query_row(
                "SELECT registers FROM inspection_sketches WHERE dataset_id = 'org/one'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(Sketch::from_bytes(&bytes).unwrap().estimate(), 1);
        let legacy: i64 = cache
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'inspected_smiles')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(legacy, 0);
    }

    #[test]
    fn failed_sketch_write_does_not_change_completed_detail() {
        let cache = Cache::open(Path::new(":memory:")).unwrap();
        let mut sketch = Sketch::new();
        sketch.insert("CCO");
        cache
            .replace_inspection(
                "org/one",
                "sha-a",
                "train.csv",
                "column",
                0,
                "smiles",
                "first",
                &sketch,
            )
            .unwrap();
        cache
            .connection
            .execute_batch(
                "CREATE TRIGGER fail_sketch BEFORE UPDATE ON inspection_sketches
             BEGIN SELECT RAISE(ABORT, 'disk write failed'); END",
            )
            .unwrap();
        sketch.insert("N");
        assert!(
            cache
                .replace_inspection(
                    "org/one", "sha-b", "new.csv", "column", 0, "smiles", "second", &sketch,
                )
                .is_err()
        );
        let saved = cache.inspection("org/one").unwrap().unwrap();
        assert_eq!(
            (saved.revision.as_str(), saved.detail_json.as_str()),
            ("sha-a", "first")
        );
        let bytes: Vec<u8> = cache
            .connection
            .query_row(
                "SELECT registers FROM inspection_sketches WHERE dataset_id='org/one'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(Sketch::from_bytes(&bytes).unwrap().estimate(), 1);
    }
    fn disposable_path() -> std::path::PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "streams-of-whiskey-hll-migration-{}-{suffix}.sqlite",
            std::process::id()
        ))
    }

    fn saved_legacy_inspection(cache: &Cache, id: &str, valid: u64, smiles: &[&str]) {
        cache
            .connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS inspected_smiles (
                 dataset_id TEXT NOT NULL,
                 smiles TEXT NOT NULL,
                 PRIMARY KEY(dataset_id, smiles)
             );
             CREATE INDEX IF NOT EXISTS inspected_smiles_by_smiles ON inspected_smiles(smiles);",
            )
            .unwrap();
        cache
            .connection
            .execute(
                "INSERT INTO inspection_results
             (dataset_id, revision, file, axis_request, found_row, found_column, detail_json)
             VALUES (?1, 'sha', 'data.csv', 'column', 0, 'smiles', ?2)",
                params![
                    id,
                    format!(r#"{{"valid_smiles":{valid},"status":"complete"}}"#)
                ],
            )
            .unwrap();
        for smile in smiles {
            cache
                .connection
                .execute(
                    "INSERT INTO inspected_smiles (dataset_id, smiles) VALUES (?1, ?2)",
                    params![id, smile],
                )
                .unwrap();
        }
    }

    #[test]
    fn explicit_migration_preserves_saved_reports_and_merges_legacy_membership() {
        let path = disposable_path();
        {
            let cache = Cache::open(&path).unwrap();
            saved_legacy_inspection(&cache, "org/one", 3, &["CCO", "N"]);
            saved_legacy_inspection(&cache, "org/two", 2, &["N", "O"]);
            saved_legacy_inspection(&cache, "org/empty", 0, &[]);
            assert!(cache.migration_needed().unwrap());
        }
        assert_eq!(migrate_hll(&path).unwrap(), 3);
        let cache = Cache::open(&path).unwrap();
        assert!(!cache.migration_needed().unwrap());
        let mut union = Sketch::new();
        let mut statement = cache
            .connection
            .prepare("SELECT registers FROM inspection_sketches")
            .unwrap();
        for bytes in statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .unwrap()
        {
            union.merge(&Sketch::from_bytes(&bytes.unwrap()).unwrap());
        }
        assert_eq!(union.estimate(), 3);
        assert!(
            cache
                .inspection("org/one")
                .unwrap()
                .unwrap()
                .detail_json
                .contains("\"valid_smiles\":3")
        );
        let format: String = cache
            .connection
            .query_row(
                "SELECT value FROM cache_metadata WHERE key='sketch_format'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(format, FORMAT);
        drop(statement);
        drop(cache);
        assert_eq!(migrate_hll(&path).unwrap(), 0);
        compact(&path).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn unsuccessful_migration_keeps_all_legacy_rows_for_retry() {
        let path = disposable_path();
        {
            let cache = Cache::open(&path).unwrap();
            saved_legacy_inspection(&cache, "org/one", 1, &["CCO"]);
            cache
                .connection
                .execute(
                    "INSERT INTO inspected_smiles (dataset_id, smiles) VALUES ('org/orphan', 'N')",
                    [],
                )
                .unwrap();
        }
        assert!(
            migrate_hll(&path)
                .unwrap_err()
                .to_string()
                .contains("without a saved inspection")
        );
        let cache = Cache::open(&path).unwrap();
        assert!(cache.migration_needed().unwrap());
        let old_count: u64 = cache
            .connection
            .query_row("SELECT COUNT(*) FROM inspected_smiles", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(old_count, 2);
        let sketches: u64 = cache
            .connection
            .query_row("SELECT COUNT(*) FROM inspection_sketches", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(sketches, 0);
        cache
            .connection
            .execute(
                "DELETE FROM inspected_smiles WHERE dataset_id='org/orphan'",
                [],
            )
            .unwrap();
        drop(cache);
        assert_eq!(migrate_hll(&path).unwrap(), 1);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn migration_rolls_back_earlier_sketches_when_later_inspection_is_incomplete() {
        let path = disposable_path();
        {
            let cache = Cache::open(&path).unwrap();
            saved_legacy_inspection(&cache, "org/one", 1, &["CCO"]);
            saved_legacy_inspection(&cache, "org/two", 2, &[]);
        }
        assert!(migrate_hll(&path).is_err());
        let cache = Cache::open(&path).unwrap();
        assert!(cache.migration_needed().unwrap());
        let sketches: u64 = cache
            .connection
            .query_row("SELECT COUNT(*) FROM inspection_sketches", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(sketches, 0);
        cache
            .connection
            .execute(
                "INSERT INTO inspected_smiles (dataset_id, smiles) VALUES ('org/two', 'N')",
                [],
            )
            .unwrap();
        drop(cache);
        assert_eq!(migrate_hll(&path).unwrap(), 2);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn migration_rejects_missing_membership_and_wrong_sketch_format() {
        let path = disposable_path();
        {
            let cache = Cache::open(&path).unwrap();
            saved_legacy_inspection(&cache, "org/one", 1, &[]);
        }
        assert!(
            migrate_hll(&path)
                .unwrap_err()
                .to_string()
                .contains("missing or inconsistent")
        );
        let cache = Cache::open(&path).unwrap();
        assert!(cache.migration_needed().unwrap());
        cache
            .connection
            .execute(
                "UPDATE cache_metadata SET value='hll:p14:other-hash:v1' WHERE key='sketch_format'",
                [],
            )
            .unwrap();
        drop(cache);
        assert!(
            Cache::open(&path)
                .err()
                .unwrap()
                .to_string()
                .contains("unsupported sketch format")
        );
        assert!(migrate_hll(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
