use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};

const TTL_SECONDS: i64 = 24 * 60 * 60;

pub struct Cache {
    connection: Connection,
}

pub struct CachedFound {
    pub revision: Option<String>,
    pub result_json: String,
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
             );",
        )?;
        Ok(Self { connection })
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
}
