//! Ingestion-only benchmark: no network, file parsing, or SMILES validation.
//! Both paths consume the same deterministic, regenerated stream of synthetic
//! SMILES-like strings; regeneration keeps memory bounded for large runs.
//! The strings stand in for values that have already passed validation.
//! Run with `cargo run --release --example benchmark_uniques -- --help`.

use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use streams_of_whiskey::hll::Sketch;

const DEFAULT_DATASETS: usize = 3;
const DEFAULT_ROWS: usize = 10_000;

struct Config {
    datasets: usize,
    rows: usize,
}

fn usage() {
    println!(
        "Usage: benchmark_uniques [--datasets N] [--rows N]\n\
         Defaults: --datasets {DEFAULT_DATASETS} --rows {DEFAULT_ROWS}\n\
         Each dataset receives N synthetic, already-validated SMILES-like strings,\n\
         with roughly half distinct and overlap between adjacent datasets.\n\
         Measures only ingestion plus per-dataset commits (including generation),\n\
         NOT network, parsing, validation, truth queries, or WAL checkpointing.\n\
         Input is regenerated identically for each strategy, not kept in memory."
    );
}

fn parse_args() -> Result<Option<Config>, Box<dyn Error>> {
    let mut config = Config {
        datasets: DEFAULT_DATASETS,
        rows: DEFAULT_ROWS,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            usage();
            return Ok(None);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {arg}"))?;
        let number: usize = value.parse()?;
        if number == 0 {
            return Err(format!("{arg} must be positive").into());
        }
        match arg.as_str() {
            "--datasets" => config.datasets = number,
            "--rows" => config.rows = number,
            _ => return Err(format!("unknown option: {arg}").into()),
        }
    }
    config
        .datasets
        .checked_mul(config.rows)
        .ok_or("datasets * rows exceeds usize")?;
    Ok(Some(config))
}

// The first N/2 identifiers in each dataset overlap the previous dataset's
// last N/2. Every identifier maps to exactly one variable-length string.
fn for_each_smiles(
    dataset: usize,
    rows: usize,
    mut insert: impl FnMut(&str) -> Result<(), rusqlite::Error>,
) -> Result<(), rusqlite::Error> {
    let distinct = rows.div_ceil(2);
    let offset = dataset * (distinct / 2);
    let mut smiles = String::with_capacity(40);
    for index in 0..rows {
        let mut identifier = offset + index % distinct;
        smiles.clear();
        for _ in 0..1 + identifier % 7 {
            smiles.push('C');
        }
        smiles.push('N');
        loop {
            smiles.push(match identifier % 3 {
                0 => 'C',
                1 => 'N',
                _ => 'O',
            });
            identifier /= 3;
            if identifier == 0 {
                break;
            }
        }
        insert(&smiles)?;
    }
    Ok(())
}

struct TempDatabaseDir(PathBuf);

impl TempDatabaseDir {
    fn new() -> io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        for attempt in 0..100 {
            let path = std::env::temp_dir().join(format!(
                "streams-of-whiskey-uniques-{}-{timestamp}-{attempt}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not create a unique benchmark directory",
        ))
    }
}

impl Drop for TempDatabaseDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn open_db(path: &Path, schema: &str) -> Result<Connection, rusqlite::Error> {
    let connection = Connection::open(path)?;
    connection.execute_batch("PRAGMA journal_mode = WAL; PRAGMA temp_store = FILE;")?;
    connection.execute_batch(schema)?;
    Ok(connection)
}

struct Sizes {
    db: u64,
    wal: u64,
    shm: u64,
}

impl Sizes {
    fn total(&self) -> u64 {
        self.db + self.wal + self.shm
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn optional_size(path: &Path) -> io::Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

fn sizes(path: &Path) -> io::Result<Sizes> {
    Ok(Sizes {
        db: fs::metadata(path)?.len(),
        wal: optional_size(&sidecar(path, "-wal"))?,
        shm: optional_size(&sidecar(path, "-shm"))?,
    })
}

struct ExactResult {
    elapsed: Duration,
    export_elapsed: Duration,
    distinct: u64,
    sizes: Sizes,
}

fn benchmark_exact(path: &Path, config: &Config) -> Result<ExactResult, Box<dyn Error>> {
    let mut connection = open_db(
        path,
        "CREATE TABLE inspected_smiles (
             dataset_id TEXT NOT NULL, smiles TEXT NOT NULL,
             PRIMARY KEY (dataset_id, smiles)
         );
         CREATE INDEX inspected_smiles_by_smiles ON inspected_smiles(smiles);
         CREATE TEMP TABLE inspection_smiles_stage (smiles TEXT PRIMARY KEY);",
    )?;
    let start = Instant::now();
    for dataset in 0..config.datasets {
        let id = format!("dataset-{dataset:06}");
        let transaction = connection.transaction()?;
        transaction.execute("DELETE FROM inspection_smiles_stage", [])?;
        {
            let mut insert = transaction.prepare_cached(
                "INSERT OR IGNORE INTO inspection_smiles_stage (smiles) VALUES (?1)",
            )?;
            for_each_smiles(dataset, config.rows, |smiles| {
                insert.execute([smiles])?;
                Ok(())
            })?;
        }
        transaction.execute("DELETE FROM inspected_smiles WHERE dataset_id = ?1", [&id])?;
        transaction.execute(
            "INSERT INTO inspected_smiles (dataset_id, smiles)
             SELECT ?1, smiles FROM inspection_smiles_stage",
            [&id],
        )?;
        transaction.commit()?;
    }
    let elapsed = start.elapsed();
    let export_start = Instant::now();
    let distinct: u64 = connection.query_row(
        "SELECT COUNT(DISTINCT smiles) FROM inspected_smiles",
        [],
        |row| row.get(0),
    )?;
    let export_elapsed = export_start.elapsed();
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(connection);
    Ok(ExactResult {
        elapsed,
        export_elapsed,
        distinct,
        sizes: sizes(path)?,
    })
}

struct HllResult {
    elapsed: Duration,
    export_elapsed: Duration,
    estimate: u64,
    sizes: Sizes,
}

fn benchmark_hll(path: &Path, config: &Config) -> Result<HllResult, Box<dyn Error>> {
    let mut connection = open_db(
        path,
        "CREATE TABLE inspection_sketches (
             dataset_id TEXT PRIMARY KEY, registers BLOB NOT NULL
         );",
    )?;
    let start = Instant::now();
    for dataset in 0..config.datasets {
        let id = format!("dataset-{dataset:06}");
        let transaction = connection.transaction()?;
        let mut sketch = Sketch::new();
        for_each_smiles(dataset, config.rows, |smiles| {
            sketch.insert(smiles);
            Ok(())
        })?;
        transaction.execute(
            "INSERT INTO inspection_sketches (dataset_id, registers) VALUES (?1, ?2)
             ON CONFLICT(dataset_id) DO UPDATE SET registers = excluded.registers",
            params![id, sketch.as_bytes()],
        )?;
        transaction.commit()?;
    }
    let elapsed = start.elapsed();
    let export_start = Instant::now();
    let mut merged = Sketch::new();
    {
        let mut statement = connection.prepare("SELECT registers FROM inspection_sketches")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            merged.merge_bytes(row.get_ref(0)?.as_blob()?)?;
        }
    }
    let estimate = merged.estimate();
    let export_elapsed = export_start.elapsed();
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(connection);
    Ok(HllResult {
        elapsed,
        estimate,
        export_elapsed,
        sizes: sizes(path)?,
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let Some(config) = parse_args()? else {
        return Ok(());
    };
    let temp = TempDatabaseDir::new()?;
    let exact = benchmark_exact(&temp.0.join("exact.sqlite3"), &config)?;
    let hll = benchmark_hll(&temp.0.join("hll.sqlite3"), &config)?;
    let input_rows = config.datasets * config.rows;
    let throughput = |elapsed: Duration| input_rows as f64 / elapsed.as_secs_f64();
    let error = (hll.estimate as f64 / exact.distinct as f64 - 1.0) * 100.0;
    println!(
        "Ingestion only (no network, parsing or validation); {input_rows} rows across {} datasets",
        config.datasets
    );
    println!("                        exact SQLite           HLL p=14");
    println!(
        "Ingest + commit       {:>12.3} s       {:>12.3} s",
        exact.elapsed.as_secs_f64(),
        hll.elapsed.as_secs_f64()
    );
    println!(
        "Input throughput      {:>12.0} rows/s  {:>12.0} rows/s",
        throughput(exact.elapsed),
        throughput(hll.elapsed)
    );
    println!(
        "Union count query    {:>12.3} s       {:>12.3} s",
        exact.export_elapsed.as_secs_f64(),
        hll.export_elapsed.as_secs_f64()
    );
    println!(
        "DB + WAL + SHM        {:>12} bytes   {:>12} bytes",
        exact.sizes.total(),
        hll.sizes.total()
    );
    println!(
        "  (DB / WAL / SHM)    {} / {} / {}       {} / {} / {} bytes",
        exact.sizes.db,
        exact.sizes.wal,
        exact.sizes.shm,
        hll.sizes.db,
        hll.sizes.wal,
        hll.sizes.shm
    );
    println!(
        "Global distinct       {:>12}         {:>12} (error {error:+.2}%)",
        exact.distinct, hll.estimate
    );
    println!(
        "Size ratio (exact / HLL): {:.2}x",
        exact.sizes.total() as f64 / hll.sizes.total() as f64
    );
    Ok(())
}
