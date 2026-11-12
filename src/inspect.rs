use std::cell::Cell;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, IsTerminal, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue, RANGE};
use serde::{Deserialize, Serialize};

use crate::cache::Cache;
use crate::direct_probe::RangeSource;
use crate::full_scan::{Axis, ScanCounts, ScanObserver};
use crate::rate::RateGate;
use crate::{
    dataset_url, full_parquet, full_text, parse_response, send_with_gate, tabular_format,
    text_probe,
};

const STREAM_TIMEOUT: Duration = Duration::from_secs(3600);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SNIFF_PREFIX_BYTES: usize = 128 * 1024;
const OUTPUT_HEADERS: &[&str] = &[
    "dataset",
    "version",
    "version_source",
    "dataset_url",
    "file",
    "file_url",
    "format",
    "scope",
    "axis",
    "found_row",
    "found_column",
    "file_bytes",
    "rows_scanned",
    "cells_scanned",
    "valid_smiles",
    "invalid_smiles",
    "empty_cells",
    "non_string_cells",
    "oversized_cells",
    "valid_of_parsed_percent",
    "status",
    "error",
    "checked_at_unix",
];
struct InspectionProgress {
    multi: Option<MultiProgress>,
    overall: Option<ProgressBar>,
    total_valid: u64,
}

struct DatasetProgress {
    bar: Option<ProgressBar>,
    overall: Option<ProgressBar>,
    completed_valid: u64,
    label: String,
    latest_valid: Cell<u64>,
    last_log: Cell<Instant>,
}

impl InspectionProgress {
    fn new(total: usize) -> Self {
        if !io::stderr().is_terminal() {
            return Self {
                multi: None,
                overall: None,
                total_valid: 0,
            };
        }
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
        let overall = multi.add(ProgressBar::new(total as u64));
        overall.set_style(
            ProgressStyle::with_template(
                "{spinner:.cyan} [{bar:24.cyan/blue}] {pos}/{len} datasets · {msg} [{elapsed_precise}]",
            )
            .expect("valid progress template"),
        );
        overall.set_message("0 valid SMILES");
        Self {
            multi: Some(multi),
            overall: Some(overall),
            total_valid: 0,
        }
    }

    fn begin(&self, index: usize, total: usize, dataset: &str, file: &str) -> DatasetProgress {
        let label = format!("{index}/{total}: {dataset} / {file}");
        let bar = if let Some(multi) = &self.multi {
            let bar = multi.add(ProgressBar::new_spinner());
            bar.set_style(
                ProgressStyle::with_template(
                    "{spinner:.cyan} {prefix} · {msg} [{elapsed_precise}]",
                )
                .expect("valid progress template"),
            );
            bar.set_prefix(format!("{index}/{total}: {dataset}"));
            bar.enable_steady_tick(Duration::from_millis(120));
            Some(bar)
        } else {
            eprintln!("Inspecting {label}");
            None
        };
        DatasetProgress {
            bar,
            overall: self.overall.clone(),
            completed_valid: self.total_valid,
            label,
            latest_valid: Cell::new(0),
            last_log: Cell::new(Instant::now()),
        }
    }

    fn finish(
        &mut self,
        current: &DatasetProgress,
        status: &str,
        reported_valid: u64,
        incomplete: usize,
        ambiguous: usize,
    ) -> io::Result<()> {
        self.total_valid = self.total_valid.saturating_add(reported_valid);
        let summary = format!(
            "{} · {status} · {reported_valid} reported valid SMILES",
            current.label
        );
        if let (Some(multi), Some(bar)) = (&self.multi, &current.bar) {
            bar.finish_and_clear();
            multi.remove(bar);
            multi.println(&summary)?;
        } else {
            eprintln!("{summary}");
        }
        if let Some(overall) = &self.overall {
            overall.inc(1);
            overall.set_message(format!(
                "{} reported valid SMILES · {ambiguous} ambiguous · {incomplete} incomplete",
                self.total_valid
            ));
        }
        Ok(())
    }
}

impl DatasetProgress {
    fn phase(&self, phase: &str) {
        if let Some(bar) = &self.bar {
            bar.set_message(format!(
                "{phase} · {} valid SMILES",
                self.latest_valid.get()
            ));
        } else {
            eprintln!("{} · {phase}", self.label);
        }
    }

    fn text_size(&self, size: Option<u64>) {
        if let Some(bar) = &self.bar {
            let template = if let Some(size) = size {
                bar.set_length(size);
                "{spinner:.cyan} [{bar:24.cyan/blue}] {bytes}/{total_bytes} {prefix} · {msg} [{elapsed_precise}]"
            } else {
                "{spinner:.cyan} {bytes} received {prefix} · {msg} [{elapsed_precise}]"
            };
            bar.set_style(ProgressStyle::with_template(template).expect("valid progress template"));
        }
    }
}

impl ScanObserver for DatasetProgress {
    fn progress(&self, counts: &ScanCounts, total_rows: Option<u64>) {
        self.latest_valid.set(counts.valid);
        if let Some(bar) = &self.bar {
            if let Some(total) = total_rows {
                if bar.length() != Some(total) {
                    bar.set_length(total);
                    bar.set_style(
                        ProgressStyle::with_template(
                            "{spinner:.cyan} [{bar:24.cyan/blue}] {pos}/{len} rows {prefix} · {msg} [{elapsed_precise}]",
                        )
                        .expect("valid progress template"),
                    );
                }
                bar.set_position(counts.rows_scanned.min(total));
            }
            bar.set_message(format!(
                "downloading + parsing · {} valid SMILES · {} rows",
                counts.valid, counts.rows_scanned
            ));
            if let Some(overall) = &self.overall {
                overall.set_message(format!(
                    "{} valid SMILES ({} current)",
                    self.completed_valid.saturating_add(counts.valid),
                    counts.valid
                ));
            }
        } else if self.last_log.get().elapsed() >= Duration::from_secs(10) {
            eprintln!(
                "{} · {} rows parsed · {} valid SMILES",
                self.label, counts.rows_scanned, counts.valid
            );
            self.last_log.set(Instant::now());
        }
    }
}

#[derive(Deserialize)]
struct FoundLocation {
    file: String,
    row: u64,
    column: String,
    #[serde(default)]
    wide: bool,
}

#[derive(Deserialize)]
struct FoundHit {
    dataset: String,
    status: String,
    location: Option<FoundLocation>,
}

struct PinnedHit {
    hit: FoundHit,
    revision: Option<String>,
}

#[derive(Deserialize)]
struct RevisionInfo {
    sha: String,
}

#[derive(Serialize)]
struct DetailRow<'a> {
    dataset: &'a str,
    version: String,
    version_source: &'static str,
    dataset_url: String,
    file: &'a str,
    file_url: String,
    format: &'static str,
    scope: &'static str,
    axis: &'static str,
    found_row: u64,
    found_column: &'a str,
    file_bytes: Option<u64>,
    rows_scanned: Option<u64>,
    cells_scanned: Option<u64>,
    valid_smiles: Option<u64>,
    invalid_smiles: Option<u64>,
    empty_cells: Option<u64>,
    non_string_cells: Option<u64>,
    oversized_cells: Option<u64>,
    valid_of_parsed_percent: Option<String>,
    status: &'static str,
    error: String,
    checked_at_unix: u64,
}

fn load_hits(cache_path: &Path, input: Option<&Path>) -> Result<Vec<PinnedHit>, Box<dyn Error>> {
    let mut found = BTreeMap::new();
    if let Some(path) = input {
        for (index, line) in BufReader::new(File::open(path)?).lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let hit: FoundHit = serde_json::from_str(&line)
                .map_err(|err| format!("{}:{}: {err}", path.display(), index + 1))?;
            if hit.status == "found" {
                if hit.location.is_none() {
                    return Err(format!(
                        "{}:{}: found result has no location",
                        path.display(),
                        index + 1
                    )
                    .into());
                }
                // A combined JSONL log may contain reruns; its latest match wins.
                found.insert(
                    hit.dataset.clone(),
                    PinnedHit {
                        hit,
                        revision: None,
                    },
                );
            }
        }
    } else {
        // The cache may contain multiple probe budgets for one dataset. Keep the
        // most recently checked pinned file-level match, not an old Viewer entry.
        for entry in Cache::open(cache_path)?.found()? {
            let hit: FoundHit = serde_json::from_str(&entry.result_json)?;
            found.entry(hit.dataset.clone()).or_insert(PinnedHit {
                hit,
                revision: entry.revision,
            });
        }
    }
    if found.is_empty() {
        return Err("no file-level found results in the selected JSONL or cache".into());
    }
    Ok(found.into_values().collect())
}

fn client(token: Option<&str>, timeout: Duration) -> Result<Client, Box<dyn Error>> {
    let mut builder = Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .timeout(timeout);
    if let Some(token) = token {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))?,
        );
        builder = builder.default_headers(headers);
    }
    Ok(builder.build()?)
}

fn hub_url(id: &str, revision: Option<&str>) -> Result<Url, String> {
    let mut url = Url::parse("https://huggingface.co").map_err(|err| err.to_string())?;
    {
        let mut path = url.path_segments_mut().map_err(|_| "invalid Hub URL")?;
        if revision.is_none() {
            path.push("api");
        }
        path.push("datasets");
        for segment in id.split('/') {
            path.push(segment);
        }
        if let Some(revision) = revision {
            path.push("tree");
            path.push(revision);
        }
    }
    Ok(url)
}

fn current_revision(client: &Client, gate: &RateGate, id: &str) -> Result<String, String> {
    let url = hub_url(id, None)?;
    let response = send_with_gate(client, client.get(url).query(&[("expand[]", "sha")]), gate)?;
    let info: RevisionInfo = parse_response(response, "Hub dataset revision")?;
    if info.sha.is_empty() {
        return Err("Hub dataset revision has no SHA".into());
    }
    Ok(info.sha)
}

fn file_size(client: &Client, gate: &RateGate, url: &Url) -> Result<u64, String> {
    let response = send_with_gate(client, client.head(url.clone()), gate)?;
    if !response.status().is_success() {
        return Err(format!("resolver HEAD: HTTP {}", response.status()));
    }
    if let Some(size) = response.content_length() {
        return Ok(size);
    }
    let response = send_with_gate(
        client,
        client.get(url.clone()).header(RANGE, "bytes=0-0"),
        gate,
    )?;
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(format!(
            "resolver did not report file size with a partial range (HTTP {})",
            response.status()
        ));
    }
    let range = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|header| header.to_str().ok())
        .ok_or("resolver omitted Content-Range")?;
    let size = range
        .strip_prefix("bytes 0-0/")
        .ok_or("resolver returned an unexpected Content-Range")?
        .parse::<u64>()
        .map_err(|err| format!("resolver file size: {err}"))?;
    Ok(size)
}

fn scan_text_stream<R: Read>(
    mut reader: R,
    format: &str,
    column: &str,
    row: u64,
    axis: Axis,
    wide: bool,
    size: Option<u64>,
    progress: &DatasetProgress,
) -> Result<Option<ScanCounts>, String> {
    if axis != Axis::Auto || wide {
        progress.phase("downloading + parsing");
        return full_text::scan_with_progress(reader, format, column, row, axis, progress)
            .map(Some);
    }
    progress.phase("sniffing first 128 KiB");
    let mut prefix = Vec::with_capacity(size.map_or(SNIFF_PREFIX_BYTES, |length| {
        length.min(SNIFF_PREFIX_BYTES as u64) as usize
    }));
    reader
        .by_ref()
        .take(SNIFF_PREFIX_BYTES as u64)
        .read_to_end(&mut prefix)
        .map_err(|error| format!("text sniff: {error}"))?;
    let at_eof = prefix.len() < SNIFF_PREFIX_BYTES
        || size.is_some_and(|length| length == prefix.len() as u64);
    if !text_probe::sniff_prefix_column(&prefix, format, column, row, at_eof)? {
        return Ok(None);
    }
    progress.phase("downloading + parsing");
    let replay = io::Cursor::new(prefix).chain(reader);
    full_text::scan_with_progress(replay, format, column, row, axis, progress).map(Some)
}

fn scan_file(
    short_client: &Client,
    stream_client: &Client,
    gate: &Arc<RateGate>,
    url: &Url,
    format: &str,
    column: &str,
    row: u64,
    axis: Axis,
    wide: bool,
    progress: &DatasetProgress,
) -> Result<(Option<ScanCounts>, Option<u64>), String> {
    if format == "parquet" {
        progress.phase("checking file size");
        let size = file_size(short_client, gate, url)?;
        let source =
            RangeSource::new_unbounded(short_client.clone(), Arc::clone(gate), url.clone(), size);
        if axis == Axis::Auto && !wide {
            progress.phase("sniffing projected Parquet rows");
            if !crate::parquet_probe::sniff_column(&source, column, row)? {
                return Ok((None, Some(size)));
            }
        }
        progress.phase("downloading Parquet metadata");
        return full_parquet::scan_with_progress(&source, column, row, axis, progress)
            .map(|counts| (Some(counts), Some(size)));
    }
    progress.phase("connecting to file");
    let response = send_with_gate(stream_client, stream_client.get(url.clone()), gate)?;
    if response.status() != StatusCode::OK {
        return Err(format!(
            "resolver full-file request: HTTP {}",
            response.status()
        ));
    }
    let size = response.content_length();
    progress.text_size(size);
    let counts = if let Some(bar) = &progress.bar {
        scan_text_stream(
            bar.wrap_read(response),
            format,
            column,
            row,
            axis,
            wide,
            size,
            progress,
        )
    } else {
        scan_text_stream(response, format, column, row, axis, wide, size, progress)
    }?;
    Ok((counts, size))
}

fn set_counts(detail: &mut DetailRow<'_>, counts: ScanCounts) {
    detail.axis = counts.axis.label();
    detail.rows_scanned = Some(counts.rows_scanned);
    detail.cells_scanned = Some(counts.cells_scanned);
    detail.valid_smiles = Some(counts.valid);
    detail.invalid_smiles = Some(counts.invalid);
    detail.empty_cells = Some(counts.empty);
    detail.non_string_cells = Some(counts.non_string);
    detail.oversized_cells = Some(counts.oversized);
    let parsed = counts.valid + counts.invalid;
    if parsed != 0 {
        detail.valid_of_parsed_percent = Some(format!(
            "{:.2}",
            counts.valid as f64 * 100.0 / parsed as f64
        ));
    }
    if counts.oversized == 0 {
        detail.status = "complete";
    } else {
        detail.status = "partial";
        detail.error = format!("{} cells exceed the 64 KiB parsing limit", counts.oversized);
    }
}

pub fn run(
    cache_path: &Path,
    input: Option<&Path>,
    output: &Path,
    axis: Axis,
) -> Result<(), Box<dyn Error>> {
    let hits = load_hits(cache_path, input)?;
    let token = std::env::var("HF_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let short_client = client(token.as_deref(), REQUEST_TIMEOUT)?;
    let stream_client = client(token.as_deref(), STREAM_TIMEOUT)?;
    let gate = Arc::new(RateGate::new());
    // Never truncate an existing report: an interrupted run leaves completed rows intact.
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .from_writer(file);
    writer.write_record(OUTPUT_HEADERS)?;
    writer.flush()?;
    let mut failures = 0_usize;
    let mut ambiguous = 0_usize;
    let mut progress = InspectionProgress::new(hits.len());
    for (index, hit) in hits.iter().enumerate() {
        let location = hit
            .hit
            .location
            .as_ref()
            .expect("found hits have a file location");
        let current = progress.begin(index + 1, hits.len(), &hit.hit.dataset, &location.file);
        let mut detail = DetailRow {
            dataset: &hit.hit.dataset,
            version: String::new(),
            version_source: if hit.revision.is_some() {
                "cached_sha"
            } else {
                "current_sha"
            },
            dataset_url: String::new(),
            file: &location.file,
            file_url: String::new(),
            format: tabular_format(&location.file).unwrap_or("unsupported"),
            scope: "pinpointed_file",
            axis: axis.label(),
            found_row: location.row,
            found_column: &location.column,
            file_bytes: None,
            rows_scanned: None,
            cells_scanned: None,
            valid_smiles: None,
            invalid_smiles: None,
            empty_cells: None,
            non_string_cells: None,
            oversized_cells: None,
            valid_of_parsed_percent: None,
            status: "error",
            error: String::new(),
            checked_at_unix: 0,
        };
        let analysis = (|| -> Result<(), String> {
            let revision = match &hit.revision {
                Some(revision) => {
                    current.phase("using cached revision");
                    revision.clone()
                }
                None => {
                    current.phase("resolving current revision");
                    current_revision(&short_client, &gate, &hit.hit.dataset)?
                }
            };
            detail.version = revision.clone();
            detail.dataset_url = hub_url(&hit.hit.dataset, Some(&revision))?.to_string();
            let url = dataset_url(&hit.hit.dataset, &revision, Some(&location.file))?;
            detail.file_url = url.to_string();
            if detail.format == "unsupported" {
                return Err(format!("unsupported file extension: {}", location.file));
            }
            let (counts, size) = scan_file(
                &short_client,
                &stream_client,
                &gate,
                &url,
                detail.format,
                &location.column,
                location.row,
                axis,
                location.wide,
                &current,
            )?;
            detail.file_bytes = size;
            if let Some(counts) = counts {
                set_counts(&mut detail, counts);
            } else {
                detail.status = "ambiguous";
                detail.error =
                    "bounded sample lacks corroborating molecular evidence; full scan not attempted"
                        .into();
            }
            Ok(())
        })();
        if let Err(error) = analysis {
            detail.error = error;
        }
        detail.checked_at_unix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if detail.status == "ambiguous" {
            ambiguous += 1;
        }
        if matches!(detail.status, "partial" | "error") {
            failures += 1;
        }
        writer.serialize(&detail)?;
        writer.flush()?;
        progress.finish(
            &current,
            detail.status,
            detail.valid_smiles.unwrap_or(0),
            failures,
            ambiguous,
        )?;
    }
    if let Some(bar) = &progress.overall {
        bar.finish_with_message(format!(
            "{} reported valid SMILES · {ambiguous} ambiguous · {failures} incomplete",
            progress.total_valid
        ));
    }
    eprintln!(
        "Wrote {} dataset reports to {} ({} ambiguous, {} incomplete)",
        hits.len(),
        output.display(),
        ambiguous,
        failures
    );
    if failures != 0 {
        return Err(format!(
            "{failures} inspections incomplete; see {}",
            output.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::full_scan::ScanCounts;
    fn quiet_progress() -> DatasetProgress {
        DatasetProgress {
            bar: None,
            overall: None,
            completed_valid: 0,
            label: "fixture".into(),
            latest_valid: Cell::new(0),
            last_log: Cell::new(Instant::now()),
        }
    }

    #[test]
    fn ambiguous_codes_stop_after_a_bounded_prefix_but_molecules_replay_into_full_scan() {
        struct Metered<'a> {
            input: &'a [u8],
            read: &'a Cell<usize>,
        }
        impl Read for Metered<'_> {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let count = self.input.read(buffer)?;
                self.read.set(self.read.get() + count);
                Ok(count)
            }
        }

        let mut categories = b"sex,type\n".to_vec();
        while categories.len() < SNIFF_PREFIX_BYTES + 4096 {
            categories.extend_from_slice(b"F,C\nM,O\n");
        }
        let consumed = Cell::new(0);
        let counts = scan_text_stream(
            Metered {
                input: &categories,
                read: &consumed,
            },
            "csv",
            "sex",
            0,
            Axis::Auto,
            false,
            Some(categories.len() as u64),
            &quiet_progress(),
        )
        .unwrap();
        assert!(counts.is_none());
        assert_eq!(consumed.get(), SNIFF_PREFIX_BYTES);

        let molecules = b"sex,type\nCCO,C\nCCN,O\n";
        let counts = scan_text_stream(
            molecules.as_slice(),
            "csv",
            "sex",
            0,
            Axis::Auto,
            false,
            Some(molecules.len() as u64),
            &quiet_progress(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            (counts.axis, counts.rows_scanned, counts.valid),
            (Axis::Column, 2, 2)
        );

        let wide = b"kind,sample_a\nSMILES,F\n";
        let counts = scan_text_stream(
            wide.as_slice(),
            "csv",
            "sample_a",
            0,
            Axis::Auto,
            false,
            Some(wide.len() as u64),
            &quiet_progress(),
        )
        .unwrap()
        .unwrap();
        assert_eq!((counts.axis, counts.valid), (Axis::Row, 1));
    }

    #[test]
    fn report_distinguishes_complete_counts_from_unparsed_oversized_cells() {
        let mut row = DetailRow {
            dataset: "org/data",
            version: String::new(),
            version_source: "cached_sha",
            dataset_url: String::new(),
            file: "train.csv",
            file_url: String::new(),
            format: "csv",
            scope: "pinpointed_file",
            axis: "auto",
            found_row: 0,
            found_column: "SMILES",
            file_bytes: None,
            rows_scanned: None,
            cells_scanned: None,
            valid_smiles: None,
            invalid_smiles: None,
            empty_cells: None,
            non_string_cells: None,
            oversized_cells: None,
            valid_of_parsed_percent: None,
            status: "error",
            error: String::new(),
            checked_at_unix: 0,
        };
        let mut counts = ScanCounts::new(Axis::Column);
        counts.rows_scanned = 2;
        counts.check(Some("CCO"));
        counts.check(Some("not a smile!"));
        set_counts(&mut row, counts);
        assert_eq!(row.status, "complete");
        assert_eq!(row.valid_smiles, Some(1));
        assert_eq!(row.valid_of_parsed_percent.as_deref(), Some("50.00"));

        let mut counts = ScanCounts::new(Axis::Column);
        counts.check(Some(&"C".repeat(crate::full_scan::MAX_CELL_BYTES + 1)));
        set_counts(&mut row, counts);
        assert_eq!(row.status, "partial");
        assert_eq!(row.oversized_cells, Some(1));
    }

    #[test]
    fn csv_report_header_has_one_column_for_every_serialized_field() {
        let mut writer = csv::WriterBuilder::new()
            .delimiter(b'\t')
            .has_headers(false)
            .from_writer(Vec::new());
        writer.write_record(OUTPUT_HEADERS).unwrap();
        let row = DetailRow {
            dataset: "org/data",
            version: "sha".into(),
            version_source: "cached_sha",
            dataset_url: "https://huggingface.co/datasets/org/data/tree/sha".into(),
            file: "train.csv",
            file_url: String::new(),
            format: "csv",
            scope: "pinpointed_file",
            axis: "column",
            found_row: 0,
            found_column: "SMILES",
            file_bytes: Some(5),
            rows_scanned: Some(1),
            cells_scanned: Some(1),
            valid_smiles: Some(1),
            invalid_smiles: Some(0),
            empty_cells: Some(0),
            non_string_cells: Some(0),
            oversized_cells: Some(0),
            valid_of_parsed_percent: Some("100.00".into()),
            status: "complete",
            error: String::new(),
            checked_at_unix: 1,
        };
        writer.serialize(row).unwrap();
        let data = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(data.as_bytes());
        let records = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].get(0), Some("org/data"));
        assert_eq!(records[0].get(14), Some("1"));
        assert_eq!(records[0].len(), OUTPUT_HEADERS.len());
    }
}
