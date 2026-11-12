mod cache;
mod direct_probe;
mod full_parquet;
mod full_scan;
mod full_text;
mod inspect;
mod parquet_probe;
mod rate;
mod text_probe;

use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use cache::Cache;
use clap::Parser;
use console::style;
use direct_probe::{FileSample, RangeSource};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use rand::Rng;
use rate::RateGate;
use rayon::prelude::*;
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, LINK};
use serde::{Deserialize, Serialize};

const HUB_DATASETS: &str = "https://huggingface.co/api/datasets";
const PAGE_SIZE: u64 = 200;
const MAX_RESPONSE_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Parser)]
#[command(about = "Screen chemistry-tagged Hugging Face CSV/TSV files for parseable SMILES")]
struct Args {
    /// Optional dataset IDs; omit to crawl the public Hub dataset listing
    datasets: Vec<String>,
    /// Stop after this many discovered datasets (default: crawl until the listing ends)
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..), conflicts_with = "datasets")]
    max_datasets: Option<u64>,
    /// Limit discovery to Hub results matching a keyword (e.g. smiles); not required
    #[arg(long, conflicts_with = "datasets")]
    search: Option<String>,
    /// Extra random rows per file, after up to 32 deterministic sniffing rows
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=100))]
    samples: u32,
    /// Maximum tabular files to probe per dataset
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=100))]
    max_files: u32,
    /// Maximum simultaneous dataset probes
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=64))]
    jobs: u32,
    /// Disable metadata screening and include Parquet/JSONL plus a deeper tree scan
    #[arg(long)]
    thorough: bool,
    /// Skip the chemistry keyword check, but keep fast CSV/TSV and access filters
    #[arg(long, conflicts_with = "thorough")]
    no_chemistry_filter: bool,
    /// Emit machine-readable JSON lines instead of human-friendly progress and color
    #[arg(long)]
    json: bool,
    /// Path to the SQLite probe cache
    #[arg(long, default_value = ".streams-of-whiskey.sqlite3")]
    cache: PathBuf,
    /// Scan previously found files completely and write a detailed TSV
    #[arg(long, conflicts_with_all = ["datasets", "max_datasets", "search", "json", "thorough", "no_chemistry_filter"])]
    inspect_found: bool,
    /// Crawl JSONL input; omit to inspect file-level matches in the SQLite cache
    #[arg(long = "input", requires = "inspect_found")]
    inspect_input: Option<PathBuf>,
    /// Inspection TSV destination (default: found-details.tsv; must not already exist)
    #[arg(long = "output", requires = "inspect_found")]
    inspect_output: Option<PathBuf>,
    /// Force a whole column or one wide row instead of automatic orientation
    #[arg(long = "axis", requires = "inspect_found", value_enum)]
    inspect_axis: Option<full_scan::Axis>,
    /// Ignore cached results and probe again
    #[arg(long)]
    refresh: bool,
}

#[derive(Deserialize)]
struct HubDataset {
    id: String,
    #[serde(default)]
    sha: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    gated: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct TreeEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
    size: Option<u64>,
}

struct TreeFiles {
    files: Vec<TreeEntry>,
    truncated: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Found,
    NotFoundInSample,
    Inconclusive,
    Skipped,
}

#[derive(Serialize, Deserialize)]
struct Location {
    file: String,
    row: u64,
    column: String,
    #[serde(default, skip_serializing_if = "is_false")]
    wide: bool,
}

#[derive(Serialize, Deserialize)]
struct DatasetResult<'a> {
    dataset: &'a str,
    has_smiles: bool,
    status: Status,
    sampled_rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<Location>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    cached: bool,
    #[serde(skip)]
    cacheable: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

fn human_result(result: &DatasetResult<'_>) -> String {
    let line = match result.status {
        Status::Found => {
            let location = result
                .location
                .as_ref()
                .expect("found results have a location");
            format!(
                "{} {}  {} · row {} · column {}",
                style("● SMILES").green().bold(),
                style(result.dataset).green().bold(),
                location.file,
                location.row,
                style(&location.column).green().bold()
            )
        }
        Status::NotFoundInSample => format!(
            "{} {}  ({} rows checked)",
            style("○ not found in sample").dim(),
            result.dataset,
            result.sampled_rows
        ),
        Status::Inconclusive => format!(
            "{} {}  ({} rows checked; {})",
            style("? inconclusive").yellow(),
            result.dataset,
            result.sampled_rows,
            result.reason.as_deref().unwrap_or("incomplete sample")
        ),
        Status::Skipped => format!(
            "{} {}  ({})",
            style("– skipped").dim(),
            result.dataset,
            result.reason.as_deref().unwrap_or("not screened")
        ),
    };
    if result.cached {
        format!("{line}  {}", style("[cached]").dim())
    } else {
        line
    }
}

struct Reporter {
    progress: Option<ProgressBar>,
    json: bool,
    found: u64,
    not_found: u64,
    inconclusive: u64,
    skipped: u64,
    reused: u64,
}

impl Reporter {
    fn new(total: Option<u64>, json: bool) -> Self {
        let progress = if !json && io::stdout().is_terminal() {
            let bar = ProgressBar::with_draw_target(total, ProgressDrawTarget::stdout());
            let template = if total.is_some() {
                "{spinner:.cyan} [{bar:30.cyan/blue}] {pos}/{len}  {msg}  [{elapsed_precise}]"
            } else {
                "{spinner:.cyan} {pos} datasets  {msg}  [{elapsed_precise}]"
            };
            bar.set_style(ProgressStyle::with_template(template).expect("valid progress template"));
            bar.enable_steady_tick(Duration::from_millis(120));
            Some(bar)
        } else {
            None
        };
        Self {
            progress,
            json,
            found: 0,
            not_found: 0,
            inconclusive: 0,
            skipped: 0,
            reused: 0,
        }
    }

    fn emit(&mut self, result: &DatasetResult<'_>) -> Result<(), Box<dyn std::error::Error>> {
        if self.json {
            let mut out = io::stdout().lock();
            serde_json::to_writer(&mut out, result)?;
            out.write_all(b"\n")?;
        } else {
            let line = human_result(result);
            if let Some(bar) = &self.progress {
                bar.suspend(|| writeln!(io::stdout().lock(), "{line}"))?;
            } else {
                writeln!(io::stdout().lock(), "{line}")?;
            }
        }
        match result.status {
            Status::Found => self.found += 1,
            Status::NotFoundInSample => self.not_found += 1,
            Status::Inconclusive => self.inconclusive += 1,
            Status::Skipped => self.skipped += 1,
        }
        if result.cached {
            self.reused += 1;
        }
        if let Some(bar) = &self.progress {
            bar.inc(1);
            bar.set_message(format!(
                "{} found · {} sampled negative · {} inconclusive · {} skipped · {} cached",
                self.found, self.not_found, self.inconclusive, self.skipped, self.reused
            ));
        }
        Ok(())
    }

    fn finish(&self) {
        if let Some(bar) = &self.progress {
            bar.set_length(bar.position());
            bar.finish_with_message(format!(
                "{} found · {} sampled negative · {} inconclusive · {} skipped · {} cached",
                self.found, self.not_found, self.inconclusive, self.skipped, self.reused
            ));
        }
    }
}

fn send_with_gate(
    client: &Client,
    request: reqwest::blocking::RequestBuilder,
    gate: &RateGate,
) -> Result<reqwest::blocking::Response, String> {
    let request = request.build().map_err(|err| err.to_string())?;
    for attempt in 1..=2 {
        gate.wait().map_err(str::to_owned)?;
        let response = client
            .execute(request.try_clone().ok_or("GET request cannot be retried")?)
            .map_err(|err| err.to_string())?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            let prior_pause = gate.remaining().is_some();
            let delay = gate.throttle(response.headers(), attempt);
            if attempt == 2 {
                gate.stop();
                return Err("HTTP 429 persisted after shared cooldown; crawl stopped".into());
            }
            if !prior_pause {
                eprintln!(
                    "Hugging Face API returned HTTP 429; pausing requests for {}s",
                    delay.as_secs()
                );
            }
            continue;
        }
        let prior_pause = gate.remaining().is_some();
        gate.observe(response.headers());
        if !prior_pause {
            if let Some(delay) = gate.remaining() {
                eprintln!(
                    "Hugging Face API quota exhausted; pausing requests for {}s",
                    delay.as_secs()
                );
            }
        }
        return Ok(response);
    }
    unreachable!("two attempts return or stop")
}

fn parse_response<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
    endpoint: &str,
) -> Result<T, String> {
    let status = response.status();
    if !status.is_success() {
        return Err(format!("{endpoint}: HTTP {status}"));
    }
    if response
        .content_length()
        .is_some_and(|n| n > MAX_RESPONSE_BYTES)
    {
        return Err(format!(
            "{endpoint}: response exceeds {MAX_RESPONSE_BYTES} bytes"
        ));
    }
    let mut bytes = Vec::new();
    response
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| err.to_string())?;
    if bytes.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(format!(
            "{endpoint}: response exceeds {MAX_RESPONSE_BYTES} bytes"
        ));
    }
    serde_json::from_slice(&bytes).map_err(|err| format!("{endpoint}: {err}"))
}

fn next_cursor(headers: &HeaderMap, path: &str) -> Result<Option<String>, String> {
    let Some(link) = headers.get(LINK) else {
        return Ok(None);
    };
    let link = link
        .to_str()
        .map_err(|err| format!("Hub Link header: {err}"))?;
    for entry in link.split(',') {
        let Some((target, attributes)) = entry.trim().split_once('>') else {
            return Err("malformed Hub Link header".into());
        };
        if !attributes.contains("rel=\"next\"") {
            continue;
        }
        let url = Url::parse(target.trim_start_matches('<'))
            .map_err(|err| format!("Hub next-page URL: {err}"))?;
        if url.scheme() != "https" || url.host_str() != Some("huggingface.co") || url.path() != path
        {
            return Err("unexpected Hub next-page URL".into());
        }
        return url
            .query_pairs()
            .find(|(key, _)| key == "cursor")
            .map(|(_, value)| value.into_owned())
            .ok_or_else(|| "Hub next-page URL has no cursor".into())
            .map(Some);
    }
    Ok(None)
}

fn list_page(
    client: &Client,
    gate: &RateGate,
    cursor: Option<&str>,
    limit: u64,
    search: Option<&str>,
) -> Result<(Vec<HubDataset>, Option<String>), String> {
    let mut request = client.get(HUB_DATASETS).query(&[
        ("sort", "likes"),
        ("direction", "-1"),
        ("limit", &limit.to_string()),
    ]);
    request = request.query(&[
        ("expand[]", "sha"),
        ("expand[]", "tags"),
        ("expand[]", "description"),
        ("expand[]", "gated"),
    ]);
    if let Some(search) = search {
        request = request.query(&[("search", search)]);
    }
    if let Some(cursor) = cursor {
        request = request.query(&[("cursor", cursor)]);
    }
    let response = send_with_gate(client, request, gate)?;
    // The cursor is in the response headers, which must be read before consuming the body.
    let cursor = if response.status().is_success() {
        next_cursor(response.headers(), "/api/datasets")?
    } else {
        None
    };
    let datasets: Vec<HubDataset> = parse_response(response, "Hub /api/datasets")?;
    if datasets.is_empty() && cursor.is_some() {
        return Err("Hub returned an empty page with a next cursor".into());
    }
    Ok((datasets, cursor))
}

fn dataset_url(id: &str, revision: &str, file: Option<&str>) -> Result<Url, String> {
    let mut url = Url::parse("https://huggingface.co").map_err(|err| err.to_string())?;
    {
        let mut segments = url.path_segments_mut().map_err(|_| "invalid Hub URL")?;
        if file.is_none() {
            segments.push("api");
        }
        segments.push("datasets");
        for segment in id.split('/') {
            segments.push(segment);
        }
        segments.push(if file.is_some() { "resolve" } else { "tree" });
        segments.push(revision);
        if let Some(file) = file {
            for segment in file.split('/') {
                segments.push(segment);
            }
        }
    }
    Ok(url)
}

fn tabular_format(path: &str) -> Option<&'static str> {
    let suffix = path.rsplit('.').next()?;
    ["parquet", "csv", "tsv", "jsonl", "ndjson"]
        .into_iter()
        .find(|format| suffix.eq_ignore_ascii_case(format))
}

const CHEMISTRY_HINTS: &[&[u8]] = &[
    b"smiles",
    b"chem",
    b"molecul",
    b"compound",
    b"drug",
    b"ligand",
    b"pubchem",
    b"zinc",
    b"tox21",
    b"qm9",
];

fn chemistry_hint(text: &str) -> bool {
    CHEMISTRY_HINTS.iter().any(|hint| {
        text.as_bytes()
            .windows(hint.len())
            .any(|window| window.eq_ignore_ascii_case(hint))
    })
}

fn fast_skip(
    dataset: &HubDataset,
    has_token: bool,
    no_chemistry_filter: bool,
) -> Option<&'static str> {
    if !has_token
        && dataset
            .gated
            .as_ref()
            .is_some_and(|gated| gated != &serde_json::Value::Bool(false))
    {
        return Some("gated repository; supply HF_TOKEN or use --thorough");
    }
    let formats = dataset.tags.iter().filter(|tag| tag.starts_with("format:"));
    if formats.clone().next().is_some()
        && !formats
            .into_iter()
            .any(|tag| tag == "format:csv" || tag == "format:tsv")
    {
        return Some("metadata lists no CSV/TSV format; use --thorough to include Parquet/JSONL");
    }
    if !no_chemistry_filter
        && !chemistry_hint(&dataset.id)
        && !dataset.tags.iter().any(|tag| chemistry_hint(tag))
        && !dataset.description.as_deref().is_some_and(chemistry_hint)
    {
        return Some("no chemistry hint in Hub metadata; use --no-chemistry-filter to include it");
    }
    None
}

fn skipped_result<'a>(dataset: &'a HubDataset, reason: &str) -> DatasetResult<'a> {
    DatasetResult {
        dataset: &dataset.id,
        has_smiles: false,
        status: Status::Skipped,
        sampled_rows: 0,
        location: None,
        reason: Some(reason.to_owned()),
        cached: false,
        cacheable: false,
    }
}

fn list_files(
    dataset: &str,
    revision: &str,
    client: &Client,
    gate: &RateGate,
    thorough: bool,
) -> Result<TreeFiles, String> {
    let url = dataset_url(dataset, revision, None)?;
    let mut cursor: Option<String> = None;
    let mut files = Vec::new();
    for _ in 0..(if thorough { 2 } else { 1 }) {
        let mut request = client
            .get(url.clone())
            .query(&[("recursive", "true"), ("limit", "1000")]);
        if let Some(cursor) = &cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        let response = send_with_gate(client, request, gate)?;
        let next = if response.status().is_success() {
            next_cursor(response.headers(), url.path())?
        } else {
            None
        };
        let entries: Vec<TreeEntry> = parse_response(response, "Hub dataset tree")?;
        for entry in entries {
            if entry.kind == "file"
                && entry.size.is_some_and(|size| size > 0)
                && tabular_format(&entry.path).is_some()
            {
                files.push(entry);
            }
        }
        if next.is_none() {
            return Ok(TreeFiles {
                files,
                truncated: false,
            });
        }
        if next == cursor {
            return Err("Hub repeated the same dataset tree cursor".into());
        }
        cursor = next;
    }
    Ok(TreeFiles {
        files,
        truncated: true,
    })
}

fn probe<'a>(
    dataset: &'a HubDataset,
    gate: &Arc<RateGate>,
    client: &Client,
    samples: u32,
    max_files: u32,
    thorough: bool,
) -> DatasetResult<'a> {
    let mut result = DatasetResult {
        dataset: &dataset.id,
        has_smiles: false,
        status: Status::NotFoundInSample,
        sampled_rows: 0,
        location: None,
        reason: None,
        cached: false,
        cacheable: true,
    };
    let revision = dataset.sha.as_deref().unwrap_or("main");
    let tree = match list_files(&dataset.id, revision, client, gate, thorough) {
        Ok(tree) => tree,
        Err(err) => {
            result.status = Status::Inconclusive;
            result.reason = Some(err);
            result.cacheable = false;
            return result;
        }
    };
    let mut files = tree.files;
    let excluded = !thorough
        && files
            .iter()
            .any(|file| !matches!(tabular_format(&file.path), Some("csv" | "tsv")));
    if !thorough {
        files.retain(|file| matches!(tabular_format(&file.path), Some("csv" | "tsv")));
    }
    if files.is_empty() {
        result.status = if thorough {
            Status::Inconclusive
        } else {
            Status::Skipped
        };
        result.reason = Some(if thorough {
            if tree.truncated {
                "tree scan stopped after 2,000 entries without finding a tabular file".into()
            } else {
                "no supported tabular files in repository".into()
            }
        } else if tree.truncated {
            "no CSV/TSV in first 1,000 entries; later files not inspected".into()
        } else {
            "no CSV/TSV files in repository".into()
        });
        result.cacheable = !tree.truncated;
        return result;
    }
    let available = files.len();
    let mut incomplete = if tree.truncated {
        Some(if thorough {
            "tree scan stopped after 2,000 entries; later files were not inspected".to_owned()
        } else {
            "tree scan stopped after 1,000 entries; later files were not inspected".to_owned()
        })
    } else if excluded {
        Some("non-CSV/TSV files were excluded in fast mode".to_owned())
    } else {
        None
    };
    if available > max_files as usize {
        let mut rng = rand::rng();
        for index in 1..max_files as usize {
            files.swap(index, rng.random_range(index..available));
        }
        files.truncate(max_files as usize);
        incomplete.get_or_insert_with(|| {
            format!("sampled {max_files} of {available} tabular files; others were not checked")
        });
    }
    for file in files {
        let url = match dataset_url(&dataset.id, revision, Some(&file.path)) {
            Ok(url) => url,
            Err(err) => {
                result.status = Status::Inconclusive;
                result.cacheable = false;
                result.reason.get_or_insert(err);
                continue;
            }
        };
        let source = RangeSource::new(
            client.clone(),
            Arc::clone(gate),
            url,
            file.size.unwrap_or(0),
        );
        let sampled = match tabular_format(&file.path).expect("tree only contains tabular files") {
            "parquet" => parquet_probe::probe(&source, samples),
            extension => text_probe::probe(&source, extension, samples),
        };
        match sampled {
            Ok(FileSample {
                found,
                sampled_rows,
                wide,
            }) => {
                result.sampled_rows += sampled_rows;
                if let Some((row, column)) = found {
                    result.status = Status::Found;
                    result.has_smiles = true;
                    result.reason = None;
                    result.location = Some(Location {
                        file: file.path,
                        row,
                        column,
                        wide,
                    });
                    return result;
                }
            }
            Err(err) => {
                result.status = Status::Inconclusive;
                result.cacheable = false;
                result
                    .reason
                    .get_or_insert_with(|| format!("{}: {err}", file.path));
            }
        }
    }
    if let Some(reason) = incomplete {
        result.status = Status::Inconclusive;
        result.reason.get_or_insert(reason);
    }
    if result.sampled_rows == 0 && result.reason.is_none() {
        result.status = Status::Inconclusive;
        result.reason = Some("sampled tabular files contain no complete rows".into());
    }
    result
}

fn probe_page(
    datasets: &[HubDataset],
    client: &Client,
    gate: &Arc<RateGate>,
    cache: &Cache,
    refresh: bool,
    samples: u32,
    max_files: u32,
    thorough: bool,
    has_token: bool,
    no_chemistry_filter: bool,
    pool: &rayon::ThreadPool,
    reporter: &mut Reporter,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut pending = Vec::with_capacity(datasets.len());
    // A negative sample under a larger budget must not reuse a smaller probe.
    for dataset in datasets {
        if !thorough {
            if let Some(reason) = fast_skip(dataset, has_token, no_chemistry_filter) {
                reporter.emit(&skipped_result(dataset, reason))?;
                continue;
            }
        }
        let cache_key = format!(
            "{}|direct:v5|thorough={thorough}|rows={samples}|files={max_files}",
            dataset.id
        );
        if !refresh {
            if let Some(json) = cache.get(&cache_key, dataset.sha.as_deref())? {
                if let Ok(mut result) = serde_json::from_str::<DatasetResult<'_>>(&json) {
                    result.cached = true;
                    reporter.emit(&result)?;
                    continue;
                }
            }
        }
        pending.push((dataset, cache_key));
    }
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = mpsc::channel();
        let pending_ref = &pending;
        scope.spawn(move || {
            pool.install(|| {
                pending_ref
                    .par_iter()
                    .for_each_with(sender, |sender, (dataset, key)| {
                        let result = probe(dataset, gate, client, samples, max_files, thorough);
                        let _ = sender.send((result, dataset.sha.as_deref(), key.as_str()));
                    });
            });
        });
        for _ in &pending {
            let (result, revision, cache_key) = receiver.recv()?;
            if gate.stopped()
                && result.reason.as_deref().is_some_and(|reason| {
                    reason.contains("rate gate stopped") || reason.contains("HTTP 429 persisted")
                })
            {
                continue;
            }
            if matches!(result.status, Status::Found) || result.cacheable {
                cache.put(cache_key, revision, &serde_json::to_string(&result)?)?;
            }
            reporter.emit(&result)?;
        }
        Ok(())
    })?;
    if gate.stopped() {
        return Err("Hub remained rate limited after a shared cooldown; completed results are cached; rerun to resume".into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.inspect_found {
        return inspect::run(
            &args.cache,
            args.inspect_input.as_deref(),
            args.inspect_output
                .as_deref()
                .unwrap_or_else(|| Path::new("found-details.tsv")),
            args.inspect_axis.unwrap_or(full_scan::Axis::Auto),
        );
    }
    let mut builder = Client::builder().timeout(Duration::from_secs(15));
    let token = std::env::var("HF_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    if let Some(token) = &token {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))?,
        );
        builder = builder.default_headers(headers);
    }
    let client = builder.build()?;
    let gate = Arc::new(RateGate::new());
    let cache = Cache::open(&args.cache)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.jobs as usize)
        .build()?;
    let mut reporter = Reporter::new(
        if args.datasets.is_empty() {
            args.max_datasets
        } else {
            Some(args.datasets.len() as u64)
        },
        args.json,
    );
    if args.datasets.is_empty() {
        let mut cursor = None;
        let mut remaining = args.max_datasets.unwrap_or(u64::MAX);
        while remaining > 0 {
            let (datasets, next) = list_page(
                &client,
                &gate,
                cursor.as_deref(),
                remaining.min(PAGE_SIZE),
                args.search.as_deref(),
            )?;
            if datasets.is_empty() {
                break;
            }
            probe_page(
                &datasets,
                &client,
                &gate,
                &cache,
                args.refresh,
                args.samples,
                args.max_files,
                args.thorough,
                token.is_some(),
                args.no_chemistry_filter,
                &pool,
                &mut reporter,
            )?;
            remaining -= datasets.len() as u64;
            if next.is_none() || next == cursor {
                if next == cursor && next.is_some() && remaining > 0 {
                    return Err("Hub repeated the same pagination cursor".into());
                }
                break;
            }
            cursor = next;
        }
    } else {
        let targets: Vec<_> = args
            .datasets
            .into_iter()
            .map(|id| HubDataset {
                id,
                sha: None,
                tags: Vec::new(),
                description: None,
                gated: None,
            })
            .collect();
        for datasets in targets.chunks(PAGE_SIZE as usize) {
            probe_page(
                datasets,
                &client,
                &gate,
                &cache,
                args.refresh,
                args.samples,
                args.max_files,
                args.thorough,
                token.is_some(),
                args.no_chemistry_filter,
                &pool,
                &mut reporter,
            )?;
        }
    }
    reporter.finish();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follows_hub_next_cursor_without_accepting_other_hosts() {
        let mut headers = HeaderMap::new();
        headers.insert(
            LINK,
            "<https://huggingface.co/api/datasets?sort=createdAt&limit=2&cursor=a%2Bb%3D>; rel=\"next\""
                .parse()
                .unwrap(),
        );
        assert_eq!(
            next_cursor(&headers, "/api/datasets").unwrap().as_deref(),
            Some("a+b=")
        );
        headers.remove(LINK);
        assert_eq!(next_cursor(&headers, "/api/datasets").unwrap(), None);
        headers.insert(
            LINK,
            "<https://example.com/api/datasets?cursor=x>; rel=\"next\""
                .parse()
                .unwrap(),
        );
        assert!(next_cursor(&headers, "/api/datasets").is_err());
    }

    #[test]
    fn repository_urls_encode_special_file_names_as_path_segments() {
        let url = dataset_url("org/my data", "revision", Some("subdir/file #1.csv")).unwrap();
        assert_eq!(
            url.path(),
            "/datasets/org/my%20data/resolve/revision/subdir/file%20%231.csv"
        );
        assert_eq!(
            dataset_url("org/my data", "revision", None).unwrap().path(),
            "/api/datasets/org/my%20data/tree/revision"
        );
    }

    #[test]
    fn fast_screen_skips_uninspected_metadata_without_calling_it_a_negative() {
        let mut dataset = HubDataset {
            id: "org/random-prompts".into(),
            sha: None,
            tags: vec!["format:csv".into()],
            description: None,
            gated: Some(serde_json::Value::Bool(false)),
        };
        assert!(
            fast_skip(&dataset, false, false)
                .unwrap()
                .contains("chemistry hint")
        );
        assert_eq!(fast_skip(&dataset, false, true), None);
        let skipped = skipped_result(&dataset, fast_skip(&dataset, false, false).unwrap());
        let json = serde_json::to_value(&skipped).unwrap();
        assert_eq!(json["status"], "skipped");
        assert_eq!(json["has_smiles"], false);
        assert_eq!(json["sampled_rows"], 0);

        dataset.description = Some("Molecular structures in this collection".into());
        assert_eq!(fast_skip(&dataset, false, false), None);
        dataset.tags = vec!["format:parquet".into()];
        assert!(
            fast_skip(&dataset, false, true)
                .unwrap()
                .contains("no CSV/TSV")
        );
        dataset.tags = vec!["format:csv".into()];
        dataset.gated = Some(serde_json::Value::String("auto".into()));
        assert!(fast_skip(&dataset, false, true).unwrap().contains("gated"));
        assert_eq!(fast_skip(&dataset, true, true), None);
    }
    #[test]
    fn terminal_result_distinguishes_match_from_sampled_negative_and_failure() {
        let mut result = DatasetResult {
            dataset: "org/molecules",
            has_smiles: true,
            status: Status::Found,
            sampled_rows: 1,
            location: Some(Location {
                file: "data/train.parquet".into(),
                row: 42,
                column: "structure".into(),
                wide: false,
            }),
            reason: None,
            cached: false,
            cacheable: true,
        };
        let found = human_result(&result);
        let found = console::strip_ansi_codes(&found);
        assert!(found.contains("SMILES org/molecules"));
        assert!(found.contains("data/train.parquet · row 42 · column structure"));

        result.status = Status::NotFoundInSample;
        result.location = None;
        assert!(console::strip_ansi_codes(&human_result(&result)).contains("not found in sample"));

        result.status = Status::Inconclusive;
        result.reason = Some("range unavailable".into());
        assert!(
            console::strip_ansi_codes(&human_result(&result))
                .contains("inconclusive org/molecules  (1 rows checked; range unavailable)")
        );
    }
    #[test]
    fn retries_one_throttle_but_stops_after_a_second_429() {
        use std::net::TcpListener;
        use std::time::Instant;

        for (second_status, should_stop) in [("200 OK", false), ("429 Too Many Requests", true)] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = std::thread::spawn(move || {
                for status in ["429 Too Many Requests", second_status] {
                    let deadline = Instant::now() + Duration::from_secs(5);
                    let mut stream = loop {
                        match listener.accept() {
                            Ok((stream, _)) => break stream,
                            Err(err)
                                if err.kind() == io::ErrorKind::WouldBlock
                                    && Instant::now() < deadline =>
                            {
                                std::thread::sleep(Duration::from_millis(5));
                            }
                            Err(err) => panic!("missing retry: {err}"),
                        }
                    };
                    let mut request = [0; 1024];
                    let size = stream.read(&mut request).unwrap();
                    assert!(String::from_utf8_lossy(&request[..size]).starts_with("GET /"));
                    write!(
                        stream,
                        "HTTP/1.1 {status}\r\nRetry-After: 0\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]"
                    )
                    .unwrap();
                }
            });
            let client = Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap();
            let gate = RateGate::new();
            let response = send_with_gate(&client, client.get(url), &gate);
            server.join().unwrap();
            assert_eq!(gate.stopped(), should_stop);
            if should_stop {
                assert!(response.unwrap_err().contains("HTTP 429 persisted"));
            } else {
                assert_eq!(response.unwrap().status(), StatusCode::OK);
            }
        }
    }
}
