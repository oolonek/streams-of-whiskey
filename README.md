# streams-of-whiskey

A Rust CLI that quickly screens likely chemistry datasets for parseable SMILES in CSV/TSV files. It reads the [Hub listing and repository tree](https://huggingface.co/docs/hub/api) and requests **bounded byte ranges** from `/datasets/{id}/resolve/{revision}/{path}`; it does not use the Dataset Viewer or download entire artifacts. Parsing uses [smiles-rs](https://github.com/earth-metabolome-initiative/smiles-rs). The default is deliberately a **low-recall, fast screen**, not a complete audit of every dataset.

```sh
cargo run --release -- --max-datasets 2000                  # fast-screen 2000 popular listing entries
cargo run --release -- --search smiles --max-datasets 200    # focus on relevant Hub search results
target/release/streams-of-whiskey --max-datasets 100000 --no-chemistry-filter  # all CSV/TSV candidates, no keyword screen
cargo run --release -- katielink/moses                       # explicit ID: bypass metadata screening
cargo run --release -- --thorough --max-files 2 openai/gsm8k # include Parquet/JSONL
cargo run --release -- --thorough --max-datasets 20          # slower, no chemistry prefilter
cargo run --release -- --samples 3 --max-files 4 --jobs 8    # more sampling/network work
cargo run --release -- --json --max-datasets 2000 > results.jsonl
# Set HF_TOKEN if the account is authorized to access gated/private files.
```

Discovery pages through the Hub listing **200 datasets at a time**, most-liked first, requesting only revision, format tags, access flag and short description. By default, entries without a chemistry clue in their ID, tags or description, entries tagged exclusively with non-CSV/TSV formats, and gated repositories without `HF_TOKEN` are labeled `skipped` **without a per-dataset request**. Chemistry hints are heuristic and miss relevant datasets; format tags may be incomplete. Explicit IDs bypass the metadata screen but still use CSV/TSV-only probing. `--thorough` disables screening and includes Parquet, JSONL and NDJSON; it is substantially slower. `--search smiles` narrows the *listing* to likely candidates. A terminal shows progress, colored matches, dim skips and sampled negatives, and yellow inconclusive results; `--json` emits plain JSONL.

`--no-chemistry-filter` removes **only** the chemistry keyword requirement for discovered datasets. Gated repositories without a token and metadata tagged exclusively with other formats remain skipped; eligible datasets still use the fast CSV/TSV-only, one-tree-page, one-file defaults. It does not turn on Parquet/JSONL or exhaustive file coverage. `--thorough` is a separate, slower mode and cannot be combined with this flag. **Neither mode excludes a column by its name.** A chemistry-like header is supporting evidence, but a generically named column can qualify through its contents.

Each JSON line has a dataset-level result:

```json
{"dataset":"org/example","has_smiles":true,"status":"found","sampled_rows":1,"location":{"file":"data/train.csv","row":0,"column":"SMILES"}}
```

`location.row` is zero-based within the file's data rows (excluding a CSV/TSV header); `column` is its header, a JSONL key, or a numeric index for a blank header. `location.wide:true` identifies an explicitly SMILES-labeled row. The sniffer parses sampled top-level strings of at most 4096 bytes in **any column**. A field named `SMILES` can contain a legitimate single atom `F`; in an unnamed or `sex`/`type` field, isolated `F`/`C` codes are too ambiguous. An explicit bond/ring/branch notation or corroborating distinct multi-atom, multi-element strings can confirm such a field regardless of its name. These are heuristics, not a chemical-identity proof: sparse/simple genuine molecules can remain ambiguous, and parser-valid nonchemical text can still look structural. `found` means a sampled field met this evidence threshold, not that the whole dataset contains chemicals. `not_found_in_sample` does not prove absence; `skipped` means **no file was checked**, and `inconclusive` means the probe failed or left files unchecked. Network/format errors never become sampled negatives.

Discovery and inspection preflight apply the same [SMILES evidence](GLOSSARY.md) rules; their read budgets and scope remain different.

Fast mode checks one 1,000-entry repository tree page, selects the first CSV/TSV file by default (`--max-files 1`), and reads at most its first 128 KiB. The sniffer inspects up to 32 initial complete rows plus `--samples` distinct random rows **within that prefix** (default 1). Other files or tree pages remain uninspected; unsuccessful probes are `inconclusive` if coverage is incomplete. `--thorough` checks up to two tree pages (2,000 entries), includes Parquet/JSONL/NDJSON, and samples up to 32 initial Parquet rows plus random rows through bounded footer/row-group reads; specify `--max-files` for more than one file. Text samples are not uniform across TB-scale files. No bounded sample guarantees finding rare molecules.

Each HTTP range is capped at 2 MiB and each probed file at 16 MiB of downloaded ranges. Servers that ignore `Range` are rejected without reading a full response. Hub JSON responses are capped at 2 MiB; requests time out after 15 seconds. `--jobs` controls simultaneous dataset probes (default 8). Oversized Parquet metadata/row groups, malformed rows, inaccessible repositories, and missing range support are `inconclusive` in thorough mode. One-time `--release` compilation takes additional time; repeated runs can invoke `target/release/streams-of-whiskey` directly.

Results are cached for 24 hours in `.streams-of-whiskey.sqlite3` (`--cache PATH`). Discovery supplies a revision SHA, so changed repositories invalidate cached results; explicit IDs use ID and TTL. Cache keys include fast/thorough mode, `--samples`, and `--max-files`; old probe results are not reused. `--refresh` bypasses cache. Metadata-only skips and network failures are not cached; completed file probes are saved as they finish.

Workers share an HTTP `429` cooldown using `Retry-After`, `RateLimit`, or bounded backoff; persistent throttling stops the crawl nonzero while retaining completed cached results. The [Hub limits API and resolver requests](https://huggingface.co/docs/hub/rate-limits), so throughput still depends on the provider and the number of actual candidates. Fast mode favors throughput and honest `skipped`/`inconclusive` labels over comprehensive coverage; `--thorough` restores the broader, slower scan. Listing order can change between runs.

## Inspect found datasets completely

The crawl is only a bounded discovery sample. To analyze **the entire pinpointed file's column** for every file-level `found` result in the SQLite cache, run:

```sh
target/release/streams-of-whiskey --inspect-found
# Interrupt and rerun the same command to resume: completed inspections remain in SQLite.
# Optionally write a new TSV report, including cached results:
target/release/streams-of-whiskey --inspect-found --output found-details-new.tsv
# Or inspect found entries in a saved crawl instead of the cache:
target/release/streams-of-whiskey --inspect-found --input results.jsonl
# For an unusual layout, override detection: --axis column or --axis row.
```

On an interactive terminal, stderr shows an overall datasets bar with the running valid-SMILES count and a separate bar for the current file. Text files show bytes received while downloading and parsing together; Parquet files show a metadata-download spinner followed by rows parsed from the projected column or row. The current file's valid count updates as records or batches are parsed. Unknown text sizes use a byte-count spinner; row mode can stop before all file bytes are read. With stderr redirected, inspection prints phase changes and periodic row/count updates instead of terminal bars. Totals in the finished summary include only completed counts, not partial observations from scans that ended in error.

When requested, the TSV has one row per found dataset: `dataset`, pinned `version` (Hub SHA), `version_source`, revision-specific `dataset_url` and `file_url`, `file`, `format`, `scope`, detected `axis`, `found_row`, `found_column`, file size where available, `rows_scanned`, `cells_scanned`, `valid_smiles`, `invalid_smiles`, `empty_cells`, `non_string_cells`, `oversized_cells`, percent valid among parsed nonempty strings, `status`, `error`, and inspection time. `status=complete` means the selected scope was fully read; `partial` means cells over 64 KiB were counted but not parsed; `ambiguous` means a bounded prefix/Parquet sample did not corroborate the found location, so no full-file count is claimed; `error` means the scan failed. Ambiguous rows are not negative evidence or transport failures. Other incomplete scans return a nonzero exit status, retaining completed results in SQLite and in the TSV if requested. An existing output file is never overwritten. Once a column qualifies, `valid_smiles` still counts parser-valid cells—not verified chemical identities for every value.

`--axis auto` (default) chooses a **row** only when the pinpointed row's first field labels it `SMILES`, `canonical_smiles`, `isomeric_smiles`, or `structure` (JSONL: a `property`/`label` field); otherwise it scans the pinpointed **column through EOF**. Explicit `--axis row` or `--axis column` bypasses the confidence preflight for manual inspection of ambiguous cases. The scope is **one pinpointed repository file**, not all dataset shards, splits or other columns. Row mode stops after the selected row. Text formats already **stream** the HTTP response into the parser without saving the file or holding it all in memory. Inspection sniffs at most the first 128 KiB of a text response and reuses those bytes for full parsing if confirmed; ambiguous fields stop early. Streaming reduces memory, not network transfer: for a full CSV/TSV/JSONL column, the resolver sends all intervening rows because these formats have no column index. A 1.37 GiB text file can still require roughly 1.37 GiB of transfer; partial sampling cannot certify every row. Parquet permits range requests for only the selected column's pages. An indexed server-side column API or a Parquet conversion would be needed to avoid transferring unrelated text columns. Each text request has a one-hour deadline; unusually large Parquet pages/metadata can fail with `error` rather than silently returning partial counts.

Cache-sourced results retain their crawl's revision SHA, including entries older than the probe-cache TTL. The new content-sniffing discovery uses a new cache key, so older parser-only matches are not reused by new crawls; old cache/JSONL inputs to inspection receive the same bounded preflight and may be `ambiguous`. Older wide-row hits without `location.wide` may require `--axis row` if their pinpointed row is beyond the sniffed prefix. JSONL results do not contain a SHA, so inspection resolves and reports the **current** Hub revision at inspection time; the file may have changed since discovery. The cached probe location is validated against the chosen file and schema, and inaccessible or changed files produce `error` rows. `HF_TOKEN` is used for authorized access where applicable.

## Publish the read-only dashboard

`--inspect-found` stores successful per-dataset inspections in the SQLite cache alongside the crawl results. A later inspection reuses a matching pinned revision, file, found location, and requested axis without downloading that file again; use `--force` with `--inspect-found` to recompute. Failed or ambiguous inspections remain retryable. SQLite is the durable local source for the dashboard; `--output PATH` optionally creates a new TSV report and refuses to overwrite an existing file.

Each successful inspection stores a **16 KiB HyperLogLog sketch per dataset**, not the individual strings. The dashboard merges current per-dataset sketches to report an **estimated** unique count of parseable SMILES text across inspected files (nominal relative standard error about 0.8%; not a guaranteed bound). Different spellings of one molecule are still different strings. `valid_smiles`, `invalid_smiles`, and the other per-file counts remain exact. `--force` replaces one dataset's saved sketch; a single global sketch could not support this. Counts cover only the pinpointed file/row or column, not uninspected shards. Sketches use stable xxHash3-64 and a versioned format; they do not retain the strings or support exact membership queries.

```sh
# One-time conversion for caches created by the previous exact-string version:
# Stop any running inspections first. Migration removes the old exact strings
# only after converting them and compacts the SQLite database to reclaim disk.
target/release/streams-of-whiskey --migrate-hll
# If conversion succeeds but compaction fails, retry compaction separately:
target/release/streams-of-whiskey --compact-cache
target/release/streams-of-whiskey --inspect-found
target/release/streams-of-whiskey --export-dashboard docs/data.json
# Check docs/data.json contains only datasets you intend to publish, then commit it with docs/.
```

Configure GitHub Pages in repository Settings → Pages to deploy from the main branch's `/docs` directory. `docs/index.html` reads `./data.json` and needs no server or browser SQLite; the crawler and export run locally, not on Pages. Preview with `python3 -m http.server 8000 --directory docs` and open `http://localhost:8000/`. Re-run the inspection and export after new crawls, then commit/push the reviewed JSON. The export contains dataset names, revision-pinned source links, scan counts and status, **not the individual SMILES**; avoid publishing access-restricted dataset metadata. A pinned file's browse link opens the Hub view; its download link fetches the original file.

Previously generated TSVs contain only aggregate counts; they cannot reconstruct any unique count. Existing SQLite caches **with** `inspected_smiles` can be migrated to sketches without redownloading files; stop running inspectors and keep a backup before the irreversible removal of the exact-string table. Migration and SQLite compaction can take substantial time and need free disk space. If a cache has only a TSV and no saved per-string data, those datasets must be re-inspected once. Matching saved inspections are reused afterward. `--refresh` refreshes discovery probes; `--force` recomputes inspected files.

The existing `docs/data.json` is a legacy exact-count snapshot; the page labels it as such without modifying it. After migrating the SQLite cache, regenerate the snapshot to publish HLL estimates. A TSV alone cannot be used to recover sketches.

## Benchmark unique-count storage

Run `cargo run --release --example benchmark_uniques -- --datasets 3 --rows 10000` for a short repeatable comparison, or increase `--rows` for a larger workload. The example feeds the same deterministic stream of already-validated SMILES-like strings into the old indexed SQLite exact-dedup path and the new per-dataset HLL path. It reports ingest/commit time, merged-count query time, exact distinct count versus estimated count, relative error, and post-checkpoint database sizes. It measures **only local counting/storage**, not HTTP transfer, SMILES parsing, migration time, or a prediction of full-crawl runtime; generated temporary databases are removed afterward.

Observed on this workstation with `--datasets 4 --rows 200000` (800,000 generated rows), across three runs:

| Metric | Exact SQLite | HLL p=14 |
| --- | ---: | ---: |
| Ingestion and commit | 4.19–4.33 s | 0.059–0.062 s |
| Post-checkpoint database size | 44,503,040 bytes | 77,824 bytes |
| Global distinct count | 250,000 (exact) | 252,476 (+0.99% for this input) |

This workload gives roughly 70× ingestion throughput and 572× less stored data for the unique-count representation; real end-to-end crawls remain constrained by HTTP and parsing.
