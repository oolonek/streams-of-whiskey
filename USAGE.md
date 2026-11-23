# Usage notes

Start with the [README](README.md) for the crawl → inspection → dashboard workflow. This page covers scope and interpretation in more detail.

## Discovery

The Hub listing is fetched in pages of 200, most-liked first; `--max-datasets` limits **listing entries**, not successful file probes. Without it, the crawl continues until the listing ends or an error/rate limit stops it; metadata hints do not change listing order. `--search smiles` restricts the listing at the Hub. Fast mode skips entries without a case-insensitive substring from [`chemistry-hints.toml`](chemistry-hints.toml) in their ID, tags, or description, entries tagged exclusively with non-CSV/TSV formats, and gated entries without `HF_TOKEN`. Edit the TOML `hints` array to tune this check (use `--chemistry-hints-file PATH` when running outside the repository). Metadata can be missing or misleading. `--no-chemistry-filter` removes only the hint check. `--thorough` skips all metadata prefilters, but inaccessible gated files may still fail during probing. Explicit IDs bypass metadata checks. Neither mode excludes columns based on their name.

Fast mode examines at most the first 1,000 repository-tree entries, selects the first CSV/TSV file by default, and reads up to its first 128 KiB. It checks up to 32 initial complete rows plus `--samples` distinct random rows **within that prefix** (default 1). `--thorough` checks up to 2,000 tree entries, includes Parquet/JSONL/NDJSON, and can sample Parquet rows using bounded footer/row-group reads. `--max-files` increases the number of files checked per dataset (default 1). Text samples are not uniform across a large file; neither mode guarantees discovery of rare structures. Unchecked files or truncated tree scans make unsuccessful probes `inconclusive`, not negative.

Discovery checks sampled strings of at most 4,096 bytes for **SMILES evidence**, not just parser validity. A labeled field can contain a valid single atom such as `F`; in an unlabeled or generic field, an isolated `F`/`C` is too ambiguous. Explicit bond/ring/branch notation or corroborating distinct multi-atom, multi-element strings can confirm a generically named field. Parser-valid nonchemical text can still look structural, and genuine sparse/simple molecules can be missed. `found` means a sampled field met the evidence threshold; `not_found_in_sample` does not prove absence. `skipped` means no file was checked, and `inconclusive` means the probe failed or did not cover all candidates. Network and format errors never become sampled negatives.

With `--json`, discovery writes one JSON object per dataset:

```json
{"dataset":"org/example","has_smiles":true,"status":"found","sampled_rows":1,"location":{"file":"data/train.csv","row":0,"column":"SMILES"}}
```

`location.row` is zero-based within data rows (excluding a CSV/TSV header); `column` is a header, JSONL key, or numeric index for a blank header. `location.wide:true` marks an explicitly SMILES-labeled row.

HTTP range requests are capped at 2 MiB and each probed file at 16 MiB of downloaded ranges; a server ignoring `Range` is rejected without downloading its whole response. Hub JSON responses are capped at 2 MiB and requests time out after 15 seconds. `--jobs` sets simultaneous dataset probes (default 8). Rate limiting uses shared cooldowns; persistent throttling ends the crawl nonzero while keeping completed results in the cache. [Hub rate limits](https://huggingface.co/docs/hub/rate-limits) and listing order affect throughput and repeatability.

Completed probes are cached for 24 hours in `.streams-of-whiskey.sqlite3` (override with `--cache PATH`). Cache keys include fast/thorough mode, `--samples`, and `--max-files`; discovered revisions invalidate changed results. Explicit IDs use ID and TTL. `--refresh` bypasses cached discovery probes. Metadata-only skips and network failures are not cached.

## Inspect a found file

```sh
target/release/streams-of-whiskey --inspect-found
target/release/streams-of-whiskey --inspect-found --output new-report.tsv
target/release/streams-of-whiskey --inspect-found --input results.jsonl
# If auto orientation cannot establish the intended scope:
target/release/streams-of-whiskey --inspect-found --axis column
```

Inspections use found locations in the SQLite cache, or in JSONL provided with `--input`. Successful inspections with the same pinned revision, file, location, and axis are reused. `--force` recomputes them; errors and ambiguous inspections can be retried. `--output` creates a TSV and refuses to overwrite an existing file. Saved crawl results retain their revision SHA, even beyond the probe-cache TTL. JSONL input has no SHA, so inspection resolves the **current** Hub revision, which may differ from discovery.

Default `--axis auto` scans a **row** only if its first field explicitly labels it `SMILES`, `canonical_smiles`, `isomeric_smiles`, or `structure` (JSONL: `property`/`label` field); otherwise it scans the pinpointed **column through EOF**. `--axis row` and `--axis column` override orientation and bypass the confidence preflight. The scope is one repository file, not all shards, splits, or columns. Both discovery and preflight apply the same SMILES-evidence rules, but with different read budgets.

TSV results contain the dataset, pinned revision and links, file and scope, found location, scanned rows/cells, valid/invalid/empty/non-string/oversized cells, percent valid, status, error, and inspection time. `complete` means the selected scope was read; `partial` means cells over 64 KiB were counted but not parsed; `ambiguous` means a bounded preflight could not confirm the location, so no full-file count is claimed; `error` means the scan failed. Ambiguity is not negative evidence. Once a column qualifies, `valid_smiles` counts parser-valid cells, not chemically verified identities. Totals on a failed scan exclude partial observations; completed inspections remain saved for resumption.

Text inspection streams the response into the parser without saving the file, but a complete text-column scan still transfers intervening rows: a gigabyte-scale CSV can require roughly a gigabyte of network transfer. Parquet can range-read the selected column. Text preflight reads up to 128 KiB and reuses it when confirmed; a text request has a one-hour deadline. Large or malformed Parquet metadata/pages can fail with `error` instead of silently returning partial counts.

## Dashboard and estimator

`--export-dashboard docs/data.json` writes a static snapshot of completed per-file inspections for the page in `docs/`. Review it before publishing: names, revisions, and links are exposed, but **individual SMILES strings are not**. Browse links point to the pinned Hub revision; download links fetch the pinned source file. GitHub Pages can publish from the main branch's `/docs` folder. Re-run inspection and export after new crawls.

Each successful inspection stores a 16 KiB HyperLogLog sketch per dataset. The dashboard merges sketches for an estimated unique count of parseable SMILES **text** (nominal relative standard error about 0.8%, not a guaranteed bound). Exact per-file valid/invalid and other cell counts remain exact. Different spellings of one molecule are different strings; sketches do not retain the strings or support exact membership queries. `--force` replaces a saved inspection's sketch.

For a local counting/storage comparison only, run `cargo run --release --example benchmark_uniques -- --datasets 3 --rows 10000`. The example compares indexed SQLite exact deduplication with per-dataset sketches using synthetic already-validated strings; it does not measure HTTP, parsing, or end-to-end crawl time.
