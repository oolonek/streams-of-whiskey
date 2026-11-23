# streams-of-whiskey

Find Hugging Face datasets with parseable SMILES, inspect the matching file, and publish the results in a static dashboard. Discovery is a **bounded sample**, not a full audit. Inspection counts cells in the selected file's column (or a labeled row), not the entire dataset.

**[View the dashboard](https://oolonek.github.io/streams-of-whiskey/).**

## Get started

```sh
cargo run --release -- --max-datasets 2000
# Reuse the compiled binary for later steps:
target/release/streams-of-whiskey --inspect-found
target/release/streams-of-whiskey --export-dashboard docs/data.json
```

The first command screens Hub listing entries and saves results in `.streams-of-whiskey.sqlite3`. The second inspects files found by the crawl and saves counts there. The third replaces the dashboard snapshot. Preview it with `python3 -m http.server 8000 --directory docs`, then open `http://localhost:8000/`. Review `docs/data.json` before committing it: dataset names and links may disclose access-restricted metadata. GitHub Pages can serve the `/docs` directory from the main branch; no server-side database is needed.

Without `--max-datasets`, discovery pages through the entire Hub listing until it ends (or an error/rate limit stops it). `--max-datasets 2000` means the first 2,000 **listing entries**, not 2,000 chemistry matches. Entries arrive most-liked first; the CLI does **not** rank chemistry datasets ahead of others. It checks each entry's metadata, skips ineligible ones without fetching their files, and probes eligible ones in that listing order. `--search smiles` asks the Hub for a narrower listing.

## Snapshot at a glance

<!-- cache-timings:start -->
Local cache report generated 2026-11-23 22:11:23 UTC; the published dashboard may differ. These are **observed completion spans, not runtimes or forecasts**.

| Metric | Observed value | Scope |
| --- | ---: | --- |
| Inspected datasets | 431 | 431 complete; 0 partial |
| Valid parsed cells | 430,125,764 | Exact sum across inspected files |
| Estimated unique valid strings | ≈340,771,196 | HyperLogLog across inspected files |
| Cached discovery probes | 9,526 | 522 `found`; metadata-only skips are not cached |
| Discovery completion window | 72h 41m 06s | 2026-11-20 20:44:10–2026-11-23 21:25:16 UTC |
| Inspections on 2026-11-20 | 20s | 20 files; 2026-11-20 20:40:29–2026-11-20 20:40:49 UTC |
| Inspections on 2026-11-22 | 35m 59s | 400 files; 2026-11-22 18:58:42–2026-11-22 19:34:41 UTC |
| Inspections on 2026-11-23 | 8h 49m 45s | 11 files; 2026-11-23 12:59:14–2026-11-23 21:48:59 UTC |
<!-- cache-timings:end -->

These are spans between the first and last **saved completions**, not measured runtimes or time required for a future run. The cache replaces earlier results and records neither crawl starts nor individual scan durations. Running this command exports the current cache figures as JSON **and refreshes the marked date and table above in `README.md`** (from the repository root; overwrites the JSON output). Re-export the dashboard separately to publish new inspection data:

```sh
target/release/streams-of-whiskey --export-timings timings.json
```

## Choose discovery coverage

| Mode | Which listed datasets get probed? | Files probed |
| --- | --- | --- |
| Default | Chemistry hint in Hub metadata; skip gated entries without `HF_TOKEN` and entries tagged only with other formats | CSV/TSV; one file by default |
| `--no-chemistry-filter` | Drop **only** the chemistry-hint check; keep the access and format checks | Same fast CSV/TSV probe as default |
| `--thorough` | No metadata prefilter (access to gated files still requires authorization) | CSV/TSV, Parquet, JSONL, NDJSON; deeper tree scan; one file by default |

The checked-in [`chemistry-hints.toml`](chemistry-hints.toml) supplies the default chemistry hints: `smiles`, `chem`, `molecul`, `compound`, `drug`, `ligand`, `pubchem`, `zinc`, `tox21`, `qm9`. The fast screen matches these case-insensitive **substrings** against a dataset's ID, Hub tags, and description. For example, `molecul` matches “molecular”; `chem` can also match unrelated words containing those letters. Metadata hints only decide which datasets to **probe**, not whether they contain SMILES. A probe checks sampled file contents for parseable SMILES with structural or labeling evidence; an isolated `C`/`F` in a generic field does not suffice. Missing metadata, nonstandard terminology, or sampling can miss real matches.

To tune the fast screen, edit the `hints` array in `chemistry-hints.toml` and rerun the same crawl command. For example:

```toml
hints = ["smiles", "ligand", "protein"]
```

The file is loaded from the current working directory for fast Hub listing crawls; if running elsewhere, pass `--chemistry-hints-file /path/to/chemistry-hints.toml`. Keep terms nonempty ASCII strings; the list replaces the checked-in defaults, so retain any terms you still want. Editing it changes only the metadata hint check, not format/access filters or file-content evidence. To probe all eligible fast-mode CSV/TSV entries regardless of keyword, use `--no-chemistry-filter`. Neither setting changes Hub listing order.

Other discovery examples:

```sh
cargo run --release -- --no-chemistry-filter --max-datasets 2000
cargo run --release -- --thorough --max-datasets 20
cargo run --release -- --search smiles --max-datasets 200
cargo run --release -- katielink/moses
```

`--thorough` and `--no-chemistry-filter` are alternative screening choices. Explicit dataset IDs bypass listing metadata checks, but use the default CSV/TSV probe unless `--thorough` is specified. `--search` filters the Hub listing; it does not change the file probe. Set `HF_TOKEN` for authorized gated/private access. Increase `--max-files` to probe more than one file per dataset; `--samples` increases sampled rows. Even thorough discovery can miss SMILES in uninspected files or rows.

## Read the results

Discovery labels matches `found`; `not_found_in_sample` is **not proof of absence**. `skipped` means no file was checked, and `inconclusive` means the probe failed or left coverage incomplete. Use `--json` for JSONL output; `--inspect-found --output new-report.tsv` writes a TSV report (it will not overwrite an existing file). Re-running `--inspect-found` resumes from saved inspections; `--force` recomputes them.

The dashboard reports exact per-file counts and an **estimated** unique count of parseable SMILES strings across inspected files (HyperLogLog, nominal relative standard error ≈0.8%). It does not retain individual strings; different spellings of the same molecule count separately. Full inspection of a text column may download the entire file.

See [usage notes](USAGE.md) for sampling limits, result fields, cache behavior, and inspection details. Run `cargo run --release -- --help` for all options.
