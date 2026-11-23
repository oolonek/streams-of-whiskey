use std::io::{BufRead, BufReader, Read};

use crate::full_scan::{Axis, ScanCounts, ScanObserver};
use crate::smiles_evidence::WideRow;

// Bound one JSON record, not the whole dataset. This still permits strings larger
// than MAX_CELL_BYTES to be classified as oversized rather than misclassified.
const MAX_JSON_RECORD_BYTES: usize = 2 * 1024 * 1024;

pub fn scan_with_progress<R: Read>(
    input: R,
    format: &str,
    column: &str,
    found_row: u64,
    axis: Axis,
    observer: &dyn ScanObserver,
) -> Result<ScanCounts, String> {
    match format {
        "csv" => scan_delimited(input, b',', column, found_row, axis, observer),
        "tsv" => scan_delimited(input, b'\t', column, found_row, axis, observer),
        "jsonl" | "ndjson" => scan_jsonl(input, column, found_row, axis, observer),
        other => Err(format!("unsupported text format: {other}")),
    }
}

#[cfg(test)]
fn scan<R: Read>(
    input: R,
    format: &str,
    column: &str,
    found_row: u64,
    axis: Axis,
) -> Result<ScanCounts, String> {
    struct Noop;
    impl ScanObserver for Noop {
        fn progress(&self, _: &ScanCounts, _: Option<u64>) {}
    }
    scan_with_progress(input, format, column, found_row, axis, &Noop)
}

fn report_row(counts: &ScanCounts, observer: &dyn ScanObserver) {
    if counts.rows_scanned == 1 || counts.rows_scanned % 512 == 0 {
        observer.progress(counts, None);
    }
}

fn csv_column(headers: &csv::StringRecord, name: &str) -> Result<usize, String> {
    if name.is_empty() {
        return Err("empty CSV column name; use the numeric index of a blank header".into());
    }
    let mut named = None;
    for (index, header) in headers.iter().enumerate() {
        if header == name {
            if named.replace(index).is_some() {
                return Err(format!("ambiguous CSV column {name:?}: duplicate headers"));
            }
        }
    }
    if let Some(index) = named {
        return Ok(index);
    }
    if let Ok(index) = name.parse::<usize>() {
        if headers.get(index) == Some("") {
            return Ok(index);
        }
    }
    Err(format!("CSV column {name:?} not found in header"))
}

fn scan_delimited<R: Read>(
    input: R,
    delimiter: u8,
    column: &str,
    found_row: u64,
    axis: Axis,
    observer: &dyn ScanObserver,
) -> Result<ScanCounts, String> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .flexible(true)
        .from_reader(input);
    let headers = reader
        .headers()
        .map_err(|err| format!("CSV header: {err}"))?
        .clone();
    let selected = csv_column(&headers, column)?;
    let mut counts = ScanCounts::new(Axis::Column);
    let mut found = false;
    let mut record = csv::StringRecord::new();
    while reader
        .read_record(&mut record)
        .map_err(|err| format!("CSV row {}: {err}", counts.rows_scanned))?
    {
        let index = counts.rows_scanned;
        if index == found_row {
            found = true;
            let wide = record
                .get(0)
                .and_then(|first| WideRow::from_label(first, true));
            let labeled = wide.is_some() && selected != 0;
            if axis == Axis::Row || (axis == Axis::Auto && labeled) {
                observer.reset_smiles()?;
                let mut row_counts = ScanCounts::new(Axis::Row);
                row_counts.rows_scanned = index + 1;
                for (field, value) in record.iter().enumerate() {
                    if field != 0 || wide.is_none() {
                        row_counts.check_with_observer(Some(value), observer)?;
                    }
                }
                // Missing fields in a ragged row still represent empty cells.
                for field in record.len()..headers.len() {
                    if field != 0 || wide.is_none() {
                        row_counts.check_with_observer(None, observer)?;
                    }
                }
                observer.progress(&row_counts, None);
                return Ok(row_counts);
            }
        }
        counts.check_with_observer(record.get(selected), observer)?;
        counts.rows_scanned += 1;
        report_row(&counts, observer);
    }
    if !found {
        return Err(format!(
            "CSV pinpointed row {found_row} not found ({} data rows)",
            counts.rows_scanned
        ));
    }
    observer.progress(&counts, None);
    Ok(counts)
}

fn check_json(
    counts: &mut ScanCounts,
    value: Option<&serde_json::Value>,
    observer: &dyn ScanObserver,
) -> Result<(), String> {
    match value {
        None => counts.check_with_observer(None, observer),
        Some(serde_json::Value::String(text)) => counts.check_with_observer(Some(text), observer),
        Some(_) => {
            counts.non_string();
            Ok(())
        }
    }
}

fn scan_jsonl<R: Read>(
    input: R,
    column: &str,
    found_row: u64,
    axis: Axis,
    observer: &dyn ScanObserver,
) -> Result<ScanCounts, String> {
    if column.is_empty() {
        return Err("empty JSONL column name".into());
    }
    let mut reader = BufReader::new(input);
    let mut line = Vec::new();
    let mut counts = ScanCounts::new(Axis::Column);
    let mut found = false;
    loop {
        line.clear();
        let read = (&mut reader)
            .take(MAX_JSON_RECORD_BYTES as u64 + 1)
            .read_until(b'\n', &mut line)
            .map_err(|err| format!("JSONL row {}: {err}", counts.rows_scanned))?;
        if read == 0 {
            break;
        }
        if read > MAX_JSON_RECORD_BYTES {
            return Err(format!(
                "JSONL row {} exceeds {MAX_JSON_RECORD_BYTES} bytes",
                counts.rows_scanned
            ));
        }
        let row = counts.rows_scanned;
        let value: serde_json::Value =
            serde_json::from_slice(&line).map_err(|err| format!("JSONL row {row}: {err}"))?;
        let object = value
            .as_object()
            .ok_or_else(|| format!("JSONL row {row}: expected a JSON object"))?;
        if row == found_row {
            found = true;
            if !object.contains_key(column) {
                return Err(format!("JSONL row {row}: column {column:?} not found"));
            }
            let wide = ["property", "label"].into_iter().find_map(|field| {
                object
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .and_then(|name| WideRow::from_label(name, true).map(|_| field))
            });
            let labeled = wide.is_some_and(|field| field != column);
            if axis == Axis::Row || (axis == Axis::Auto && labeled) {
                observer.reset_smiles()?;
                let mut row_counts = ScanCounts::new(Axis::Row);
                row_counts.rows_scanned = row + 1;
                for (field, value) in object {
                    if Some(field.as_str()) != wide {
                        check_json(&mut row_counts, Some(value), observer)?;
                    }
                }
                observer.progress(&row_counts, None);
                return Ok(row_counts);
            }
        }
        check_json(&mut counts, object.get(column), observer)?;
        counts.rows_scanned += 1;
        report_row(&counts, observer);
    }
    if !found {
        return Err(format!(
            "JSONL pinpointed row {found_row} not found ({} data rows)",
            counts.rows_scanned
        ));
    }
    observer.progress(&counts, None);
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[derive(Default)]
    struct Snapshots(RefCell<Vec<(Axis, u64, u64, u64, Option<u64>)>>);

    impl ScanObserver for Snapshots {
        fn progress(&self, counts: &ScanCounts, total_rows: Option<u64>) {
            self.0.borrow_mut().push((
                counts.axis,
                counts.rows_scanned,
                counts.cells_scanned,
                counts.valid,
                total_rows,
            ));
        }
    }

    #[test]
    fn every_text_format_streams_only_exact_valid_strings() {
        #[derive(Default)]
        struct Collected(RefCell<Vec<String>>);
        impl ScanObserver for Collected {
            fn progress(&self, _: &ScanCounts, _: Option<u64>) {}
            fn valid_smiles(&self, smiles: &str) -> Result<(), String> {
                self.0.borrow_mut().push(smiles.to_owned());
                Ok(())
            }
        }

        for (format, input) in [
            ("csv", "smiles\nCCO\nC(C)O\nCCO\nnot a smiles!\n"),
            ("tsv", "smiles\nCCO\nC(C)O\nCCO\nnot a smiles!\n"),
            (
                "jsonl",
                "{\"smiles\":\"CCO\"}\n{\"smiles\":\"C(C)O\"}\n{\"smiles\":\"CCO\"}\n{\"smiles\":4}\n{\"smiles\":\"not a smiles!\"}\n",
            ),
            (
                "ndjson",
                "{\"smiles\":\"CCO\"}\n{\"smiles\":\"C(C)O\"}\n{\"smiles\":\"CCO\"}\n{\"smiles\":null}\n{\"smiles\":\"not a smiles!\"}\n",
            ),
        ] {
            let collected = Collected::default();
            scan_with_progress(
                input.as_bytes(),
                format,
                "smiles",
                0,
                Axis::Column,
                &collected,
            )
            .unwrap();
            assert_eq!(*collected.0.borrow(), ["CCO", "C(C)O", "CCO"], "{format}");
        }
    }

    #[test]
    fn wide_row_discards_column_candidates_before_the_pinpoint() {
        #[derive(Default)]
        struct Collected(RefCell<Vec<String>>);
        impl ScanObserver for Collected {
            fn progress(&self, _: &ScanCounts, _: Option<u64>) {}
            fn valid_smiles(&self, smiles: &str) -> Result<(), String> {
                self.0.borrow_mut().push(smiles.to_owned());
                Ok(())
            }
            fn reset_smiles(&self) -> Result<(), String> {
                self.0.borrow_mut().clear();
                Ok(())
            }
        }
        let collected = Collected::default();
        let counts = scan_with_progress(
            &b"property,a,b\nother,CCO,N\nSMILES,C(C)O,O\n"[..],
            "csv",
            "a",
            1,
            Axis::Auto,
            &collected,
        )
        .unwrap();
        assert_eq!(counts.axis, Axis::Row);
        assert_eq!(*collected.0.borrow(), ["C(C)O", "O"]);
    }

    #[test]
    fn progress_reports_counted_rows_and_completion_for_text_formats() {
        for (format, header, row) in [
            ("csv", "smiles\n", "CCO\n"),
            ("tsv", "smiles\n", "CCO\n"),
            ("jsonl", "", "{\"smiles\":\"CCO\"}\n"),
            ("ndjson", "", "{\"smiles\":\"CCO\"}\n"),
        ] {
            let input = format!("{header}{}", row.repeat(513));
            let snapshots = Snapshots::default();
            let counts = scan_with_progress(
                input.as_bytes(),
                format,
                "smiles",
                0,
                Axis::Column,
                &snapshots,
            )
            .unwrap();
            assert_eq!(counts.valid, 513);
            assert_eq!(
                *snapshots.0.borrow(),
                [
                    (Axis::Column, 1, 1, 1, None),
                    (Axis::Column, 512, 512, 512, None),
                    (Axis::Column, 513, 513, 513, None),
                ],
                "{format}"
            );
        }
    }

    #[test]
    fn progress_reports_wide_completion_but_not_error_completion() {
        for (format, input) in [
            ("csv", "property,a,b\nSMILES,CCO,N\nignored,C,C\n"),
            (
                "jsonl",
                "{\"property\":\"SMILES\",\"a\":\"CCO\",\"b\":\"N\"}\n{broken}\n",
            ),
        ] {
            let snapshots = Snapshots::default();
            let counts =
                scan_with_progress(input.as_bytes(), format, "a", 0, Axis::Auto, &snapshots)
                    .unwrap();
            assert_eq!(
                *snapshots.0.borrow(),
                [(
                    Axis::Row,
                    counts.rows_scanned,
                    counts.cells_scanned,
                    counts.valid,
                    None
                )]
            );
        }
        for (format, input, column) in [
            ("csv", &b"a\nCCO\n\xff\n"[..], "a"),
            ("jsonl", &b"{\"a\":\"CCO\"}\n{broken}\n"[..], "a"),
        ] {
            let snapshots = Snapshots::default();
            assert!(
                scan_with_progress(input, format, column, 0, Axis::Column, &snapshots).is_err()
            );
            assert_eq!(*snapshots.0.borrow(), [(Axis::Column, 1, 1, 1, None)]);
        }
    }

    fn assert_counts(counts: ScanCounts, axis: Axis, rows: u64, cells: [u64; 5]) {
        assert_eq!(counts.axis, axis);
        assert_eq!(counts.rows_scanned, rows);
        assert_eq!(
            [
                counts.valid,
                counts.invalid,
                counts.empty,
                counts.non_string,
                counts.oversized
            ],
            cells
        );
        assert_eq!(counts.cells_scanned, cells.into_iter().sum::<u64>());
    }

    #[test]
    fn quoted_csv_and_tsv_scan_through_eof() {
        let csv = b"name,smiles\n\"multiline\nname\",CCO\nother,not-a-smiles\nlast,\n";
        assert_counts(
            scan(&csv[..], "csv", "smiles", 0, Axis::Auto).unwrap(),
            Axis::Column,
            3,
            [1, 1, 1, 0, 0],
        );
        let tsv = b"name\tsmiles\n\"first\tname\"\tCCO\n\"second\nname\"\tN\n";
        assert_counts(
            scan(&tsv[..], "tsv", "smiles", 0, Axis::Column).unwrap(),
            Axis::Column,
            2,
            [2, 0, 0, 0, 0],
        );
    }

    #[test]
    fn wide_csv_row_excludes_label_and_stops_at_pinpoint() {
        let csv = b"property,a,b,c\nSMILES,CCO,not-a-smiles,\nignored,CCO,CCO,CCO\n";
        assert_counts(
            scan(&csv[..], "csv", "a", 0, Axis::Auto).unwrap(),
            Axis::Row,
            1,
            [1, 1, 1, 0, 0],
        );
        assert_counts(
            scan(&csv[..], "csv", "a", 0, Axis::Column).unwrap(),
            Axis::Column,
            2,
            [2, 0, 0, 0, 0],
        );
        assert_counts(
            scan(&csv[..], "csv", "a", 1, Axis::Row).unwrap(),
            Axis::Row,
            2,
            [3, 1, 0, 0, 0],
        );
        assert_counts(
            scan(&csv[..], "csv", "property", 0, Axis::Auto).unwrap(),
            Axis::Column,
            2,
            [0, 2, 0, 0, 0],
        );
        assert_counts(
            scan(&csv[..], "csv", "property", 0, Axis::Row).unwrap(),
            Axis::Row,
            1,
            [1, 1, 1, 0, 0],
        );
        let oversized = format!("property,a\nstructure,{}\n", "C".repeat(65_537));
        assert_counts(
            scan(oversized.as_bytes(), "csv", "a", 0, Axis::Auto).unwrap(),
            Axis::Row,
            1,
            [0, 0, 0, 0, 1],
        );
    }

    #[test]
    fn blank_header_indices_duplicates_and_missing_rows() {
        assert_counts(
            scan(&b"a,,b\nX,CCO,Y\nZ,,W\n"[..], "csv", "1", 0, Axis::Column).unwrap(),
            Axis::Column,
            2,
            [1, 0, 1, 0, 0],
        );
        assert!(
            scan(&b"a,a\nCCO,N\n"[..], "csv", "a", 0, Axis::Auto)
                .unwrap_err()
                .contains("duplicate")
        );
        assert!(
            scan(&b"a,b\nCCO,N\n"[..], "csv", "missing", 0, Axis::Auto)
                .unwrap_err()
                .contains("not found")
        );
        assert!(
            scan(&b"a,b\nCCO,N\n"[..], "csv", "a", 1, Axis::Column)
                .unwrap_err()
                .contains("row 1")
        );
        assert_counts(
            scan(&b"a,b\nCCO\nN,CCO\n"[..], "csv", "b", 1, Axis::Column).unwrap(),
            Axis::Column,
            2,
            [1, 0, 1, 0, 0],
        );
    }

    #[test]
    fn jsonl_wide_nested_missing_non_string_and_oversized() {
        let input = b"{\"property\":\"SMILES\",\"a\":\"CCO\",\"b\":{\"nested\":\"CCO\"},\"c\":null,\"d\":\"\"}\n{\"a\":\"N\"}\n";
        assert_counts(
            scan(&input[..], "jsonl", "a", 0, Axis::Auto).unwrap(),
            Axis::Row,
            1,
            [1, 0, 1, 2, 0],
        );
        assert_counts(
            scan(&input[..], "ndjson", "a", 0, Axis::Column).unwrap(),
            Axis::Column,
            2,
            [2, 0, 0, 0, 0],
        );
        assert_counts(
            scan(&input[..], "jsonl", "b", 0, Axis::Column).unwrap(),
            Axis::Column,
            2,
            [0, 0, 1, 1, 0],
        );
        assert_counts(
            scan(&input[..], "jsonl", "property", 0, Axis::Auto).unwrap(),
            Axis::Column,
            2,
            [0, 1, 1, 0, 0],
        );
        assert_counts(
            scan(&input[..], "jsonl", "a", 1, Axis::Row).unwrap(),
            Axis::Row,
            2,
            [1, 0, 0, 0, 0],
        );
        let large = format!(
            "{{\"label\":\"SMILES\",\"a\":\"{}\"}}\n",
            "C".repeat(65_537)
        );
        assert_counts(
            scan(large.as_bytes(), "jsonl", "a", 0, Axis::Auto).unwrap(),
            Axis::Row,
            1,
            [0, 0, 0, 0, 1],
        );
    }

    #[test]
    fn malformed_records_and_missing_json_column_are_errors() {
        assert!(
            scan(&b"a,b\nN,\xff\n"[..], "csv", "a", 0, Axis::Column)
                .unwrap_err()
                .contains("CSV row 0")
        );
        assert!(
            scan(&b"{broken}\n"[..], "jsonl", "a", 0, Axis::Column)
                .unwrap_err()
                .contains("JSONL row 0")
        );
        assert!(
            scan(&b"[1,2]\n"[..], "jsonl", "a", 0, Axis::Column)
                .unwrap_err()
                .contains("expected a JSON object")
        );
        assert!(
            scan(&b"{\"other\":\"CCO\"}\n"[..], "jsonl", "a", 0, Axis::Column)
                .unwrap_err()
                .contains("column \"a\" not found")
        );
        let trailing_error = b"{\"label\":\"structure\",\"a\":\"CCO\"}\n{broken}\n";
        assert_counts(
            scan(&trailing_error[..], "jsonl", "a", 0, Axis::Auto).unwrap(),
            Axis::Row,
            1,
            [1, 0, 0, 0, 0],
        );
        assert!(
            scan(&trailing_error[..], "jsonl", "a", 0, Axis::Column)
                .unwrap_err()
                .contains("JSONL row 1")
        );
        let huge = format!("{{\"a\":\"{}\"}}\n", "C".repeat(MAX_JSON_RECORD_BYTES));
        assert!(
            scan(huge.as_bytes(), "jsonl", "a", 0, Axis::Column)
                .unwrap_err()
                .contains("exceeds")
        );
    }
}
