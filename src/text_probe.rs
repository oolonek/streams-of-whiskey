use crate::direct_probe::{FileSample, RangeSource};
use crate::smiles_evidence::{SmilesEvidence, WideRow};
use rand::seq::index;
use std::collections::HashMap;

const PREFIX_BYTES: usize = 128 * 1024;
const MAX_RECORD_BYTES: usize = 64 * 1024;
const MAX_RECORDS: usize = 4096;

/// Samples complete rows in the first 128 KiB only; after the first 32 rows,
/// random rows are selected from that prefix, not from the entire file.
pub fn probe(source: &RangeSource, extension: &str, samples: u32) -> Result<FileSample, String> {
    let len = source.size.min(PREFIX_BYTES as u64) as usize;
    if len == 0 {
        return Ok(FileSample {
            found: None,
            sampled_rows: 0,
            wide: false,
        });
    }
    let bytes = source.read(0, len)?;
    sample_prefix(&bytes, extension, samples, source.size <= len as u64)
}

fn sample_prefix(
    bytes: &[u8],
    extension: &str,
    samples: u32,
    at_eof: bool,
) -> Result<FileSample, String> {
    match extension
        .trim_start_matches('.')
        .to_ascii_lowercase()
        .as_str()
    {
        "csv" => sample_csv(bytes, b',', samples, at_eof),
        "tsv" => sample_csv(bytes, b'\t', samples, at_eof),
        "jsonl" | "ndjson" => sample_jsonl(bytes, samples, at_eof),
        _ => Err(format!("unsupported text dataset extension: {extension}")),
    }
}

/// Preflight one selected field using complete records in a bounded prefix.
/// Column evidence can come from any row; a wide-row label must be on the
/// exact row the caller intends to inspect.
pub fn sniff_prefix_column(
    bytes: &[u8],
    format: &str,
    column: &str,
    found_row: u64,
    at_eof: bool,
) -> Result<bool, String> {
    let limit = bytes.len().min(PREFIX_BYTES);
    let at_eof = at_eof && limit == bytes.len();
    let bytes = &bytes[..limit];
    match format.trim_start_matches('.').to_ascii_lowercase().as_str() {
        "csv" | "tsv" => {
            let delimiter = if format.trim_start_matches('.').eq_ignore_ascii_case("csv") {
                b','
            } else {
                b'\t'
            };
            let (headers, rows) = csv_rows(bytes, delimiter, at_eof)?;
            let selected = headers
                .iter()
                .position(|header| header == column)
                .or_else(|| column.parse::<usize>().ok());
            let Some(selected) = selected else {
                return Ok(false);
            };
            let mut evidence = SmilesEvidence::new(column);
            for (index, row) in rows.iter().enumerate() {
                let Some(text) = row.get(selected) else {
                    continue;
                };
                if index as u64 == found_row
                    && WideRow::from_label(row.get(0).unwrap_or_default(), selected != 0)
                        .is_some_and(|wide| wide.accepts(text))
                {
                    return Ok(true);
                }
                if evidence.observe(index as u64, text).is_some() {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        "jsonl" | "ndjson" => {
            let rows = jsonl_rows(bytes, at_eof)?;
            let mut evidence = SmilesEvidence::new(column);
            for (index, row) in rows.iter().enumerate() {
                let Some(object) = row.as_object() else {
                    continue;
                };
                let wide = ["property", "label"].iter().find_map(|name| {
                    object
                        .get(*name)
                        .and_then(|value| value.as_str())
                        .and_then(|label| WideRow::from_label(label, true))
                });
                let Some(text) = object.get(column).and_then(|value| value.as_str()) else {
                    continue;
                };
                if index as u64 == found_row
                    && column != "property"
                    && column != "label"
                    && wide.is_some_and(|row| row.accepts(text))
                {
                    return Ok(true);
                }
                if evidence.observe(index as u64, text).is_some() {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Err(format!("unsupported text dataset extension: {format}")),
    }
}

fn sample_rows<F>(row_count: usize, samples: u32, mut column: F) -> Result<FileSample, String>
where
    F: FnMut(usize) -> Result<Option<(u64, String, bool)>, String>,
{
    let mut result = FileSample {
        found: None,
        sampled_rows: 0,
        wide: false,
    };
    let prefix = row_count.min(32);
    let additional = (samples as usize).min(row_count - prefix);
    let mut random_rows =
        index::sample(&mut rand::rng(), row_count - prefix, additional).into_vec();
    random_rows.sort_unstable();
    for row in (0..prefix).chain(random_rows.into_iter().map(|index| index + prefix)) {
        result.sampled_rows += 1;
        if let Some((found_row, name, wide)) = column(row)? {
            result.found = Some((found_row, name));
            result.wide = wide;
            break;
        }
    }
    Ok(result)
}

// CSV's reader accepts an unterminated quoted field at EOF. Find the last
// genuinely complete record before constructing it, including quoted newlines.
fn complete_csv_len(bytes: &[u8], delimiter: u8, at_eof: bool) -> Result<usize, String> {
    let (mut quoted, mut field_start, mut after_quote) = (false, true, false);
    let (mut boundary, mut record_start) = (0, 0);
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if quoted {
            if byte == b'"' {
                if bytes.get(i + 1) == Some(&b'"') {
                    i += 1;
                } else {
                    quoted = false;
                    after_quote = true;
                }
            }
        } else if byte == b'\n' {
            boundary = i + 1;
            record_start = boundary;
            field_start = true;
            after_quote = false;
        } else if byte == delimiter {
            field_start = true;
            after_quote = false;
        } else if byte == b'"' && field_start {
            quoted = true;
            field_start = false;
        } else if byte != b'\r' || !after_quote {
            field_start = false;
        }
        if i + 1 - record_start > MAX_RECORD_BYTES {
            return Err(format!("text record exceeds {MAX_RECORD_BYTES} bytes"));
        }
        i += 1;
    }
    if at_eof {
        if quoted {
            return Err("unterminated quoted CSV field".into());
        }
        Ok(bytes.len())
    } else {
        Ok(boundary)
    }
}

fn csv_rows(
    bytes: &[u8],
    delimiter: u8,
    at_eof: bool,
) -> Result<(csv::StringRecord, Vec<csv::StringRecord>), String> {
    let complete = complete_csv_len(bytes, delimiter, at_eof)?;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .flexible(true)
        .from_reader(&bytes[..complete]);
    let headers = reader
        .headers()
        .map_err(|err| format!("CSV header: {err}"))?
        .clone();
    let rows = reader
        .records()
        .take(MAX_RECORDS)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("CSV row: {err}"))?;
    Ok((headers, rows))
}

fn sample_csv(
    bytes: &[u8],
    delimiter: u8,
    samples: u32,
    at_eof: bool,
) -> Result<FileSample, String> {
    let (headers, rows) = csv_rows(bytes, delimiter, at_eof)?;
    let mut evidence: Vec<SmilesEvidence> = Vec::new();
    sample_rows(rows.len(), samples, |index| {
        let row = &rows[index];
        let wide = WideRow::from_label(row.get(0).unwrap_or_default(), true);
        for (col, text) in row.iter().enumerate() {
            if col >= evidence.len() {
                evidence.push(SmilesEvidence::new(headers.get(col).unwrap_or_default()));
            }
            let name = || {
                headers
                    .get(col)
                    .filter(|header| !header.is_empty())
                    .map_or_else(|| col.to_string(), str::to_owned)
            };
            if col > 0 && wide.is_some_and(|row| row.accepts(text)) {
                return Ok(Some((index as u64, name(), true)));
            }
            if let Some(found_row) = evidence[col].observe(index as u64, text) {
                return Ok(Some((found_row, name(), wide.is_some() && col > 0)));
            }
        }
        Ok(None)
    })
}

fn jsonl_rows(bytes: &[u8], at_eof: bool) -> Result<Vec<serde_json::Value>, String> {
    let mut lines = Vec::new();
    let mut start = 0;
    for (end, &byte) in bytes.iter().enumerate() {
        if byte == b'\n' {
            if end + 1 - start > MAX_RECORD_BYTES {
                return Err(format!("text record exceeds {MAX_RECORD_BYTES} bytes"));
            }
            if lines.len() < MAX_RECORDS {
                lines.push(&bytes[start..end]);
            }
            start = end + 1;
        }
    }
    if bytes.len() - start > MAX_RECORD_BYTES {
        return Err(format!("text record exceeds {MAX_RECORD_BYTES} bytes"));
    }
    if at_eof && start < bytes.len() && lines.len() < MAX_RECORDS {
        lines.push(&bytes[start..]);
    }
    // Validate all complete lines, even those not selected for SMILES checking.
    lines
        .into_iter()
        .map(|line| serde_json::from_slice::<serde_json::Value>(line))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("JSONL row: {err}"))
}

fn sample_jsonl(bytes: &[u8], samples: u32, at_eof: bool) -> Result<FileSample, String> {
    let rows = jsonl_rows(bytes, at_eof)?;
    let mut evidence: HashMap<&str, SmilesEvidence> = HashMap::new();
    sample_rows(rows.len(), samples, |index| {
        let Some(object) = rows[index].as_object() else {
            return Ok(None);
        };
        let wide = ["property", "label"].iter().find_map(|name| {
            object
                .get(*name)
                .and_then(|value| value.as_str())
                .and_then(|label| WideRow::from_label(label, true))
        });
        for (name, value) in object {
            let Some(text) = value.as_str() else {
                continue;
            };
            if name != "property" && name != "label" && wide.is_some_and(|row| row.accepts(text)) {
                return Ok(Some((index as u64, name.clone(), true)));
            }
            if let Some(found_row) = evidence
                .entry(name.as_str())
                .or_insert_with(|| SmilesEvidence::new(name))
                .observe(index as u64, text)
            {
                return Ok(Some((found_row, name.clone(), wide.is_some())));
            }
        }
        Ok(None)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_csv_delimiters_and_newlines_preserve_row_ordinals() {
        let input = b"description,SMILES\n\"not, a molecule\ncontinued\",C1CC\nother,CCO\n";
        let source = RangeSource::from_bytes(input);
        let result = probe(&source, "csv", 1).unwrap();
        assert_eq!(result.found, Some((1, "SMILES".into())));
        assert_eq!(result.sampled_rows, 2);
        assert!(!result.wide);
    }

    #[test]
    fn incomplete_csv_tail_is_not_sampled_even_if_quoted_newline_ends_prefix() {
        let input = b"name,smiles\nfirst,C1CC\n\"unfinished\n";
        let result = sample_prefix(input, "csv", 5, false).unwrap();
        assert_eq!(result.found, None);
        assert_eq!(result.sampled_rows, 1);

        assert!(sample_prefix(input, "csv", 5, true).is_err());
    }

    #[test]
    fn jsonl_uses_top_level_string_field_names_and_rejects_bad_complete_rows() {
        let input = b"{\"nested\":{\"smiles\":\"CCO\"},\"count\":2}\n{\"smiles\":\"CCO\"}\n";
        let result = sample_prefix(input, "ndjson", 1, true).unwrap();
        assert_eq!(result.found, Some((1, "smiles".into())));
        assert_eq!(result.sampled_rows, 2);
        assert!(!result.wide);
        assert!(sample_prefix(b"{bad}\n", "jsonl", 0, true).is_err());
    }

    #[test]
    fn empty_and_header_only_text_have_no_data_rows() {
        assert_eq!(
            sample_prefix(b"name,smiles\n", "tsv", 9, true)
                .unwrap()
                .sampled_rows,
            0
        );
        assert_eq!(
            sample_prefix(b"", "jsonl", 9, true).unwrap().sampled_rows,
            0
        );
    }

    #[test]
    fn csv_labeled_wide_unnamed_columns_fall_back_to_indices() {
        let result = sample_prefix(b"name,\nSMILES,CCO\n", "csv", 0, true).unwrap();
        assert_eq!(result.found, Some((0, "1".into())));
        assert!(result.wide);
    }

    #[test]
    fn categorical_and_prose_csv_tsv_fields_are_not_molecules() {
        for extension in ["csv", "tsv"] {
            let delimiter = if extension == "csv" { "," } else { "\t" };
            let input = format!(
                "sex{delimiter}type{delimiter}class{delimiter}description\n\
                 F{delimiter}C{delimiter}I{delimiter}Long ordinary text about everyday things\n\
                 M{delimiter}O{delimiter}II{delimiter}Another ordinary description\n\
                 F{delimiter}C{delimiter}IV{delimiter}No molecular structures here\n"
            );
            let result = sample_prefix(input.as_bytes(), extension, 1, true).unwrap();
            assert_eq!(result.found, None);
            assert_eq!(result.sampled_rows, 3);
            assert!(!result.wide);
        }
    }

    #[test]
    fn generic_csv_tsv_columns_confirm_from_content_with_one_sample() {
        for (extension, delimiter) in [("csv", ","), ("tsv", "\t")] {
            let input = format!("sex{delimiter}type\nCCO{delimiter}C\nCCN{delimiter}O\n");
            let result = sample_prefix(input.as_bytes(), extension, 1, true).unwrap();
            assert_eq!(result.found, Some((0, "sex".into())));
            assert_eq!(result.sampled_rows, 2);
            assert!(!result.wide);

            let diagnostic = format!("sex{delimiter}type\nF{delimiter}C\nN#N{delimiter}O\n");
            let result = sample_prefix(diagnostic.as_bytes(), extension, 1, true).unwrap();
            assert_eq!(result.found, Some((1, "sex".into())));
            assert_eq!(result.sampled_rows, 2);

            let misleading_header = format!("description{delimiter}sex\nN#N{delimiter}F\n");
            let result = sample_prefix(misleading_header.as_bytes(), extension, 1, true).unwrap();
            assert_eq!(result.found, Some((0, "description".into())));
        }
    }

    #[test]
    fn long_prose_in_another_column_does_not_hide_structures() {
        let prose = "ordinary words ".repeat(300);
        let input = format!("description,sex\n{prose},N#N\n");
        let result = sample_prefix(input.as_bytes(), "csv", 1, true).unwrap();
        assert_eq!(result.found, Some((0, "sex".into())));
        let jsonl = format!(r#"{{"description":"{prose}","sex":"N#N"}}"#);
        let result = sample_prefix(jsonl.as_bytes(), "jsonl", 1, true).unwrap();
        assert_eq!(result.found, Some((0, "sex".into())));
    }

    #[test]
    fn named_structure_fields_accept_single_atoms_without_category_false_positives() {
        for (extension, delimiter) in [("csv", ","), ("tsv", "\t")] {
            for header in ["SMILES", "canonical_smiles"] {
                let input =
                    format!("sex{delimiter}type{delimiter}{header}\nF{delimiter}C{delimiter}F\n");
                let result = sample_prefix(input.as_bytes(), extension, 0, true).unwrap();
                assert_eq!(result.found, Some((0, header.into())));
                assert!(!result.wide);
            }
            let input = format!("property{delimiter}sample_a\nSMILES{delimiter}C\n");
            let result = sample_prefix(input.as_bytes(), extension, 0, true).unwrap();
            assert_eq!(result.found, Some((0, "sample_a".into())));
            assert!(result.wide);
        }
    }

    #[test]
    fn jsonl_uses_content_not_structure_field_names() {
        let plain =
            br#"{"sex":"F","type":"C","description":"Long ordinary text about everyday things"}"#;
        let result = sample_prefix(plain, "jsonl", 0, true).unwrap();
        assert_eq!(result.found, None);
        assert_eq!(result.sampled_rows, 1);
        assert!(!result.wide);

        for field in ["SMILES", "canonical_smiles"] {
            let input = format!(r#"{{"sex":"F","type":"C","{field}":"F"}}"#);
            let result = sample_prefix(input.as_bytes(), "jsonl", 0, true).unwrap();
            assert_eq!(result.found, Some((0, field.into())));
            assert!(!result.wide);
        }
        for label_field in ["property", "label"] {
            let input = format!(r#"{{"{label_field}":"SMILES","sample_a":"C"}}"#);
            let result = sample_prefix(input.as_bytes(), "jsonl", 0, true).unwrap();
            assert_eq!(result.found, Some((0, "sample_a".into())));
            assert!(result.wide);

            let label_only = format!(r#"{{"{label_field}":"SMILES"}}"#);
            let result = sample_prefix(label_only.as_bytes(), "jsonl", 0, true).unwrap();
            assert_eq!(result.found, None);
        }
    }

    #[test]
    fn generic_jsonl_columns_confirm_from_content_with_one_sample() {
        let input = b"{\"sex\":\"CCO\",\"type\":\"C\"}\n{\"sex\":\"CCN\",\"type\":\"O\"}\n";
        let result = sample_prefix(input, "jsonl", 1, true).unwrap();
        assert_eq!(result.found, Some((0, "sex".into())));
        assert_eq!(result.sampled_rows, 2);
        let input = b"{\"sex\":\"F\",\"type\":\"C\"}\n{\"sex\":\"N#N\",\"type\":\"O\"}\n";
        let result = sample_prefix(input, "jsonl", 1, true).unwrap();
        assert_eq!(result.found, Some((1, "sex".into())));
        assert_eq!(result.sampled_rows, 2);
        let categorical = b"{\"sex\":\"F\",\"type\":\"C\",\"class\":\"I\"}\n{\"sex\":\"M\",\"type\":\"O\",\"class\":\"IV\"}\n";
        assert_eq!(
            sample_prefix(categorical, "jsonl", 1, true).unwrap().found,
            None
        );
        let misleading = b"{\"description\":\"N#N\",\"sex\":\"F\"}\n";
        assert_eq!(
            sample_prefix(misleading, "jsonl", 1, true).unwrap().found,
            Some((0, "description".into()))
        );
    }

    #[test]
    fn prefix_preflight_checks_selected_column_and_exact_wide_row() {
        for (format, delimiter) in [("csv", ","), ("tsv", "\t")] {
            let generic = format!("sex{delimiter}type\nCCO{delimiter}C\nCCN{delimiter}O\n");
            assert!(sniff_prefix_column(generic.as_bytes(), format, "sex", 20, true).unwrap());
            assert!(!sniff_prefix_column(generic.as_bytes(), format, "type", 0, true).unwrap());
            let named = format!("SMILES{delimiter}sex\nF{delimiter}M\n");
            assert!(sniff_prefix_column(named.as_bytes(), format, "SMILES", 0, true).unwrap());
            let wide =
                format!("property{delimiter}sample_a\nother{delimiter}F\nSMILES{delimiter}C\n");
            assert!(sniff_prefix_column(wide.as_bytes(), format, "sample_a", 1, true).unwrap());
            assert!(!sniff_prefix_column(wide.as_bytes(), format, "sample_a", 0, true).unwrap());
            let incomplete = format!("sex{delimiter}type\nF{delimiter}C\nN#N{delimiter}O");
            assert!(!sniff_prefix_column(incomplete.as_bytes(), format, "sex", 1, false).unwrap());
            assert!(sniff_prefix_column(incomplete.as_bytes(), format, "sex", 1, true).unwrap());
        }
        let generic = b"{\"sex\":\"CCO\"}\n{\"sex\":\"CCN\"}\n";
        assert!(sniff_prefix_column(generic, "jsonl", "sex", 30, true).unwrap());
        let wide = b"{\"property\":\"other\",\"sample_a\":\"F\"}\n{\"property\":\"SMILES\",\"sample_a\":\"C\"}\n";
        assert!(sniff_prefix_column(wide, "jsonl", "sample_a", 1, true).unwrap());
        assert!(!sniff_prefix_column(wide, "jsonl", "sample_a", 0, true).unwrap());
        let labels = b"{\"property\":\"SMILES\",\"label\":\"SMILES\",\"sample_a\":\"F\"}\n";
        assert!(!sniff_prefix_column(labels, "jsonl", "property", 0, true).unwrap());
        assert!(!sniff_prefix_column(labels, "jsonl", "label", 0, true).unwrap());
        assert!(sniff_prefix_column(labels, "jsonl", "sample_a", 0, true).unwrap());
        let incomplete = b"{\"sex\":\"F\"}\n{\"sex\":\"N#N\"}";
        assert!(!sniff_prefix_column(incomplete, "jsonl", "sex", 1, false).unwrap());
        assert!(sniff_prefix_column(incomplete, "jsonl", "sex", 1, true).unwrap());
        assert!(sniff_prefix_column(b"{\"SMILES\":\"F\"}\n", "jsonl", "SMILES", 0, true).unwrap());
    }

    #[test]
    fn one_sample_still_inspects_thirty_two_prefix_rows() {
        let mut csv = b"sex,type\n".to_vec();
        let mut jsonl = Vec::new();
        for row in 0..33 {
            let value = if row == 30 {
                "CCO"
            } else if row == 31 {
                "CCN"
            } else {
                "F"
            };
            csv.extend_from_slice(format!("{value},C\n").as_bytes());
            jsonl.extend_from_slice(format!("{{\"sex\":\"{value}\"}}\n").as_bytes());
        }
        for (format, bytes) in [("csv", csv), ("jsonl", jsonl)] {
            let result = sample_prefix(&bytes, format, 1, true).unwrap();
            assert_eq!(result.found, Some((30, "sex".into())));
            assert_eq!(result.sampled_rows, 32);
        }
    }

    #[test]
    fn sampling_never_reads_beyond_the_prefix() {
        let mut input = b"smiles\n".to_vec();
        while input.len() <= PREFIX_BYTES {
            input.extend_from_slice(b"C1CC\n");
        }
        input.extend_from_slice(b"CCO\n");
        let source = RangeSource::from_bytes(&input);
        let result = probe(&source, "csv", 50).unwrap();
        assert_eq!(result.found, None);
        assert_eq!(result.sampled_rows, 82);
    }
}
