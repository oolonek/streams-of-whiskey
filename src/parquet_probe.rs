use std::collections::BTreeSet;
use std::io::{self, Read};

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::DataType;
use bytes::{Buf, Bytes, BytesMut};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowSelection,
    RowSelector,
};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use rand::Rng;

use crate::direct_probe::{FileSample, RangeSource};
use crate::smiles_evidence::{SmilesEvidence, WideRow, preferred_column};

const MAX_METADATA: usize = 8 * 1024 * 1024;
const MAX_SELECTED_COLUMN_BYTES: u64 = 8 * 1024 * 1024;
const MAX_SAMPLES: usize = 512;
const READ_CHUNK: usize = 64 * 1024;
const MAX_RANGE: usize = 2 * 1024 * 1024;

#[derive(Clone)]
struct RangedParquet(RangeSource);

impl Length for RangedParquet {
    fn len(&self) -> u64 {
        self.0.size
    }
}

struct RangedStream {
    source: RangeSource,
    offset: u64,
    buffered: Bytes,
}

impl Read for RangedStream {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.offset == self.source.size {
            return Ok(0);
        }
        if self.buffered.is_empty() {
            let len = (self.source.size - self.offset).min(READ_CHUNK as u64) as usize;
            self.buffered = self
                .source
                .read(self.offset, len)
                .map_err(io::Error::other)?;
        }
        let len = output.len().min(self.buffered.len());
        output[..len].copy_from_slice(&self.buffered[..len]);
        self.buffered.advance(len);
        self.offset += len as u64;
        Ok(len)
    }
}

impl ChunkReader for RangedParquet {
    type T = RangedStream;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.0.size {
            return Err(ParquetError::General(
                "Parquet stream starts past end of file".into(),
            ));
        }
        Ok(RangedStream {
            source: self.0.clone(),
            offset: start,
            buffered: Bytes::new(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if length as u64 > MAX_SELECTED_COLUMN_BYTES {
            return Err(ParquetError::General(
                "Parquet range exceeds the bounded read size".into(),
            ));
        }
        if length <= MAX_RANGE {
            return self.0.read(start, length).map_err(ParquetError::General);
        }
        let mut data = BytesMut::with_capacity(length);
        for offset in (0..length).step_by(MAX_RANGE) {
            let len = (length - offset).min(MAX_RANGE);
            let absolute = start.checked_add(offset as u64).ok_or_else(|| {
                ParquetError::General("Parquet range overflows file offset".into())
            })?;
            data.extend_from_slice(&self.0.read(absolute, len).map_err(ParquetError::General)?);
        }
        Ok(data.freeze())
    }
}

fn sample_indices(total: u64, additional: u32) -> Result<Vec<u64>, String> {
    // A fixed first-page window corroborates generic columns before random
    // sampling; one random observation alone rarely sees a second molecule.
    let prefix = total.min(32);
    let count = prefix + u64::from(additional).min(total - prefix);
    if count > MAX_SAMPLES as u64 {
        return Err(format!(
            "Parquet sampling is limited to {MAX_SAMPLES} rows per file"
        ));
    }
    let mut chosen: BTreeSet<u64> = (0..prefix).collect();
    let mut rng = rand::rng();
    while chosen.len() < count as usize {
        chosen.insert(rng.random_range(prefix..total));
    }
    Ok(chosen.into_iter().collect())
}

pub(crate) fn is_string_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, value) => matches!(
            value.as_ref(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        _ => false,
    }
}

fn selected_string_columns(schema: &arrow_schema::Schema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| is_string_type(field.data_type()).then_some(index))
        .collect()
}

pub(crate) fn string_cell(array: &dyn Array, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_any().downcast_ref::<StringArray>()?.value(row)),
        DataType::LargeUtf8 => Some(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()?
                .value(row),
        ),
        DataType::Utf8View => Some(array.as_any().downcast_ref::<StringViewArray>()?.value(row)),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary_opt()?;
            macro_rules! key {
                ($type:ty) => {
                    usize::try_from(dictionary.keys().as_primitive::<$type>().value(row)).ok()?
                };
            }
            let index = match dictionary.keys().data_type() {
                DataType::Int8 => key!(Int8Type),
                DataType::Int16 => key!(Int16Type),
                DataType::Int32 => key!(Int32Type),
                DataType::Int64 => key!(Int64Type),
                DataType::UInt8 => key!(UInt8Type),
                DataType::UInt16 => key!(UInt16Type),
                DataType::UInt32 => key!(UInt32Type),
                DataType::UInt64 => key!(UInt64Type),
                _ => return None,
            };
            string_cell(dictionary.values().as_ref(), index)
        }
        _ => None,
    }
}

fn find_wide_smiles(batch: &RecordBatch) -> Option<String> {
    let text = string_cell(batch.column(0).as_ref(), 0)?;
    // The row was selected only after its first field supplied a wide-row label.
    WideRow::parseable(text).then(|| batch.schema().field(0).name().to_owned())
}

fn check_footer(source: &RangeSource) -> Result<(), String> {
    if source.size < 12 {
        return Err("Parquet file is too short for a footer".into());
    }
    let footer = source.read(source.size - 8, 8)?;
    if &footer[4..] != b"PAR1" {
        return Err("Parquet footer has an invalid magic value".into());
    }
    let metadata_size = u32::from_le_bytes(footer[..4].try_into().unwrap()) as usize;
    if metadata_size > MAX_METADATA || metadata_size as u64 > source.size - 8 {
        return Err("Parquet footer metadata exceeds the bounded metadata limit".into());
    }
    Ok(())
}

fn selected_reader(
    builder: ParquetRecordBatchReaderBuilder<RangedParquet>,
    source: &RangeSource,
    groups: &[u64],
    positions: &[u64],
    columns: Vec<usize>,
) -> Result<ParquetRecordBatchReader, String> {
    let metadata = builder.metadata();
    if metadata.memory_size() > 4 * MAX_METADATA {
        return Err("decoded Parquet metadata exceeds the memory limit".into());
    }
    let projection = ProjectionMask::roots(builder.parquet_schema(), columns);
    let selected_leaves: Vec<_> = (0..builder.parquet_schema().num_columns())
        .filter(|&index| projection.leaf_included(index))
        .collect();
    if selected_leaves.is_empty() {
        return Err("Parquet string fields have no physical columns".into());
    }

    let mut selected_groups = Vec::new();
    let mut selectors = Vec::with_capacity(positions.len() * 2 + 1);
    let mut group_start = 0_u64;
    let mut group_index = 0;
    let mut selected_group_rows = 0_u64;
    let mut last_selected = 0_u64;
    let mut compressed = 0_u64;
    let mut decoded = 0_u64;
    for &position in positions {
        while position >= groups[group_index] {
            group_start = groups[group_index];
            group_index += 1;
        }
        if selected_groups.last() != Some(&group_index) {
            if let Some(&previous) = selected_groups.last() {
                selected_group_rows += groups[previous]
                    - if previous == 0 {
                        0
                    } else {
                        groups[previous - 1]
                    };
            }
            selected_groups.push(group_index);
            let group = metadata.row_group(group_index);
            // Arrow's sync reader can buffer entire projected column chunks even for one row.
            // Reject oversized groups before it can allocate or download them.
            for &column in &selected_leaves {
                let chunk = group.column(column);
                let size = u64::try_from(chunk.compressed_size())
                    .map_err(|_| "Parquet column has a negative compressed size")?;
                let expanded = u64::try_from(chunk.uncompressed_size())
                    .map_err(|_| "Parquet column has a negative uncompressed size")?;
                let (offset, length) = chunk.byte_range();
                if length != size
                    || offset
                        .checked_add(length)
                        .is_none_or(|end| end > source.size)
                {
                    return Err("Parquet column chunk points beyond the file".into());
                }
                compressed = compressed
                    .checked_add(size)
                    .ok_or("Parquet column sizes overflow")?;
                decoded = decoded
                    .checked_add(expanded)
                    .ok_or("Parquet decoded sizes overflow")?;
            }
            if compressed > MAX_SELECTED_COLUMN_BYTES || decoded > MAX_SELECTED_COLUMN_BYTES {
                return Err("Parquet selected row groups exceed the bounded decode limit".into());
            }
        }
        // RowSelection is relative to the selected row groups, not to the file.
        let target = selected_group_rows + position - group_start;
        let skipped = target - last_selected;
        if skipped != 0 {
            selectors.push(RowSelector::skip(
                usize::try_from(skipped)
                    .map_err(|_| "Parquet row selection exceeds platform size")?,
            ));
        }
        selectors.push(RowSelector::select(1));
        last_selected = target + 1;
    }
    builder
        .with_projection(projection)
        .with_row_groups(selected_groups)
        .with_row_selection(RowSelection::from(selectors))
        .with_batch_size(1)
        .build()
        .map_err(|error| format!("Parquet reader: {error}"))
}

fn next_sample(reader: &mut ParquetRecordBatchReader) -> Result<RecordBatch, String> {
    let batch = reader
        .next()
        .ok_or("Parquet reader returned fewer rows than the selected row count")?
        .map_err(|error| format!("Parquet row: {error}"))?;
    if batch.num_rows() != 1 {
        return Err("Parquet reader returned an unexpected batch size".into());
    }
    Ok(batch)
}

fn row_groups(
    builder: &ParquetRecordBatchReaderBuilder<RangedParquet>,
) -> Result<(u64, Vec<u64>), String> {
    let metadata = builder.metadata();
    if metadata.memory_size() > 4 * MAX_METADATA {
        return Err("decoded Parquet metadata exceeds the memory limit".into());
    }
    let total = u64::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| "Parquet file reports a negative row count")?;
    let mut running = 0_u64;
    let mut groups = Vec::with_capacity(metadata.num_row_groups());
    for group in metadata.row_groups() {
        let rows = u64::try_from(group.num_rows())
            .map_err(|_| "Parquet row group reports a negative row count")?;
        running = running
            .checked_add(rows)
            .ok_or("Parquet row counts overflow")?;
        groups.push(running);
    }
    if running != total {
        return Err("Parquet row groups do not match the file row count".into());
    }
    Ok((total, groups))
}

fn column_reader(
    source: &RangeSource,
    metadata: &ArrowReaderMetadata,
    groups: &[u64],
    positions: &[u64],
    index: usize,
) -> Result<ParquetRecordBatchReader, String> {
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        RangedParquet(source.clone()),
        metadata.clone(),
    );
    selected_reader(builder, source, groups, positions, vec![index])
}

// Oversized chunks are not evidence of absence. Other columns may still be
// inspected; if none qualifies, report that the file remains inconclusive.
fn maybe_column_reader(
    source: &RangeSource,
    metadata: &ArrowReaderMetadata,
    groups: &[u64],
    positions: &[u64],
    index: usize,
    uncheckable: &mut bool,
) -> Result<Option<ParquetRecordBatchReader>, String> {
    match column_reader(source, metadata, groups, positions, index) {
        Ok(reader) => Ok(Some(reader)),
        Err(error) if error.contains("bounded decode limit") => {
            *uncheckable = true;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

pub fn probe(source: &RangeSource, samples: u32) -> Result<FileSample, String> {
    check_footer(source)?;
    let metadata = ArrowReaderMetadata::load(&RangedParquet(source.clone()), Default::default())
        .map_err(|error| format!("Parquet metadata: {error}"))?;
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        RangedParquet(source.clone()),
        metadata.clone(),
    );
    let (total, groups) = row_groups(&builder)?;
    let mut string_columns = selected_string_columns(builder.schema());
    if string_columns.is_empty() || total == 0 {
        return Ok(FileSample {
            found: None,
            sampled_rows: 0,
            wide: false,
        });
    }
    let positions = sample_indices(total, samples)?;
    let mut observed = BTreeSet::new();
    let mut uncheckable = false;

    // Only a literal row label licenses atom-sized values in otherwise generic
    // wide fields. Project that one label field first, not the entire row.
    if string_columns.len() > 1 {
        let first = string_columns[0];
        if let Some(mut reader) = maybe_column_reader(
            source,
            &metadata,
            &groups,
            &positions,
            first,
            &mut uncheckable,
        )? {
            let mut wide_positions = Vec::new();
            for &position in &positions {
                let batch = next_sample(&mut reader)?;
                observed.insert(position);
                if string_cell(batch.column(0).as_ref(), 0)
                    .and_then(|label| WideRow::from_label(label, string_columns[1] > 0))
                    .is_some()
                {
                    wide_positions.push(position);
                }
            }
            drop(reader);
            if !wide_positions.is_empty() {
                for &index in string_columns.iter().skip(1) {
                    let Some(mut reader) = maybe_column_reader(
                        source,
                        &metadata,
                        &groups,
                        &wide_positions,
                        index,
                        &mut uncheckable,
                    )?
                    else {
                        continue;
                    };
                    for &position in &wide_positions {
                        let batch = next_sample(&mut reader)?;
                        if let Some(column) = find_wide_smiles(&batch) {
                            return Ok(FileSample {
                                found: Some((position, column)),
                                sampled_rows: observed.len(),
                                wide: true,
                            });
                        }
                    }
                }
            }
        }
    }

    // Names affect only ordering and confidence in SmilesEvidence. Every
    // top-level string column gets its own bounded projection and evidence
    // history; no structural name is required to qualify.
    string_columns.sort_by_key(|&index| !preferred_column(builder.schema().field(index).name()));
    for index in string_columns {
        let Some(mut reader) = maybe_column_reader(
            source,
            &metadata,
            &groups,
            &positions,
            index,
            &mut uncheckable,
        )?
        else {
            continue;
        };
        let column = builder.schema().field(index).name();
        let mut evidence = SmilesEvidence::new(column);
        for &position in &positions {
            let batch = next_sample(&mut reader)?;
            observed.insert(position);
            if let Some(row) = string_cell(batch.column(0).as_ref(), 0)
                .and_then(|text| evidence.observe(position, text))
            {
                return Ok(FileSample {
                    found: Some((row, column.to_owned())),
                    sampled_rows: observed.len(),
                    wide: false,
                });
            }
        }
    }
    if uncheckable {
        return Err("Parquet selected row groups exceed the bounded decode limit; string fields remain uninspected".into());
    }
    Ok(FileSample {
        found: None,
        sampled_rows: observed.len(),
        wide: false,
    })
}

/// Verify a pinpointed string column without projecting unrelated fields.
pub fn sniff_column(source: &RangeSource, column: &str, found_row: u64) -> Result<bool, String> {
    check_footer(source)?;
    let metadata = ArrowReaderMetadata::load(&RangedParquet(source.clone()), Default::default())
        .map_err(|error| format!("Parquet metadata: {error}"))?;
    let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        RangedParquet(source.clone()),
        metadata.clone(),
    );
    let (total, groups) = row_groups(&builder)?;
    if found_row >= total {
        return Err(format!("Parquet row {found_row} lies outside {total} rows"));
    }
    let index = builder
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == column && is_string_type(field.data_type()))
        .ok_or_else(|| format!("Parquet string column '{column}' does not exist"))?;
    let mut reader = column_reader(source, &metadata, &groups, &[found_row], index)?;
    let pinned = next_sample(&mut reader)?;
    let text = string_cell(pinned.column(0).as_ref(), 0);
    let mut evidence = SmilesEvidence::new(column);
    if text
        .and_then(|text| evidence.observe(found_row, text))
        .is_some()
    {
        return Ok(true);
    }
    let string_columns = selected_string_columns(builder.schema());
    if text.is_some_and(WideRow::parseable)
        && string_columns.first().is_some_and(|&first| first != index)
    {
        let mut label_reader =
            column_reader(source, &metadata, &groups, &[found_row], string_columns[0])?;
        let label = next_sample(&mut label_reader)?;
        if string_cell(label.column(0).as_ref(), 0)
            .and_then(|value| WideRow::from_label(value, index > 0))
            .is_some()
        {
            return Ok(true);
        }
    }
    // Gather independent content evidence from a bounded neighborhood and
    // the first rows without downloading unrelated columns.
    let mut positions = BTreeSet::new();
    for row in 0..total.min(32) {
        if row != found_row {
            positions.insert(row);
        }
    }
    for row in found_row.saturating_sub(16)..total.min(found_row.saturating_add(17)) {
        if row != found_row {
            positions.insert(row);
        }
    }
    if positions.is_empty() {
        return Ok(false);
    }
    let positions: Vec<_> = positions.into_iter().collect();
    let mut reader = column_reader(source, &metadata, &groups, &positions, index)?;
    for position in positions {
        let batch = next_sample(&mut reader)?;
        if string_cell(batch.column(0).as_ref(), 0)
            .and_then(|text| evidence.observe(position, text))
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, DictionaryArray, Int32Array, StructArray};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;

    fn fixture(columns: Vec<(&str, ArrayRef)>, group_size: usize) -> Vec<u8> {
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let mut bytes = Vec::new();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(group_size))
            .build();
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    #[test]
    fn first_row_and_only_string_columns() {
        let data = fixture(
            vec![
                ("integer", Arc::new(Int32Array::from(vec![123, 456]))),
                (
                    "smiles",
                    Arc::new(StringArray::from(vec!["CCO", "invalid smiles!"])),
                ),
            ],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 1).unwrap();
        assert_eq!(sample.found, Some((0, "smiles".into())));
        assert_eq!(sample.sampled_rows, 1);
    }

    #[test]
    fn later_row_group_and_null_values() {
        let data = fixture(
            vec![(
                "structure",
                Arc::new(StringArray::from(vec![None, Some("N#N")])),
            )],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 1).unwrap();
        assert_eq!(sample.found, Some((1, "structure".into())));
        assert_eq!(sample.sampled_rows, 2);
    }

    #[test]
    fn nested_columns_before_string_fields_do_not_shift_column_bounds() {
        let nested = StructArray::from(vec![
            (
                Arc::new(arrow_schema::Field::new(
                    "image_text",
                    DataType::Utf8,
                    false,
                )),
                Arc::new(StringArray::from(vec!["C"])) as ArrayRef,
            ),
            (
                Arc::new(arrow_schema::Field::new("image_id", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
            ),
        ]);
        let data = fixture(
            vec![
                ("image", Arc::new(nested)),
                ("smiles", Arc::new(StringArray::from(vec!["N#N"]))),
            ],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 0).unwrap();
        assert_eq!(sample.found, Some((0, "smiles".into())));
    }

    #[test]
    fn dictionary_encoded_string_column_is_sampled() {
        let dictionary: DictionaryArray<Int8Type> = vec!["CCO", "CCO"].into_iter().collect();
        let data = fixture(vec![("smiles", Arc::new(dictionary))], 2);
        let sample = probe(&RangeSource::from_bytes(&data), 0).unwrap();
        assert_eq!(sample.found, Some((0, "smiles".into())));
    }

    #[test]
    fn numeric_only_files_do_not_parse_or_fetch_column_values() {
        let data = fixture(vec![("number", Arc::new(Int32Array::from(vec![123])))], 1);
        let sample = probe(&RangeSource::from_bytes(&data), 0).unwrap();
        assert!(sample.found.is_none());
        assert_eq!(sample.sampled_rows, 0);
    }

    #[test]
    fn rejects_invalid_and_oversized_cells_without_skipping_other_columns() {
        let giant = "C".repeat(4097);
        let data = fixture(
            vec![
                (
                    "canonical_smiles",
                    Arc::new(StringArray::from(vec![giant.as_str()])),
                ),
                ("structure", Arc::new(StringArray::from(vec!["O"]))),
            ],
            1,
        );
        assert_eq!(
            probe(&RangeSource::from_bytes(&data), 0).unwrap().found,
            Some((0, "structure".into()))
        );
        let data = fixture(
            vec![("text", Arc::new(StringArray::from(vec!["not smiles!"])))],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 0).unwrap();
        assert!(sample.found.is_none());
        assert_eq!(sample.sampled_rows, 1);
    }

    #[test]
    fn category_codes_are_not_structure_evidence() {
        let data = fixture(
            vec![
                ("sex", Arc::new(StringArray::from(vec!["F", "M"]))),
                ("type", Arc::new(StringArray::from(vec!["C", "O"]))),
            ],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 1).unwrap();
        assert_eq!(sample.found, None);
        assert_eq!(sample.sampled_rows, 2);
        assert!(!sample.wide);
    }

    #[test]
    fn generic_molecules_in_sex_are_found_without_a_structure_header() {
        let data = fixture(
            vec![
                ("sex", Arc::new(StringArray::from(vec!["CCO", "CCN"]))),
                ("type", Arc::new(StringArray::from(vec!["II", "III"]))),
            ],
            1,
        );
        let source = RangeSource::from_bytes(&data);
        let sample = probe(&source, 0).unwrap();
        assert_eq!(sample.found, Some((0, "sex".into())));
        assert_eq!(sample.sampled_rows, 2);
        assert!(!sample.wide);
        assert!(sniff_column(&source, "sex", 0).unwrap());
        assert!(!sniff_column(&source, "type", 0).unwrap());

        let data = fixture(vec![("sex", Arc::new(StringArray::from(vec!["N#N"])))], 1);
        let source = RangeSource::from_bytes(&data);
        assert_eq!(probe(&source, 0).unwrap().found, Some((0, "sex".into())));
        assert!(sniff_column(&source, "sex", 0).unwrap());
    }

    #[test]
    fn pinned_column_uses_content_and_wide_label_without_name_filtering() {
        let data = fixture(
            vec![
                ("kind", Arc::new(StringArray::from(vec!["SMILES", "type"]))),
                ("sex", Arc::new(StringArray::from(vec!["F", "C"]))),
            ],
            1,
        );
        let source = RangeSource::from_bytes(&data);
        assert_eq!(probe(&source, 1).unwrap().found, Some((0, "sex".into())));
        assert!(sniff_column(&source, "sex", 0).unwrap());
        assert!(!sniff_column(&source, "sex", 1).unwrap());
    }

    #[test]
    fn named_structure_field_accepts_single_atom() {
        let data = fixture(
            vec![
                ("sex", Arc::new(StringArray::from(vec!["F"]))),
                ("canonical_smiles", Arc::new(StringArray::from(vec!["C"]))),
            ],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 0).unwrap();
        assert_eq!(sample.found, Some((0, "canonical_smiles".into())));
        assert!(!sample.wide);
    }

    #[test]
    fn wide_row_requires_explicit_label_in_first_string_field() {
        let data = fixture(
            vec![
                ("id", Arc::new(Int32Array::from(vec![10, 11]))),
                ("kind", Arc::new(StringArray::from(vec!["type", "SMILES"]))),
                ("a", Arc::new(StringArray::from(vec!["F", "invalid!"]))),
                ("b", Arc::new(StringArray::from(vec!["C", "O"]))),
            ],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 1).unwrap();
        assert_eq!(sample.found, Some((1, "b".into())));
        assert_eq!(sample.sampled_rows, 2);
        assert!(sample.wide);
        let data = fixture(
            vec![
                ("kind", Arc::new(StringArray::from(vec!["SMILES", "type"]))),
                ("value", Arc::new(StringArray::from(vec!["invalid!", "F"]))),
            ],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 1).unwrap();
        assert_eq!(sample.found, None);
        assert_eq!(sample.sampled_rows, 2);
        assert!(!sample.wide);
    }

    #[test]
    fn wide_row_survives_an_unmatched_named_structure_column() {
        let data = fixture(
            vec![
                ("kind", Arc::new(StringArray::from(vec!["SMILES"]))),
                ("smiles", Arc::new(StringArray::from(vec!["invalid!"]))),
                ("sample_a", Arc::new(StringArray::from(vec!["CCO"]))),
            ],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 0).unwrap();
        assert_eq!(sample.found, Some((0, "sample_a".into())));
        assert!(sample.wide);
        assert_eq!(sample.sampled_rows, 1);
    }

    #[test]
    fn named_projection_skips_huge_unrelated_text() {
        let huge = "x".repeat(MAX_SELECTED_COLUMN_BYTES as usize + 1);
        let data = fixture(
            vec![
                ("notes", Arc::new(StringArray::from(vec![huge.as_str()]))),
                ("smiles", Arc::new(StringArray::from(vec!["F"]))),
            ],
            1,
        );
        let sample = probe(&RangeSource::from_bytes(&data), 0).unwrap();
        assert_eq!(sample.found, Some((0, "smiles".into())));
        assert!(!sample.wide);
    }

    #[test]
    fn oversized_unrelated_prose_does_not_hide_generic_chemical_content() {
        let huge = "x".repeat(MAX_SELECTED_COLUMN_BYTES as usize + 1);
        let data = fixture(
            vec![
                ("notes", Arc::new(StringArray::from(vec![huge.as_str()]))),
                ("sex", Arc::new(StringArray::from(vec!["N#N"]))),
            ],
            1,
        );
        let source = RangeSource::from_bytes(&data);
        assert_eq!(probe(&source, 0).unwrap().found, Some((0, "sex".into())));
        assert!(sniff_column(&source, "sex", 0).unwrap());

        let data = fixture(
            vec![
                ("notes", Arc::new(StringArray::from(vec![huge.as_str()]))),
                ("sex", Arc::new(StringArray::from(vec!["F"]))),
            ],
            1,
        );
        let error = probe(&RangeSource::from_bytes(&data), 0).err().unwrap();
        assert!(error.contains("bounded decode limit"), "{error}");
    }

    #[test]
    fn oversized_row_group_is_inconclusive_not_a_negative_sample() {
        let huge = "C".repeat(MAX_SELECTED_COLUMN_BYTES as usize + 1);
        let data = fixture(
            vec![("smiles", Arc::new(StringArray::from(vec![huge.as_str()])))],
            1,
        );
        let error = probe(&RangeSource::from_bytes(&data), 0).err().unwrap();
        assert!(error.contains("bounded decode limit"), "{error}");
    }

    #[test]
    fn oversized_footer_and_truncated_file_are_errors() {
        let mut data = fixture(vec![("smiles", Arc::new(StringArray::from(vec!["C"])))], 1);
        let footer_offset = data.len() - 8;
        data[footer_offset..footer_offset + 4]
            .copy_from_slice(&((MAX_METADATA + 1) as u32).to_le_bytes());
        assert!(probe(&RangeSource::from_bytes(&data), 0).is_err());
        assert!(probe(&RangeSource::from_bytes(b"PAR1"), 0).is_err());
    }
}
