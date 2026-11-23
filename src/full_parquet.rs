use std::io::{self, Read};

use arrow_schema::DataType;
use bytes::{Buf, Bytes, BytesMut};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

use crate::direct_probe::RangeSource;
use crate::full_scan::{Axis, ScanCounts, ScanObserver};
use crate::parquet_probe::{is_string_type, string_cell};
use crate::smiles_evidence::WideRow;

const MAX_METADATA: usize = 8 * 1024 * 1024;
const MAX_CHUNK: usize = 16 * 1024 * 1024;
const HTTP_RANGE: usize = 2 * 1024 * 1024;
const STREAM_BUFFER: usize = 64 * 1024;
const BATCH_SIZE: usize = 1024;

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
            let length = (self.source.size - self.offset).min(STREAM_BUFFER as u64) as usize;
            self.buffered = self
                .source
                .read(self.offset, length)
                .map_err(io::Error::other)?;
        }
        let length = output.len().min(self.buffered.len());
        output[..length].copy_from_slice(&self.buffered[..length]);
        self.buffered.advance(length);
        self.offset += length as u64;
        Ok(length)
    }
}

impl ChunkReader for RangedParquet {
    type T = RangedStream;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.0.size {
            return Err(ParquetError::General(
                "Parquet stream starts beyond file".into(),
            ));
        }
        Ok(RangedStream {
            source: self.0.clone(),
            offset: start,
            buffered: Bytes::new(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        if length > MAX_CHUNK
            || start
                .checked_add(length as u64)
                .is_none_or(|end| end > self.0.size)
        {
            return Err(ParquetError::General(
                "Parquet page or metadata exceeds bounded file range".into(),
            ));
        }
        if length <= HTTP_RANGE {
            return self.0.read(start, length).map_err(ParquetError::General);
        }
        let mut bytes = BytesMut::with_capacity(length);
        for offset in (0..length).step_by(HTTP_RANGE) {
            let amount = (length - offset).min(HTTP_RANGE);
            bytes.extend_from_slice(
                &self
                    .0
                    .read(start + offset as u64, amount)
                    .map_err(ParquetError::General)?,
            );
        }
        Ok(bytes.freeze())
    }
}

fn validate_footer(source: &RangeSource) -> Result<(), String> {
    if source.size < 12 {
        return Err("Parquet file is too short for a footer".into());
    }
    let footer = source.read(source.size - 8, 8)?;
    if &footer[4..] != b"PAR1" {
        return Err("Parquet footer has an invalid magic value".into());
    }
    let length = u32::from_le_bytes(footer[..4].try_into().unwrap()) as usize;
    if length > MAX_METADATA || length as u64 > source.size - 8 {
        return Err("Parquet footer metadata exceeds the bounded metadata limit".into());
    }
    Ok(())
}

fn is_scalar(data_type: &DataType) -> bool {
    is_string_type(data_type)
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Duration(_)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
}

fn check_chunks(
    source: &RangeSource,
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema: &parquet::schema::types::SchemaDescriptor,
    roots: &[usize],
    groups: &[usize],
) -> Result<(), String> {
    let projection = ProjectionMask::roots(schema, roots.iter().copied());
    let leaves: Vec<_> = (0..schema.num_columns())
        .filter(|&i| projection.leaf_included(i))
        .collect();
    if leaves.is_empty() {
        return Err("Parquet selected fields have no physical columns".into());
    }
    for &index in groups {
        let group = metadata.row_group(index);
        for &leaf in &leaves {
            let column = group.column(leaf);
            let size = u64::try_from(column.compressed_size())
                .map_err(|_| "Parquet column has a negative compressed size")?;
            u64::try_from(column.uncompressed_size())
                .map_err(|_| "Parquet column has a negative decoded size")?;
            let data_offset = u64::try_from(column.data_page_offset()).map_err(|_| {
                format!("Parquet row group {index} column {leaf} has a negative data offset")
            })?;
            let offset = u64::try_from(
                column
                    .dictionary_page_offset()
                    .unwrap_or(column.data_page_offset()),
            )
            .map_err(|_| {
                format!("Parquet row group {index} column {leaf} has a negative dictionary offset")
            })?;
            if data_offset > source.size
                || offset.checked_add(size).is_none_or(|end| end > source.size)
            {
                return Err(format!(
                    "Parquet row group {index} column {leaf} points beyond the file"
                ));
            }
        }
    }
    Ok(())
}

pub fn scan_with_progress(
    source: &RangeSource,
    column: &str,
    found_row: u64,
    axis: Axis,
    observer: &dyn ScanObserver,
) -> Result<ScanCounts, String> {
    validate_footer(source)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(RangedParquet(source.clone()))
        .map_err(|error| format!("Parquet metadata: {error}"))?;
    let metadata = builder.metadata();
    if metadata.memory_size() > 4 * MAX_METADATA {
        return Err("decoded Parquet metadata exceeds the memory limit".into());
    }
    let total = u64::try_from(metadata.file_metadata().num_rows())
        .map_err(|_| "Parquet file reports a negative row count")?;
    if found_row >= total {
        return Err(format!("Parquet row {found_row} lies outside {total} rows"));
    }
    let schema = builder.schema().clone();
    let fields = schema.fields();
    let mut matching = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() == column);
    let target = matching
        .next()
        .map(|(index, _)| index)
        .ok_or_else(|| format!("Parquet column '{column}' does not exist"))?;
    if matching.next().is_some() {
        return Err(format!("Parquet column '{column}' is ambiguous"));
    }
    if !is_string_type(fields[target].data_type()) {
        return Err(format!(
            "Parquet column '{column}' is not a top-level string field"
        ));
    }
    let mut group_start = 0_u64;
    let mut selected_group = None;
    for (index, group) in metadata.row_groups().iter().enumerate() {
        let rows = u64::try_from(group.num_rows())
            .map_err(|_| "Parquet row group has a negative row count")?;
        let end = group_start
            .checked_add(rows)
            .ok_or("Parquet row counts overflow")?;
        if selected_group.is_none() && found_row < end {
            selected_group = Some((index, found_row - group_start));
        }
        group_start = end;
    }
    if group_start != total {
        return Err("Parquet row groups do not match the file row count".into());
    }
    let (group_index, local_row) = selected_group.ok_or("Parquet selected row group is missing")?;

    let first_string = fields
        .iter()
        .position(|field| is_string_type(field.data_type()));
    let mut wide = false;
    if axis != Axis::Column && !(axis == Axis::Auto && WideRow::from_label(column, true).is_some())
    {
        if let Some(first) = first_string {
            if first != target {
                check_chunks(
                    source,
                    metadata,
                    builder.parquet_schema(),
                    &[first],
                    &[group_index],
                )?;
                let probe = ParquetRecordBatchReaderBuilder::try_new(RangedParquet(source.clone()))
                    .map_err(|error| format!("Parquet metadata: {error}"))?;
                let mut reader = probe
                    .with_projection(ProjectionMask::roots(builder.parquet_schema(), [first]))
                    .with_row_groups(vec![group_index])
                    .with_row_selection(RowSelection::from(vec![
                        RowSelector::skip(
                            usize::try_from(local_row)
                                .map_err(|_| "Parquet row offset exceeds platform size")?,
                        ),
                        RowSelector::select(1),
                    ]))
                    .with_batch_size(1)
                    .build()
                    .map_err(|error| format!("Parquet label reader: {error}"))?;
                let batch = reader
                    .next()
                    .ok_or("Parquet label row is missing")?
                    .map_err(|error| format!("Parquet label row: {error}"))?;
                if batch.num_rows() != 1 {
                    return Err("Parquet label reader returned an unexpected row count".into());
                }
                wide = string_cell(batch.column(0).as_ref(), 0)
                    .and_then(|label| WideRow::from_label(label, target > 0))
                    .is_some();
            }
        }
    }
    let chosen = match axis {
        Axis::Auto if wide => Axis::Row,
        Axis::Auto => Axis::Column,
        other => other,
    };
    let mut counts = ScanCounts::new(chosen);
    let progress_total = Some(if chosen == Axis::Column { total } else { 1 });
    observer.progress(&counts, progress_total);
    let roots: Vec<usize> = if chosen == Axis::Column {
        vec![target]
    } else {
        fields
            .iter()
            .enumerate()
            .filter_map(|(index, field)| is_scalar(field.data_type()).then_some(index))
            .collect()
    };
    let groups: Vec<usize> = if chosen == Axis::Column {
        (0..metadata.num_row_groups()).collect()
    } else {
        vec![group_index]
    };
    check_chunks(source, metadata, builder.parquet_schema(), &roots, &groups)?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
    let mut reader_builder = builder
        .with_projection(projection)
        .with_batch_size(BATCH_SIZE);
    if chosen == Axis::Row {
        reader_builder = reader_builder
            .with_row_groups(groups)
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(
                    usize::try_from(local_row)
                        .map_err(|_| "Parquet row offset exceeds platform size")?,
                ),
                RowSelector::select(1),
            ]))
            .with_batch_size(1);
    }
    let mut reader = reader_builder
        .build()
        .map_err(|error| format!("Parquet reader: {error}"))?;
    for batch in &mut reader {
        let batch = batch.map_err(|error| format!("Parquet data: {error}"))?;
        counts.rows_scanned += batch.num_rows() as u64;
        for (offset, &index) in roots.iter().enumerate() {
            // A wide-table label identifies the row; it is not a candidate molecule.
            if chosen == Axis::Row && wide && first_string == Some(index) {
                continue;
            }
            let array = batch.column(offset).as_ref();
            for row in 0..batch.num_rows() {
                if is_string_type(fields[index].data_type()) {
                    let cell = string_cell(array, row);
                    if !array.is_null(row) && cell.is_none() {
                        return Err(format!(
                            "Parquet string field '{}' could not be decoded",
                            fields[index].name()
                        ));
                    }
                    counts.check_with_observer(cell, observer)?;
                } else {
                    counts.non_string();
                }
            }
        }
        observer.progress(&counts, progress_total);
    }
    if (chosen == Axis::Column && counts.rows_scanned != total)
        || (chosen == Axis::Row && counts.rows_scanned != 1)
    {
        return Err(format!(
            "Parquet reader yielded {} rows, expected {}",
            counts.rows_scanned,
            if chosen == Axis::Column { total } else { 1 }
        ));
    }
    if chosen == Axis::Row {
        counts.rows_scanned = found_row + 1;
        observer.progress(&counts, progress_total);
    }
    Ok(counts)
}

#[cfg(test)]
fn scan(
    source: &RangeSource,
    column: &str,
    found_row: u64,
    axis: Axis,
) -> Result<ScanCounts, String> {
    struct Noop;
    impl ScanObserver for Noop {
        fn progress(&self, _: &ScanCounts, _: Option<u64>) {}
    }
    scan_with_progress(source, column, found_row, axis, &Noop)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::types::Int8Type;
    use arrow_array::{ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;

    #[derive(Default)]
    struct Recording(std::cell::RefCell<Vec<(Axis, u64, u64, u64, u64, Option<u64>)>>);

    impl ScanObserver for Recording {
        fn progress(&self, counts: &ScanCounts, total_rows: Option<u64>) {
            self.0.borrow_mut().push((
                counts.axis,
                counts.rows_scanned,
                counts.cells_scanned,
                counts.valid,
                counts.invalid,
                total_rows,
            ));
        }
    }

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
    fn scans_all_groups_including_empty_and_null_cells() {
        let data = fixture(
            vec![
                (
                    "smiles",
                    Arc::new(StringArray::from(vec![
                        Some("CCO"),
                        None,
                        Some("bad smiles!"),
                        Some(""),
                        Some("N#N"),
                    ])),
                ),
                ("unrelated", Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))),
            ],
            2,
        );
        let progress = Recording::default();
        let counts = scan_with_progress(
            &RangeSource::from_bytes(&data),
            "smiles",
            3,
            Axis::Auto,
            &progress,
        )
        .unwrap();
        assert_eq!(counts.axis, Axis::Column);
        assert_eq!((counts.rows_scanned, counts.cells_scanned), (5, 5));
        assert_eq!((counts.valid, counts.invalid, counts.empty), (2, 1, 2));
        assert_eq!(
            *progress.0.borrow(),
            [
                (Axis::Column, 0, 0, 0, 0, Some(5)),
                (Axis::Column, 5, 5, 2, 1, Some(5)),
            ]
        );
    }

    #[test]
    fn parquet_streams_decoded_strings_without_normalizing_or_duplicate_filtering() {
        #[derive(Default)]
        struct Collected(std::cell::RefCell<Vec<String>>);
        impl ScanObserver for Collected {
            fn progress(&self, _: &ScanCounts, _: Option<u64>) {}
            fn valid_smiles(&self, smiles: &str) -> Result<(), String> {
                self.0.borrow_mut().push(smiles.to_owned());
                Ok(())
            }
        }
        let data = fixture(
            vec![(
                "smiles",
                Arc::new(StringArray::from(vec![
                    "CCO",
                    "C(C)O",
                    "CCO",
                    "bad smiles!",
                ])),
            )],
            2,
        );
        let observer = Collected::default();
        scan_with_progress(
            &RangeSource::from_bytes(&data),
            "smiles",
            0,
            Axis::Column,
            &observer,
        )
        .unwrap();
        assert_eq!(*observer.0.borrow(), ["CCO", "C(C)O", "CCO"]);
    }

    #[test]
    fn reports_valid_count_before_the_end_of_a_large_parquet_column() {
        let rows = BATCH_SIZE + 1;
        let data = fixture(
            vec![("smiles", Arc::new(StringArray::from(vec!["CCO"; rows])))],
            BATCH_SIZE,
        );
        let progress = Recording::default();
        let counts = scan_with_progress(
            &RangeSource::from_bytes(&data),
            "smiles",
            0,
            Axis::Column,
            &progress,
        )
        .unwrap();
        assert_eq!(counts.valid, rows as u64);
        let snapshots = progress.0.borrow();
        assert_eq!(snapshots.first().unwrap().1, 0);
        assert!(snapshots.iter().any(|snapshot| {
            snapshot.1 > 0
                && snapshot.1 < rows as u64
                && snapshot.3 == snapshot.1
                && snapshot.5 == Some(rows as u64)
        }));
        assert_eq!(snapshots.last().unwrap().3, rows as u64);
    }

    #[test]
    fn scans_dictionary_encoded_strings() {
        let dictionary: DictionaryArray<Int8Type> = vec![Some("C"), None, Some("not smiles!")]
            .into_iter()
            .collect();
        let data = fixture(vec![("molecules", Arc::new(dictionary))], 1);
        let counts = scan(
            &RangeSource::from_bytes(&data),
            "molecules",
            0,
            Axis::Column,
        )
        .unwrap();
        assert_eq!(
            (
                counts.valid,
                counts.invalid,
                counts.empty,
                counts.rows_scanned
            ),
            (1, 1, 1, 3)
        );
    }

    #[test]
    fn wide_row_at_later_group_skips_label_and_other_rows() {
        let data = fixture(
            vec![
                (
                    "kind",
                    Arc::new(StringArray::from(vec!["other", "SMILES", "other"])),
                ),
                ("hit", Arc::new(StringArray::from(vec!["O", "CCO", "N"]))),
                (
                    "other",
                    Arc::new(StringArray::from(vec!["O", "bad smiles!", "N"])),
                ),
                ("integer", Arc::new(Int32Array::from(vec![1, 2, 3]))),
            ],
            1,
        );
        let progress = Recording::default();
        let counts = scan_with_progress(
            &RangeSource::from_bytes(&data),
            "hit",
            1,
            Axis::Auto,
            &progress,
        )
        .unwrap();
        assert_eq!(counts.axis, Axis::Row);
        assert_eq!((counts.rows_scanned, counts.cells_scanned), (2, 3));
        assert_eq!((counts.valid, counts.invalid, counts.non_string), (1, 1, 1));
        assert_eq!(
            *progress.0.borrow(),
            [
                (Axis::Row, 0, 0, 0, 0, Some(1)),
                (Axis::Row, 1, 3, 1, 1, Some(1)),
                (Axis::Row, 2, 3, 1, 1, Some(1)),
            ]
        );
        let column = scan(&RangeSource::from_bytes(&data), "hit", 1, Axis::Column).unwrap();
        assert_eq!(
            (column.axis, column.rows_scanned, column.cells_scanned),
            (Axis::Column, 3, 3)
        );
    }

    #[test]
    fn missing_column_row_and_malformed_metadata_fail() {
        let mut data = fixture(vec![("smiles", Arc::new(StringArray::from(vec!["C"])))], 1);
        let source = RangeSource::from_bytes(&data);
        assert!(
            scan(&source, "missing", 0, Axis::Column)
                .unwrap_err()
                .contains("does not exist")
        );
        assert!(
            scan(&source, "smiles", 1, Axis::Column)
                .unwrap_err()
                .contains("outside")
        );
        let offset = data.len() - 8;
        data[offset..offset + 4].copy_from_slice(&((MAX_METADATA + 1) as u32).to_le_bytes());
        assert!(scan(&RangeSource::from_bytes(&data), "smiles", 0, Axis::Column).is_err());
        assert!(scan(&RangeSource::from_bytes(b"PAR1"), "smiles", 0, Axis::Column).is_err());
    }
}
