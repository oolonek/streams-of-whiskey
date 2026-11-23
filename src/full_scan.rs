use smiles_rs::prelude::Smiles;

// Cells above this size are reported, never silently treated as invalid SMILES.
pub const MAX_CELL_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub enum Axis {
    Auto,
    Column,
    Row,
}

impl Axis {
    pub fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Column => "column",
            Self::Row => "row",
        }
    }
}

#[derive(Debug)]
pub struct ScanCounts {
    pub axis: Axis,
    pub rows_scanned: u64,
    pub cells_scanned: u64,
    pub valid: u64,
    pub invalid: u64,
    pub empty: u64,
    pub non_string: u64,
    pub oversized: u64,
}

impl ScanCounts {
    pub fn new(axis: Axis) -> Self {
        Self {
            axis,
            rows_scanned: 0,
            cells_scanned: 0,
            valid: 0,
            invalid: 0,
            empty: 0,
            non_string: 0,
            oversized: 0,
        }
    }

    #[cfg(test)]
    pub fn check(&mut self, cell: Option<&str>) {
        self.classify(cell);
    }

    /// Classify once and forward only validated, unmodified strings to a sink.
    pub fn check_with_observer(
        &mut self,
        cell: Option<&str>,
        observer: &dyn ScanObserver,
    ) -> Result<(), String> {
        if self.classify(cell) {
            observer.valid_smiles(cell.expect("validated cell is a string"))?;
        }
        Ok(())
    }

    fn classify(&mut self, cell: Option<&str>) -> bool {
        self.cells_scanned += 1;
        match cell {
            None | Some("") => self.empty += 1,
            Some(text) if text.len() > MAX_CELL_BYTES => self.oversized += 1,
            Some(text) if Smiles::from_str(text).is_ok() => {
                self.valid += 1;
                return true;
            }
            Some(_) => self.invalid += 1,
        }
        false
    }

    pub fn non_string(&mut self) {
        self.cells_scanned += 1;
        self.non_string += 1;
    }
}

/// Periodic scan snapshots; text scans report bytes independently via the
/// response reader, while Parquet scans supply the projected row total.
pub trait ScanObserver {
    fn progress(&self, counts: &ScanCounts, total_rows: Option<u64>);
    /// Called once per validated cell, including duplicates.
    fn valid_smiles(&self, _smiles: &str) -> Result<(), String> {
        Ok(())
    }
    /// Discard candidates from a column prefix when a wide row changes scan scope.
    fn reset_smiles(&self) -> Result<(), String> {
        Ok(())
    }
}
