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

    pub fn check(&mut self, cell: Option<&str>) {
        self.cells_scanned += 1;
        match cell {
            None | Some("") => self.empty += 1,
            Some(text) if text.len() > MAX_CELL_BYTES => self.oversized += 1,
            Some(text) if Smiles::from_str(text).is_ok() => self.valid += 1,
            Some(_) => self.invalid += 1,
        }
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
}

/// A structure-like header is supporting evidence, never a column filter:
/// unnamed columns can still qualify through their parsed molecular content.
pub fn is_structure_column(name: &str) -> bool {
    let mut tokens = name
        .rsplit(|c: char| !c.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty());
    let suffix = tokens.next().unwrap_or("");
    let base = if !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        tokens.next().unwrap_or(suffix)
    } else {
        suffix
    };
    if ["id", "name", "description", "text"]
        .iter()
        .any(|word| suffix.eq_ignore_ascii_case(word))
    {
        return false;
    }
    name.as_bytes()
        .windows(6)
        .any(|part| part.eq_ignore_ascii_case(b"smiles"))
        || ["smi", "smile", "mol", "molecule", "structure"]
            .iter()
            .any(|word| base.eq_ignore_ascii_case(word))
}
/// Inspect a bounded sample of one column without equating single atom codes
/// such as `F` with evidence that the column stores molecular structures.
pub struct SmilesSniffer {
    named: bool,
    first_complex: Option<(u64, String)>,
}

impl SmilesSniffer {
    pub fn new(column: &str) -> Self {
        Self {
            named: is_structure_column(column),
            first_complex: None,
        }
    }

    /// Return the pinpointed row once the sample contains sufficient evidence.
    /// Labeled fields allow valid single atoms. Otherwise, explicit SMILES
    /// syntax suffices, or two distinct multi-atom, multi-element molecules.
    pub fn observe(&mut self, row: u64, text: &str) -> Option<u64> {
        if text.is_empty() || text.len() > 4096 {
            return None;
        }
        let molecule = Smiles::from_str(text).ok()?;
        if self.named {
            return Some(row);
        }
        let atoms = molecule.nodes();
        if atoms.len() > 1
            && text.bytes().any(|byte| {
                matches!(
                    byte,
                    b'=' | b'#' | b'[' | b']' | b'(' | b')' | b'@' | b'/' | b'\\'
                ) || byte.is_ascii_digit()
            })
        {
            return Some(row);
        }
        if atoms.len() < 3 {
            return None;
        }
        let first_element = atoms[0].element();
        if !atoms.iter().any(|atom| atom.element() != first_element) {
            return None;
        }
        match &self.first_complex {
            Some((first_row, first)) if first != text => Some(*first_row),
            Some(_) => None,
            None => {
                self.first_complex = Some((row, text.to_owned()));
                None
            }
        }
    }
}

/// A row named SMILES/structure in its first field denotes a wide table of
/// molecules across columns. Otherwise the pinpointed column is inspected.
pub fn is_wide_label(first: &str, selected_column: usize) -> bool {
    selected_column > 0
        && ["smiles", "canonical_smiles", "isomeric_smiles", "structure"]
            .iter()
            .any(|name| first.trim().eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn molecule_fields_accept_variants_but_reject_labels_and_prose() {
        for column in [
            "SMILES",
            "canonical_smiles",
            "isomericSmiles",
            "ligand_smi",
            "structure_1",
            "mol",
            "molecule",
        ] {
            assert!(is_structure_column(column), "{column}");
        }
        for column in [
            "sex",
            "type",
            "Part",
            "text",
            "molecule_name",
            "smiles_id",
            "smiles_description",
            "",
        ] {
            assert!(!is_structure_column(column), "{column}");
        }
    }

    #[test]
    fn sniffer_uses_molecular_content_even_in_a_sex_column() {
        let mut generic = SmilesSniffer::new("sex");
        for (row, value) in ["F", "M", "C", "II", "III", "a long ordinary sentence"]
            .into_iter()
            .enumerate()
        {
            assert_eq!(generic.observe(row as u64, value), None);
        }
        assert_eq!(generic.observe(6, "CCO"), None);
        assert_eq!(generic.observe(7, "CCN"), Some(6));
        let mut generic = SmilesSniffer::new("sex");
        assert_eq!(generic.observe(0, "N#N"), Some(0));
        let mut named = SmilesSniffer::new("SMILES");
        assert_eq!(named.observe(0, "F"), Some(0));
    }
}
