use smiles_rs::prelude::Smiles;

const MAX_EVIDENCE_BYTES: usize = 4096;

/// Confidence in a sampled field; a parseable category code alone is not evidence.
pub struct SmilesEvidence {
    named: bool,
    first_complex: Option<(u64, String)>,
}

impl SmilesEvidence {
    pub fn new(column: &str) -> Self {
        Self {
            named: preferred_column(column),
            first_complex: None,
        }
    }

    /// Return the pinpointed row once the sample contains sufficient evidence.
    /// Labeled fields allow valid single atoms. Otherwise, explicit SMILES
    /// syntax suffices, or two distinct multi-atom, multi-element molecules.
    pub fn observe(&mut self, row: u64, text: &str) -> Option<u64> {
        if text.is_empty() || text.len() > MAX_EVIDENCE_BYTES {
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

/// A recognized first-field label for a wide table. Format readers identify
/// the first field; only this module decides what its label means.
#[derive(Clone, Copy)]
pub struct WideRow;

impl WideRow {
    pub fn from_label(label: &str, value_field: bool) -> Option<Self> {
        (value_field
            && ["smiles", "canonical_smiles", "isomeric_smiles", "structure"]
                .iter()
                .any(|name| label.trim().eq_ignore_ascii_case(name)))
        .then_some(Self)
    }

    pub fn accepts(self, text: &str) -> bool {
        Self::parseable(text)
    }

    /// Check a candidate before fetching its wide-row label.
    pub fn parseable(text: &str) -> bool {
        !text.is_empty() && text.len() <= MAX_EVIDENCE_BYTES && Smiles::from_str(text).is_ok()
    }
}

/// Names influence confidence and Parquet probe order; they never filter fields.
pub fn preferred_column(name: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_fields_accept_single_atoms_but_generic_fields_need_content_evidence() {
        for column in [
            "SMILES",
            "canonical_smiles",
            "isomericSmiles",
            "ligand_smi",
            "structure_1",
            "mol",
            "molecule",
        ] {
            assert_eq!(
                SmilesEvidence::new(column).observe(0, "F"),
                Some(0),
                "{column}"
            );
        }
        for column in [
            "sex",
            "type",
            "text",
            "molecule_name",
            "smiles_id",
            "smiles_description",
            "",
        ] {
            assert_eq!(
                SmilesEvidence::new(column).observe(0, "F"),
                None,
                "{column}"
            );
        }
        let mut generic = SmilesEvidence::new("sex");
        assert_eq!(generic.observe(0, "CCO"), None);
        assert_eq!(generic.observe(1, "CCN"), Some(0));
        assert_eq!(SmilesEvidence::new("type").observe(0, "N#N"), Some(0));
    }

    #[test]
    fn wide_labels_accept_single_atoms_only_outside_the_label_field() {
        assert!(WideRow::from_label(" SMILES ", true).is_some_and(|row| row.accepts("F")));
        assert!(WideRow::from_label("structure", true).is_some_and(|row| row.accepts("C")));
        assert!(WideRow::from_label("SMILES", false).is_none());
        assert!(WideRow::from_label("sex", true).is_none());
        assert!(
            !WideRow::from_label("SMILES", true)
                .unwrap()
                .accepts("not smiles!")
        );
    }
}
