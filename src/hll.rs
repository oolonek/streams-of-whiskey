//! HLL p=14 with XXH3-64 (default secret, seed 0) over the original UTF-8 bytes.
//! Raw registers have no header: the database's `sketch_format` metadata is
//! required to interpret them. Changing the hash or register layout needs a
//! new format identifier and an explicit migration from the original strings.

use twox_hash::xxhash3_64::Hasher;

pub const FORMAT: &str = "hll:p14:xxh3_64:seed0:v1";
const PRECISION: u32 = 14;
const REGISTERS: usize = 1 << PRECISION;
const MAX_RANK: u8 = (64 - PRECISION + 1) as u8;

#[derive(Clone, Debug)]
pub struct Sketch {
    registers: Box<[u8]>,
}

impl Default for Sketch {
    fn default() -> Self {
        Self::new()
    }
}

impl Sketch {
    pub fn new() -> Self {
        Self {
            registers: vec![0; REGISTERS].into_boxed_slice(),
        }
    }

    pub fn clear(&mut self) {
        self.registers.fill(0);
    }

    pub fn insert(&mut self, value: &str) {
        let hash = Hasher::oneshot(value.as_bytes());
        let index = (hash >> (64 - PRECISION)) as usize;
        let rank = ((hash << PRECISION).leading_zeros() + 1).min(MAX_RANK as u32) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    pub fn merge(&mut self, other: &Self) {
        for (register, &incoming) in self.registers.iter_mut().zip(other.registers.iter()) {
            *register = (*register).max(incoming);
        }
    }
    /// Validate and merge a persisted sketch without allocating a second copy.
    pub fn merge_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        validate_bytes(bytes)?;
        for (register, &incoming) in self.registers.iter_mut().zip(bytes.iter()) {
            *register = (*register).max(incoming);
        }
        Ok(())
    }

    pub fn estimate(&self) -> u64 {
        let m = REGISTERS as f64;
        let mut harmonic = 0.0;
        let mut empty = 0;
        for &rank in self.registers.iter() {
            harmonic += 2.0_f64.powi(-(rank as i32));
            empty += usize::from(rank == 0);
        }
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / harmonic;
        let estimate = if raw <= 2.5 * m && empty > 0 {
            m * (m / empty as f64).ln()
        } else if raw > (u64::MAX as f64) / 30.0 {
            // Large-range correction for a finite 64-bit hash space.
            let hash_space = 2.0_f64.powi(64);
            -hash_space * (-raw / hash_space).ln_1p()
        } else {
            raw
        };
        estimate.round() as u64
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.registers
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        validate_bytes(bytes)?;
        Ok(Self {
            registers: bytes.into(),
        })
    }
}

fn validate_bytes(bytes: &[u8]) -> Result<(), String> {
    if bytes.len() != REGISTERS {
        return Err(format!(
            "HLL sketch must contain {REGISTERS} registers, got {}",
            bytes.len()
        ));
    }
    if let Some((index, rank)) = bytes
        .iter()
        .copied()
        .enumerate()
        .find(|(_, rank)| *rank > MAX_RANK)
    {
        return Err(format!(
            "invalid HLL rank {rank} in register {index} (maximum {MAX_RANK})"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_duplicates_and_small_union_use_linear_counting() {
        let mut one = Sketch::new();
        let mut two = Sketch::new();
        assert_eq!(one.estimate(), 0);
        for _ in 0..10 {
            one.insert("CCO");
        }
        one.insert("N");
        two.insert("CCO");
        two.insert("O");
        one.merge(&two);
        assert_eq!(one.estimate(), 3);
        assert_eq!(Sketch::from_bytes(one.as_bytes()).unwrap().estimate(), 3);
    }

    #[test]
    fn rejects_truncated_and_impossible_registers() {
        assert!(Sketch::from_bytes(&[0; 100]).is_err());
        let mut bytes = vec![0; REGISTERS];
        bytes[7] = MAX_RANK + 1;
        assert!(Sketch::from_bytes(&bytes).is_err());
        bytes[7] = MAX_RANK;
        assert!(Sketch::from_bytes(&bytes).is_ok());
    }

    #[test]
    fn merging_persisted_sketch_rejects_corruption_without_changing_count() {
        let mut saved = Sketch::new();
        saved.insert("CCO");
        let mut current = Sketch::new();
        current.insert("N");
        current.merge_bytes(saved.as_bytes()).unwrap();
        assert_eq!(current.estimate(), 2);
        let mut corrupted = saved.as_bytes().to_vec();
        corrupted[REGISTERS - 1] = MAX_RANK + 1;
        assert!(current.merge_bytes(&corrupted).is_err());
        assert_eq!(current.estimate(), 2);
    }

    #[test]
    fn many_distinct_strings_are_estimated_within_expected_error() {
        let mut sketch = Sketch::new();
        for value in 0..100_000 {
            sketch.insert(&format!("molecule-{value}"));
        }
        assert!((98_000..=102_000).contains(&sketch.estimate()));
    }
}
