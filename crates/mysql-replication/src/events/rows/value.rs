use crate::{Result, error::invalid};
use std::fmt;

/// A wire integer without an assumed SQL signedness.
/// TABLE_MAP's required metadata does not identify signed/unsigned columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntegerValue<'a> {
    pub(super) data: &'a [u8],
}
impl<'a> IntegerValue<'a> {
    pub fn raw_bytes(self) -> &'a [u8] {
        self.data
    }
    /// Interprets the actual wire width as an unsigned integer.
    pub fn unsigned(self) -> u64 {
        let mut bytes = [0; 8];
        bytes[..self.data.len()].copy_from_slice(self.data);
        u64::from_le_bytes(bytes)
    }
    /// Sign-extends the actual wire width, including three-byte MEDIUMINT.
    pub fn signed(self) -> i64 {
        let shift = (8 - self.data.len()) * 8;
        ((self.unsigned() << shift) as i64) >> shift
    }
}

/// Exact packed DECIMAL, borrowing the validated wire bytes.
/// Formatting preserves scale and never passes through floating point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalValue<'a> {
    data: &'a [u8],
    precision: u8,
    scale: u8,
}
impl<'a> DecimalValue<'a> {
    pub(super) fn new(data: &'a [u8], precision: u8, scale: u8) -> Result<Self> {
        let value = Self {
            data,
            precision,
            scale,
        };
        if value
            .groups()
            .any(|(digits, n)| n >= 10u32.pow(digits as u32))
        {
            return Err(invalid("invalid packed DECIMAL digit group"));
        }
        Ok(value)
    }
    pub fn raw_bytes(self) -> &'a [u8] {
        self.data
    }
    pub fn precision(self) -> u8 {
        self.precision
    }
    pub fn scale(self) -> u8 {
        self.scale
    }
    fn negative(&self) -> bool {
        self.data[0] & 0x80 == 0
    }
    fn groups(&self) -> impl Iterator<Item = (usize, u32)> + '_ {
        let integral = usize::from(self.precision - self.scale);
        let scale = usize::from(self.scale);
        let mut offset = 0;
        std::iter::once(integral % 9)
            .filter(|&n| n != 0)
            .chain(std::iter::repeat_n(9, integral / 9))
            .chain(std::iter::repeat_n(9, scale / 9))
            .chain(std::iter::once(scale % 9).filter(|&n| n != 0))
            .map(move |digits| {
                let width = decimal_group_size(digits);
                let mut n = 0;
                for &byte in &self.data[offset..offset + width] {
                    let byte = if offset == 0 {
                        // Only the first byte carries the sign bit.
                        byte ^ 0x80
                    } else {
                        byte
                    };
                    let byte = if self.negative() { !byte } else { byte };
                    n = (n << 8) | u32::from(byte);
                    offset += 1;
                }
                (digits, n)
            })
    }
}
impl fmt::Display for DecimalValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.negative() && self.groups().any(|(_, n)| n != 0) {
            f.write_str("-")?;
        }
        let integral = usize::from(self.precision - self.scale).div_ceil(9);
        let mut groups = self.groups();
        let mut started = false;
        for (_, n) in groups.by_ref().take(integral) {
            if started {
                write!(f, "{n:09}")?;
            } else if n != 0 {
                write!(f, "{n}")?;
                started = true;
            }
        }
        if !started {
            f.write_str("0")?;
        }
        if self.scale != 0 {
            f.write_str(".")?;
            for (digits, n) in groups {
                write!(f, "{n:0digits$}")?;
            }
        }
        Ok(())
    }
}

pub(super) fn decimal_group_size(digits: usize) -> usize {
    const SIZES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    SIZES[digits]
}

/// Protocol values, independent from a table whitelist or CDC primary key.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum RowValue<'a> {
    Null,
    Integer(IntegerValue<'a>),
    Float(f32),
    Double(f64),
    Decimal(DecimalValue<'a>),
    /// CHAR/VARCHAR and BLOB bytes, without guessing a character set.
    Bytes(&'a [u8]),
    Bit(u64),
    Enum(u16),
    Set(u64),
    Year(u16),
    /// Temporal, JSON, geometry and vector bytes, without semantic conversion.
    /// Consult the column definition for the type and encoding metadata.
    Encoded(&'a [u8]),
}
