use crate::{
    Result,
    error::{Error, invalid},
};

pub(crate) struct Cursor<'a> {
    rest: &'a [u8],
}

impl<'a> Cursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { rest: data }
    }
    pub fn remaining(&self) -> &'a [u8] {
        self.rest
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let value = self.rest.get(..n).ok_or(Error::Truncated {
            need: n,
            have: self.rest.len(),
        })?;
        self.rest = &self.rest[n..];
        Ok(value)
    }
    pub fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn le(&mut self, n: usize) -> Result<u64> {
        if n > 8 {
            return Err(invalid("integer wider than 8 bytes"));
        }
        let mut bytes = [0; 8];
        bytes[..n].copy_from_slice(self.take(n)?);
        Ok(u64::from_le_bytes(bytes))
    }
    pub fn lenenc(&mut self) -> Result<usize> {
        let value = match self.byte()? {
            0xfc => self.le(2)?,
            0xfd => self.le(3)?,
            0xfe => self.le(8)?,
            0xfb | 0xff => return Err(invalid("NULL/reserved length encoding")),
            n => u64::from(n),
        };
        usize::try_from(value).map_err(|_| invalid("length does not fit usize"))
    }
    pub fn name(&mut self) -> Result<&'a str> {
        let n = usize::from(self.byte()?);
        let value = std::str::from_utf8(self.take(n)?)?;
        if self.byte()? != 0 {
            return Err(invalid("name terminator is not NUL"));
        }
        Ok(value)
    }
    pub fn bitmap(&mut self, columns: usize) -> Result<&'a [u8]> {
        // Padding bits are unspecified by the protocol (NULL maps often use 1).
        self.take(columns.div_ceil(8))
    }
}

pub(crate) fn bit(data: &[u8], index: usize) -> bool {
    data.get(index / 8)
        .is_some_and(|b| b & (1 << (index % 8)) != 0)
}

pub(crate) fn set_bits(data: &[u8]) -> impl Iterator<Item = usize> + '_ {
    data.iter().enumerate().flat_map(|(byte_index, &byte)| {
        let mut bits = byte;
        std::iter::from_fn(move || {
            if bits == 0 {
                return None;
            }
            let index = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            Some(byte_index * 8 + index)
        })
    })
}
