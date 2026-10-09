use crate::{error::invalid, Error, Result};
pub(crate) struct Cursor<'a> {
    rest: &'a [u8],
}
impl<'a> Cursor<'a> {
    pub fn new(rest: &'a [u8]) -> Self {
        Self { rest }
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
    pub fn name(&mut self) -> Result<&'a [u8]> {
        let n = usize::from(self.byte()?);
        let value = self.take(n)?;
        if self.byte()? != 0 {
            return Err(invalid("name terminator is not NUL"));
        }
        Ok(value)
    }
    pub fn varint(&mut self) -> Result<u64> {
        let mut buffer = mysql_common::io::ParseBuf(self.rest);
        let value = mysql_common::misc::read_varlen_uint(&mut buffer)?;
        self.rest = buffer.0;
        Ok(value)
    }
    pub fn signed_varint(&mut self) -> Result<i64> {
        let value = self.varint()?;
        let magnitude = (value >> 1) as i64;
        Ok(if value & 1 == 0 {
            magnitude
        } else {
            -magnitude - 1
        })
    }
    pub fn finish(self) -> Result<()> {
        if !self.rest.is_empty() {
            return Err(invalid("unexpected trailing event bytes"));
        }
        Ok(())
    }
}
