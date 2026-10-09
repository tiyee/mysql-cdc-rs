//! Shared numeric version parsing for protocol producers and CDC adapters.
use crate::{Result, error::invalid};

/// Parses three numeric components, optionally followed by a nonempty `-` or `+`
/// suffix. This validates syntax only; consumers select supported vendors and
/// series separately, including custom CDC adapters.
pub fn parse_server_version(raw: &str) -> Result<(u32, u32, u32)> {
    if raw.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(invalid("invalid server version"));
    }
    let core = if let Some((core, suffix)) = raw.split_once(['-', '+']) {
        if suffix.is_empty() {
            return Err(invalid("empty server version suffix"));
        }
        core
    } else {
        raw
    };
    let mut parts = core.split('.');
    let mut number = || -> Result<u32> {
        let part = parts
            .next()
            .ok_or_else(|| invalid("incomplete server version"))?;
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid("invalid server version component"));
        }
        part.parse()
            .map_err(|_| invalid("server version component overflows u32"))
    };
    let version = (number()?, number()?, number()?);
    if parts.next().is_some() {
        return Err(invalid("extra server version component"));
    }
    Ok(version)
}
