//! Synchronous replication configuration without CDC schema or checkpoints.
use crate::{
    BinlogEvent, BinlogFrame, BinlogParser, Error, FrameDecoder, MAX_EVENT_BYTES, Result,
    error::invalid, event_type as code, events::BinlogMode, parser::ParserConfig,
};
use mysql::{
    BinlogDumpFlags, BinlogRequest, BinlogStream, Conn, Opts, OptsBuilder, SslOpts,
    prelude::Queryable,
};
use mysql_common::binlog::{
    consts::BinlogChecksumAlg,
    events::{BinlogEventHeader, Event as DriverEvent},
};
use std::{path::PathBuf, time::Duration};

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub url_env: String,
    pub tls_ca: Option<PathBuf>,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
}
impl ConnectionConfig {
    pub fn new(url_env: impl Into<String>) -> Self {
        Self {
            url_env: url_env.into(),
            tls_ca: None,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(90),
        }
    }
}
pub fn connect(config: &ConnectionConfig) -> Result<Conn> {
    let url = std::env::var(&config.url_env)
        .map_err(|_| invalid(format!("missing environment variable {}", config.url_env)))?;
    let opts = Opts::from_url(&url).map_err(|_| invalid("invalid MySQL URL"))?;
    let mut builder = OptsBuilder::from_opts(opts)
        .tcp_connect_timeout(Some(config.connect_timeout))
        .read_timeout(Some(config.read_timeout));
    if let Some(ca) = &config.tls_ca {
        builder = builder.ssl_opts(SslOpts::default().with_root_cert_path(Some(ca.clone())));
    }
    Ok(Conn::new(builder)?)
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowImage {
    Minimal,
    Full,
    NoBlob,
}
impl RowImage {
    fn value(self) -> &'static str {
        match self {
            Self::Minimal => "MINIMAL",
            Self::Full => "FULL",
            Self::NoBlob => "NOBLOB",
        }
    }
}
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub server_id: u32,
    pub file: String,
    pub position: u64,
    pub non_blocking: bool,
    /// Verifies upstream logging; never changes a global server setting.
    pub required_row_image: Option<RowImage>,
    pub parser: ParserConfig,
}
impl ReplicationConfig {
    pub fn new(server_id: u32, file: impl Into<String>, position: u64) -> Self {
        Self {
            server_id,
            file: file.into(),
            position,
            non_blocking: false,
            required_row_image: None,
            parser: ParserConfig::default(),
        }
    }
}

/// Raw network transport for adapters that own event-body parsing.
pub struct ReplicationConnection {
    stream: BinlogStream,
    current: Option<DriverEvent>,
    buffer: Vec<u8>,
    framing: FrameDecoder,
}
impl ReplicationConnection {
    pub fn connect(connection: &ConnectionConfig, replication: ReplicationConfig) -> Result<Self> {
        Self::from_connection(connect(connection)?, replication)
    }
    pub fn from_connection(mut conn: Conn, config: ReplicationConfig) -> Result<Self> {
        if config.server_id == 0 || config.file.is_empty() || config.position < 4 {
            return Err(invalid(
                "replication requires server_id, file and position >= 4",
            ));
        }
        if let Some(image) = config.required_row_image {
            let actual: Option<String> = conn.query_first("SELECT @@global.binlog_row_image")?;
            if !actual
                .as_deref()
                .is_some_and(|s| s.eq_ignore_ascii_case(image.value()))
            {
                return Err(Error::Unsupported(format!(
                    "requires binlog_row_image={}",
                    image.value()
                )));
            }
        }
        conn.query_drop("SET @master_heartbeat_period=30000000000")?;
        let request = BinlogRequest::new(config.server_id)
            .with_filename(config.file.as_bytes())
            .with_pos(config.position)
            .with_flags(if config.non_blocking {
                BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK
            } else {
                BinlogDumpFlags::empty()
            });
        Ok(Self {
            stream: conn.get_binlog_stream(request)?,
            current: None,
            buffer: Vec::new(),
            framing: FrameDecoder::new(),
        })
    }
    /// CDC's fast path: checksum verification and a borrowed body, with no raw
    /// frame assembly, GTID metadata decoding or row-value materialization.
    pub fn next_view(&mut self) -> Result<Option<crate::EventView<'_>>> {
        self.current = self.stream.next().transpose()?;
        let Some(event) = &self.current else {
            return Ok(None);
        };
        validate_driver(event)?;
        if event.header().event_type_raw() == code::FORMAT_DESCRIPTION_EVENT {
            let algorithm = event
                .footer()
                .get_checksum_alg()
                .map_err(|e| Error::Unsupported(e.to_string()))?;
            self.framing
                .set_checksum(algorithm == Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32));
        }
        Ok(Some(crate::EventView {
            header: header_view(event.header()),
            body: event.data(),
        }))
    }
    pub fn next_frame(&mut self) -> Result<Option<BinlogFrame<'_>>> {
        read_frame(&mut self.stream, &mut self.buffer, &mut self.framing)
    }
}

/// Typed event subscription; the image mode is explicit and fixed.
pub struct ReplicationStream<M: BinlogMode> {
    connection: ReplicationConnection,
    parser: BinlogParser<M>,
}
impl<M: BinlogMode> ReplicationStream<M> {
    pub fn connect(connection: &ConnectionConfig, config: ReplicationConfig) -> Result<Self> {
        let config = bind_image_mode::<M>(config)?;
        Self::from_connection(connect(connection)?, config)
    }
    pub fn from_connection(conn: Conn, config: ReplicationConfig) -> Result<Self> {
        let config = bind_image_mode::<M>(config)?;
        let parser = BinlogParser::new(config.parser);
        Ok(Self {
            connection: ReplicationConnection::from_connection(conn, config)?,
            parser,
        })
    }
    pub fn next_event(&mut self) -> Result<Option<BinlogEvent<'_, M>>> {
        let Some(frame) = self.connection.next_frame()? else {
            return Ok(None);
        };
        self.parser.parse(frame).map(Some)
    }
}

fn bind_image_mode<M: BinlogMode>(mut config: ReplicationConfig) -> Result<ReplicationConfig> {
    let expected = M::ROW_IMAGE.into();
    if config
        .required_row_image
        .is_some_and(|mode| mode != expected)
    {
        return Err(invalid(
            "replication configuration conflicts with the generic image mode",
        ));
    }
    config.required_row_image = Some(expected);
    Ok(config)
}

fn read_frame<'a>(
    stream: &mut BinlogStream,
    buffer: &'a mut Vec<u8>,
    framing: &mut FrameDecoder,
) -> Result<Option<BinlogFrame<'a>>> {
    let Some(event) = stream.next().transpose()? else {
        return Ok(None);
    };
    retain_driver_bytes(&event, buffer)?;
    framing.decode(buffer).map(Some)
}

fn header_view(header: BinlogEventHeader) -> crate::EventHeader {
    crate::EventHeader {
        timestamp: header.timestamp(),
        event_type: header.event_type_raw(),
        server_id: header.server_id(),
        event_size: header.event_size(),
        log_pos: header.log_pos(),
        flags: header.flags_raw(),
    }
}
fn validate_driver(event: &DriverEvent) -> Result<()> {
    let length = event.header().event_size() as usize;
    if !(19..=MAX_EVENT_BYTES).contains(&length) {
        return Err(invalid("event size outside 19..64MiB"));
    }
    let algorithm = event
        .footer()
        .get_checksum_alg()
        .map_err(|e| Error::Unsupported(e.to_string()))?;
    if algorithm == Some(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32)
        && event.checksum().map(u32::from_le_bytes)
            != Some(event.calc_checksum(BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32))
    {
        return Err(Error::Checksum);
    }
    let trailer = usize::from(event.checksum().is_some()) * 4
        + usize::from(event.header().event_type_raw() == code::FORMAT_DESCRIPTION_EVENT);
    if 19 + event.data().len() + trailer != length {
        return Err(invalid("driver did not preserve the complete wire event"));
    }
    Ok(())
}

// The driver splits the wire bytes into header, body, algorithm and checksum.
// Retain each original value; Event::write must not be used because it recalculates
// the checksum and can normalize the original trailer.
fn retain_driver_bytes(event: &DriverEvent, buffer: &mut Vec<u8>) -> Result<()> {
    let header = event.header();
    let len = header.event_size() as usize;
    if !(19..=MAX_EVENT_BYTES).contains(&len) {
        return Err(invalid("event size outside 19..64MiB"));
    }
    buffer.clear();
    buffer.reserve(len);
    buffer.extend_from_slice(&header.timestamp().to_le_bytes());
    buffer.push(header.event_type_raw());
    buffer.extend_from_slice(&header.server_id().to_le_bytes());
    buffer.extend_from_slice(&header.event_size().to_le_bytes());
    buffer.extend_from_slice(&header.log_pos().to_le_bytes());
    buffer.extend_from_slice(&header.flags_raw().to_le_bytes());
    buffer.extend_from_slice(event.data());
    if header.event_type_raw() == code::FORMAT_DESCRIPTION_EVENT {
        let algorithm = event
            .footer()
            .get_checksum_alg()
            .map_err(|e| Error::Unsupported(e.to_string()))?
            .ok_or_else(|| invalid("FDE checksum algorithm missing"))?;
        buffer.push(match algorithm {
            BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_OFF => 0,
            BinlogChecksumAlg::BINLOG_CHECKSUM_ALG_CRC32 => 1,
        });
    }
    if let Some(checksum) = event.checksum() {
        buffer.extend_from_slice(&checksum);
    }
    if buffer.len() != len {
        return Err(invalid("driver did not preserve the complete wire event"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_common::binlog::{consts::BinlogVersion, events::FormatDescriptionEvent};
    #[test]
    fn driver_trailer_is_retained_instead_of_recalculated() {
        let mut body = vec![0; 57 + 43];
        body[..2].copy_from_slice(&4u16.to_le_bytes());
        body[2..8].copy_from_slice(b"8.4.0\0");
        body[56] = 19;
        for algorithm in [0u8, 1] {
            let mut raw = vec![0; 19];
            raw[4] = code::FORMAT_DESCRIPTION_EVENT;
            raw[5..9].copy_from_slice(&123u32.to_le_bytes());
            raw[17..19].copy_from_slice(&0x8001u16.to_le_bytes());
            raw.extend_from_slice(&body);
            raw.push(algorithm);
            raw.extend_from_slice(&0x12345678u32.to_le_bytes());
            let length = raw.len() as u32;
            raw[9..13].copy_from_slice(&length.to_le_bytes());
            let fde = FormatDescriptionEvent::new(BinlogVersion::Version4);
            let event = DriverEvent::read(&fde, &raw[..]).unwrap();
            let mut retained = Vec::new();
            retain_driver_bytes(&event, &mut retained).unwrap();
            assert_eq!(retained, raw);
        }
    }
}
