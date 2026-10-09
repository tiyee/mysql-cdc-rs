use crate::TableMeta;
use serde::{
    Serialize, Serializer,
    ser::{SerializeSeq, SerializeStruct},
};
use std::{borrow::Cow, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum PrimaryKey<'a> {
    Signed(i64),
    Unsigned(u64),
    Text(Cow<'a, str>),
    /// JSON string, never a floating point number.
    Decimal(String),
}

impl PrimaryKey<'_> {
    pub fn into_owned(self) -> PrimaryKey<'static> {
        match self {
            Self::Signed(v) => PrimaryKey::Signed(v),
            Self::Unsigned(v) => PrimaryKey::Unsigned(v),
            Self::Text(v) => PrimaryKey::Text(Cow::Owned(v.into_owned())),
            Self::Decimal(v) => PrimaryKey::Decimal(v),
        }
    }
    pub(crate) fn heap_bytes(&self) -> usize {
        match self {
            Self::Text(v) => v.len(),
            Self::Decimal(v) => v.len(),
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

/// Borrowed callback view: no row vector and no copies of non-key column values.
#[derive(Debug)]
pub struct Change<'a> {
    pub table: &'a Arc<TableMeta>,
    pub operation: Operation,
    pub pk: PrimaryKey<'a>,
    pub previous_pk: Option<PrimaryKey<'a>>,
    /// One shared index list per ROWS event, in ordinal order.
    pub changed: &'a Arc<[u16]>,
}

impl Change<'_> {
    pub fn into_owned(self) -> OwnedChange {
        OwnedChange {
            table: Arc::clone(self.table),
            operation: self.operation,
            pk: self.pk.into_owned(),
            previous_pk: self.previous_pk.map(PrimaryKey::into_owned),
            changed: Arc::clone(self.changed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OwnedChange {
    pub table: Arc<TableMeta>,
    pub operation: Operation,
    pub pk: PrimaryKey<'static>,
    pub previous_pk: Option<PrimaryKey<'static>>,
    pub changed: Arc<[u16]>,
}

impl OwnedChange {
    pub fn changed_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.changed
            .iter()
            .map(|&i| self.table.columns[usize::from(i)].name.as_str())
    }
    pub(crate) fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.pk.heap_bytes()
            + self.previous_pk.as_ref().map_or(0, PrimaryKey::heap_bytes)
            + self.changed.len() * 2
    }
}

struct Names<'a>(&'a TableMeta, &'a [u16]);
impl Serialize for Names<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.1.len()))?;
        for &i in self.1 {
            let col = self
                .0
                .columns
                .get(usize::from(i))
                .ok_or_else(|| serde::ser::Error::custom("invalid column index"))?;
            seq.serialize_element(&col.name)?;
        }
        seq.end()
    }
}

impl Serialize for OwnedChange {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut out = serializer
            .serialize_struct("Change", if self.previous_pk.is_some() { 6 } else { 5 })?;
        out.serialize_field("schema", &self.table.schema)?;
        out.serialize_field("table", &self.table.table)?;
        out.serialize_field("operation", &self.operation)?;
        out.serialize_field("pk", &self.pk)?;
        if let Some(pk) = &self.previous_pk {
            out.serialize_field("previous_pk", pk)?;
        }
        out.serialize_field("changed", &Names(&self.table, &self.changed))?;
        out.end()
    }
}
