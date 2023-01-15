use std::collections::BTreeMap;
use bytecheck::CheckBytes;
use datacake_crdt::HLCTimestamp;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::with::CopyOptimize;

#[repr(C)]
#[derive(Debug, Serialize, Deserialize, Archive)]
#[archive_attr(repr(C), derive(CheckBytes))]
/// Represents a raw document for the index.
///
/// This includes a timestamp of when the document was created, this is
/// in the form of a [HLCTimestamp].
pub struct RawDocument {
    pub timestamp: HLCTimestamp,
    pub value: BTreeMap<String, TopLevelValue>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize, Archive, serde::Deserialize)]
#[serde(untagged)]
#[archive_attr(derive(CheckBytes))]
pub enum TopLevelValue {
    /// A single `u64` value.
    U64(u64),
    /// A single `i64` value.
    I64(i64),
    /// A single `f64` value.
    F64(f64),
    /// A single `string` value.
    String(String),
    /// A single `bytes` value.
    Bytes(#[with(CopyOptimize)] Vec<u8>),
    /// A dynamic `JSON` object.
    Json(BTreeMap<String, JsonValue>),
    /// A multi `u64` value.
    MultiU64(#[with(CopyOptimize)] Vec<u64>),
    /// A multi `i64` value.
    MultiI64(#[with(CopyOptimize)] Vec<i64>),
    /// A multi `f64` value.
    MultiF64(#[with(CopyOptimize)] Vec<f64>),
    /// A multi `string` value.
    MultiString(Vec<String>),
    /// A multi `bytes` value.
    MultiBytes(Vec<Vec<u8>>),
    /// A set of dynamic `JSON` object.
    MultiJson(Vec<BTreeMap<String, JsonValue>>),
}

#[repr(C)]
#[derive(Debug,Serialize, Deserialize, Archive, serde::Deserialize)]
#[serde(untagged)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(
    derive(CheckBytes, Debug),
    check_bytes(
        bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
    )
)]
pub enum JsonValue {
    Null,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    String(String),
    Array(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        Vec<JsonValue>,
    ),
    Object(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        BTreeMap<String, JsonValue>,
    ),
}
