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

impl TopLevelValue {
    /// Computes the primary key hash of the given value.
    ///
    /// It is not recommended to use nested values as the primary key as it can lead
    /// to excess work being done.
    ///
    /// JSON values are also **not** supported.
    pub fn as_primary_key(&self) -> [u8; 32] {
        match self {
            TopLevelValue::U64(v) => blake3::hash(&v.to_be_bytes()),
            TopLevelValue::I64(v) => blake3::hash(&v.to_be_bytes()),
            TopLevelValue::F64(v) => blake3::hash(&v.to_be_bytes()),
            TopLevelValue::String(v) => blake3::hash(v.as_bytes()),
            TopLevelValue::Bytes(v) => blake3::hash(v),
            TopLevelValue::MultiU64(v) => {
                let mut hasher = blake3::Hasher::new();
                for value in v {
                    hasher.update(&value.to_be_bytes());
                }
                hasher.finalize()
            },
            TopLevelValue::MultiI64(v) => {
                let mut hasher = blake3::Hasher::new();
                for value in v {
                    hasher.update(&value.to_be_bytes());
                }
                hasher.finalize()
            },
            TopLevelValue::MultiF64(v) => {
                let mut hasher = blake3::Hasher::new();
                for value in v {
                    hasher.update(&value.to_be_bytes());
                }
                hasher.finalize()
            },
            TopLevelValue::MultiString(v) => {
                let mut hasher = blake3::Hasher::new();
                for value in v {
                    hasher.update(value.as_bytes());
                }
                hasher.finalize()
            },
            TopLevelValue::MultiBytes(v) => {
                let mut hasher = blake3::Hasher::new();
                for value in v {
                    hasher.update(value);
                }
                hasher.finalize()
            },
            TopLevelValue::Json(_) => unimplemented!("Primary key does not support JSON"),
            TopLevelValue::MultiJson(_) => unimplemented!("Primary key does not support JSON"),
        }.into()
    }
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
