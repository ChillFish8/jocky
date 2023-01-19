use serde_json::{Map, Value};

use crate::doc_block::ValueType;

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub enum DocValue<'a> {
    /// A single `u64` value.
    U64(u64),
    /// A single `i64` value.
    I64(i64),
    /// A single `f64` value.
    F64(f64),
    /// A single `string` value.
    String(&'a str),
    /// A single `bytes` value.
    Bytes(Vec<u8>),
    /// A multi `u64` value.
    MultiU64(Vec<u64>),
    /// A multi `i64` value.
    MultiI64(Vec<i64>),
    /// A multi `f64` value.
    MultiF64(Vec<f64>),
    /// A multi `string` value.
    MultiString(Vec<&'a str>),
    /// A multi `bytes` value.
    MultiBytes(Vec<Vec<u8>>),
    /// A dynamic `JSON` object.
    Json(Map<String, Value>),
    /// A set of dynamic `JSON` object.
    MultiJson(Vec<Map<String, Value>>),
}

impl<'a> DocValue<'a> {
    #[inline]
    /// Returns if the document value is a multi-value field or a single value varient.
    pub fn is_multi(&self) -> bool {
        matches!(
            self,
            DocValue::MultiU64(_)
                | DocValue::MultiI64(_)
                | DocValue::MultiF64(_)
                | DocValue::MultiString(_)
                | DocValue::MultiBytes(_)
                | DocValue::MultiJson(_)
        )
    }

    #[inline]
    /// Returns the value type equivalent of this value.
    pub fn value_type(&self) -> ValueType {
        match self {
            DocValue::U64(_) => ValueType::U64,
            DocValue::I64(_) => ValueType::I64,
            DocValue::F64(_) => ValueType::F64,
            DocValue::String(_) => ValueType::String,
            DocValue::Bytes(_) => ValueType::Bytes,
            DocValue::MultiU64(_) => ValueType::U64,
            DocValue::MultiI64(_) => ValueType::I64,
            DocValue::MultiF64(_) => ValueType::F64,
            DocValue::MultiString(_) => ValueType::String,
            DocValue::MultiBytes(_) => ValueType::Bytes,
            DocValue::Json(_) => ValueType::Json,
            DocValue::MultiJson(_) => ValueType::Json,
        }
    }
}


macro_rules! cast {
    ($tp:expr, $el:expr) => {{

    }};
}
