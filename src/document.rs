use std::borrow::Cow;
use std::collections::BTreeMap;
use std::mem;

use datacake_crdt::HLCTimestamp;
use serde_json::{Map, Value};

use crate::doc_block::ValueType;

/// A document that allows for zero-copy string deserialization via serde
/// while maintaining an owned value.
///
/// This essentially is just a wrapper struct holding onto the raw reference data
/// and the deserialized view of the data.
pub struct ReferencingDoc {
    #[allow(unused)]
    raw: String,
    pub(crate) ts: HLCTimestamp,
    values: BTreeMap<Cow<'static, str>, DocValue<'static>>,
}

impl ReferencingDoc {
    /// Creates a new document using reference data to the raw string.
    pub fn new(raw: String, ts: HLCTimestamp) -> Result<Self, serde_json::Error> {
        let s_ref = unsafe { mem::transmute::<_, &'static str>(raw.as_str()) };
        let values = serde_json::from_str(s_ref)?;
        Ok(Self { raw, ts, values })
    }

    /// Creates a referencing document using the provided owned data.
    pub fn from_owned(
        values: BTreeMap<Cow<'static, str>, DocValue<'static>>,
        ts: HLCTimestamp,
    ) -> Self {
        Self {
            raw: Default::default(),
            ts,
            values,
        }
    }

    #[inline]
    /// Get a reference to the inner doc data.
    pub fn as_values(&self) -> &BTreeMap<Cow<'static, str>, DocValue> {
        &self.values
    }

    #[inline]
    /// Get the document creation timestamp.
    pub fn timestamp(&self) -> HLCTimestamp {
        self.ts
    }
}

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
    String(Cow<'a, str>),
    /// A single `bytes` value.
    Bytes(Cow<'a, [u8]>),
    /// A multi `u64` value.
    MultiU64(Vec<u64>),
    /// A multi `i64` value.
    MultiI64(Vec<i64>),
    /// A multi `f64` value.
    MultiF64(Vec<f64>),
    /// A multi `string` value.
    MultiString(Vec<Cow<'a, str>>),
    /// A multi `bytes` value.
    MultiBytes(Vec<Cow<'a, [u8]>>),
    /// A dynamic `JSON` object.
    Json(Map<String, Value>),
    /// A set of dynamic `JSON` object.
    MultiJson(Vec<Map<String, Value>>),
}

impl<'a> DocValue<'a> {
    #[inline]
    /// Returns if the document value is a multi-value field or a single value variant.
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

impl<'a> From<Vec<&'a str>> for DocValue<'a> {
    fn from(value: Vec<&'a str>) -> Self {
        let value = value.into_iter().map(Cow::Borrowed).collect();
        Self::MultiString(value)
    }
}

impl<'a> From<Vec<String>> for DocValue<'a> {
    fn from(value: Vec<String>) -> Self {
        let value = value.into_iter().map(Cow::Owned).collect();
        Self::MultiString(value)
    }
}

impl<'a> From<&'a [u8]> for DocValue<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Bytes(Cow::Borrowed(value))
    }
}
impl<'a> From<Vec<&'a [u8]>> for DocValue<'a> {
    fn from(value: Vec<&'a [u8]>) -> Self {
        let value = value.into_iter().map(Cow::Borrowed).collect();
        Self::MultiBytes(value)
    }
}

impl<'a> From<Vec<Vec<u8>>> for DocValue<'a> {
    fn from(value: Vec<Vec<u8>>) -> Self {
        let value = value.into_iter().map(Cow::Owned).collect();
        Self::MultiBytes(value)
    }
}

#[macro_export]
macro_rules! doc_values {
    (
        $($name:expr => $val:expr $(,)?)*
    ) => {{
        let mut doc: std::collections::BTreeMap<std::borrow::Cow<'static, str>, $crate::DocValue<'static>>  = std::collections::BTreeMap::new();

        $(
            doc.insert(std::borrow::Cow::Owned($name.to_string()), DocValue::from($val));
        )*

        doc
    }};
}

macro_rules! impl_from {
    ($t:ident, $var:ident, $tp:ty) => {
        impl<'a> From<$tp> for $t<'a> {
            fn from(value: $tp) -> Self {
                Self::$var(value.into())
            }
        }
    };
}

impl_from!(DocValue, U64, u64);
impl_from!(DocValue, I64, i64);
impl_from!(DocValue, F64, f64);
impl_from!(DocValue, U64, u32);
impl_from!(DocValue, I64, i32);
impl_from!(DocValue, F64, f32);
impl_from!(DocValue, String, &'a str);
impl_from!(DocValue, String, String);
impl_from!(DocValue, String, Cow<'a, str>);
impl_from!(DocValue, Bytes, Vec<u8>);
impl_from!(DocValue, MultiU64, Vec<u64>);
impl_from!(DocValue, MultiU64, &[u64]);
impl_from!(DocValue, MultiI64, Vec<i64>);
impl_from!(DocValue, MultiI64, &[i64]);
impl_from!(DocValue, MultiF64, Vec<f64>);
impl_from!(DocValue, MultiF64, &[f64]);
impl_from!(DocValue, MultiString, Vec<Cow<'a, str>>);
impl_from!(DocValue, MultiString, &[Cow<'a, str>]);
impl_from!(DocValue, Json, Map<String, Value>);
impl_from!(DocValue, MultiJson, Vec<Map<String, Value>>);
impl_from!(DocValue, MultiJson, &[Map<String, Value>]);
