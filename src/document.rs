use std::borrow::Cow;
use std::collections::BTreeMap;
use std::{fmt, mem};

use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer};
use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};
use smallvec::SmallVec;

use crate::doc_block::ValueType;

const STACK_LEN: usize = 4;

/// A document that allows for zero-copy string deserialization via serde
/// while maintaining an owned value.
///
/// This essentially is just a wrapper struct holding onto the raw reference data
/// and the deserialized view of the data.
pub struct ReferencingDoc {
    #[allow(unused)]
    raw: String,
    pub(crate) ts: u64,
    values: BTreeMap<Cow<'static, str>, DocField<'static>>,
}

impl ReferencingDoc {
    /// Creates a new document using reference data to the raw string.
    pub fn new(raw: String, ts: u64) -> Result<Self, serde_json::Error> {
        let s_ref = unsafe { mem::transmute::<_, &'static str>(raw.as_str()) };
        let values = serde_json::from_str(s_ref)?;
        Ok(Self { raw, ts, values })
    }

    /// Creates a referencing document using the provided owned data.
    pub fn from_owned(
        values: BTreeMap<Cow<'static, str>, DocField<'static>>,
        ts: u64,
    ) -> Self {
        Self {
            raw: Default::default(),
            ts,
            values,
        }
    }

    #[inline]
    /// Get a reference to the inner doc data.
    pub fn as_values(&self) -> &BTreeMap<Cow<'static, str>, DocField<'static>> {
        &self.values
    }

    #[inline]
    /// Get the document creation timestamp.
    pub fn timestamp(&self) -> u64 {
        self.ts
    }
}

#[derive(Debug)]
pub enum DocField<'a> {
    /// A single value field.
    Single(DocValue<'a>),
    /// A multi-value field.
    Many(SmallVec<[DocValue<'a>; STACK_LEN]>),
}

impl<'a> DocField<'a> {
    pub fn is_multi(&self) -> bool {
        matches!(self, Self::Many(_))
    }

    #[inline]
    /// Returns the value type equivalent of this value.
    ///
    /// If a field is multi-valued
    pub fn value_type(&self) -> ValueType {
        match self {
            DocField::Single(v) => v.value_type(),
            DocField::Many(values) => {
                if values.is_empty() {
                    return ValueType::Null;
                }
                values[0].value_type()
            },
        }
    }
}

impl<'a> From<SmallVec<[DocValue<'a>; STACK_LEN]>> for DocField<'a> {
    #[inline]
    fn from(value: SmallVec<[DocValue<'a>; STACK_LEN]>) -> Self {
        Self::Many(value)
    }
}

impl<'a> From<Vec<DocValue<'a>>> for DocField<'a> {
    #[inline]
    fn from(value: Vec<DocValue<'a>>) -> Self {
        Self::Many(SmallVec::from_vec(value))
    }
}

impl<'a, T: Into<DocValue<'a>>> From<T> for DocField<'a> {
    #[inline]
    fn from(value: T) -> Self {
        Self::Single(value.into())
    }
}

impl<'de> Deserialize<'de> for DocField<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = DocField<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, int or float")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(DocValue::I64(v).into())
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(DocValue::U64(v).into())
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(DocValue::F64(v).into())
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(DocValue::String(Cow::Owned(v.to_owned())).into())
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::String(Cow::Borrowed(v)).into())
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(DocValue::String(Cow::Owned(v)).into())
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::Bytes(Cow::Owned(v.to_vec())).into())
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::Bytes(Cow::Borrowed(v)).into())
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::Bytes(Cow::Owned(v)).into())
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::Null.into())
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                Map::deserialize(MapAccessDeserializer::new(map))
                    .map(DocValue::Json)
                    .map(DocField::from)
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                SmallVec::deserialize(SeqAccessDeserializer::new(seq))
                    .map(DocField::Many)
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[derive(Debug)]
pub enum DocValue<'a> {
    /// A single `null` value.
    Null,
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
    /// A dynamic `JSON` object.
    Json(Map<String, Value>),
}

impl<'a> DocValue<'a> {
    #[inline]
    /// Returns the value type equivalent of this value.
    pub fn value_type(&self) -> ValueType {
        match self {
            DocValue::U64(_) => ValueType::U64,
            DocValue::I64(_) => ValueType::I64,
            DocValue::F64(_) => ValueType::F64,
            DocValue::String(_) => ValueType::String,
            DocValue::Bytes(_) => ValueType::Bytes,
            DocValue::Json(_) => ValueType::Json,
            DocValue::Null => ValueType::Null,
        }
    }
}

impl<'a> From<&'a [u8]> for DocValue<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Bytes(Cow::Borrowed(value))
    }
}

impl<'a, 'de: 'a> Deserialize<'de> for DocValue<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = DocValue<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, int or float")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(DocValue::I64(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(DocValue::U64(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(DocValue::F64(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(DocValue::String(Cow::Owned(v.to_owned())))
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::String(Cow::Borrowed(v)))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(DocValue::String(Cow::Owned(v)))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::Bytes(Cow::Owned(v.to_vec())))
            }

            fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::Bytes(Cow::Borrowed(v)))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::Bytes(Cow::Owned(v)))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(DocValue::Null)
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                Map::deserialize(MapAccessDeserializer::new(map)).map(DocValue::Json)
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[macro_export]
macro_rules! doc_values {
    (
        $($name:expr => $val:expr $(,)?)*
    ) => {{
        let mut doc: std::collections::BTreeMap<std::borrow::Cow<'static, str>, $crate::DocField<'static>>  = std::collections::BTreeMap::new();

        $(
            doc.insert(std::borrow::Cow::Owned($name.to_string()), $crate::DocField::from($val));
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
impl_from!(DocValue, Json, Map<String, Value>);
