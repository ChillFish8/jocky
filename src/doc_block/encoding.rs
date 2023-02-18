use std::borrow::Cow;
use std::collections::BTreeMap;
use std::mem::size_of;

use bytecheck::CheckBytes;
use datacake_crdt::HLCTimestamp;
use rkyv::{Archive, Deserialize, Serialize};
use tantivy::HasLen;

use crate::document::DocValue;

#[repr(u8)]
#[derive(
    Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Archive, Serialize, Deserialize,
)]
#[archive_attr(derive(CheckBytes))]
/// The type of each field value.
pub enum ValueType {
    /// The field value is of type `string`.
    String = 0,
    /// The field value is of type `u64`.
    U64 = 1,
    /// The field value is of type `i64`.
    I64 = 2,
    /// The field value is of type `f64`.
    F64 = 3,
    /// The field value is of type `bytes`.
    Bytes = 4,
    /// The field value is of type `json`.
    Json = 5,
}

/// The ID of the field in the doc.
type FieldId = u16;
/// The length of the field value in bytes.
type FieldLen = u32;

/// The size of the per-document header.
const DOC_HEADER_SIZE: usize = 20;

#[derive(Debug)]
/// The metadata information about the doc structure.
pub struct DocHeader {
    /// The timestamp the document was created.
    pub timestamp: HLCTimestamp,
    /// The number of `string` fields in the doc.
    pub num_string: u16,
    /// The number of `u64` fields in the doc.
    pub num_u64: u16,
    /// The number of `i64` fields in the doc.
    pub num_i64: u16,
    /// The number of `f64` fields in the doc.
    pub num_f64: u16,
    /// The number of `bytes` fields in the doc.
    pub num_bytes: u16,
    /// The number of `json` fields in the doc.
    pub num_json: u16,
}

impl DocHeader {
    /// Creates a new empty document header.
    pub fn new(timestamp: HLCTimestamp) -> Self {
        Self {
            timestamp,
            num_string: 0,
            num_u64: 0,
            num_i64: 0,
            num_f64: 0,
            num_bytes: 0,
            num_json: 0,
        }
    }

    /// Writes the current doc header metadata into a given buffer.
    pub fn write_to(&self, writer: &mut Vec<u8>) {
        writer.reserve(DOC_HEADER_SIZE);
        writer.extend_from_slice(&self.timestamp.as_u64().to_le_bytes());
        writer.extend_from_slice(&self.num_string.to_le_bytes());
        writer.extend_from_slice(&self.num_u64.to_le_bytes());
        writer.extend_from_slice(&self.num_i64.to_le_bytes());
        writer.extend_from_slice(&self.num_f64.to_le_bytes());
        writer.extend_from_slice(&self.num_bytes.to_le_bytes());
        writer.extend_from_slice(&self.num_json.to_le_bytes());
    }

    /// Attempts to read the header from the start of the reader.
    pub fn try_read_from(mut reader: &[u8]) -> Option<Self> {
        if reader.len() < DOC_HEADER_SIZE {
            return None;
        }

        Some(Self {
            timestamp: read_timestamp(&mut reader)?,
            num_string: read_u16_le(&mut reader)?,
            num_u64: read_u16_le(&mut reader)?,
            num_i64: read_u16_le(&mut reader)?,
            num_f64: read_u16_le(&mut reader)?,
            num_bytes: read_u16_le(&mut reader)?,
            num_json: read_u16_le(&mut reader)?,
        })
    }

    #[inline]
    /// The total number of fields contained within the document.
    pub fn num_fields(&self) -> usize {
        self.num_string as usize
            + self.num_u64 as usize
            + self.num_i64 as usize
            + self.num_f64 as usize
            + self.num_bytes as usize
            + self.num_json as usize
    }

    /// Reads a set of document fields from a given buffer according to the document header.
    pub fn read_document_fields<'a>(
        &self,
        mut doc_buffer: &'a [u8],
        contains_header: bool,
    ) -> Vec<Field<'a>> {
        if contains_header {
            doc_buffer = &doc_buffer[DOC_HEADER_SIZE..];
        }

        let mut fields = Vec::with_capacity(self.num_fields());

        // The order is important here as the values are sorted by their type.
        read_fields(
            ValueType::String,
            self.num_string,
            &mut doc_buffer,
            &mut fields,
        );
        read_fields(ValueType::U64, self.num_u64, &mut doc_buffer, &mut fields);
        read_fields(ValueType::I64, self.num_i64, &mut doc_buffer, &mut fields);
        read_fields(ValueType::F64, self.num_f64, &mut doc_buffer, &mut fields);
        read_fields(
            ValueType::Bytes,
            self.num_bytes,
            &mut doc_buffer,
            &mut fields,
        );
        read_fields(ValueType::Json, self.num_json, &mut doc_buffer, &mut fields);

        fields
    }

    /// Increments a field type's count based on the provided value type.
    fn increment_count_on_type(&mut self, value_type: ValueType) {
        match value_type {
            ValueType::String => {
                self.num_string += 1;
            },
            ValueType::U64 => {
                self.num_u64 += 1;
            },
            ValueType::I64 => {
                self.num_i64 += 1;
            },
            ValueType::F64 => {
                self.num_f64 += 1;
            },
            ValueType::Bytes => {
                self.num_bytes += 1;
            },
            ValueType::Json => {
                self.num_json += 1;
            },
        }
    }
}

/// Encodes a document value into a provided value.
///
/// This writes the document header and it's specific values.
///
/// An optional hash key can be specified to compute the document's digest.
pub fn encode_document_to<'a: 'b, 'b, S: AsRef<str> + 'b>(
    buffer: &mut Vec<u8>,
    ts: HLCTimestamp,
    fields_lookup: &BTreeMap<String, FieldId>,
    num_fields: usize,
    fields: impl IntoIterator<Item = (&'b S, &'b DocValue<'a>)>,
    hash_key: Option<FieldId>,
) -> u64 {
    let mut hasher = blake3::Hasher::new();

    let mut header = DocHeader::new(ts);
    let mut encoding_fields = Vec::with_capacity(num_fields);
    for (field_name, value) in fields {
        if let Some(field_id) = fields_lookup.get(field_name.as_ref()) {
            encoding_fields.push((*field_id, value));
            header.increment_count_on_type(value.value_type());
        }
    }

    // We must sort the values so that they are correctly organised when reading.
    encoding_fields.sort_by_key(|(_, v)| v.value_type());

    header.write_to(buffer);
    for (field_id, value) in encoding_fields {
        let should_hash = hash_key.map(|v| v == field_id).unwrap_or(true);
        encode_value(buffer, field_id, value, &mut hasher, should_hash);
    }

    let mut reader = hasher.finalize_xof();
    let mut buff = [0; 8];
    reader.fill(&mut buff[..]);
    u64::from_be_bytes(buff)
}

#[derive(Debug, thiserror::Error)]
#[error("Unable to deserialize field data into value with type: {0:?}")]
pub struct Corrupted(ValueType);

/// Attempts to convert the raw field into a doc value.
///
/// This will not allocated any values apart from JSON values which
/// must be owned.
pub fn field_to_value(field: Field) -> Result<DocValue, Corrupted> {
    let val = match field.value_type {
        ValueType::String => {
            let data = simdutf8::basic::from_utf8(field.value)
                .map_err(|_| Corrupted(field.value_type))?;
            DocValue::from(data)
        },
        ValueType::U64 => {
            let data = field
                .value
                .try_into()
                .map_err(|_| Corrupted(field.value_type))?;
            DocValue::from(u64::from_le_bytes(data))
        },
        ValueType::I64 => {
            let data = field
                .value
                .try_into()
                .map_err(|_| Corrupted(field.value_type))?;
            DocValue::from(i64::from_le_bytes(data))
        },
        ValueType::F64 => {
            let data = field
                .value
                .try_into()
                .map_err(|_| Corrupted(field.value_type))?;
            DocValue::from(f64::from_le_bytes(data))
        },
        ValueType::Bytes => DocValue::Bytes(Cow::Borrowed(field.value)),
        ValueType::Json => {
            let data = serde_cbor::from_slice(field.value)
                .map_err(|_| Corrupted(field.value_type))?;
            DocValue::Json(data)
        },
    };

    Ok(val)
}

#[inline]
/// Writes a single doc value into the buffer.
///
/// This is done in a log-format so multi-value fields
/// are another entry into the log.
fn encode_value(
    buffer: &mut Vec<u8>,
    field_id: FieldId,
    value: &DocValue,
    hasher: &mut blake3::Hasher,
    should_hash: bool,
) {
    let start = buffer.len();
    buffer.reserve(size_of::<FieldId>() + 8);

    if !value.is_multi() {
        buffer.extend_from_slice(&field_id.to_le_bytes());
    }

    match value {
        DocValue::U64(v) => buffer.extend_from_slice(&v.to_le_bytes()),
        DocValue::I64(v) => buffer.extend_from_slice(&v.to_le_bytes()),
        DocValue::F64(v) => buffer.extend_from_slice(&v.to_le_bytes()),
        DocValue::String(v) => {
            buffer.extend_from_slice(&(v.len() as FieldLen).to_le_bytes());
            buffer.extend_from_slice(v.as_bytes());
        },
        DocValue::Bytes(v) => {
            buffer.extend_from_slice(&(v.len() as FieldLen).to_le_bytes());
            buffer.extend_from_slice(v);
        },
        DocValue::MultiU64(values) => {
            for v in values {
                buffer.extend_from_slice(&field_id.to_le_bytes());
                buffer.extend_from_slice(&v.to_le_bytes());
            }
        },
        DocValue::MultiI64(values) => {
            for v in values {
                buffer.extend_from_slice(&field_id.to_le_bytes());
                buffer.extend_from_slice(&v.to_le_bytes());
            }
        },
        DocValue::MultiF64(values) => {
            for v in values {
                buffer.extend_from_slice(&field_id.to_le_bytes());
                buffer.extend_from_slice(&v.to_le_bytes());
            }
        },
        DocValue::MultiString(values) => {
            for v in values {
                buffer.extend_from_slice(&(v.len() as FieldLen).to_le_bytes());
                buffer.extend_from_slice(&field_id.to_le_bytes());
                buffer.extend_from_slice(v.as_bytes());
            }
        },
        DocValue::MultiBytes(values) => {
            for v in values {
                buffer.extend_from_slice(&(v.len() as FieldLen).to_le_bytes());
                buffer.extend_from_slice(&field_id.to_le_bytes());
                buffer.extend_from_slice(v);
            }
        },
        DocValue::Json(v) => {
            let v = serde_cbor::to_vec(v).expect("Encode valid JSON.");
            buffer.extend_from_slice(&(v.len() as FieldLen).to_le_bytes());
            buffer.extend_from_slice(&v);
        },
        DocValue::MultiJson(values) => {
            for v in values {
                buffer.extend_from_slice(&field_id.to_le_bytes());
                let v = serde_cbor::to_vec(v).expect("Encode valid JSON.");
                buffer.extend_from_slice(&(v.len() as FieldLen).to_le_bytes());
                buffer.extend_from_slice(&v);
            }
        },
    }

    if should_hash {
        hasher.update(&buffer[start..]);
    }
}

pub struct Field<'a> {
    /// The value type of the field.
    pub value_type: ValueType,
    /// The ID of the field.
    pub field_id: FieldId,
    /// The value of the field in bytes.
    pub value: &'a [u8],
}

#[inline]
fn read_u16_le(buffer: &mut &[u8]) -> Option<u16> {
    let (int_bytes, rest) = buffer.split_at(size_of::<u16>());
    *buffer = rest;

    let slice = int_bytes.try_into().ok()?;
    Some(u16::from_le_bytes(slice))
}

#[inline]
fn read_timestamp(buffer: &mut &[u8]) -> Option<HLCTimestamp> {
    let (int_bytes, rest) = buffer.split_at(size_of::<u64>());
    *buffer = rest;

    let slice = int_bytes.try_into().ok()?;
    Some(HLCTimestamp::from_u64(u64::from_le_bytes(slice)))
}

#[inline]
/// Reads a set of field entries from a given buffer according to the value type and
/// the number of fields that are supposed to exist for that type.
///
/// Panics if there are fewer entries than specified.
fn read_fields<'a>(
    value_type: ValueType,
    num: u16,
    buffer: &mut &'a [u8],
    output: &mut Vec<Field<'a>>,
) {
    for _ in 0..num {
        let (field_id_bytes, rest) = buffer.split_at(size_of::<FieldId>());
        *buffer = rest;

        let slice = field_id_bytes
            .try_into()
            .expect("Read correct number of bytes but failed to cast into array.");
        let field_id = FieldId::from_le_bytes(slice);
        match value_type {
            ValueType::String => {
                read_var_length_field(value_type, field_id, buffer, output)
            },
            ValueType::U64 => read_known_length_field(
                value_type,
                field_id,
                buffer,
                output,
                size_of::<u64>(),
            ),
            ValueType::I64 => read_known_length_field(
                value_type,
                field_id,
                buffer,
                output,
                size_of::<i64>(),
            ),
            ValueType::F64 => read_known_length_field(
                value_type,
                field_id,
                buffer,
                output,
                size_of::<f64>(),
            ),
            ValueType::Bytes => {
                read_var_length_field(value_type, field_id, buffer, output)
            },
            ValueType::Json => {
                read_var_length_field(value_type, field_id, buffer, output)
            },
        }
    }
}

#[inline]
/// Reads a field entry of a unknown size from the buffer.
///
/// It is assumed that the length of the value is contained within the prefix
/// of the buffer.
fn read_var_length_field<'a>(
    value_type: ValueType,
    field_id: FieldId,
    buffer: &mut &'a [u8],
    output: &mut Vec<Field<'a>>,
) {
    let (field_len_bytes, rest) = buffer.split_at(size_of::<FieldLen>());
    *buffer = rest;

    let slice = field_len_bytes
        .try_into()
        .expect("Read correct number of bytes but failed to cast into array.");
    let field_len = FieldLen::from_le_bytes(slice);

    read_known_length_field(value_type, field_id, buffer, output, field_len as usize);
}

#[inline]
/// Reads a field entry of a known size from the buffer.
fn read_known_length_field<'a>(
    value_type: ValueType,
    field_id: FieldId,
    buffer: &mut &'a [u8],
    output: &mut Vec<Field<'a>>,
    len: usize,
) {
    let (value, rest) = buffer.split_at(len);
    *buffer = rest;

    output.push(Field {
        value_type,
        field_id,
        value,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc_values;

    fn get_lookup() -> BTreeMap<String, FieldId> {
        let mut fields = BTreeMap::new();
        fields.insert("name".to_string(), 0);
        fields.insert("age".to_string(), 1);
        fields.insert("time".to_string(), 2);
        fields
    }

    #[test]
    fn test_serialize() {
        let values = doc_values! {
            "name" => "bobby",
            "age" => 15_u64,
            "time" => 12312311241241_i64,
        };

        let ts = HLCTimestamp::now(0, 0);
        let mut output = Vec::new();
        encode_document_to(&mut output, ts, &get_lookup(), values.len(), &values, None);
        assert_eq!(output.len(), 51);
    }

    #[test]
    fn test_deserialize() {
        let values = doc_values! {
            "name" => "bobby",
            "age" => 15_u64,
            "time" => 12312311241241_i64,
        };

        dbg!(size_of::<DocHeader>());
        let ts = HLCTimestamp::now(0, 0);
        let mut output = Vec::new();
        encode_document_to(&mut output, ts, &get_lookup(), values.len(), &values, None);
        assert_eq!(output.len(), 51);

        let header = DocHeader::try_read_from(&output).expect("Read header");
        assert_eq!(header.timestamp, ts);
        assert_eq!(header.num_u64, 1);
        assert_eq!(header.num_i64, 1);
        assert_eq!(header.num_f64, 0);
        assert_eq!(header.num_string, 1);
        assert_eq!(header.num_json, 0);
        assert_eq!(header.num_bytes, 0);

        let fields = header.read_document_fields(&output, true);
        assert_eq!(fields.len(), 3);

        assert_eq!(fields[0].value_type, ValueType::String);
        assert_eq!(fields[1].value_type, ValueType::U64);
        assert_eq!(fields[2].value_type, ValueType::I64);
    }
}
