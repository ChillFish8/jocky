use std::collections::BTreeMap;

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::ValueType;

#[repr(C)]
#[derive(Archive, Serialize, Deserialize)]
#[archive_attr(repr(C), derive(CheckBytes))]
pub struct BasicSchema {
    /// The field names mapping to a given field ID.
    fields: BTreeMap<String, u16>,
    /// More detailed information.
    field_info: Vec<FieldInfo>,
}

impl BasicSchema {
    /// Create a new basic schema.
    pub fn new(fields: BTreeMap<String, u16>, field_info: Vec<FieldInfo>) -> Self {
        Self { fields, field_info }
    }

    #[inline]
    /// The field names mapping to a given field ID.
    pub fn fields(&self) -> &BTreeMap<String, u16> {
        &self.fields
    }

    #[inline]
    /// Get the specific field information.
    pub fn info(&self, field_id: u16) -> &FieldInfo {
        &self.field_info[field_id as usize]
    }
}

#[repr(C)]
#[derive(Archive, Serialize, Deserialize)]
#[archive_attr(repr(C), derive(CheckBytes))]
/// Field specific info describing the structure of the document.
pub struct FieldInfo {
    value_type: ValueType,
    is_multi: bool,
}

impl FieldInfo {
    /// Create a new field info.
    pub fn new(value_type: ValueType, is_multi: bool) -> Self {
        Self {
            value_type,
            is_multi,
        }
    }

    #[inline]
    /// The value type of the doc field.
    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    #[inline]
    /// Is the field multi-valued.
    pub fn is_multi(&self) -> bool {
        self.is_multi
    }
}