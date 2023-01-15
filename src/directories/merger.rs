use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::directories::{DirectoryReader, DirectoryWriter};

pub struct DirectoryMerger {
    readers: BTreeMap<PathBuf, DirectoryReader>,
    writer: DirectoryWriter,
}
