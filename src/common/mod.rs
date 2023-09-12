// STD Imports
use std::collections::HashMap;

// Third party library imports
use serde::{Deserialize, Serialize};

pub const BLOCK_SIZE: usize = 8192;
pub type Block = [u8; BLOCK_SIZE];

#[derive(Serialize, Deserialize)]
pub enum ColumnType {
    Int, // i64
    Boolean,
    String, // for short strings
    Clob,   // for long strings
    Blob,   // for any size binary data
}

pub type ColumnHeader = (String, ColumnType);
pub type TableInfo = Vec<ColumnHeader>;
pub type SchemaMap = HashMap<String, (ColumnType, u64)>;
pub type DBSchema = HashMap<String, TableInfo>;
