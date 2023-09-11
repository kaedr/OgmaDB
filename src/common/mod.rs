// STD Imports
use std::collections::HashMap;

// Third party library imports
use serde::{Deserialize, Serialize};

pub const BLOCK_SIZE: u64 = 8192;
pub type Block = [u8; BLOCK_SIZE];

pub enum ColumnType {
    Int,   // i64
    Boolean,
    String, // for short strings
    Clob,   // for long strings
    Blob,   // for any size binary data
}

#[derive(Serialize, Deserialize)]
pub type ColumnHeader = (String, ColumnType);

#[derive(Serialize, Deserialize)]
pub type TableInfo = Vec<ColumnHeader>;

#[derive(Serialize, Deserialize)]
pub type SchemaMap = HashMap<String, (ColumnType, u64)>;

#[derive(Serialize, Deserialize)]
pub type DBSchema = HashMap<String, TableInfo>;
