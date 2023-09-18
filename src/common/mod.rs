// STD Imports
use std::collections::HashMap;

// Third party library imports
use serde::{Deserialize, Serialize};

pub mod network;
pub mod error;

pub const BLOCK_SIZE: usize = 8192;
pub type Block = [u8; BLOCK_SIZE];

pub const COLUMN_WIDTH: usize = 8;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ColumnType {
    Int, // i64
    Boolean,
    String, // for short strings
    Clob,   // for long strings
    Blob,   // for any size binary data
}

pub type Row = Vec<u8>;
pub type ColumnHeader = (String, ColumnType);
pub type TableInfo = Vec<ColumnHeader>;
pub type TableInfoMap = HashMap<String, (ColumnType, u64)>;
pub type DBSchema = HashMap<String, TableInfo>;

pub fn map_table_info(table_info: &TableInfo) -> TableInfoMap {
    let mut table_map: TableInfoMap = HashMap::new();

    for (offset, (column_name, column_type)) in table_info.iter().enumerate() {
        table_map.insert(
            column_name.to_owned(),
            (column_type.to_owned(), offset as u64),
        );
    }

    table_map
}

pub trait AsRows {
    fn as_rows(&self, columns: usize) -> Vec<Row>;
}

impl AsRows for Block {
    fn as_rows(&self, columns: usize) -> Vec<Row> {
        let chunk_size = columns * COLUMN_WIDTH;
        self.chunks_exact(chunk_size)
            .map(|row| row.to_owned())
            .collect::<Vec<Row>>()
    }
}
