// STD Imports
use std::collections::HashMap;

// Third party library imports
use byteorder::{ByteOrder, LE};
use serde::{Deserialize, Serialize};

pub mod error;
pub mod network;

pub const BLOCK_SIZE: usize = 8192;
pub type Block = [u8; BLOCK_SIZE];

pub const COLUMN_WIDTH: usize = 8;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ColumnType {
    Integer, // i64
    Boolean,
    Text, // for short strings
    Clob, // for long strings
    Blob, // for any size binary data
}

pub enum DataType {
    Integer(i64),
    Text([char; 8]),
    ClobRef(u64),
    BlobRef(u64),
}

pub type Row = Vec<DataType>;
pub type RawRow = Vec<u64>;
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
    fn as_rows(&self, columns: usize) -> Vec<RawRow>;
}

impl AsRows for Block {
    fn as_rows(&self, columns: usize) -> Vec<RawRow> {
        let chunk_size = columns * COLUMN_WIDTH;
        self.chunks_exact(chunk_size)
            .map(|row| {
                row.chunks_exact(COLUMN_WIDTH)
                    .map(|bytes| LE::read_u64(bytes))
                    .collect()
            })
            .collect::<Vec<RawRow>>()
    }
}

impl AsRows for Vec<Block> {
    fn as_rows(&self, columns: usize) -> Vec<RawRow> {
        let chunk_size = columns * COLUMN_WIDTH;
        self.iter()
            .map(|block| {
                block
                    .chunks_exact(chunk_size)
                    .map(|row| {
                        row.chunks_exact(COLUMN_WIDTH)
                            .map(|bytes| LE::read_u64(bytes))
                            .collect::<RawRow>()
                    })
                    .collect::<Vec<RawRow>>()
            })
            .flatten()
            .collect()
    }
}
