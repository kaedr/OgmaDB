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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ColumnType {
    Integer, // i64
    Boolean,
    Text, // for short strings
    Clob, // for long strings
    Blob, // for any size binary data
}

#[derive(Debug)]
pub enum DataType {
    Integer(i64),
    Boolean(bool),
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

pub fn convert_field(field: u64, to_type: &ColumnType) -> DataType {
    match to_type {
        ColumnType::Integer => DataType::Integer(field as i64),
        ColumnType::Boolean => DataType::Boolean(field != 0),
        ColumnType::Text => DataType::Text(field.to_le_bytes().map(|byte| byte as char)),
        ColumnType::Clob => DataType::ClobRef(field),
        ColumnType::Blob => DataType::BlobRef(field),
    }
}

pub fn convert_row(raw_row: RawRow, table_info: &TableInfo) -> Row {
    raw_row
        .iter()
        .zip(table_info.iter())
        .map(|(field, (_, to_type))| convert_field(field.to_owned(), to_type))
        .collect()
}

pub fn convert_row_field(raw_row: &RawRow, to_type: &ColumnType, offset: u64) -> Option<DataType> {
    Some(convert_field(*raw_row.get(offset as usize)?, to_type))
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(l0), Self::Integer(r0)) => l0 == r0,
            (Self::Boolean(l0), Self::Boolean(r0)) => l0 == r0,
            (Self::Text(l0), Self::Text(r0)) => l0 == r0,
            (Self::ClobRef(l0), Self::ClobRef(r0)) => l0 == r0,
            (Self::BlobRef(l0), Self::BlobRef(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Integer(l0), Self::Integer(r0)) => l0.partial_cmp(r0),
            (Self::Boolean(l0), Self::Boolean(r0)) => l0.partial_cmp(r0),
            (Self::Text(l0), Self::Text(r0)) => l0.partial_cmp(r0),
            (Self::ClobRef(l0), Self::ClobRef(r0)) => l0.partial_cmp(r0),
            (Self::BlobRef(l0), Self::BlobRef(r0)) => l0.partial_cmp(r0),
            _ => None,
        }
    }
}

impl From<i64> for DataType {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<bool> for DataType {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<[char; 8]> for DataType {
    fn from(value: [char; 8]) -> Self {
        Self::Text(value)
    }
}

pub trait AsRawRows {
    fn as_rows(&self, columns: usize) -> Vec<RawRow>;
    fn as_filtered_rows<P>(&self, columns: usize, predicate: &mut P) -> Vec<RawRow>
    where
        P: FnMut(&RawRow) -> bool;
}

impl AsRawRows for Block {
    fn as_rows(&self, columns: usize) -> Vec<RawRow> {
        self.as_filtered_rows(columns, &mut |_| true)
    }

    fn as_filtered_rows<P>(&self, columns: usize, predicate: &mut P) -> Vec<RawRow>
    where
        P: FnMut(&RawRow) -> bool,
    {
        let chunk_size = columns * COLUMN_WIDTH;
        self.chunks_exact(chunk_size)
            .map(|row| row.chunks_exact(COLUMN_WIDTH).map(LE::read_u64).collect())
            .filter(|row: &RawRow| row[0] != 0u64) // Filter any rows with a 0 id
            .filter(predicate)
            .collect::<Vec<RawRow>>()
    }
}

impl AsRawRows for Vec<Block> {
    fn as_rows(&self, columns: usize) -> Vec<RawRow> {
        self.as_filtered_rows(columns, &mut |_| true)
    }

    fn as_filtered_rows<P>(&self, columns: usize, predicate: &mut P) -> Vec<RawRow>
    where
        P: FnMut(&RawRow) -> bool,
    {
        self.iter()
            .flat_map(|block| block.as_filtered_rows(columns, predicate))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_table_info() -> TableInfo {
        vec![
            ("ID".into(), ColumnType::Integer),
            ("value".into(), ColumnType::Integer),
            ("is_munged".into(), ColumnType::Boolean),
            ("munged_value".into(), ColumnType::Text),
            ("plethora".into(), ColumnType::Clob),
            ("amorphous".into(), ColumnType::Blob),
        ]
    }

    #[test]
    fn test_table_info_mapping() {
        let table_info = make_table_info();
        let info_map = map_table_info(&table_info);

        assert_eq!(table_info.len(), info_map.len());

        for (index, (name, typing)) in table_info.iter().enumerate() {
            let (type_of, offset) = info_map.get(name).unwrap();
            assert_eq!(type_of, typing);
            assert_eq!(*offset, index as u64);
        }
    }

    #[test]
    fn test_field_and_row_conversion() {
        let table_info = make_table_info();
        let raw_row: RawRow = vec![1u64, 1u64, 0u64, 0u64, 64u64, 128u64];

        for item in convert_row(raw_row, &table_info) {
            match item {
                DataType::Integer(val) => assert_eq!(val, 1i64),
                DataType::Boolean(val) => assert_eq!(val, false),
                DataType::Text(val) => assert_eq!(val, [0x0 as char; 8]),
                DataType::ClobRef(val) => assert_eq!(val, 64u64),
                DataType::BlobRef(val) => assert_eq!(val, 128u64),
            }
        }
    }
}
