use std::collections::HashMap;

const BLOCK_SIZE: u64 = 8192;
type Block = [u8; BLOCK_SIZE];

enum ColumnType {
    Int,   // i64
    Boolean,
    String, // for short strings
    Clob,   // for long strings
    Blob,   // for any size binary data
}

type ColumnHeader = (String, ColumnType);
type TableInfo = Vec<ColumnHeader>;
type SchemaMap = HashMap<String, (ColumnType, u64)>;
type DBSchema = HashMap<String, TableInfo>;
