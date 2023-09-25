// Rust Builtin Imports

use std::cmp::Ordering;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::Read;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, io::Write};

// Third party library imports
use serde_json::from_str;

use crate::common::{convert_row_field, AsRawRows, DataType, RawRow};
// First party library imports
use crate::common::{error::Error, map_table_info, Block, DBSchema, TableInfoMap, BLOCK_SIZE};

mod cache;

struct PathInfo<'a> {
    base_path: &'a Path,
    db_name: &'a OsStr,
    extension: &'a OsStr,
}

impl<'a> PathInfo<'a> {
    fn from_path(full_path: &'a Path) -> Option<Self> {
        if let (Some(base_path), Some(db_name), Some(extension)) = (
            full_path.parent(),
            full_path.file_stem(),
            full_path.extension(),
        ) {
            Some(Self {
                base_path,
                db_name,
                extension,
            })
        } else {
            None
        }
    }

    fn generate_table_path(&self, table_name: &String) -> PathBuf {
        let mut path = PathBuf::from(self.base_path);
        let mut table_filename = OsString::new();
        table_filename.push(self.db_name);
        table_filename.push("_");
        table_filename.push(table_name);
        table_filename.push(".");
        table_filename.push(self.extension);

        path.push(table_filename);
        path
    }
}

pub struct DataBase {
    schema_file: File,
    schema: DBSchema,
    tables: HashMap<String, File>,
    queries: HashMap<u64, Vec<RawRow>>,
}

impl DataBase {
    pub fn create(path: &Path, schema: DBSchema) -> Result<Self, Error> {
        if let Some(path_info) = PathInfo::from_path(path) {
            let mut schema_file = File::create(path)?;
            schema_file.write_all(&serde_json::to_vec(&schema)?)?;

            let mut tables = HashMap::new();

            for table_name in schema.keys() {
                let table_path = path_info.generate_table_path(table_name);
                let table_file = File::create(table_path)?;
                tables.insert(table_name.to_owned(), table_file);
            }

            Ok(DataBase {
                schema_file,
                schema,
                tables,
                queries: HashMap::new(),
            })
        } else {
            Err(Error::PathError(format!(
                "Failed to parse PathInfo from {}",
                &path.display()
            )))
        }
    }

    pub fn open(path: &Path) -> Result<Self, Error> {
        if let Some(path_info) = PathInfo::from_path(path) {
            let mut schema_file = File::options().read(true).write(true).open(path)?;

            let mut raw_schema = String::new();
            schema_file.read_to_string(&mut raw_schema)?;

            let schema: DBSchema = from_str(&raw_schema)?;

            let mut tables = HashMap::new();

            for table_name in schema.keys() {
                let table_path = path_info.generate_table_path(table_name);
                let table_file = File::options().read(true).write(true).open(table_path)?;
                tables.insert(table_name.to_owned(), table_file);
            }

            Ok(DataBase {
                schema_file,
                schema,
                tables,
                queries: HashMap::new(),
            })
        } else {
            Err(Error::PathError(format!(
                "Failed to parse PathInfo from {}",
                &path.display()
            )))
        }
    }

    pub fn store(&self, table_name: &str, data: Vec<Block>) -> Result<(), Error> {
        match self.tables.get(table_name) {
            Some(table) => {
                for (index, datum) in data.iter().enumerate() {
                    table.write_at(datum, (index * BLOCK_SIZE) as u64)?;
                }
                Ok(())
            }
            None => Err(Error::SchemaError(format!(
                "Table {} does not exist",
                table_name
            ))),
        }
    }

    pub fn store_block_at(
        &self,
        table_name: &str,
        offset: u64,
        block: &Block,
    ) -> Result<(), Error> {
        match self.tables.get(table_name) {
            Some(table) => {
                table.write_at(block, offset * BLOCK_SIZE as u64)?;
                Ok(())
            }
            None => Err(Error::SchemaError(format!(
                "Table {} does not exist",
                table_name
            ))),
        }
    }

    pub fn load(&self, table_name: &str) -> Result<(TableInfoMap, Vec<Block>), Error> {
        match (self.tables.get(table_name), self.schema.get(table_name)) {
            (Some(table), Some(table_info)) => {
                let table_map = map_table_info(table_info);
                let mut data = Vec::new();

                let mut buf = [0u8; BLOCK_SIZE];
                let mut offset = 0u64;
                // Behold, I learned how to Do While in Rust, and it's... interesting
                while {
                    // Read a block from the file
                    let bytes_read = table.read_at(&mut buf, offset * BLOCK_SIZE as u64)?;
                    bytes_read > 0
                } {
                    // push it to our Vec
                    data.push(buf);
                    // Set state for next iteration
                    offset += 1;
                    buf = [0u8; BLOCK_SIZE];
                }
                Ok((table_map, data))
            }
            // TODO: Maybe come up with better error for missing data file
            (None, Some(_)) => Err(Error::SchemaError(format!(
                "Table {} is missing its data file",
                table_name
            ))),
            (Some(_), None) => Err(Error::SchemaError(format!(
                "Table {} has data file, but is missing in schema",
                table_name
            ))),
            (None, None) => Err(Error::SchemaError(format!(
                "Table {} does not exist",
                table_name
            ))),
        }
    }

    pub fn load_block_at(&self, table_name: &str, offset: u64) -> Result<Block, Error> {
        match self.tables.get(table_name) {
            Some(table) => {
                let mut buf = [0u8; BLOCK_SIZE];
                table.read_at(&mut buf, offset * BLOCK_SIZE as u64)?;
                Ok(buf)
            }
            None => Err(Error::SchemaError(format!(
                "Table {} does not exist",
                table_name
            ))),
        }
    }

    pub fn execute(&mut self, action: Action) -> Reaction {
        match action {
            Action::GetAll(query) => match self.begin_query(query, vec![FilterType::All]) {
                Ok((qid, schema)) => Reaction::QueryStart { schema, qid },
                Err(err) => Reaction::Error(err),
            },
            Action::GetMore(qid) => {
                if let Some(data) = self.queries.remove(&qid) {
                    Reaction::Data(data)
                } else {
                    Reaction::Empty
                }
            }
            Action::GetFiltered(query, filters) => match self.begin_query(query, filters) {
                Ok((qid, schema)) => Reaction::QueryStart { schema, qid },
                Err(err) => Reaction::Error(err),
            },
        }
    }

    fn begin_query(
        &mut self,
        query: String,
        filters: Vec<FilterType>,
    ) -> Result<(u64, TableInfoMap), Error> {
        let (table_schema, data) = self.load(&query)?;
        let mut qid = rand::random();
        // Make sure that qid isn't in use...
        while self.queries.contains_key(&qid) {
            qid = rand::random();
        }
        self.queries.insert(
            qid,
            data.as_filtered_rows(table_schema.len(), &mut |raw_row| {
                apply_filters(raw_row, &filters, &table_schema)
            }),
        );
        Ok((qid, table_schema))
    }
}

fn apply_filters(raw_row: &RawRow, filters: &Vec<FilterType>, table_schema: &TableInfoMap) -> bool {
    filters.iter().all(|filter| apply_filter(raw_row, filter, table_schema))
}

fn apply_filter(raw_row: &RawRow, filter: &FilterType, table_schema: &TableInfoMap) -> bool {
    match filter {
        FilterType::GreaterThanEqualTo(column, value) => {
            has_ordering(
                value,
                field_from_row(raw_row, column, table_schema),
                Ordering::Equal,
            ) || has_ordering(
                value,
                field_from_row(raw_row, column, table_schema),
                Ordering::Greater,
            )
        }
        FilterType::GreaterThan(column, value) => has_ordering(
            value,
            field_from_row(raw_row, column, table_schema),
            Ordering::Greater,
        ),
        FilterType::LessThanEqualTo(column, value) => {
            has_ordering(
                value,
                field_from_row(raw_row, column, table_schema),
                Ordering::Equal,
            ) || has_ordering(
                value,
                field_from_row(raw_row, column, table_schema),
                Ordering::Less,
            )
        }
        FilterType::LessThan(column, value) => has_ordering(
            value,
            field_from_row(raw_row, column, table_schema),
            Ordering::Less,
        ),
        FilterType::EqualTo(column, value) => has_ordering(
            value,
            field_from_row(raw_row, column, table_schema),
            Ordering::Equal,
        ),
        FilterType::Between(column, lower, upper) => {
            has_ordering(
                lower,
                field_from_row(raw_row, column, table_schema),
                Ordering::Less,
            ) && has_ordering(
                upper,
                field_from_row(raw_row, column, table_schema),
                Ordering::Greater,
            )
        }
        FilterType::In(column, values) => values.iter().any(|value| {
            has_ordering(
                value,
                field_from_row(raw_row, column, table_schema),
                Ordering::Equal,
            )
        }),
        FilterType::All => true,
    }
}

fn field_from_row(
    raw_row: &RawRow,
    column_name: &String,
    table_schema: &TableInfoMap,
) -> Option<DataType> {
    let (to_type, offset) = table_schema.get(column_name)?;
    convert_row_field(raw_row, to_type, *offset)
}

fn has_ordering(left: &DataType, right: Option<DataType>, order: Ordering) -> bool {
    if let Some(right) = right {
        println!("Comparing {:?} to {:?} -- {:?}", left, right, left.partial_cmp(&right));
        left.partial_cmp(&right) == Some(order)
    } else {
        false
    }
}

pub enum Action {
    GetAll(String),
    GetFiltered(String, Vec<FilterType>),
    GetMore(u64),
}

pub enum FilterType {
    GreaterThanEqualTo(String, DataType),
    GreaterThan(String, DataType),
    LessThanEqualTo(String, DataType),
    LessThan(String, DataType),
    EqualTo(String, DataType),
    Between(String, DataType, DataType),
    In(String, Vec<DataType>),
    All,
}

pub enum Reaction {
    Error(Error),
    QueryStart { schema: TableInfoMap, qid: u64 },
    Data(Vec<RawRow>),
    Empty,
}

#[cfg(test)]
mod tests {
    use byteorder::{ByteOrder, LE};

    use crate::common::{ColumnType, TableInfo};

    use super::*;

    fn test_data() -> (RawRow, TableInfoMap) {
        let word = LE::read_u64("bird\0\0\0\0".as_bytes());
        let raw_row: RawRow = vec![8675309u64, 0u64, word];
        let table_info: TableInfo = vec![
            ("ID".into(), ColumnType::Integer),
            ("truthy".into(), ColumnType::Boolean),
            ("word".into(), ColumnType::Text),
        ];
        (raw_row, map_table_info(&table_info))
    }

    #[test]
    fn test_filter_gtet() {
        let (raw_row, table_schema) = test_data();
        let (int_over, int_eq, int_under) = (
            FilterType::GreaterThanEqualTo("ID".into(), DataType::Integer(8675310i64)),
            FilterType::GreaterThanEqualTo("ID".into(), DataType::Integer(8675309i64)),
            FilterType::GreaterThanEqualTo("ID".into(), DataType::Integer(8675308i64)),
        );
        assert!(
            apply_filter(&raw_row, &int_over, &table_schema),
            "Int GTET failed on greater."
        );
        assert!(
            apply_filter(&raw_row, &int_eq, &table_schema),
            "Int GTET failed on equal."
        );
        assert!(
            !apply_filter(&raw_row, &int_under, &table_schema),
            "Int GTET failed of lesser."
        );

        let (text_under, text_eq, text_over) = (
            FilterType::GreaterThanEqualTo("word".into(), DataType::Text(['b'; 8])),
            FilterType::GreaterThanEqualTo(
                "word".into(),
                DataType::Text(['b', 'i', 'r', 'd', '\0', '\0', '\0', '\0']),
            ),
            FilterType::GreaterThanEqualTo("word".into(), DataType::Text(['c'; 8])),
        );
        assert!(
            apply_filter(&raw_row, &text_over, &table_schema),
            "Text GTET failed on greater."
        );
        assert!(
            apply_filter(&raw_row, &text_eq, &table_schema),
            "Text GTET failed on equal."
        );
        assert!(
            !apply_filter(&raw_row, &text_under, &table_schema),
            "Text GTET failed of lesser."
        );
    }

    #[test]
    fn test_filter_gt() {
        let (raw_row, table_schema) = test_data();
        let (int_over, int_eq, int_under) = (
            FilterType::GreaterThan("ID".into(), DataType::Integer(8675310i64)),
            FilterType::GreaterThan("ID".into(), DataType::Integer(8675309i64)),
            FilterType::GreaterThan("ID".into(), DataType::Integer(8675308i64)),
        );
        assert!(
            apply_filter(&raw_row, &int_over, &table_schema),
            "Int GT failed on greater."
        );
        assert!(
            !apply_filter(&raw_row, &int_eq, &table_schema),
            "Int GT failed on equal."
        );
        assert!(
            !apply_filter(&raw_row, &int_under, &table_schema),
            "Int GT failed of lesser."
        );

        let (text_under, text_eq, text_over) = (
            FilterType::GreaterThan("word".into(), DataType::Text(['b'; 8])),
            FilterType::GreaterThan(
                "word".into(),
                DataType::Text(['b', 'i', 'r', 'd', '\0', '\0', '\0', '\0']),
            ),
            FilterType::GreaterThan("word".into(), DataType::Text(['c'; 8])),
        );
        assert!(
            apply_filter(&raw_row, &text_over, &table_schema),
            "Text GT failed on greater."
        );
        assert!(
            !apply_filter(&raw_row, &text_eq, &table_schema),
            "Text GT failed on equal."
        );
        assert!(
            !apply_filter(&raw_row, &text_under, &table_schema),
            "Text GT failed of lesser."
        );
    }

    #[test]
    fn test_filter_ltet() {
        let (raw_row, table_schema) = test_data();
        let (int_over, int_eq, int_under) = (
            FilterType::LessThanEqualTo("ID".into(), DataType::Integer(8675310i64)),
            FilterType::LessThanEqualTo("ID".into(), DataType::Integer(8675309i64)),
            FilterType::LessThanEqualTo("ID".into(), DataType::Integer(8675308i64)),
        );
        assert!(
            !apply_filter(&raw_row, &int_over, &table_schema),
            "Int LTET failed on greater."
        );
        assert!(
            apply_filter(&raw_row, &int_eq, &table_schema),
            "Int LTET failed on equal."
        );
        assert!(
            apply_filter(&raw_row, &int_under, &table_schema),
            "Int LTET failed of lesser."
        );

        let (text_under, text_eq, text_over) = (
            FilterType::LessThanEqualTo("word".into(), DataType::Text(['b'; 8])),
            FilterType::LessThanEqualTo(
                "word".into(),
                DataType::Text(['b', 'i', 'r', 'd', '\0', '\0', '\0', '\0']),
            ),
            FilterType::LessThanEqualTo("word".into(), DataType::Text(['c'; 8])),
        );
        assert!(
            !apply_filter(&raw_row, &text_over, &table_schema),
            "Text LTET failed on greater."
        );
        assert!(
            apply_filter(&raw_row, &text_eq, &table_schema),
            "Text LTET failed on equal."
        );
        assert!(
            apply_filter(&raw_row, &text_under, &table_schema),
            "Text LTET failed of lesser."
        );
    }

    #[test]
    fn test_filter_lt() {
        let (raw_row, table_schema) = test_data();
        let (int_over, int_eq, int_under) = (
            FilterType::LessThan("ID".into(), DataType::Integer(8675310i64)),
            FilterType::LessThan("ID".into(), DataType::Integer(8675309i64)),
            FilterType::LessThan("ID".into(), DataType::Integer(8675308i64)),
        );
        assert!(
            !apply_filter(&raw_row, &int_over, &table_schema),
            "Int LT failed on greater."
        );
        assert!(
            !apply_filter(&raw_row, &int_eq, &table_schema),
            "Int LT failed on equal."
        );
        assert!(
            apply_filter(&raw_row, &int_under, &table_schema),
            "Int LT failed of lesser."
        );

        let (text_under, text_eq, text_over) = (
            FilterType::LessThan("word".into(), DataType::Text(['b'; 8])),
            FilterType::LessThan(
                "word".into(),
                DataType::Text(['b', 'i', 'r', 'd', '\0', '\0', '\0', '\0']),
            ),
            FilterType::LessThan("word".into(), DataType::Text(['c'; 8])),
        );
        assert!(
            !apply_filter(&raw_row, &text_over, &table_schema),
            "Text LT failed on greater."
        );
        assert!(
            !apply_filter(&raw_row, &text_eq, &table_schema),
            "Text LT failed on equal."
        );
        assert!(
            apply_filter(&raw_row, &text_under, &table_schema),
            "Text LT failed of lesser."
        );
    }

    #[test]
    fn test_filter_et() {
        let (raw_row, table_schema) = test_data();
        let (int_over, int_eq, int_under) = (
            FilterType::EqualTo("ID".into(), DataType::Integer(8675310i64)),
            FilterType::EqualTo("ID".into(), DataType::Integer(8675309i64)),
            FilterType::EqualTo("ID".into(), DataType::Integer(8675308i64)),
        );
        assert!(
            !apply_filter(&raw_row, &int_over, &table_schema),
            "Int ET failed on greater."
        );
        assert!(
            apply_filter(&raw_row, &int_eq, &table_schema),
            "Int ET failed on equal."
        );
        assert!(
            !apply_filter(&raw_row, &int_under, &table_schema),
            "Int ET failed of lesser."
        );

        let (text_under, text_eq, text_over) = (
            FilterType::EqualTo("word".into(), DataType::Text(['b'; 8])),
            FilterType::EqualTo(
                "word".into(),
                DataType::Text(['b', 'i', 'r', 'd', '\0', '\0', '\0', '\0']),
            ),
            FilterType::EqualTo("word".into(), DataType::Text(['c'; 8])),
        );
        assert!(
            !apply_filter(&raw_row, &text_over, &table_schema),
            "Text ET failed on greater."
        );
        assert!(
            apply_filter(&raw_row, &text_eq, &table_schema),
            "Text ET failed on equal."
        );
        assert!(
            !apply_filter(&raw_row, &text_under, &table_schema),
            "Text ET failed of lesser."
        );
    }

    #[test]
    fn test_filter_bt() {
        let (raw_row, table_schema) = test_data();
        let (int_over, int_eq, int_under) = (
            FilterType::Between(
                "ID".into(),
                DataType::Integer(8675310i64),
                DataType::Integer(8675312i64),
            ),
            FilterType::Between(
                "ID".into(),
                DataType::Integer(8675308i64),
                DataType::Integer(8675310i64),
            ),
            FilterType::Between(
                "ID".into(),
                DataType::Integer(8675306i64),
                DataType::Integer(8675306i64),
            ),
        );
        assert!(
            !apply_filter(&raw_row, &int_over, &table_schema),
            "Int BT failed on below."
        );
        assert!(
            apply_filter(&raw_row, &int_eq, &table_schema),
            "Int BT failed on within."
        );
        assert!(
            !apply_filter(&raw_row, &int_under, &table_schema),
            "Int BT failed of above."
        );

        let (text_over, text_eq, text_under) = (
            FilterType::Between(
                "word".into(),
                DataType::Text(['a'; 8]),
                DataType::Text(['b'; 8]),
            ),
            FilterType::Between(
                "word".into(),
                DataType::Text(['a'; 8]),
                DataType::Text(['c'; 8]),
            ),
            FilterType::Between(
                "word".into(),
                DataType::Text(['c'; 8]),
                DataType::Text(['d'; 8]),
            ),
        );
        assert!(
            !apply_filter(&raw_row, &text_over, &table_schema),
            "Text BT failed on greater."
        );
        assert!(
            apply_filter(&raw_row, &text_eq, &table_schema),
            "Text BT failed on equal."
        );
        assert!(
            !apply_filter(&raw_row, &text_under, &table_schema),
            "Text BT failed of lesser."
        );
    }

    #[test]
    fn test_filter_in() {
        let (raw_row, table_schema) = test_data();
        let (int_within, int_without) = (
            FilterType::In(
                "ID".into(),
                vec![
                    DataType::Integer(8675308i64),
                    DataType::Integer(8675309i64),
                    DataType::Integer(8675310i64),
                ],
            ),
            FilterType::In(
                "ID".into(),
                vec![
                    DataType::Integer(8675308i64),
                    DataType::Integer(8675301i64),
                    DataType::Integer(8675310i64),
                ],
            ),
        );
        assert!(
            apply_filter(&raw_row, &int_within, &table_schema),
            "Int IN failed on in"
        );
        assert!(
            !apply_filter(&raw_row, &int_without, &table_schema),
            "Int IN failed on out"
        );

        let (bool_within, bool_without) = (
            FilterType::In(
                "truthy".into(),
                vec![DataType::Boolean(true), DataType::Boolean(false)],
            ),
            FilterType::In(
                "truthy".into(),
                vec![DataType::Boolean(true), DataType::Boolean(true)],
            ),
        );
        assert!(
            apply_filter(&raw_row, &bool_within, &table_schema),
            "Bool IN failed on in"
        );
        assert!(
            !apply_filter(&raw_row, &bool_without, &table_schema),
            "Bool IN failed on out"
        );

        let (text_within, text_without) = (
            FilterType::In(
                "word".into(),
                vec![
                    DataType::Text(['b'; 8]),
                    DataType::Text(['b'; 8]),
                    DataType::Text(['b', 'i', 'r', 'd', '\0', '\0', '\0', '\0']),
                ],
            ),
            FilterType::In(
                "word".into(),
                vec![
                    DataType::Text(['a'; 8]),
                    DataType::Text(['b'; 8]),
                    DataType::Text(['c'; 8]),
                ],
            ),
        );
        assert!(
            apply_filter(&raw_row, &text_within, &table_schema),
            "Text IN failed on in"
        );
        assert!(
            !apply_filter(&raw_row, &text_without, &table_schema),
            "Text IN failed on out"
        );
    }

    #[test]
    fn test_filter_all() {
        let (raw_row, table_schema) = test_data();
        let all = FilterType::All;
        assert!(apply_filter(&raw_row, &all, &table_schema))
    }
}
