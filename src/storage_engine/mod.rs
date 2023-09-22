// Rust Builtin Imports

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::Read;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, io::Write};

// Third party library imports
use serde_json::from_str;

use crate::common::{AsRawRows, DataType, RawRow};
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

    pub fn execute(&mut self, action: Action) -> Result<Reaction, Error> {
        match action {
            Action::GetAll(table_name) => {
                let (qid, schema) = self.begin_query(table_name)?;
                Ok(Reaction::QueryStart { schema, qid })
            }
            Action::GetMore(qid) => {
                if let Some(data) = self.queries.remove(&qid) {
                    Ok(Reaction::Data(data))
                } else {
                    Ok(Reaction::Empty)
                }
            }
        }
    }

    pub fn begin_query(&mut self, query: String) -> Result<(u64, TableInfoMap), Error> {
        let (table_schema, data) = self.load(&query)?;
        let mut qid = rand::random();
        // Make sure that qid isn't in use...
        while self.queries.contains_key(&qid) {
            qid = rand::random();
        }
        self.queries.insert(qid, data.as_rows(table_schema.len()));
        Ok((qid, table_schema))
    }
}

pub enum Action {
    GetAll(String),
    GetMore(u64),
}

pub enum Reaction {
    QueryStart { schema: TableInfoMap, qid: u64 },
    Data(Vec<RawRow>),
    Empty,
}
