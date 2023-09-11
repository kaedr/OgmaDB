// Rust Builtin Imports
use std::fs::File;
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::{collections::HashMap, io::Write};

// Third party library imports
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec};

// First party library imports
use crate::table::DataTypeLabel;

const PAGE_SIZE: u64 = 8192;
const USIZE_BYTES: usize = size_of::<usize>();
type Offset = u64;

pub struct DataFile {
    handle: File,
    schema: Schema,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    tables: HashMap<String, TableData>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableData {
    position: Offset,
    fields: Vec<DataTypeLabel>,
}

impl DataFile {
    pub fn new<P: AsRef<Path>>(path: P, schema: Schema) -> Result<Self, std::io::Error> {
        let mut file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        file.write_all(&schema.as_bytes()?)?;

        Ok(DataFile {
            handle: file,
            schema,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let mut file = File::options()
            .read(true)
            .write(true)
            .open(path)?;

        let mut schema_size_bytes = [0u8; USIZE_BYTES];

        file.read_at(&mut schema_size_bytes, 0)?;

        let schema_size = usize::from_ne_bytes(schema_size_bytes);

        let mut schema_buffer = vec![0u8; schema_size];

        file.read_exact_at(&mut schema_buffer, USIZE_BYTES as u64)?;

        let tables = from_slice(&schema_buffer)?;

        println!("{:?}", tables);

        Ok(DataFile { handle: file, schema: Schema {tables} })
    }
}

impl Schema {
    pub fn as_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut the_bytes = Vec::new();
        let mut ser_schema = to_vec(&self.tables)?;
        let schema_len = ser_schema.len().to_ne_bytes();
        the_bytes.extend_from_slice(&schema_len);
        the_bytes.append(&mut ser_schema);
        Ok(the_bytes)
    }
}

pub fn fool_around() {
    let a_table = TableData {
        position: 1,
        fields: vec![DataTypeLabel::UInt, DataTypeLabel::UInt],
    };
    let b_table = TableData {
        position: 2,
        fields: vec![
            DataTypeLabel::UInt,
            DataTypeLabel::SInt,
            DataTypeLabel::SInt,
        ],
    };

    let mut tables = HashMap::new();

    tables.insert("A".into(), a_table);
    tables.insert("B".into(), b_table);

    let schema = Schema { tables };

    match DataFile::new("./test.ogmadb", schema) {
        Ok(_) => println!("Success Creating!"),
        Err(err) => println!("{err}"),
    }

    match DataFile::open("./test.ogmadb") {
        Ok(_) => println!("Success Reading!"),
        Err(err) => println!("{err}"),
    }
}
