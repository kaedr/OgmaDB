// Rust Builtin Imports

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, io::Write};

// Third party library imports

// First party library imports

use crate::common::{ColumnType, DBSchema, TableInfo};

fn table_path(base_path: &Path, db_name: &OsStr, extension: &OsStr, table_name: &str) -> PathBuf {
    let mut path = PathBuf::from(base_path);
    let mut table_filename = OsString::new();
    table_filename.push(db_name);
    table_filename.push("_");
    table_filename.push(table_name);
    table_filename.push(".");
    table_filename.push(extension);

    path.push(table_filename);
    path
}

pub enum Error {
    IOError(std::io::Error),
    PathError(String),
    SerdeError(serde_json::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IOError(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::SerdeError(value)
    }
}

pub struct DataBase {
    schema: DBSchema,
    tables: HashMap<String, File>,
}

impl DataBase {
    pub fn create(path: PathBuf, schema: DBSchema) -> Result<Self, Error> {
        if let (Some(base_path), Some(db_name), Some(extension)) =
            (path.parent(), path.file_stem(), path.extension())
        {
            let mut schema_file = File::create(&path)?;
            schema_file.write_all(&serde_json::to_vec(&schema)?)?;

            let mut tables = HashMap::new();

            for table_name in schema.keys() {
                let table_path = table_path(base_path, db_name, extension, table_name);
                let table_file = File::create(table_path)?;
                tables.insert(table_name.to_owned(), table_file);
            }

            Ok(DataBase { schema, tables })
        } else {
            Err(Error::PathError(format!(
                "Failed to parse DB Name from {}",
                &path.display()
            )))
        }
    }

    //     pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
    //         let file = File::options()
    //             .read(true)
    //             .write(true)
    //             .open(path)?;

    //         let mut schema_size_bytes = [0u8; USIZE_BYTES];

    //         file.read_at(&mut schema_size_bytes, 0)?;

    //         let schema_size = usize::from_ne_bytes(schema_size_bytes);

    //         let mut schema_buffer = vec![0u8; schema_size];

    //         file.read_exact_at(&mut schema_buffer, USIZE_BYTES as u64)?;

    //         let tables = from_slice(&schema_buffer)?;

    //         println!("{:?}", tables);

    //         Ok(DataFile { handle: file, schema: Schema {tables} })
    //     }
}

pub fn fool_around() {
    let mut schema = HashMap::new();
    schema.insert(
        "attributes".into(),
        vec![
            ("Strength".into(), ColumnType::Int),
            ("Dexterity".into(), ColumnType::Int),
            ("Constitution".into(), ColumnType::Int),
            ("Intelligence".into(), ColumnType::Int),
            ("Wisdom".into(), ColumnType::Int),
            ("Charisma".into(), ColumnType::Int),
        ],
    );
    schema.insert(
        "currency".into(),
        vec![
            ("Platinum".into(), ColumnType::Int),
            ("Gold".into(), ColumnType::Int),
            ("Silver".into(), ColumnType::Int),
            ("Copper".into(), ColumnType::Int),
        ],
    );

    match DataBase::create("./data/test.ogmadb".into(), schema) {
        Ok(_) => println!("Success Creating!"),
        Err(_) => println!("broke"),
    }

    // match Database::open("./test.ogmadb") {
    //     Ok(_) => println!("Success Reading!"),
    //     Err(err) => println!("{err}"),
    // }
}
