use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::storage_engine::Error;

use super::{Row, TableInfoMap};

#[derive(Serialize, Deserialize, Debug)]
pub enum RequestType {
    Query(String),
}

impl RequestType {
    pub fn send<W>(&self, mut writer: W) -> Result<(), Error>
    where
        W: Write,
    {
        serde_json::to_writer(&mut writer, &self)?;
        Ok(writer.flush()?)
    }

    pub fn receive<R>(reader: R) -> Result<Self, Error>
    where
        R: Read,
    {
        Ok(serde_json::from_reader(reader)?)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ResponseType {
    Error(Error),
    Data(QueriedData),
}

impl ResponseType {
    pub fn send<W>(&self, mut writer: W) -> Result<(), Error>
    where
        W: Write,
    {
        serde_json::to_writer(&mut writer, &self)?;
        Ok(writer.flush()?)
    }

    pub fn receive<R>(reader: R) -> Result<Self, Error>
    where
        R: Read,
    {
        Ok(serde_json::from_reader(reader)?)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueriedData {
    schema: TableInfoMap,
    rows: Vec<Row>,
}

impl QueriedData {
    pub fn new(schema: TableInfoMap, rows: Vec<Row>) -> Self {
        Self { schema, rows }
    }
}
