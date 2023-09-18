use std::{io::{Read, Write, BufReader, BufWriter, BufRead}, net::TcpStream};

use serde::{Deserialize, Serialize};

use crate::common::error::Error;

use super::{Row, TableInfoMap};

pub struct BufSocket {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl BufSocket {
    pub fn new(stream: TcpStream) -> Result<Self, Error> {
        let writer = BufWriter::new(stream.try_clone()?);
        let reader = BufReader::new(stream);
        Ok(Self { reader, writer })
    }

    pub fn read_line(&mut self) -> Result<String, Error> {
        let mut buf = String::new();
        self.reader.read_line(&mut buf)?;
        Ok(buf)
    }

    pub fn write_all(&mut self, buf: &[u8]) -> Result<(), Error>{
        self.writer.write_all(buf)?;
        self.writer.write_all(&[0xA])?;
        self.writer.flush()?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RequestType {
    Query(String),
}

impl RequestType {
    pub fn send(&self, buf_sock: &mut BufSocket) -> Result<(), Error> {
        let buf = serde_json::to_vec(self)?;
        buf_sock.write_all(&buf)
    }

    pub fn receive(buf_sock: &mut BufSocket) -> Result<Self, Error> {
        let s = buf_sock.read_line()?;
        let request = serde_json::from_str(&s)?;
        Ok(request)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ResponseType {
    Error(Error),
    Data(QueriedData),
}

impl ResponseType {
    pub fn send(&self, buf_sock: &mut BufSocket) -> Result<(), Error> {
        let buf = serde_json::to_vec(self)?;
        buf_sock.write_all(&buf)
    }

    pub fn receive(buf_sock: &mut BufSocket) -> Result<Self, Error> {
        let s = buf_sock.read_line()?;
        let response = serde_json::from_str(&s)?;
        Ok(response)
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
