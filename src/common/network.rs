use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::TcpStream,
};

use serde::{Deserialize, Serialize};

use crate::common::error::Error;

use super::{RawRow, TableInfoMap};

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

    pub fn write_all(&mut self, buf: &[u8]) -> Result<(), Error> {
        self.writer.write_all(buf)?;
        self.writer.write_all(&[0xA])?;
        self.writer.flush()?;
        Ok(())
    }
}

pub trait Client {
    fn send(&mut self, request: &RequestType) -> Result<(), Error>;
    fn receive(&mut self) -> Result<ResponseType, Error>;
}

impl Client for BufSocket {
    fn send(&mut self, request: &RequestType) -> Result<(), Error> {
        let buf = serde_json::to_vec(request)?;
        self.write_all(&buf)
    }

    fn receive(&mut self) -> Result<ResponseType, Error> {
        let s = self.read_line()?;
        let response = serde_json::from_str(&s)?;
        Ok(response)
    }
}

pub trait Server {
    fn send(&mut self, response: &ResponseType) -> Result<(), Error>;
    fn receive(&mut self) -> Result<RequestType, Error>;
}

impl Server for BufSocket {
    fn send(&mut self, response: &ResponseType) -> Result<(), Error> {
        let buf = serde_json::to_vec(response)?;
        self.write_all(&buf)
    }

    fn receive(&mut self) -> Result<RequestType, Error> {
        let s = self.read_line()?;
        let request = serde_json::from_str(&s)?;
        Ok(request)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RequestType {
    Query(String),
    More(u64),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ResponseType {
    Error(Error),
    QueryHandle { schema: TableInfoMap, qid: u64 },
    Data(Vec<RawRow>),
    Empty,
}
