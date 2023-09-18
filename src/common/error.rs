use serde::de::Visitor;

use serde::Deserialize;

use serde;

use serde::Serialize;

use std;

#[derive(Debug)]
pub enum Error {
    IOError(std::io::Error),
    PathError(String),
    SerdeError(serde_json::Error),
    SchemaError(String),
    // StringForm exists for client deserialization, since we can't guarantee
    // underlying error types will give us a from_string method
    StringForm(String),
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Error::IOError(err) => serializer.serialize_str(err.to_string().as_ref()),
            Error::PathError(err) => serializer.serialize_str(err),
            Error::SerdeError(err) => serializer.serialize_str(err.to_string().as_ref()),
            Error::SchemaError(err) => serializer.serialize_str(err),
            Error::StringForm(err) => serializer.serialize_str(err),
        }
    }
}

impl<'de> Deserialize<'de> for Error {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(StringFormVisitor)
    }
}

pub(crate) struct StringFormVisitor;

impl<'de> Visitor<'de> for StringFormVisitor {
    type Value = Error;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a storage_engine::Error in String form")
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Error::StringForm(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error, {
        Ok(Error::StringForm(v.to_owned()))
    }
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
