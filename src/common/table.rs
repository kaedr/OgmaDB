use std::io;
use crate::common::{TableInfo, Row};

pub struct Table {
    name: String,
    table_info: TableInfo,
    relations: Vec<Row> 
}

impl Table {
    pub fn new(name: String, table_info: TableInfo) -> Self {
        Self {
            name,
            table_info,
            relations: Vec::new(),
        }
    }

    pub fn from(name: String, table_info: TableInfo, relations: Vec<Row>) -> Self{
        unimplemented!()
    }
    
    pub fn describe(self) -> io::Result<String>{
        let cols = self.table_info.into_iter();
        for col in cols{
            println!("{:?}", col);
        }
        Ok("".to_string())
    }
}

