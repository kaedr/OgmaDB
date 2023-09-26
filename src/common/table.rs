use crate::common::{TableInfo, Row, ColumnHeader};

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
        Self {
            name, 
            table_info, 
            relations
        }
    }
    
    pub fn describe(self) -> String{
        let mut cols = self.table_info.clone().into_iter();  
        let output = String::new(); 
        if let Some(col) = cols.next(){
            format!("{}{}{:?}",output, "\n", col);      //TODO probably won't want to use {:?} in production
        }
        output
    }

    pub fn show(self) -> String{
        let mut cols = self.table_info.clone().into_iter();  
        let output = String::new();
        if let Some(col) = cols.next(){
            format!("{}{}{:?}", output, "\t", col);     //TODO won't want :? in production
        }  
        output
    }
}
