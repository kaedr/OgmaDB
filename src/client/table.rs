use ogma_db::common::{TableInfoMap, RawRow, };

pub struct Table {
    name: String,
    table_info: TableInfoMap,
    relations: Vec<RawRow> 
}

impl Table {
    pub fn new(name: String, table_info: TableInfoMap) -> Self {
        Self {
            name,
            table_info,
            relations: Vec::new(),
        }
    }

    pub fn from(name: String, table_info: TableInfoMap, relations: Vec<RawRow>) -> Self{
        Self {
            name, 
            table_info, 
            relations
        }
    }

    pub fn insert_relation(mut self, relation: RawRow){
        self.relations.push(relation)
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

    pub fn select(self, cols: Vec<String>) -> String{
        unimplemented!()
    }
}
