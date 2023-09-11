// Rust Builtin Imports
use std::collections::{BTreeMap, HashMap};

// Third party library imports
use serde::{Deserialize, Serialize};

// These are indicators of the data types the columns will contain, not actual data types.
#[derive(Serialize, Deserialize, Debug)]
pub enum DataTypeLabel {
    UInt, // u64
    SInt, // i64
    // maybe some kind of fpoint data type?
    Boolean,
    String, // for short strings
    Clob,   // for long strings
    Blob,   // for any size binary data
            //and so on, whatever
}

type ColumnHeader = (String, DataTypeLabel); // (Column Name, Column Type). Note that these columns
                                             // are an abstraction which refers to positions in
                                             // relation vecs, not an actual data structure themselves.
type TableColumnHeaders = Vec<ColumnHeader>; // Vector containing all columns in table
type Relation = HashMap<String, DataType>;
type DataType = String; // This should eventually be an enum or something. String for now.
                        // reason: simplicity/tutorial adherence

struct Table {
    name: String,
    columns: TableColumnHeaders,
    relations: BTreeMap<usize, Relation>,
}

impl Table {
    fn new(name: String, columns: Vec<ColumnHeader>) -> Self {
        Self {
            name,
            columns,
            relations: BTreeMap::new(),
        }
    }
}
