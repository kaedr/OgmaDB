use std::collections::{BTreeMap, HashMap};

use crate::common::Block;

struct Cache {
    tables: HashMap<String, TableCache>,
}

struct TableCache {
    row_length: u64,
    blocks: BTreeMap<u64, SmartBlock>,
}

struct SmartBlock {
    rows: Vec<RowInfo>,
    block: Block,
}

struct RowInfo {
    id: u64,
    offset: u64,
}
