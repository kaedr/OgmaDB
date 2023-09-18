use std::{collections::HashMap, path::Path};

use ogma_db::{common::*, storage_engine::DataBase};

pub fn init_test_db() {
    let mut schema: DBSchema = HashMap::new();
    schema.insert(
        "attributes".into(),
        vec![
            ("index".into(), ColumnType::Int),
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
            ("index".into(), ColumnType::Int),
            ("Platinum".into(), ColumnType::Int),
            ("Gold".into(), ColumnType::Int),
            ("Silver".into(), ColumnType::Int),
            ("Copper".into(), ColumnType::Int),
        ],
    );

    match DataBase::create(Path::new("./data/test.ogmadb"), schema) {
        Ok(db) => {
            println!("Success Creating!");
            let data = vec![mint()];
            db.store("currency", data).unwrap();
        }
        Err(_) => println!("broke creating"),
    }
}

fn mint() -> Block {
    const ROW_WIDTH: usize = 5 * COLUMN_WIDTH;
    const ROWS_IN_BLOCK: usize = BLOCK_SIZE / ROW_WIDTH;
    let mut block = [0u8; BLOCK_SIZE];
    for index in 0..ROWS_IN_BLOCK {
        let row_id = index + 1;
        let mut row = [0u8; ROW_WIDTH];
        row[..8].clone_from_slice(&row_id.to_le_bytes());
        row[8..16].clone_from_slice(&(row_id % 100).to_le_bytes());
        row[16..24].clone_from_slice(&(row_id * 3 % 10).to_le_bytes());
        row[24..32].clone_from_slice(&(row_id * 5 % 10).to_le_bytes());
        row[32..].clone_from_slice(&(row_id * 7 % 10).to_le_bytes());
        block[index * ROW_WIDTH..index * ROW_WIDTH + ROW_WIDTH].clone_from_slice(&row);
        // println!("Row: {index} - {:?}", row)
    }
    // println!("{:?}", block);
    block
}
