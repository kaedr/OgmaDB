pub mod common;
pub mod parser;
pub mod query_engine;
pub mod storage_engine;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::{collections::HashMap, path::Path};

    use super::{common::*, storage_engine::DataBase};

    pub fn init_test_db() {
        let mut schema: DBSchema = HashMap::new();
        schema.insert(
            "attributes".into(),
            vec![
                ("index".into(), ColumnType::Integer),
                ("Strength".into(), ColumnType::Integer),
                ("Dexterity".into(), ColumnType::Integer),
                ("Constitution".into(), ColumnType::Integer),
                ("Intelligence".into(), ColumnType::Integer),
                ("Wisdom".into(), ColumnType::Integer),
                ("Charisma".into(), ColumnType::Integer),
            ],
        );
        schema.insert(
            "currency".into(),
            vec![
                ("index".into(), ColumnType::Integer),
                ("Platinum".into(), ColumnType::Integer),
                ("Gold".into(), ColumnType::Integer),
                ("Silver".into(), ColumnType::Integer),
                ("Copper".into(), ColumnType::Integer),
            ],
        );

        match DataBase::create(Path::new("./data/test.ogmadb"), schema) {
            Ok(db) => {
                println!("Success Creating!");
                let data = vec![
                    mint(0),
                    mint(204),
                    mint(408),
                    mint(612),
                    mint(816),
                    mint(1020),
                ];
                db.store("currency", data).unwrap();
            }
            Err(_) => println!("broke creating"),
        }
    }

    pub fn mint(start_id: usize) -> Block {
        const ROW_WIDTH: usize = 5 * COLUMN_WIDTH;
        const ROWS_IN_BLOCK: usize = BLOCK_SIZE / ROW_WIDTH;
        let mut block = [0u8; BLOCK_SIZE];
        for index in 0..ROWS_IN_BLOCK {
            let row_id = start_id + index + 1;
            let mut row = [0u8; ROW_WIDTH];
            row[..8].clone_from_slice(&row_id.to_le_bytes());
            row[8..16].clone_from_slice(&(row_id % 100).to_le_bytes());
            row[16..24].clone_from_slice(&(row_id * 3 % 10).to_le_bytes());
            row[24..32].clone_from_slice(&(row_id * 5 % 10).to_le_bytes());
            row[32..].clone_from_slice(&(row_id * 7 % 10).to_le_bytes());
            block[index * ROW_WIDTH..index * ROW_WIDTH + ROW_WIDTH].clone_from_slice(&row);
        }
        block
    }
}
