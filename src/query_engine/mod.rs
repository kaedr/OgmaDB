use crate::storage_engine::Action;

pub fn process_query(query: String) -> Action {
    println!("Processing Query: {}", query);
    Action::GetAll("currency".into())
}
