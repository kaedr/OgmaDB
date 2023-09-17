// use rustyline::Result as RLResult;
// use ogma_db::parser::repl;

mod network;
mod test_code;

use test_code::init_test_db;

fn main() {
    // repl()
    init_test_db();
    network::start_server("127.0.0.1:7971").unwrap();
}
