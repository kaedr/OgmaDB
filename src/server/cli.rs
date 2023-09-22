// use rustyline::Result as RLResult;
// use ogma_db::parser::repl;

mod network;

fn main() {
    // repl()
    network::start_server("127.0.0.1:7971").unwrap();
}
