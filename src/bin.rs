
use rustyline::Result as RLResult;
use ogma_db::parser::repl;

fn main() -> RLResult<()> {
    repl()
}