use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result as RLResult};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn generate_ast(sql: &str) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql)
}

pub fn repl() -> RLResult<()> {
    let mut cli = DefaultEditor::new()?;

    #[cfg(feature = "with-file-history")]
    if cli.load_history(".ogma_history").is_err() {
        println!("No history file...");
    }

    let mut statement = String::new();
    loop {
        let prompt = if statement.is_empty() { ">>> " } else { "..> " };
        let readline = cli.readline(prompt);
        match readline {
            Ok(line) => {
                statement.push_str(&line);
                if !statement.ends_with('\\') {
                    match generate_ast(&statement.replace('\\', " ")) {
                        Ok(ast) => {
                            println!("AST: {:?}", ast);
                            // TODO: handle this error with something other than a crash...
                            cli.add_history_entry(statement)?;
                        }
                        Err(err) => println!("{}", err),
                    }
                    // Once we reach the end of a statement, clean house
                    statement = String::new();
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    #[cfg(feature = "with-file-history")]
    cli.save_history(".ogma_history");

    Ok(())
}
