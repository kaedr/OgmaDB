mod table;
mod crud;
use {std::net::TcpStream, crate::table::Table};


use std::collections::HashMap;

use ogma_db::common::{
    error::{Error, self},
    network::{BufSocket, Client, RequestType, ResponseType}, TableInfoMap,
};

fn main() {
    let tables: HashMap<u64, Table> = HashMap::new();
    if let Ok(mut buf_sock) = connect("127.0.0.1:7971") {
        println!("Connected to the server!");
        let example_query =
            RequestType::Query(String::from("The actual contents don't matter yet!"));
        buf_sock
            .send(&example_query)
            .expect("Blew up while sending...");
        let example_response = buf_sock.receive().expect("Blew up while receiving...");
        println!("Response: {:?} -- was received", example_response);

        match example_response {
            ResponseType::Error(_) => todo!(),
            ResponseType::QueryHandle { qid, .. } => {
                buf_sock
                    .send(&RequestType::More(qid))
                    .expect("Blew up while sending...");
                let another_response = buf_sock.receive().expect("Blew up while receiving...");
                println!("Response: {:?} -- was received", another_response);
            }
            ResponseType::Data(_) => todo!(),
            ResponseType::Empty => todo!(),
        }               
    } else {
        println!("Couldn't connect to server...");
    }
}

fn connect<A>(addr: A) -> Result<BufSocket, Error>
where
    A: std::net::ToSocketAddrs,
{
    let stream = TcpStream::connect(addr)?;
    BufSocket::new(stream)
}

fn do_query(mut tables_map: HashMap<u64, Table>, mut buf_sock: BufSocket, user_query: String)->Box<Table>{
    let query = RequestType::Query(user_query);
    buf_sock.send(&query).expect("Sending query failed");
    let response = buf_sock.receive().expect("Blew up while receiving...");
    if let ResponseType::QueryHandle {qid, schema} = response {
        let mut table: (u64, (String, TableInfoMap));
        if tables_map.contains_key(&qid){
            let table = tables_map.get_mut(&qid);
            table = Table::new(query, TableInfoMap);

        }
        else {
            tables_map.insert(qid, Table::new(user_query, schema));
            table = tables_map.get_mut(qid)
        }
    }
}
