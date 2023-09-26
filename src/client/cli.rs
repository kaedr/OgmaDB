mod table;
use std::net::TcpStream;

use ogma_db::common::{
    error::Error,
    network::{BufSocket, Client, RequestType, ResponseType},
};

fn main() {
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
