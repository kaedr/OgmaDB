use std::net::TcpStream;

use ogma_db::common::{
    error::Error,
    network::{BufSocket, RequestType, ResponseType},
};

fn main() {
    if let Ok(mut buf_sock) = connect("127.0.0.1:7971") {
        println!("Connected to the server!");
        let example_query =
            RequestType::Query(String::from("The actual contents don't matter yet!"));
        example_query
            .send(&mut buf_sock)
            .expect("Blew up while sending...");
        //stream.shutdown(std::net::Shutdown::Write).expect("Blew up while sending...");
        println!("Request: {:?} -- was sent", example_query);
        let example_response =
            ResponseType::receive(&mut buf_sock).expect("Blew up while receiving...");
        println!("Response: {:?} -- was received", example_response);

        match example_response {
            ResponseType::Error(_) => todo!(),
            ResponseType::QueryHandle { schema, qid } => {
                RequestType::More(qid).send(&mut buf_sock).expect("Blew up while sending...");
                let another_response =
                    ResponseType::receive(&mut buf_sock).expect("Blew up while receiving...");
                println!("Response: {:?} -- was received", another_response);
            },
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
