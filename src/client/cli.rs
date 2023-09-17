use std::net::TcpStream;

use ogma_db::common::network::{RequestType, ResponseType};

fn main() {
    if let Ok(stream) = TcpStream::connect("127.0.0.1:7971") {
        println!("Connected to the server!");
        let example_query =
            RequestType::Query(String::from("The actual contents don't matter yet!"));
        example_query
            .send(&stream)
            .expect("Blew up while sending...");
        stream.shutdown(std::net::Shutdown::Write).expect("Blew up while sending...");
        println!("Request: {:?} -- was sent", example_query);
        let example_response = ResponseType::receive(&stream).expect("Blew up while receiving...");
        println!("Response: {:?} -- was received", example_response)
    } else {
        println!("Couldn't connect to server...");
    }
}
