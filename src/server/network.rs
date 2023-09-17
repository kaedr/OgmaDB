use std::{
    net::{TcpListener, ToSocketAddrs},
    path::Path,
};

use ogma_db::{
    common::{
        network::{QueriedData, RequestType, ResponseType},
        AsRows,
    },
    storage_engine::DataBase,
};

pub fn start_server<A: ToSocketAddrs>(addr: A) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr)?;

    match DataBase::open(Path::new("./data/test.ogmadb")) {
        Ok(db) => {
            for stream in listener.incoming() {
                let stream = stream?;
                let request = RequestType::receive(&stream).expect("Broke receiving request");
                println!("Request: {:?} -- received", request);
                match db.load("currency") {
                    Ok((schema, blocks)) => {
                        let mut rows = Vec::new();
                        for block in blocks {
                            rows.extend(block.as_rows(schema.len()));
                        }
                        let query_response = ResponseType::Data(QueriedData::new(schema, rows));
                        query_response
                            .send(&stream)
                            .expect("Blew up while sending...");
                        println!("Response: {:?} -- was sent", query_response);
                    }
                    Err(err) => {
                        let error_response = ResponseType::Error(err);
                        error_response
                            .send(&stream)
                            .expect("Blew up while sending...");
                        println!("Response: {:?} -- was sent", error_response);
                    }
                };
            }
        }
        Err(_) => println!("broke reading db files"),
    }
    Ok(())
}
