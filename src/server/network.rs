use std::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    path::Path,
};

use ogma_db::{
    common::{
        error::Error,
        network::{BufSocket, RequestType, ResponseType, Server},
    },
    query_engine::process_query,
    storage_engine::{Action, DataBase, Reaction},
};

pub fn start_server<A: ToSocketAddrs>(addr: A) -> Result<(), Error> {
    let listener = TcpListener::bind(addr)?;

    let mut db = DataBase::open(Path::new("./data/test.ogmadb"))?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_client(&mut db, stream).unwrap_or_else(|err| println!("{:?}", err))
            }
            Err(err) => println!("{:?}", err),
        }
    }
    Ok(())
}

fn handle_client(db: &mut DataBase, stream: TcpStream) -> Result<(), Error> {
    let mut buf_sock = BufSocket::new(stream)?;
    // TODO, keep track of client queries to drop when client disconnects
    // let queries = Vec::<u64>::new();
    loop {
        match buf_sock.receive() {
            Ok(request) => match handle_request(db, &mut buf_sock, request) {
                Ok(_) => (),
                Err(err) => {
                    if let Err(err) = handle_error(&mut buf_sock, err) {
                        break Err(err);
                    }
                }
            },
            Err(err) => {
                if let Err(err) = handle_error(&mut buf_sock, err) {
                    break Err(err);
                }
            }
        }
    }
}

fn handle_error(buf_sock: &mut BufSocket, err: Error) -> Result<(), Error> {
    let error_response = ResponseType::Error(err);
    match buf_sock.send(&error_response) {
        Ok(_) => Ok(println!("Response: {:?} -- was sent", error_response)),
        Err(err) => {
            println!("Encountered: {:?}", err);
            println!("Trying to send: {:?}", error_response);
            Err(Error::MetaError(Box::new(err)))
        }
    }
}

fn handle_request(
    db: &mut DataBase,
    buf_sock: &mut BufSocket,
    request: RequestType,
) -> Result<(), Error> {
    println!("Request: {:?} -- received", request);
    let reaction = match request {
        RequestType::Query(query) => db.execute(process_query(query)),
        RequestType::More(qid) => db.execute(Action::GetMore(qid)),
    };

    let response = match reaction {
        Reaction::Error(err) => ResponseType::Error(err),
        Reaction::QueryStart { schema, qid } => ResponseType::QueryHandle { schema, qid },
        Reaction::Data(data) => ResponseType::Data(data),
        Reaction::Empty => ResponseType::Empty,
    };

    buf_sock.send(&response)
}
