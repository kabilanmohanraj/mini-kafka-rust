#![allow(unused_imports)]
#![allow(dead_code)]
mod common;

use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use common::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, Header, KafkaMessage, KafkaPayload, RequestHeader, ResponseHeader};

fn handle_stream(mut stream: TcpStream) {
    // read request
    let mut buf = [0; 1024];
    stream.read(&mut buf).unwrap();

    // compute response
    let response = KafkaMessage{
        size: 0,
        header: Header::Response(ResponseHeader::new(7)),
        body: KafkaPayload { payload: Box::new(ApiVersionsRequest{}) }
    };

    // write response
    stream.write(&response.encode()).unwrap();
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092");

    match listener {
        Ok(listener) => {
            println!("Listening on 9092");
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        println!("Connection established");
                        handle_stream(stream);
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
}
