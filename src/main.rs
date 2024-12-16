#![allow(unused_imports)]
#![allow(dead_code)]
mod common;
mod broker;

use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use common::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, Header, KafkaMessage, KafkaPayload, RequestHeader, ResponseHeader};
use broker::utils::decode_kafka_request;

fn handle_stream(mut stream: TcpStream) {
    // read request
    let mut buf = [0; 1024];
    stream.read(&mut buf).unwrap();

    // decode request from the client
    let request = decode_kafka_request(&buf);

    // extract correlation ID from the request header
    let correlation_id = match &request.header {
        Header::Request(req_header) => req_header.correlation_id,
        _ => {
            panic!("Expected RequestHeader in the KafkaMessage");
        }
    };

    // compute response
    let response = KafkaMessage{
        size: 0,
        header: Header::Response(ResponseHeader::new(correlation_id)),
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
