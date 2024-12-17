#![allow(unused_imports)]
#![allow(dead_code)]
mod common;
mod broker;

use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use common::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, Encodable, KafkaBody, KafkaHeader, KafkaMessage, RequestHeader, ResponseHeader};
use broker::utils::decode_kafka_request;

fn handle_stream(mut stream: TcpStream) {
    // read request
    let mut buf = [0; 1024];
    stream.read(&mut buf).unwrap();

    // decode request from the client
    let request = decode_kafka_request(&buf);

    // extract correlation ID from the request header
    let correlation_id = match &request.header {
        KafkaHeader::Request(req_header) => req_header.correlation_id,
        _ => {
            panic!("Expected RequestHeader in the KafkaMessage");
        }
    };

    let header = KafkaHeader::Response(ResponseHeader::new(correlation_id)).encode();
    let body = KafkaBody::Response(
        Box::new(ApiVersionsResponse{
            error_code: 35,
            api_versions: vec![]
        })).encode();

    let size = ( header.len() + body.len() ) as i32;
    println!("{}", size);

    // compute response
    let response = KafkaMessage{
        size: size,
        header: KafkaHeader::Response(ResponseHeader::new(correlation_id)),
        body: KafkaBody::Response(
                            Box::new(ApiVersionsResponse{
                                error_code: 0,
                                api_versions: vec![ApiKey{api_key: 18, min_version: 0, max_version: 4}]
                            }))
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
