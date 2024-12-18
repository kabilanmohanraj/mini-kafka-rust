#![allow(unused_imports)]
#![allow(dead_code)]
mod common;
mod broker;

use std::{error, io::{Read, Write}, net::{TcpListener, TcpStream}};
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

    let error_code = match &request.header {
        KafkaHeader::Request(req_header) => {
            if req_header.api_version > 4 || req_header.api_version < 0 {
                println!("{}", 35);
                35
            } else {
                0
            }
        },
        _ => {
            35
        }
    };

    let response = KafkaMessage{
        size: 0,
        header: KafkaHeader::Response(ResponseHeader::new(correlation_id)),
        body: KafkaBody::Response(
                            Box::new(ApiVersionsResponse{
                                error_code: error_code,
                                api_versions: vec![ApiKey{api_key: 18, min_version: 0, max_version: 4}],
                                throttle_time_ms: 20
                            }))
    };

    // encode the response
    let encoded_response = response.encode();

    // write encoded response to the socket 
    stream.write(&encoded_response).unwrap();
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
