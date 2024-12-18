#![allow(unused_imports)]
#![allow(dead_code)]
mod common;
mod broker;
mod client;
mod types;

use std::{error, io::{Read, Write}, net::{TcpListener, TcpStream}};
use common::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, Encodable, KafkaBody, KafkaHeader, KafkaMessage, RequestHeader, ResponseHeader};
use broker::utils::decode_kafka_request;

fn handle_connection(mut stream: TcpStream) {
    // read request
    let mut buf = [0; 1024];
    println!("Client connected: {:?}", stream.peer_addr());

    while let Ok(bytes_read) = stream.read(&mut buf) {
        if bytes_read == 0 {
            println!("Client disconnected");
            break; // exit the loop only when the client closes the connection
        }

        // decode request from the client
        let request = decode_kafka_request(&buf[..bytes_read]);

        // extract correlation ID and error code from the request header
        let correlation_id = match &request.header {
            KafkaHeader::Request(req_header) => req_header.correlation_id,
            _ => {
                println!("Invalid request header");
                break;
            }
        };

        let error_code = match &request.header {
            KafkaHeader::Request(req_header) => {
                if req_header.api_version > 4 || req_header.api_version < 0 {
                    35
                } else {
                    0
                }
            }
            _ => 35,
        };

        // create response
        let response = KafkaMessage {
            size: 0,
            header: KafkaHeader::Response(ResponseHeader::new(correlation_id)),
            body: KafkaBody::Response(Box::new(ApiVersionsResponse {
                error_code,
                api_versions: vec![ApiKey {
                    api_key: 18,
                    min_version: 0,
                    max_version: 4,
                }],
                throttle_time_ms: 20,
            })),
        };

        // encode the response
        let encoded_response = response.encode();

        // write encoded response to the socket
        if let Err(e) = stream.write_all(&encoded_response) {
            println!("Error writing to stream: {}", e);
            break;
        }

        println!("Response sent, waiting for the next request...");
    }

    println!("Connection closed...")
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
                        std::thread::spawn(move || {
                            handle_connection(stream);
                        });
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
