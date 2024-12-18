use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::Arc};

use crate::{common::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, Encodable, KafkaBody, KafkaHeader, KafkaMessage, RequestHeader, ResponseHeader}, types::CompactString, utils::build_api_version_map};

fn handle_connection(mut stream: TcpStream, api_version_map: &HashMap<i16, (i16, i16)>) {
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
                match api_version_map.get(&req_header.api_key) {
                    Some(supported_versions) => {
                        if req_header.api_version > supported_versions.1 || req_header.api_version < supported_versions.0 {
                            35 as i16
                        } else {
                            0 as i16
                        }
                    },
                    None => 35 as i16,
                }
            }
            _ => 35 as i16,
        };

        // create response
        let response = KafkaMessage {
            size: 0,
            header: KafkaHeader::Response(ResponseHeader::new(correlation_id)),
            body: KafkaBody::Response(Box::new(ApiVersionsResponse {
                error_code,
                // api_versions: vec![ApiKey {
                //     api_key: 18,
                //     min_version: 0,
                //     max_version: 4,
                // }],
                api_versions: api_version_map.iter().map(|(&api_key, &(min_version, max_version))| ApiKey {
                    api_key,
                    min_version,
                    max_version,
                }).collect(),
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


fn get_all_apis() -> Vec<ApiKey> {
    let apis: Vec<ApiKey> = Vec::new();

    apis
}


pub fn init_broker() {
    // initialize metadata
    let api_version_map = Arc::new(build_api_version_map());

    // create TCP listener
    let listener = TcpListener::bind("127.0.0.1:9092");

    match listener {
        Ok(listener) => {
            println!("Listening on 9092");
            
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        println!("Connection established");
                        let map_clone = Arc::clone(&api_version_map);
                        std::thread::spawn(move || {
                            handle_connection(stream, &map_clone);
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

pub fn decode_kafka_request(bytes: &[u8]) -> KafkaMessage {
    
    let temp: &[u8; 4] = &bytes[0..4].try_into().expect("Could not get message size from buffer...\n");
    let message_size = i32::from_be_bytes(*temp);
    // println!("{}", message_size);

    let header = RequestHeader::decode(bytes);

    match header.api_key {
        0 => {}
        _ => {}
    }


    // construct the Kafka message from bytes
    let dummy = ApiVersionsRequest{
        client_software_name: CompactString::new("kafka-client-script".to_string()),
        client_software_version: CompactString::new("0.1".to_string())
    };

    KafkaMessage {
        size: message_size,
        header: KafkaHeader::Request(header),
        body: KafkaBody::Request(Box::new(dummy))
    }
}


fn get_error_code(request: &KafkaMessage) -> i16 {
    match &request.header {
        KafkaHeader::Request(req_header) => {
            
            if req_header.api_version > 4 || req_header.api_version < 0 {
                35 as i16
            } else {
                0 as i16
            }
        }
        _ => 35 as i16,
    }
}