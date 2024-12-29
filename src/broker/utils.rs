use core::error;
use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::Arc};

use crate::{api_versions::{get_all_apis, get_supported_api_versions}, broker::traits::RequestProcess, common::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, DescribeTopicPartitionsResponse, KafkaBody, KafkaHeader, KafkaMessage, RequestHeader, ResponseHeader, ResponseTopic, TaggedFields}, primitive_types::CompactString};
use crate::traits::{Encodable, Decodable};
use crate::broker::traits::Request;

use crate::broker::broker::Broker;

pub fn process_request(mut stream: TcpStream, broker: Arc<Broker>) {
    // read request
    let mut buf = [0; 1024];
    println!("Client connected: {:?}", stream.peer_addr());

    while let Ok(bytes_read) = stream.read(&mut buf) {
        if bytes_read == 0 {
            println!("Client disconnected");
            break; // exit the loop only when the client closes the connection
        }

        // decode request sent by the client
        let request = match KafkaMessage::decode(&buf[..bytes_read]) {
            Ok((kmessage, _)) => kmessage,
            Err(_) => {
                println!("Error decoding request");
                break;
            }
        };

        // extract correlation ID and error code from the request header
        let correlation_id = match &request.header {
            KafkaHeader::Request(req_header) => req_header.correlation_id,
            _ => {
                println!("Invalid request header");
                break;
            }
        };

        let error_code = validate_api_version(&request.header);
        println!("Error code: {}", error_code);
        // create client response
        let kmessage: KafkaMessage;

        if error_code != 0 {
            println!("Unsupported API version");

            // create error response
            // requests with unsupported API version are treated as ApiVersionsRequest v0
            // from the Kafka codebase -> https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/RequestContext.java#L111
            kmessage = KafkaMessage {
                size: 0,
                header: KafkaHeader::Response(ResponseHeader::new(correlation_id, 0)),
                body: KafkaBody::Response(Box::new(ApiVersionsResponse {
                    error_code,
                    api_versions: get_all_apis().iter()
                                        .map(|&(api_key, (min_version, max_version))| ApiKey {
                                            api_key,
                                            min_version,
                                            max_version,
                                            tagged_fields: TaggedFields(None),
                                        }).collect(),
                    throttle_time_ms: 0,
                    tagged_fields: TaggedFields(None),
                })),
            };

        } else {

            // create valid response
            kmessage = match request.body.process() {
                Ok(response) => KafkaMessage {
                    size: 0,
                    header: KafkaHeader::Response(ResponseHeader::new(correlation_id, find_header_version(request.header.get_api_key())),
                    ),
                    body: response,
                },
                Err(_) => {
                    println!("Error processing request");
                    break;
                }
            };

        }

        // encode the response
        let encoded_response = kmessage.encode();

        // write encoded response to the socket
        if let Err(e) = stream.write_all(&encoded_response) {
            println!("Error writing to stream: {}", e);
            break;
        }

        println!("Response sent, waiting for the next request...");
    }

    // return the borrowed connection to the pool
    broker.return_connection(stream);
    println!("Connection closed...")
}

pub fn build_api_version_map() -> HashMap<i16, (i16, i16)> {
    let mut api_versions: HashMap<i16, (i16, i16)> = HashMap::new();

    // Hardcode the API versions here
    // key => api_key
    // value => tuple(min_version, max_version)
    api_versions.insert(18, (0, 4));
    api_versions.insert(75, (0, 0));

    api_versions
}

fn validate_api_version(req_header: &KafkaHeader) -> i16 {

    let error_code = match req_header {
        KafkaHeader::Request(req_header) => {
            match get_supported_api_versions(req_header.api_key) {
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

    error_code
}

fn find_header_version(api_key: i16) -> i8 {
    match api_key {
        18 => 0,
        75 => 1,
        _ => 1,
    }
}