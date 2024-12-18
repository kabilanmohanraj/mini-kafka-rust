use std::str;

use crate::types::CompactString;

//
// KafkaMessage schema
//

pub struct KafkaMessage {
    pub size: i32,
    pub header: KafkaHeader,
    pub body: KafkaBody
}

impl KafkaMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();

        let (bytes_temp, message_len) = self.encode_helper();

        // encode message size
        buf.extend((message_len+1).to_be_bytes());
        buf.extend(bytes_temp);
        buf.push(0); // TODO: conditional for tagged fields

        buf
    }

    fn encode_helper(&self) -> (Vec<u8>, i32) {
        let mut bytes: Vec<u8> = Vec::new();

        let mut message_len = 0;
        
        // encode header information
        let header_encode = self.header.encode();
        message_len += header_encode.len() as i32;
        bytes.extend(header_encode);

        // encode message body
        let body_encode = self.body.encode();
        message_len += body_encode.len() as i32;
        bytes.extend(body_encode);

        (bytes, message_len)
    }
}


//
// KafkaMessage header schema
//

pub enum KafkaHeader {
    Request(RequestHeader),
    Response(ResponseHeader)
}

pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

pub struct ResponseHeader {
    pub correlation_id: i32
}

//
// Impl blocks for Kafka messages
//

impl KafkaHeader {
    pub fn encode(&self) -> Vec<u8> {
        match &self {
            KafkaHeader::Request(request_header) => {
                request_header.encode()
            }
            KafkaHeader::Response(response_header) => {
                response_header.encode()
            }
        }
    }
}

impl RequestHeader {
    pub fn new(api_key: i16, api_version: i16, correlation_id: i32) -> Self {
        Self { api_key, 
            api_version, 
            correlation_id 
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut header_bytes: Vec<u8> = Vec::new();
        header_bytes.extend(self.api_key.to_be_bytes());
        header_bytes.extend(self.api_version.to_be_bytes());
        header_bytes.extend(self.correlation_id.to_be_bytes());

        header_bytes
    }

    pub fn decode(bytes: &[u8]) -> RequestHeader {
        let temp: &[u8; 2] = &bytes[4..6].try_into().expect("Could not get request API key from buffer...\n");
        let api_key = i16::from_be_bytes(*temp);
        // println!("{}", api_key);

        let temp: &[u8; 2] = &bytes[6..8].try_into().expect("Could not get request API key from buffer...\n");
        let api_version = i16::from_be_bytes(*temp);
        // println!("{}", api_version);

        let temp: &[u8; 4] = &bytes[8..12].try_into().expect("Could not get correlation id from buffer...\n");
        let correlation_id = i32::from_be_bytes(*temp);
        // println!("{}", correlation_id);

        RequestHeader {
            api_key,
            api_version,
            correlation_id
        }

    }
}

impl ResponseHeader {
    pub fn new(correlation_id: i32) -> Self {
        Self {
            correlation_id 
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut header_bytes: Vec<u8> = Vec::new();
        header_bytes.extend(self.correlation_id.to_be_bytes());

        header_bytes
    }
}


//
// Traits
//

pub trait Encodable {
    fn encode(&self) -> Vec<u8>;
}


//
// KafkaMessage Body schema
//

pub enum KafkaBody {
    Request(Box<dyn Encodable>),
    Response(Box<dyn Encodable>)
}

impl Encodable for KafkaBody {
    fn encode(&self) -> Vec<u8> {
        match self {
            KafkaBody::Request(request) => {
                request.encode()
            }
            KafkaBody::Response(response) => {
                response.encode()
            }
        }
    }
}


// API SPECIFIC SCHEMA ARE DEFINED BELOW
// =======================================

//
// ApiVersions
//

// ApiVersions Request (Version: 4) => client_software_name client_software_version TAG_BUFFER 
//   client_software_name => COMPACT_STRING
//   client_software_version => COMPACT_STRING

// COMPACT_STRING - Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT . 
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
pub struct ApiVersionsRequest {
    pub client_software_name: CompactString,
    pub client_software_version: CompactString
    // optional TAG_BUFFER
}

// ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER 
//   error_code => INT16
//   api_keys => api_key min_version max_version TAG_BUFFER 
//     api_key => INT16
//     min_version => INT16
//     max_version => INT16
//   throttle_time_ms => INT32
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_versions: Vec<ApiKey>,
    pub throttle_time_ms: i32
}

pub struct ApiKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16
}

//
// DescribeTopicPartitions
//

pub struct DescribeTopicPartitionsRequest {}

pub struct DescribeTopicPartitionsResponse {}


//
// Fetch
//

pub struct FetchRequest {}

pub struct FetchResponse {}


//
// Produce
//

pub struct ProduceRequest {}

pub struct ProduceResponse {}
