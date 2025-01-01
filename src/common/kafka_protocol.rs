use std::str;

use crate::broker::traits::Request;
use crate::errors::KafkaError;
use super::primitive_types::{CompactArray, CompactNullableString, CompactString};
use super::traits::{Decodable, Encodable, Codec};


//
// KafkaMessage schema
//

// Client Request
pub struct ClientRequest {
    pub size: i32,
    pub header: KafkaHeader,
    pub body: KafkaBody
}

// Client Response
pub struct ClientResponse {
    pub size: i32,
    pub header: KafkaHeader,
    pub body: KafkaBody
}

pub struct KafkaMessage {
    pub size: i32,
    pub header: KafkaHeader,
    pub body: KafkaBody,
}

impl KafkaMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();

        let (bytes_temp, message_len) = self.encode_helper();

        // encode message size
        buf.extend((message_len+1).to_be_bytes());
        buf.extend(bytes_temp);

        buf
    }

    fn encode_helper(&self) -> (Vec<u8>, i32) {
        let mut bytes: Vec<u8> = Vec::new();

        let mut message_len = 0;
        
        // encode header information
        let header_encoded = self.header.encode();
        message_len += header_encoded.len() as i32;
        bytes.extend(header_encoded);

        // encode message body
        let body_encoded = self.body.encode();
        message_len += body_encoded.len() as i32;
        bytes.extend(body_encoded);

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
    pub client_id: String,
    pub tagged_fields: TaggedFields
}

pub struct ResponseHeader {
    pub correlation_id: i32,
    pub tagged_fields: TaggedFields,
    pub header_version: i8
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

    pub fn get_api_key(&self) -> i16 {
        match &self {
            KafkaHeader::Request(request_header) => {
                request_header.api_key
            }
            _ => {
                0
            }
        }
    }
}

impl RequestHeader {
    pub fn new(api_key: i16, api_version: i16, correlation_id: i32) -> Self {
        Self { api_key, 
            api_version, 
            correlation_id,
            client_id: String::new(),
            tagged_fields: TaggedFields(None)
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut header_bytes: Vec<u8> = Vec::new();
        header_bytes.extend(self.api_key.to_be_bytes());
        header_bytes.extend(self.api_version.to_be_bytes());
        header_bytes.extend(self.correlation_id.to_be_bytes());

        let client_id_len = self.client_id.len() as i16;
        header_bytes.extend(client_id_len.to_be_bytes());
        header_bytes.extend(self.client_id.as_bytes());

        header_bytes.extend(self.tagged_fields.encode());

        header_bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<(RequestHeader, usize), KafkaError> {

        let mut offset = 0;

        let temp: &[u8; 2] = &bytes[0..2].try_into().expect("Could not get request API key from buffer...\n");
        let api_key = i16::from_be_bytes(*temp);
        // println!("{}", api_key);

        let temp: &[u8; 2] = &bytes[2..4].try_into().expect("Could not get request API key from buffer...\n");
        let api_version = i16::from_be_bytes(*temp);
        // println!("{}", api_version);

        let temp: &[u8; 4] = &bytes[4..8].try_into().expect("Could not get correlation id from buffer...\n");
        let correlation_id = i32::from_be_bytes(*temp);
        offset += 8;

        let temp: &[u8; 2] = &bytes[offset..offset+2].try_into().expect("Could not get client id length from buffer...\n");
        let client_id_len = i16::from_be_bytes(*temp);
        offset += 2;

        if offset + client_id_len as usize > bytes.len() {
            return Err(KafkaError::DecodeError);
        }
        
        let client_id = match str::from_utf8(&bytes[offset..offset+client_id_len as usize]) {
            Ok(value) => value.to_string(),
            Err(_) => return Err(KafkaError::DecodeError)
        };
        offset += client_id_len as usize;

        // FIXME: Dynamically compute offset here
        let (tagged_fields, tf_len) = match TaggedFields::decode(&bytes) {
            Ok((tagged_fields, tf_len)) => (tagged_fields, tf_len),
            Err(_) => return Err(KafkaError::DecodeError)
        };
        offset += tf_len;

        Ok( (RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
            tagged_fields
        }, offset) )

    }
}

impl ResponseHeader {
    pub fn new(correlation_id: i32, header_version: i8) -> Self {
        Self {
            correlation_id,
            tagged_fields: TaggedFields(None),
            header_version: header_version
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut header_bytes: Vec<u8> = Vec::new();

        header_bytes.extend(self.correlation_id.to_be_bytes());
        
        match self.header_version {
            0 => {}
            _ => {
                header_bytes.extend(self.tagged_fields.encode());
            }
        }

        header_bytes
    }
}


//
// KafkaMessage Body schema
//

pub enum KafkaBody {
    Request(Box<dyn Request>),
    Response(Box<dyn Codec>)
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


// =======================================
// API SPECIFIC SCHEMA ARE DEFINED BELOW
// =======================================

pub struct TaggedField {
    // TODO:
}

impl Encodable for TaggedField {
    fn encode(&self) -> Vec<u8> {
        Vec::new()
    }
}

impl Decodable for TaggedField {
    fn decode(_buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        Ok((TaggedField{}, 1))
    }
}

pub struct TaggedFields(pub Option<CompactArray<TaggedField>>);

impl TaggedFields {
    pub fn new(fields: Option<CompactArray<TaggedField>>) -> Self {
        TaggedFields(fields)
    }

    pub fn empty() -> Self {
        TaggedFields(None)
    }
}

impl Encodable for TaggedFields {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match &self.0 {
            Some(fields) => {
                for field in fields.data.iter() {
                    buf.extend(field.encode());
                }
            }
            None => {
                buf.push(0); // No tagged fields
            }
        }
        buf
    }
}

impl Decodable for TaggedFields {
    fn decode(_buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        Ok((TaggedFields(None), 1))
    }
}


//
// ApiVersions API
//

// ApiVersions Request (Version: 4) => client_software_name client_software_version TAG_BUFFER 
//   client_software_name => COMPACT_STRING
//   client_software_version => COMPACT_STRING

// COMPACT_STRING - Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT . 
// Then N bytes follow which are the UTF-8 encoding of the character sequence.
pub struct ApiVersionsRequest {
    pub client_software_name: CompactString,
    pub client_software_version: CompactString,
    pub tagged_fields: TaggedFields
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
    pub throttle_time_ms: i32,
    pub tagged_fields: TaggedFields
}

pub struct ApiKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub tagged_fields: TaggedFields
}


//
// DescribeTopicPartitions API
//

// DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER 
//   topics => name TAG_BUFFER 
//     name => COMPACT_STRING
//   response_partition_limit => INT32
//   cursor => topic_name partition_index TAG_BUFFER 
//     topic_name => COMPACT_STRING
//     partition_index => INT32
pub struct DescribeTopicPartitionsRequest {
    pub topics: CompactArray<RequestTopic>,
    pub response_partition_limit: i32,
    pub cursor: Option<Cursor>,
    pub tagged_fields: TaggedFields
}

pub struct RequestTopic {
    pub name: CompactString,
    pub tagged_fields: TaggedFields
}

pub struct Cursor {
    pub topic_name: CompactString,
    pub partition_index: i32,
    pub tagged_fields: TaggedFields
}

// DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor TAG_BUFFER 
//   throttle_time_ms => INT32
//   topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER 
//     error_code => INT16
//     name => COMPACT_NULLABLE_STRING
//     topic_id => UUID
//     is_internal => BOOLEAN
//     partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas] TAG_BUFFER 
//       error_code => INT16
//       partition_index => INT32
//       leader_id => INT32
//       leader_epoch => INT32
//       replica_nodes => INT32
//       isr_nodes => INT32
//       eligible_leader_replicas => INT32
//       last_known_elr => INT32
//       offline_replicas => INT32
//     topic_authorized_operations => INT32
//   next_cursor => topic_name partition_index TAG_BUFFER 
//     topic_name => COMPACT_STRING
//     partition_index => INT32
pub struct DescribeTopicPartitionsResponse {
    pub throttle_time_ms: i32, 
    pub topics: CompactArray<ResponseTopic>, 
    pub next_cursor: Option<Cursor>,
    pub tagged_fields: TaggedFields
}

impl DescribeTopicPartitionsResponse {
    pub fn empty(&self) -> DescribeTopicPartitionsResponse {
        DescribeTopicPartitionsResponse {
            throttle_time_ms: 0,
            topics: CompactArray { data: vec![] },
            next_cursor: None,
            tagged_fields: TaggedFields(None)
        }
    }

    pub fn default() -> DescribeTopicPartitionsResponse {
        DescribeTopicPartitionsResponse {
            throttle_time_ms: 0,
            topics: CompactArray { data: vec![
                ResponseTopic {
                    error_code: 3,
                    name: CompactNullableString {
                        data: Some( CompactString { data: "test".to_string() } )
                    },
                    topic_id: "00000000-0000-0000-0000-000000000000".parse::<uuid::Uuid>().unwrap().to_bytes_le(),
                    is_internal: false,
                    partitions: CompactArray { data: vec![] },
                    topic_authorized_operations: 10,
                    tagged_fields: TaggedFields(None)
                }
            ] },
            next_cursor: None,
            tagged_fields: TaggedFields(None)
        }
    }
}

pub struct ResponseTopic {
    pub error_code: i16,
    pub name: CompactNullableString,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: CompactArray<PartitionMetadata>,
    pub topic_authorized_operations: i32,
    pub tagged_fields: TaggedFields
}

pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,   
    pub leader_id: i32,               
    pub leader_epoch: i32,            
    pub replica_nodes: Vec<i32>,    
    pub isr_nodes: Vec<i32>,        
    pub eligible_leader_replicas: Vec<i32>,
    pub last_known_elr: Vec<i32>, 
    pub offline_replicas: Vec<i32>,
    pub tagged_fields: TaggedFields
}


//
// Fetch API
//

pub struct FetchRequest {}

pub struct FetchResponse {}


//
// Produce
//

pub struct ProduceRequest {}

pub struct ProduceResponse {}
