use std::str;

//
// Header constructs for Kafka messages
//

pub enum Header {
    Request(RequestHeader),
    Response(ResponseHeader)
}

impl Header {
    pub fn encode(&self) -> Vec<u8> {
        match &self {
            Header::Request(request_header) => {
                request_header.encode()
            }
            Header::Response(response_header) => {
                response_header.encode()
            }
        }
    }
}

pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
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
        println!("{}", api_key);

        let temp: &[u8; 2] = &bytes[6..8].try_into().expect("Could not get request API key from buffer...\n");
        let api_version = i16::from_be_bytes(*temp);
        println!("{}", api_version);

        let temp: &[u8; 4] = &bytes[8..12].try_into().expect("Could not get request API key from buffer...\n");
        let correlation_id = i32::from_be_bytes(*temp);
        println!("{}", correlation_id);

        RequestHeader {
            api_key,
            api_version,
            correlation_id
        }

    }
}

pub struct ResponseHeader {
    pub correlation_id: i32
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
// Payloads for Kafka messages
//

pub struct KafkaPayload {
    pub payload: Box<dyn Encodable> // runt-time polymorphism
}

impl Encodable for KafkaPayload {
    fn encode(&self) -> Vec<u8> {
        self.payload.encode()
    }
}


//
// ApiVersions
//

pub struct ApiVersionsRequest {}

impl Encodable for ApiVersionsRequest {
    fn encode(&self) -> Vec<u8> {
        Vec::new()
    }
}


pub struct ApiKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16
}

pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_versions: Vec<ApiKey>
}

impl Encodable for ApiVersionsResponse {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.error_code.to_be_bytes());
        bytes.extend((self.api_versions.len() as i32).to_be_bytes());

        for api_key in &self.api_versions {
            bytes.extend(&api_key.api_key.to_be_bytes());
            bytes.extend(&api_key.min_version.to_be_bytes());
            bytes.extend(&api_key.max_version.to_be_bytes());
        }

        bytes
    }
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



//
// KafkaMessage
//

pub struct KafkaMessage {
    pub size: i32,
    pub header: Header,
    pub body: KafkaPayload
}

impl KafkaMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();

        // encode message size
        bytes.extend(self.size.to_be_bytes());

        // encode header information
        bytes.extend(self.header.encode());

        // encode message body
        bytes.extend(self.body.encode());

        bytes
    }
}