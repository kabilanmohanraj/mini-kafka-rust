use crate::{common::{ApiVersionsRequest, Encodable, KafkaBody, KafkaHeader, KafkaMessage, RequestHeader}, types::CompactString};

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