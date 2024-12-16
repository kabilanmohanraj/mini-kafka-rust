use crate::common::{ApiVersionsRequest, Encodable, KafkaMessage, KafkaPayload, RequestHeader};

pub fn decode_kafka_request(bytes: &[u8]) -> KafkaMessage {
    
    let temp: &[u8; 4] = &bytes[0..4].try_into().expect("Could not get message size from buffer...\n");
    let message_size = i32::from_be_bytes(*temp);
    println!("{}", message_size);

    let header = RequestHeader::decode(bytes);

    match header.api_key {
        0 => {}
        _ => {}
    }


    // construct the Kafka message from bytes
    let dummy = ApiVersionsRequest{};
    KafkaMessage {
        size: message_size,
        header: crate::common::Header::Request(header),
        body: KafkaPayload {
            payload: Box::new(dummy)
        }
    }
}