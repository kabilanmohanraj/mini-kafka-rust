use crate::common::traits::Decodable;
use crate::errors::{BrokerError, KafkaError};
use crate::common::kafka_protocol::{KafkaMessage, KafkaHeader, KafkaBody, RequestHeader, TaggedFields, ApiVersionsRequest, DescribeTopicPartitionsRequest, RequestTopic, Cursor};
use crate::common::primitive_types::{CompactString, CompactArray};



impl Decodable for KafkaMessage {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        // decode message size
        let message_size = match buf[0..4].try_into() {
            Ok(temp) => i32::from_be_bytes(temp),
            Err(_) => return Err(KafkaError::DecodeError)
        };
        offset += 4;

        // decode header information
        let (request_header, header_byte_length) = RequestHeader::decode(&buf[offset..])?;
        offset += header_byte_length;

        // decode message body
        let request: KafkaBody = match request_header.api_key {
            18 => {
                match ApiVersionsRequest::decode(&buf[offset..]) {
                    Ok((kmessage, _)) => KafkaBody::Request(Box::new(kmessage)),
                    Err(_) => return Err(KafkaError::DecodeError)
                }
            }
            75 => {
                match DescribeTopicPartitionsRequest::decode(&buf[offset..]) {
                    Ok((kmessage, _)) => KafkaBody::Request(Box::new(kmessage)),
                    Err(_) => return Err(KafkaError::DecodeError)
                }
            }
            _ => { return Err(KafkaError::BrokerError(BrokerError::UnknownError)) }
        };

        return Ok( (KafkaMessage {
            size: message_size,
            header: KafkaHeader::Request(request_header),
            body: request,
        }, 0) )
    }
}

impl Decodable for ApiVersionsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        let client_software_name = match CompactString::decode(&buf[offset..]) {
            Ok((client_software_name, data_length)) => {
                offset += data_length;
                client_software_name
            }
            Err(_) => return Err(KafkaError::DecodeError)
        };

        let client_software_version = match CompactString::decode(&buf[offset..]) {
            Ok((client_software_version, data_length)) => {
                offset += data_length;
                client_software_version
            }
            Err(_) => return Err(KafkaError::DecodeError)
        };
        
        
        let tagged_fields = match TaggedFields::decode(&buf[offset..]) {
            Ok((tagged_fields, _)) => {
                // offset += data_length;
                tagged_fields
            }
            Err(_) => return Err(KafkaError::DecodeError)
        };

        Ok(  
            (ApiVersionsRequest {
                client_software_name,
                client_software_version,
                tagged_fields
            }, 0)
        )
    }
}

impl Decodable for DescribeTopicPartitionsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        // decode request topics
        let (request_topics, topics_bytes) = CompactArray::<RequestTopic>::decode(&buf[offset..])?;
        offset += topics_bytes;

        println!("Request topic name: {:?}", request_topics.data[0].name.data);
        
        // decode response partition limit
        let temp: &[u8; 4] = match buf[offset..offset+4].try_into() {
            Ok(val) => val,
            Err(_) => return Err(KafkaError::DecodeError),
        };
        let response_partition_limit = i32::from_be_bytes(*temp);
        offset += 4;

        // decode cursor
        let cursor: Option<Cursor> = if buf[offset] == 0xFF {
            None
        } else {
            match Cursor::decode(&buf[offset..]) {
                Ok((cursor, cursor_len)) => {
                    offset += cursor_len;
                    Some(cursor)
                }, 
                Err(_) => return Err(KafkaError::DecodeError)
            }
        };

        let (tagged_fields, tf_len) = TaggedFields::decode(buf)?;
        offset += tf_len;

        Ok( (DescribeTopicPartitionsRequest {
            topics: request_topics,
            response_partition_limit,
            cursor,
            tagged_fields
        }, offset) )
    }
}

impl Decodable for RequestTopic {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        let (name, name_len) = CompactString::decode(&buf[offset..])?;
        offset += name_len;

        let (tagged_fields, tf_len) = TaggedFields::decode(buf)?;
        offset += tf_len;

        Ok( (RequestTopic {
            name,
            tagged_fields
        }, offset) )
    }
}

impl Decodable for Cursor {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        let (topic_name, topic_name_len) = CompactString::decode(&buf[offset..])?;
        offset += topic_name_len;

        let temp: &[u8; 4] = &buf[offset..offset+4].try_into().expect("Could not get partition index from buffer...\n");
        let partition_index = i32::from_be_bytes(*temp);
        offset += 4;

        let tagged_fields = TaggedFields::empty();

        Ok( (Cursor {
            topic_name,
            partition_index,
            tagged_fields
        }, offset) )
    }
}
