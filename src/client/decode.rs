use crate::{common::{ApiVersionsResponse, DescribeTopicPartitionsResponse, TaggedFields}, errors::KafkaError, primitive_types::CompactArray, traits::Decodable};

impl Decodable for ApiVersionsResponse {
    fn decode(_buf: &[u8]) -> Result<(Self, usize), KafkaError> {

        Ok((ApiVersionsResponse {
            error_code: 0,
            api_versions: vec![],
            throttle_time_ms: 0,
            tagged_fields: TaggedFields(None)
        }, 0))
    }
}

impl Decodable for DescribeTopicPartitionsResponse {
    fn decode(_buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        Ok( (DescribeTopicPartitionsResponse {
            throttle_time_ms: 0,
            topics: CompactArray { data: vec![] },
            next_cursor: None,
            tagged_fields: TaggedFields(None)
        }, 0) )
    }
}