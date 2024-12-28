use std::collections::HashMap;

use uuid::Uuid;

use crate::common::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, KafkaBody, KafkaMessage, ResponseTopic, TaggedFields};
use crate::errors::BrokerError;
use crate::primitive_types::{CompactArray, CompactNullableString};
use crate::traits::Codec;

use crate::broker::traits::RequestProcess;

use super::utils::build_api_version_map;

impl RequestProcess for KafkaBody {
    fn process(&self) -> Result<KafkaBody, BrokerError> {
        match self {
            KafkaBody::Request(request) => {
                request.process()
            },
            KafkaBody::Response(_) => {
                Err(BrokerError::UnknownError)
            }
        }
    }
}

impl RequestProcess for DescribeTopicPartitionsRequest {
    fn process(&self) -> Result<KafkaBody, BrokerError> {

        Ok( KafkaBody::Response(Box::new(DescribeTopicPartitionsResponse {
            throttle_time_ms: 0,
            topics: CompactArray { 
                data: vec![
                    ResponseTopic {
                        error_code: 3,
                        name: CompactNullableString {
                            data: Some(self.topics.data[0].name.clone())
                        },
                        topic_id: "00000000-0000-0000-0000-000000000000".parse::<Uuid>().unwrap().to_bytes_le(),
                        is_internal: false,
                        partitions: CompactArray { data: vec![] },
                        topic_authorized_operations: 10,
                        tagged_fields: TaggedFields(None)
                    }
            ] },
            next_cursor: None,
            tagged_fields: TaggedFields(None),
        }))
        )
    }
}

impl RequestProcess for ApiVersionsRequest {
    fn process(&self) -> Result<KafkaBody, BrokerError> {

        let api_version_map: HashMap<i16, (i16, i16)> = build_api_version_map();
        
        // create response
        let response_body = KafkaBody::Response(Box::new(
            ApiVersionsResponse {
                error_code: 0,
                api_versions: api_version_map.iter()
                                .map(|(&api_key, &(min_version, max_version))| ApiKey {
                                    api_key,
                                    min_version,
                                    max_version,
                                    tagged_fields: TaggedFields(None),
                                }).collect(),
                throttle_time_ms: 0,
                tagged_fields: TaggedFields(None),
        }));

        Ok(response_body)
    }
}