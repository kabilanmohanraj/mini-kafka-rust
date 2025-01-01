use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use crate::common::kafka_record::{PartitionRecord, RecordBatch, RecordValue};
use crate::common::traits::Decodable;
use crate::errors::BrokerError;
use crate::api_versions::get_all_apis;
use crate::common::kafka_protocol::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, KafkaBody, PartitionMetadata, ResponseTopic, TaggedFields};
use crate::common::primitive_types::{CompactArray, CompactNullableString, CompactString};

use crate::broker::traits::RequestProcess;

use uuid::Uuid;

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
        // open cluster metadata file
        let metadata_file_path = Path::new("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
        let mut metadata_file = File::open(&metadata_file_path).map_err(|_| BrokerError::UnknownError)?;

        // decode cluster metadata
        let mut buf: Vec<u8> = Vec::new();
        metadata_file.read_to_end(&mut buf).map_err(|_| BrokerError::UnknownError)?;

        println!("Initial Buffer length: {:?}", buf.len());
        println!("Decoding record batch...");

        let mut record_batches: Vec<RecordBatch> = Vec::new();
        let mut offset = 0;

        while offset < buf.len() {
            let (record_batch, batch_byte_len) = match RecordBatch::decode(&buf[offset..]) {
                Ok( (record_batch, batch_byte_len) ) => {
                    (record_batch, batch_byte_len)
                }
                Err(_) => {
                    return Err(BrokerError::UnknownError);
                }
            };

            record_batches.push(record_batch);
            offset += batch_byte_len;
        }

        // iterate over record values from record batches
        let topic_name_to_uuid: HashMap<String, Uuid> = record_batches
            .iter()
            .flat_map(|record_batch| {
                record_batch.records.iter().filter_map(|metadata_record| {
                    match &metadata_record.value {
                        RecordValue::TopicRecord(topic_record) => {
                            Some((topic_record.topic_name.data.clone(), topic_record.topic_id))
                        }
                        _ => None,
                    }
                })
            })
            .collect();

        for (topic_name, uuid) in &topic_name_to_uuid {
            println!("Topic Name: {}, UUID: {}", topic_name, uuid);
        }

        let mut topic_uuid_to_partitions: HashMap<Uuid, Vec<&PartitionRecord>> = HashMap::new();
        for record_batch in &record_batches {
            for metadata_record in &record_batch.records {
                if let RecordValue::PartitionRecord(partition_record) = &metadata_record.value {
                    topic_uuid_to_partitions
                        .entry(partition_record.topic_id)
                        .or_insert_with(Vec::new)
                        .push(partition_record);
                }
            }
        }

        for (uuid, temp_partitions) in &topic_uuid_to_partitions {
            println!("Topic UUID: {}, #Partitions: {}", uuid, temp_partitions.len());
        }

        let mut response_topics: Vec<ResponseTopic> = Vec::new();

        for request_topic in &self.topics.data {
            let request_topic_name = request_topic.name.data.clone();

            // create response placeholders
            let request_topic_uuid: Uuid = "00000000-0000-0000-0000-000000000000".parse::<Uuid>().unwrap();
            let mut response_topic = ResponseTopic {
                error_code: 3,
                name: CompactNullableString {
                    data: Some(CompactString { data: request_topic_name.clone() })
                },
                topic_id: request_topic_uuid.to_bytes_le(),
                is_internal: false,
                partitions: CompactArray { data: vec![] },
                topic_authorized_operations: 0,
                tagged_fields: TaggedFields::empty()
            };
            
            match topic_name_to_uuid.get(&request_topic_name) {
                Some(topic_uuid) => {
                    response_topic.error_code = 0;
                    response_topic.name.data = Some(CompactString { data: request_topic_name.clone() });
                    response_topic.topic_id = topic_uuid.as_bytes().clone();

                    match topic_uuid_to_partitions.get(topic_uuid) {
                        Some(partitions) => {
                            println!("Got {} partitions for topic: {}", partitions.len(), request_topic_name);
                        }
                        None => {
                            println!("No partitions found for topic: {}", request_topic_name);
                        }
                    };
                    
                    match topic_uuid_to_partitions.get(topic_uuid) {
                        Some(partitions) => {
                            let mut response_partitions: Vec<PartitionMetadata> = Vec::new();
                            for partition in partitions {
                                println!(" ======> hereeeeee");
                                let response_partition = PartitionMetadata {
                                    error_code: 0,
                                    partition_index: partition.partition_id,
                                    leader_id: partition.leader,
                                    leader_epoch: partition.leader_epoch,
                                    replica_nodes: partition.replica_array.data.clone(),
                                    isr_nodes: partition.isr_array.data.clone(),
                                    eligible_leader_replicas: vec![],
                                    last_known_elr: vec![],
                                    offline_replicas: vec![],
                                    tagged_fields: TaggedFields::empty(),
                                };

                                response_partitions.push(response_partition);
                            }

                            response_topic.partitions = CompactArray { data: response_partitions };
                        }
                        None => {}
                    };
                },
                None => {
                    response_topic.error_code = 3;
                },
            };

            response_topics.push(response_topic);
        }

        // craft final response
        let response_body = KafkaBody::Response(Box::new(
            DescribeTopicPartitionsResponse {
                throttle_time_ms: 0,
                topics: CompactArray { data: response_topics },
                next_cursor: None,
                tagged_fields: TaggedFields::empty(),
            }
        ));

        Ok( response_body )
    }
}

impl RequestProcess for ApiVersionsRequest {
    fn process(&self) -> Result<KafkaBody, BrokerError> {
        // create response
        let response_body = KafkaBody::Response(Box::new(
            ApiVersionsResponse {
                error_code: 0,
                api_versions: get_all_apis().iter()
                                .map(|&(api_key, (min_version, max_version))| ApiKey {
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