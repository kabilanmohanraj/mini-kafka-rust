use crate::common::{ApiVersionsResponse, Cursor, DescribeTopicPartitionsResponse, PartitionMetadata, ResponseTopic, TaggedFields};
use crate::traits::Encodable;
use crate::primitive_types::{CompactNullableString, VarInt};

impl Encodable for ApiVersionsResponse {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.error_code.to_be_bytes());
        bytes.extend(((self.api_versions.len() as i8) + 1).to_be_bytes());

        for api in &self.api_versions {
            bytes.extend(api.api_key.to_be_bytes());
            bytes.extend(api.min_version.to_be_bytes());
            bytes.extend(api.max_version.to_be_bytes());
            bytes.extend(api.tagged_fields.encode());
        }

        bytes.extend(self.throttle_time_ms.to_be_bytes());
        // bytes.extend(self.tagged_fields.encode());

        bytes
    }
}

impl Encodable for DescribeTopicPartitionsResponse {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend(self.throttle_time_ms.to_be_bytes());
        buf.extend(self.topics.encode());

        if let Some(next_cursor) = &self.next_cursor {
            buf.extend(next_cursor.encode());
        } else {
            buf.push(0xFF); // next cursor is null
        }
        
        buf.extend(self.tagged_fields.encode());

        buf
    }
}

impl Encodable for ResponseTopic {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();

        buf.extend(self.error_code.to_be_bytes());
        buf.extend(self.name.encode());
        buf.extend(self.topic_id);
        buf.push(self.is_internal as u8);

        
        let arr_length = VarInt {
            data: self.partitions.data.len() as i32 + 1
        };

        buf.extend(arr_length.encode());
        for partition in &self.partitions.data {
            buf.extend(partition.encode());
        }

        buf.extend(self.topic_authorized_operations.to_be_bytes());

        buf.extend(self.tagged_fields.encode());

        buf
    }
}

impl Encodable for PartitionMetadata {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();

        buf.extend(self.error_code.to_be_bytes());
        buf.extend(self.partition_index.to_be_bytes());
        buf.extend(self.leader_id.to_be_bytes());
        buf.extend(self.leader_epoch.to_be_bytes());

        buf.extend((self.replica_nodes.len() as i32).to_be_bytes()); 
        for node in &self.replica_nodes {
            buf.extend(node.to_be_bytes());
        }

        buf.extend((self.isr_nodes.len() as i32).to_be_bytes()); 
        for node in &self.isr_nodes {
            buf.extend(node.to_be_bytes());
        }

        buf.extend((self.eligible_leader_replicas.len() as i32).to_be_bytes()); 
        for replica in &self.eligible_leader_replicas {
            buf.extend(replica.to_be_bytes());
        }

        buf.extend((self.last_known_elr.len() as i32).to_be_bytes());
        for elr in &self.last_known_elr {
            buf.extend(elr.to_be_bytes());
        }

        buf.extend((self.offline_replicas.len() as i32).to_be_bytes());
        for replica in &self.offline_replicas {
            buf.extend(replica.to_be_bytes());
        }

        buf.extend(self.tagged_fields.encode());

        buf
    }
}

impl Encodable for Cursor {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend(self.topic_name.encode());
        buf.extend(self.partition_index.to_be_bytes());
        buf.extend(self.tagged_fields.encode());

        buf
    }
}