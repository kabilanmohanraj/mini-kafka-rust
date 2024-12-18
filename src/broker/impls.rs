use crate::common::{ApiVersionsResponse, Encodable};

impl Encodable for ApiVersionsResponse {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.error_code.to_be_bytes());
        bytes.extend(((self.api_versions.len()+1) as i8).to_be_bytes());

        for api_key in &self.api_versions {
            bytes.extend(&api_key.api_key.to_be_bytes());
            bytes.extend(&api_key.min_version.to_be_bytes());
            bytes.extend(&api_key.max_version.to_be_bytes());
            bytes.push(0);
        }

        bytes.extend(self.throttle_time_ms.to_be_bytes());

        bytes
    }
}

