use crate::common::{ApiVersionsRequest, Encodable};

impl Encodable for ApiVersionsRequest {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend(self.client_software_name.encode());
        buf.extend(self.client_software_version.encode());

        buf
    }
}