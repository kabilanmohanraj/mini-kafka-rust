// TODO: implement UNSIGNED_VARINT and encoding and decoding logic

use crate::common::Encodable;

pub struct CompactString {
    // length: u8, // TODO: use UNSIGNED_VARINT here
    pub data: String
}

impl CompactString {
    pub fn new(data: String) -> Self {
        CompactString {
            data: data
        }
    }
}

impl Encodable for CompactString {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend((self.data.len()).to_be_bytes());
        buf.extend(self.data.bytes());

        buf
    }
}