use crate::errors::KafkaError;

//
// Common traits
//

pub trait Encodable {
    fn encode(&self) -> Vec<u8>;
    // fn encode(&self) -> Result<Vec<u8>, KafkaError>;
}

pub trait Decodable {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError>
    where
        Self: Sized;
}

pub trait Codec: Encodable + Decodable {}

// Blanket implementation for the EncodeDecode trait
impl<T> Codec for T where T: Encodable + Decodable {}