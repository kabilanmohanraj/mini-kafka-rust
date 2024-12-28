pub enum KafkaError {
    BrokerError(BrokerError),
    IoError(std::io::Error),
    DecodeError,
    EncodeError,
}

pub enum BrokerError {
    NoError,
    UnknownError,
    UnsupportedVersion,
    UnknownTopicOrPartition,
}