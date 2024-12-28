use crate::{errors::KafkaError, traits::{Decodable, Encodable}};


//
// VAR_INT
//

pub fn encode_var_int(mut integer: u32) -> Vec<u8> {
    // Protobuf's zig-zag encoding for VARINT
    let mut res: Vec<u8> = Vec::new();

    loop {
        // take the lower 7 bits of the integer
        let mut chunk = (integer & 0x7F) as u8;

        // check if there are more bits to encode
        integer >>= 7;
        
        if integer != 0 {
            // set the continuation bit if more bits are present
            chunk |= 0x80;
        }

        res.push(chunk);

        // if no more bits to encode, break
        if integer == 0 {
            break;
        }
    }

    // println!("Byte vector: {:?}", res);
    // for byte in &res {
    //     for i in (0..8).rev() {
    //         print!("{}", (byte >> i) & 1);
    //     }
    //     print!(" ");
    // }
    // println!();

    res
}
pub struct VarInt {
    pub data: i32
}

impl Encodable for VarInt {
    fn encode(&self) -> Vec<u8> {
        encode_var_int(self.data as u32)
    }
}

impl Decodable for VarInt {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        let mut chunk_arr: Vec<u8> = Vec::new();

        let mut i = 0;
        while i < buf.len() && (buf[i] & 0x80) == 0x80 {
            chunk_arr.push(buf[i] & 0x7F); // strip the continuation bit
            i += 1;
        }

        // check if we've reached the end of the buffer without a terminating byte
        if i >= buf.len() {
            println!("Incomplete VarInt: No terminating byte found");
            return Err(KafkaError::DecodeError);
        }

        chunk_arr.push(buf[i] & 0x7F); // add the last byte (without MSB)

        chunk_arr.reverse(); // little-endian to big-endian conversion

        // decode the VarInt from chunks
        let mut integer: i32 = 0;
        for chunk in &chunk_arr {
            integer <<= 7;
            integer |= *chunk as i32;
        }

        Ok( (VarInt { data: integer }, chunk_arr.len()) )
    }
}



//
// COMPACT_STRING
//

#[derive(Clone)]
pub struct CompactString {
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

        // encode length prefix as a varint
        buf.extend(
            VarInt{
                data: self.data.len() as i32 + 1
            }.encode());
        buf.extend(self.data.bytes());

        buf
    }
}

impl Decodable for CompactString {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        let mut byte_offset = 0;

        match VarInt::decode(buf) {
            Ok( (varint, varint_byte_length) ) => {
                byte_offset += varint_byte_length;
                let data_length = varint.data - 1;

                if data_length == 0 {
                    return Ok( (CompactString {
                        data: String::new()
                    }, byte_offset) );
                }

                if buf.len() < byte_offset + data_length as usize {
                    println!("Buffer does not contain enough data for the string");
                    return Err(KafkaError::DecodeError);
                }

                match String::from_utf8(buf[byte_offset..byte_offset + data_length as usize].to_vec()) {
                    Ok(data) => {
                        byte_offset += data_length as usize;
                        return Ok((CompactString {
                            data
                        }, byte_offset));
                    },
                    Err(_) => {
                        println!("Could not decode UTF-8 string");
                        return Err(KafkaError::DecodeError);
                    }
                }
            },
            Err(_) => {
                println!("Could not decode VarInt");
                return Err(KafkaError::DecodeError);
            }
        }
    }
}


//
// NULLABLE_STRING
//

pub struct NullableString {
    pub data: Option<String>
}

impl NullableString {
    pub fn new(data: Option<String>) -> Self {
        NullableString {
            data: data
        }
    }
}

impl Encodable for NullableString {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();

        match &self.data {
            Some(value) => {
                buf.extend((value.len() as i16).to_be_bytes()); // length prefix
                buf.extend(value.bytes());
            }
            None => {
                buf.extend((-1 as i16).to_be_bytes()); // -1 indicates a null string
            }
        }
        buf
    }
}

impl Decodable for NullableString {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        // get length prefix
        if buf.len() < 2 {
            println!("Buffer too short to decode NullableString");
            return Err(KafkaError::DecodeError);
        }

        let length = i16::from_be_bytes([buf[0], buf[1]]);

        if length == -1 {
            // length of -1 indicates a null string
            return Ok( (NullableString{ data: None }, 0) )
        }

        let str_bytes = &buf[2..]; // get remaining bytes
        if str_bytes.len() < length as usize {
            println!("Buffer does not contain enough data for the string");
            return Err(KafkaError::DecodeError);
        }

        let string_data = String::from_utf8(str_bytes[..length as usize].to_vec())
            .expect("Failed to decode UTF-8 string");

        Ok( (NullableString { data: Some(string_data) }, 2 + length as usize) )
    }
}


//
// COMPACT_NULLABLE_STRING
//

pub struct CompactNullableString {
    pub data: Option<CompactString>
}

impl CompactNullableString {
    pub fn new(data: Option<CompactString>) -> Self {
        CompactNullableString {
            data: data
        }
    }
}

impl Encodable for CompactNullableString {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();

        match &self.data {
            Some(value) => {
                buf.extend(value.encode());
            }
            None => {
                buf.extend((0 as u8).to_be_bytes()); // length of 0 indicates a null string
            }
        }
        buf
    }
}

impl Decodable for CompactNullableString {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {

        let mut byte_offset = 0;

        match VarInt::decode(buf) {
            Ok( (varint,varint_byte_length) ) => {
                byte_offset += varint_byte_length;
                let data_length = varint.data - 1;



                if data_length == 0 {
                    return Ok((CompactNullableString {
                        data: None
                    }, byte_offset));
                }

                if buf.len() < byte_offset + data_length as usize {
                    println!("Buffer does not contain enough data for the string");
                    return Err(KafkaError::DecodeError);
                }

                let data = CompactString::decode(&buf[byte_offset..byte_offset + data_length as usize])?;
                byte_offset += data.1;

                return Ok((CompactNullableString {
                    data: Some(data.0)
                }, byte_offset));
            },
            Err(_) => {
                println!("Could not decode VarInt");
                return Err(KafkaError::DecodeError);
            }
        }
    }
}


//
// COMPACT_ARRAY
//

pub struct CompactArray<T> {
    pub data: Vec<T>
}

impl<T> CompactArray<T> {
    pub fn new(data: Vec<T>) -> Self {
        CompactArray {
            data: data
        }
    }
}

impl<T: Encodable> Encodable for CompactArray<T> {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();

        buf.extend(
            VarInt {
                data: self.data.len() as i32 + 1
            }.encode()
        );

        for item in &self.data {
            buf.extend(item.encode());
        }

        buf
    }
}

// TODO: Placeholder implementation
impl<T: Decodable> Decodable for CompactArray<T> {
    fn decode(buf: &[u8]) -> Result<(Self, usize), KafkaError> {
        let mut byte_offset = 0;

        match VarInt::decode(buf) {
            Ok( (varint,varint_byte_length) ) => {
                byte_offset += varint_byte_length;
                let array_length = varint.data - 1;

                let mut array: Vec<T> = Vec::new();
                for _ in 0..array_length {
                    let item = T::decode(&buf[byte_offset..])?;
                    byte_offset += item.1;
                    array.push(item.0);
                }

                return Ok((CompactArray {
                    data: array
                }, byte_offset));
            },
            Err(_) => {
                println!("Could not decode VarInt");
                return Err(KafkaError::DecodeError);
            }
        }
    }
}