use avro_rs::Schema;
use core::fmt;
use serde::export::Formatter;

#[derive(Debug)]
pub enum RegistryError {
    All,
}

impl fmt::Display for RegistryError {
    fn fmt(&self, _f: &mut Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

type RegistryResult<T> = Result<T, RegistryError>;

pub struct RegistryClient {}

impl RegistryClient {
    pub fn new() -> RegistryClient {
        RegistryClient {}
    }

    pub fn get_schema_by_subject(&self, _subject: &str) -> RegistryResult<(u32, Schema)> {
        Err(RegistryError::All)
    }

    pub fn register_schema(&self, _subject: &str, _schema: &Schema) -> RegistryResult<u32> {
        Err(RegistryError::All)
    }
}

/// Returns a subject name using Topic Name strategy
/// May be subject to change
pub fn get_subject(topic: &str) -> String {
    format!("{}-value", topic)
}

pub fn append_schema_id(id: u32, encoded_bytes: Vec<u8>) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::new();
    let id_bytes: [u8; 4] = u32::to_be_bytes(id);
    result.extend_from_slice(&id_bytes);
    result.extend_from_slice(encoded_bytes.as_slice());
    result
}
