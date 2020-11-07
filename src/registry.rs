use avro_rs::Schema;
use core::fmt;
use serde::export::Formatter;
use crate::context::SslCtx;
use serde_json::json;
use serde::Deserialize;

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

pub struct RegistryClient {
    url: String,
}

impl RegistryClient {
    pub fn new(url: &str) -> RegistryClient {
        RegistryClient {
           url: url.to_string()
        }
    }

    pub fn get_schema_by_subject(&self, subject: &str) -> RegistryResult<(u32, Schema)> {
        Err(RegistryError::All)
    }

    pub fn register_schema(&self, subject: &str, raw_schema: &str) -> RegistryResult<(u32, Schema)> {
        let mut req = ureq::post(&format!("{}/subjects/{}/versions", self.url, subject))
            .set("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
            .build();

        // req.set_tls_connector(self.tls_connector.clone());

        let resp = req.send_json(json!({"schema": raw_schema}));

        if let Some(err) = resp.synthetic_error() {
            panic!();
        };

        let status = resp.status(); //TODO check it

        let body: PostResp = resp.into_json_deserialize().unwrap(); //TODO

        Ok((body.id, Schema::parse_str(&raw_schema).unwrap())) //TODO
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

#[derive(Deserialize)]
struct PostResp {
    id: u32,
}
