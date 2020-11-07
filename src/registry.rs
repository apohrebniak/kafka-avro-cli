use crate::context::SslCtx;
use avro_rs::Schema;
use core::fmt;
use serde::de::DeserializeOwned;
use serde::export::Formatter;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use serde_json::{json, Value};
use ureq::Request;

const ACCEPT_HEADER_VALUE: &str =
    "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json";

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
            url: url.to_string(), //TODO: strip /
        }
    }

    pub fn get_schema_by_subject(&self, subject: &str) -> RegistryResult<(u32, Schema)> {
        let resp: GetResp = do_request(
            ureq::get,
            &format!("{}/subjects/{}/versions/latest", self.url, subject),
            None,
        );

        Ok((resp.id, Schema::parse_str(&resp.schema).unwrap())) //TODO
    }

    pub fn register_schema(
        &self,
        subject: &str,
        raw_schema: &str,
    ) -> RegistryResult<(u32, Schema)> {
        let resp: PostResp = do_request(
            ureq::post,
            &format!("{}/subjects/{}/versions", self.url, subject),
            Some(json!({ "schema": raw_schema })),
        );

        Ok((resp.id, Schema::parse_str(&raw_schema).unwrap())) //TODO
    }
}

fn do_request<T: DeserializeOwned>(
    func: fn(&str) -> Request,
    url: &str,
    json: Option<JsonValue>,
) -> T {
    let mut req = func(url).set("Accept", ACCEPT_HEADER_VALUE).build();

    // req.set_tls_connector(self.tls_connector.clone());

    let resp = match json {
        None => req.call(),
        Some(body) => req.send_json(body),
    };

    if let Some(err) = resp.synthetic_error() {
        panic!();
    };

    let status = resp.status(); //TODO check it

    resp.into_json_deserialize::<T>().unwrap() //TODO
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

#[derive(Deserialize)]
struct GetResp {
    id: u32,
    schema: String,
}
