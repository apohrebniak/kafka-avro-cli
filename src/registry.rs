use crate::context::{AppCtx, SslCtx};
use avro_rs::Schema;
use core::fmt;
use native_tls::{Certificate, Identity, TlsConnector, TlsConnectorBuilder};
use serde::de::DeserializeOwned;
use serde::export::Formatter;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use serde_json::{json, Value};
use std::io::Read;
use std::sync::Arc;
use std::{fs, io};
use ureq::Request;

const ACCEPT_HEADER_VALUE: &str =
    "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json";

#[derive(thiserror::Error, Debug)]
pub enum RegistryError {
    All,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Tls(#[from] native_tls::Error),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, _f: &mut Formatter<'_>) -> fmt::Result {
        unimplemented!() //TODO
    }
}

type RegistryResult<T> = Result<T, RegistryError>;

pub struct RegistryClient {
    url: String,
    tls_connector: Option<Arc<TlsConnector>>,
}

impl RegistryClient {
    pub fn new(ctx: &AppCtx) -> RegistryResult<RegistryClient> {
        let tls_connector = if ctx.ssl.enabled {
            RegistryClient::get_tls_connector(&ctx.ssl)?
        } else {
            None
        };

        Ok(RegistryClient {
            url: ctx
                .avro_ctx
                .registry_url
                .as_ref()
                .expect("registry url expected")
                .trim()
                .to_string(),
            tls_connector: tls_connector.map(|c| Arc::new(c)),
        })
    }

    pub fn get_schema_by_subject(&self, subject: &str) -> RegistryResult<(u32, String)> {
        let resp: GetResp = self.do_request(
            ureq::get,
            &format!("{}/subjects/{}/versions/latest", self.url, subject),
            None,
        );

        Ok((resp.id, resp.schema))
    }

    pub fn register_schema(&self, subject: &str, raw_schema: &str) -> RegistryResult<u32> {
        let resp: PostResp = self.do_request(
            ureq::post,
            &format!("{}/subjects/{}/versions", self.url, subject),
            Some(json!({ "schema": raw_schema })),
        );

        Ok(resp.id)
    }

    fn do_request<T: DeserializeOwned>(
        &self,
        func: fn(&str) -> Request,
        url: &str,
        json: Option<JsonValue>,
    ) -> T {
        let mut req = func(url).set("Accept", ACCEPT_HEADER_VALUE).build();

        if let Some(connector) = self.tls_connector.as_ref() {
            req.set_tls_connector(connector.clone());
        }

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

    fn get_tls_connector(ssl: &SslCtx) -> RegistryResult<Option<TlsConnector>> {
        let mut builder = TlsConnector::builder();

        builder.danger_accept_invalid_certs(!ssl.cert_validate);
        builder.danger_accept_invalid_hostnames(!ssl.host_validate);

        if let Some(ref ca_path) = &ssl.ca_location {
            let cert = RegistryClient::get_root_cert(ca_path)?;
            builder.add_root_certificate(cert);
        }

        if let (Some(ref pkcs_path), Some(ref password)) =
            (&ssl.keystore_location, &ssl.keystore_password)
        {
            let identity = RegistryClient::get_identity(pkcs_path, password)?;
            builder.identity(identity);
        }

        builder.build().map(|c| Some(c)).map_err(|e| e.into())
    }

    fn get_root_cert(ca_path: &str) -> RegistryResult<Certificate> {
        fs::read(ca_path)
            .map_err(|e| e.into())
            .and_then(|pem| Certificate::from_pem(&pem).map_err(|e| e.into()))
    }

    fn get_identity(pkcs_path: &str, password: &str) -> RegistryResult<Identity> {
        fs::read(pkcs_path)
            .map_err(|e| e.into())
            .and_then(|der| Identity::from_pkcs12(&der, password).map_err(|e| e.into()))
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

#[derive(Deserialize)]
struct GetResp {
    id: u32,
    schema: String,
}
