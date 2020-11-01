use crate::error::CliError::{Avro, Json, Kafka, Mapping, SchemaRegistry, IO};
use avro_rs;

use core::fmt;
use core::fmt::Debug;
use schema_registry_converter;
use schema_registry_converter::error::SRCError;
use serde::export::Formatter;
use serde_json;
use std::io;

pub enum CliError {
    SchemaRegistry(schema_registry_converter::error::SRCError),
    Avro(avro_rs::Error),
    IO(io::Error),
    Json(serde_json::Error),
    Kafka(rdkafka::error::KafkaError),
    Mapping(String, String),
}

impl Debug for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SchemaRegistry(e) => write!(f, "schema registry error: {}", e),
            Avro(e) => write!(f, "avro error: {:?}", e),
            IO(e) => write!(f, "schema IO error: {}", e),
            Json(e) => write!(f, "json parsing error: {}", e),
            Kafka(e) => write!(f, "kafka error: {}", e),
            Mapping(schema, value) => write!(f, "cannot convert {} into {}", value, schema),
        }
    }
}

impl From<schema_registry_converter::error::SRCError> for CliError {
    fn from(err: SRCError) -> Self {
        SchemaRegistry(err)
    }
}

impl From<avro_rs::Error> for CliError {
    fn from(err: avro_rs::Error) -> Self {
        Avro(err)
    }
}

impl From<io::Error> for CliError {
    fn from(err: io::Error) -> Self {
        IO(err)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(err: serde_json::Error) -> Self {
        Json(err)
    }
}

impl From<rdkafka::error::KafkaError> for CliError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        Kafka(err)
    }
}
