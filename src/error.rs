use crate::error::CliError::{Avro, Empty, Json, Kafka, SchemaRegistry, IO};
use avro_rs;
use schema_registry_converter;
use schema_registry_converter::error::SRCError;
use serde_json;
use std::io;

#[derive(Debug)]
pub enum CliError {
    Empty,
    SchemaRegistry(schema_registry_converter::error::SRCError),
    Avro(avro_rs::Error),
    IO(io::Error),
    Json(serde_json::Error),
    Kafka(rdkafka::error::KafkaError),
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

impl From<()> for CliError {
    fn from(err: ()) -> Self {
        Empty
    }
}
