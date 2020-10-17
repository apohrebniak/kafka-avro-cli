use crate::context::{AppCtx, AvroCtx};
use crate::error::CliError;
use avro_rs::schema::RecordField;
use avro_rs::schema::Schema::Uuid;
use avro_rs::types::Value as AvroValue;
use avro_rs::{AvroResult, Decimal, Reader, Schema, Writer};
use core::fmt;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common;
use schema_registry_converter::schema_registry_common::{
    RegisteredSchema, SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use serde::export::Formatter;
use serde::export::Option::Some;
use serde::Deserializer;
use serde::{Serialize, Serializer};
use serde_json::{Value as JsonValue, Value};
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::read_to_string;

pub fn get_schema(ctx: &AppCtx) -> Result<(u32, Schema), CliError> {
    let schema = &ctx.avro_ctx.schema;
    let schema_file = &ctx.avro_ctx.schema_file;
    let registry_url = ctx
        .avro_ctx
        .registry_url
        .as_ref()
        .expect("registry url expected")
        .clone();
    let subject_strategy =
        &SubjectNameStrategy::TopicNameStrategy(ctx.kafka_ctx.topic.clone(), false);

    // schema passed as an arg
    if let Some(raw_schema) = schema {
        let registered_schema = register_schema(
            registry_url,
            schema_registry_common::get_subject(subject_strategy).unwrap(),
            raw_schema.as_str(),
        )?;

        return Schema::parse_str(registered_schema.schema.as_str())
            .map(|schema| (registered_schema.id, schema))
            .map_err(|e| e.into());
    }

    // schema stored in the file
    if let Some(schema_path) = schema_file {
        let raw_schema = read_to_string(schema_path)?;
        let registered_schema = register_schema(
            registry_url,
            schema_registry_common::get_subject(subject_strategy).unwrap(),
            raw_schema.as_str(),
        )?;

        return Schema::parse_str(registered_schema.schema.as_str())
            .map(|schema| (registered_schema.id, schema))
            .map_err(|e| e.into());
    }

    // fetch schema
    let registered_schema = fetch_schema(registry_url, subject_strategy)?;
    return Schema::parse_str(registered_schema.schema.as_str())
        .map(|schema| (registered_schema.id, schema))
        .map_err(|e| e.into());
}

fn fetch_schema(
    registry_url: String,
    strategy: &SubjectNameStrategy,
) -> Result<RegisteredSchema, SRCError> {
    schema_registry_converter::blocking::schema_registry::get_schema_by_subject(
        &SrSettings::new(registry_url),
        strategy,
    )
}

/// Register user defined Avro schema.
/// Returns parsed schema and ID
fn register_schema(
    registry_url: String,
    subject: String,
    raw_schema: &str,
) -> Result<RegisteredSchema, SRCError> {
    schema_registry_converter::blocking::schema_registry::post_schema(
        &SrSettings::new(registry_url),
        subject,
        SuppliedSchema {
            name: None,
            schema_type: SchemaType::Avro,
            schema: raw_schema.to_string(),
            references: vec![],
        },
    )
}

pub fn encode(value: AvroValue, schema: &Schema, schema_id: u32) -> AvroResult<Vec<u8>> {
    avro_rs::to_avro_datum(schema, value)
        .map(|bytes| schema_registry_common::get_payload(schema_id, bytes))
}

pub fn to_avro(json: &JsonValue, schema: &Schema) -> Result<AvroValue, ()> {
    match (schema, json) {
        (Schema::Null, JsonValue::Null) => Ok(AvroValue::Null),
        (Schema::Boolean, JsonValue::Bool(b)) => Ok(AvroValue::Boolean(b.clone())),
        (Schema::Int, JsonValue::Number(ref n)) if n.is_i64() => {
            Ok(AvroValue::Int(n.as_f64().unwrap() as i32))
        }
        (Schema::Long, JsonValue::Number(ref n)) if n.is_i64() => {
            Ok(AvroValue::Long(n.as_i64().unwrap()))
        }
        (Schema::Float, JsonValue::Number(ref n)) if n.is_f64() => {
            Ok(AvroValue::Float(n.as_f64().unwrap() as f32))
        }
        (Schema::Double, JsonValue::Number(ref n)) if n.is_f64() => {
            Ok(AvroValue::Double(n.as_f64().unwrap()))
        }
        (Schema::String, JsonValue::String(s)) => Ok(AvroValue::String(s.clone())),
        (Schema::Array(ref el), JsonValue::Array(ref vs)) => {
            let items: Vec<AvroValue> = vs.iter().map(|v| to_avro(v, el).unwrap()).collect();
            Ok(AvroValue::Array(items))
        }
        (Schema::Map(ref el), JsonValue::Object(ref map)) => {
            let items: HashMap<String, AvroValue> = map
                .iter()
                .map(|(key, value)| (key.clone(), to_avro(value, el).unwrap()))
                .collect();
            Ok(AvroValue::Map(items))
        }
        (Schema::Record { fields, lookup, .. }, JsonValue::Object(map)) => {
            let items: Vec<(String, AvroValue)> = map
                .iter()
                .map(|(key, value)| {
                    let field: &RecordField = fields.get(lookup.get(key).unwrap().clone()).unwrap();
                    (key.clone(), to_avro(value, &field.schema).unwrap())
                })
                .collect();
            Ok(AvroValue::Record(items))
        }
        (Schema::Enum { .. }, JsonValue::String(s)) => Err(()),
        (Schema::Union(union_schema), json_value) => {
            match &json_value {
                Value::Null => Ok(AvroValue::Union(Box::new(AvroValue::Null))),
                Value::Bool(b) => Ok(AvroValue::Union(Box::new(AvroValue::Boolean(b.clone())))),
                Value::Number(ref n) => {
                    if n.is_i64() {
                        let (_, schema) = union_schema
                            .find_schema(&AvroValue::Long(0))
                            .or_else(|| union_schema.find_schema(&AvroValue::Int(0)))
                            .unwrap(); //TODO
                        to_avro(json_value, schema)
                    } else if n.is_f64() {
                        let (_, schema) = union_schema
                            .find_schema(&AvroValue::Double(0f64))
                            .or_else(|| union_schema.find_schema(&AvroValue::Float(0f32)))
                            .unwrap(); //TODO
                        to_avro(json_value, schema)
                    } else {
                        Err(())
                    }
                }
                Value::String(ref s) => {
                    let schema = union_schema
                        .find_schema(&AvroValue::String(String::new()))
                        .map(|(_, schema)| schema)
                        .unwrap(); //TODO
                    to_avro(json_value, schema)
                }
                Value::Array(items) => {
                    let schema = union_schema
                        .find_schema(&AvroValue::Array(Vec::new()))
                        .map(|(_, schema)| schema)
                        .unwrap(); //TODO
                    to_avro(json_value, schema)
                }
                Value::Object(map) => {
                    let (_, schema) = union_schema
                        .find_schema(&AvroValue::Record(Vec::new()))
                        .or_else(|| union_schema.find_schema(&AvroValue::Map(HashMap::new())))
                        .unwrap(); //TODO
                    to_avro(json_value, schema)
                }
            }
        }
        _ => Err(()),
    }
}
