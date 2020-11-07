use crate::context::AvroCtx;
use crate::error::CliError;
use crate::registry;
use avro_rs::schema::UnionSchema;

use avro_rs::types::Value as AvroValue;
use avro_rs::{AvroResult, Schema};

use serde::export::Option::Some;

use serde_json::{Value as JsonValue, Value};
use std::collections::HashMap;

/// Parse Avro schema.
pub fn parse_schema(raw_schema: &str) -> Result<Schema, CliError> {
    Schema::parse_str(raw_schema).map_err(|e| e.into())
}

pub fn get_registered_schema(
    //TODO: consider change
    ctx: &AvroCtx,
    topic: &str,
) -> Result<(u32, Schema), CliError> {
    let _registry_url = ctx
        .registry_url
        .as_ref()
        .expect("registry url expected")
        .clone();

    let subject = registry::get_subject(topic);
    let registry_client = registry::RegistryClient::new(&_registry_url);

    match &ctx.schema {
        Some(raw_schema) => {
            registry_client
                .register_schema(&subject, &raw_schema)
                .map_err(|e| e.into())
        }
        None => registry_client
            .get_schema_by_subject(&subject)
            .map_err(|e| e.into()),
    }
}

pub fn encode(value: AvroValue, schema: &Schema) -> AvroResult<Vec<u8>> {
    avro_rs::to_avro_datum(schema, value)
}

pub fn encode_with_schema_id(
    value: AvroValue,
    schema: &Schema,
    schema_id: u32,
) -> AvroResult<Vec<u8>> {
    avro_rs::to_avro_datum(schema, value).map(|bytes| registry::append_schema_id(schema_id, bytes))
}

pub fn map_with_schema(json: &JsonValue, schema: &Schema) -> Result<AvroValue, CliError> {
    match (schema, json) {
        (Schema::Null, JsonValue::Null) => Ok(AvroValue::Null),
        (Schema::Boolean, JsonValue::Bool(b)) => Ok(AvroValue::Boolean(*b)),
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
        (Schema::Array(ref el), JsonValue::Array(ref vals)) => {
            let items: Vec<AvroValue> = vals
                .iter()
                .map(|v| map_with_schema(v, el))
                .collect::<Result<Vec<AvroValue>, CliError>>()?;
            Ok(AvroValue::Array(items))
        }
        (Schema::Map(ref el), JsonValue::Object(ref map)) => {
            let items: HashMap<String, AvroValue> = map
                .iter()
                .map(|(key, value)| map_with_schema(value, el).map(|value| (key.clone(), value)))
                .collect::<Result<HashMap<String, AvroValue>, CliError>>()?;
            Ok(AvroValue::Map(items))
        }
        (Schema::Record { fields, .. }, JsonValue::Object(value_map)) => {
            let items = fields
                .iter()
                .map(|field| match value_map.get(&field.name) {
                    Some(value) => map_with_schema(value, &field.schema)
                        .map(|value| (field.name.clone(), value)),
                    None => map_with_schema(&JsonValue::Null, &field.schema)
                        .map(|value| (field.name.clone(), value)),
                })
                .collect::<Result<Vec<(String, AvroValue)>, CliError>>()?;
            Ok(AvroValue::Record(items))
        }
        (Schema::Enum { symbols, .. }, JsonValue::String(value)) => {
            match symbols.iter().enumerate().find(|pair| value.eq((*pair).1)) {
                Some((idx, s)) => Ok(AvroValue::Enum(idx as i32, s.clone())),
                None => Err(CliError::Mapping(symbols.join(","), value.clone())),
            }
        }
        (Schema::Union(union_schema), json_value) => {
            match get_suitable_type_from_union(union_schema, json_value) {
                Some(s) => map_with_schema(json_value, s).map(|v| AvroValue::Union(Box::new(v))),
                None => Err(CliError::Mapping(
                    Schema::Union(union_schema.clone()).canonical_form(),
                    json_value.to_string(),
                )),
            }
        }
        (s, j) => Err(CliError::Mapping(s.canonical_form(), j.to_string())),
    }
}

//TODO: find another way to get the schema
fn get_suitable_type_from_union<'s, 'v>(
    union_schema: &'s UnionSchema,
    json_value: &'v JsonValue,
) -> Option<&'s Schema> {
    match json_value {
        Value::Null => union_schema.find_schema(&AvroValue::Null).map(|(_, s)| s),
        Value::Bool(_b) => union_schema
            .find_schema(&AvroValue::Boolean(false))
            .map(|(_, s)| s),
        Value::Number(_n) => union_schema
            .find_schema(&AvroValue::Long(0))
            .or_else(|| union_schema.find_schema(&AvroValue::Int(0)))
            .or_else(|| union_schema.find_schema(&AvroValue::Float(0f32)))
            .or_else(|| union_schema.find_schema(&AvroValue::Double(0f64)))
            .map(|(_, s)| s),
        Value::String(ref _s) => union_schema
            .find_schema(&AvroValue::String(String::new()))
            .map(|(_, s)| s),
        Value::Array(_items) => union_schema
            .find_schema(&AvroValue::Array(Vec::new()))
            .map(|(_, s)| s),
        Value::Object(_map) => union_schema
            .find_schema(&AvroValue::Record(Vec::new()))
            .or_else(|| union_schema.find_schema(&AvroValue::Map(HashMap::new())))
            .map(|(_, s)| s),
    }
}
