use crate::avro::get_schema;
use crate::context::{parse_app_ctx, AppCmd, AppCtx, AvroCtx, KafkaCtx};
use crate::error::CliError;
use crate::producer::Producer;
use avro_rs::types::Value as AvroValue;
use clap::{App, AppSettings, Arg, ArgGroup, ArgMatches};
use serde_json::Value as JsonValue;
use std::error::Error;
use std::process;

mod avro;
mod context;
mod data;
pub mod error;
mod producer;

fn main() -> Result<(), CliError> {
    let arg_matches = match_args();
    let app_ctx = &parse_app_ctx(&arg_matches);

    match app_ctx.command {
        AppCmd::Produce => produce(&app_ctx),
        AppCmd::Consume => Ok(()),
    }
}

fn produce(ctx: &AppCtx) -> Result<(), CliError> {
    // read payload
    let payload = if let Some(ref data) = ctx.payload {
        vec![data.clone()]
    } else if let Some(ref path) = ctx.payload_file {
        data::read_payload(path)?
    } else {
        panic!("payload expected")
    };

    //is it Avro?
    let encoded: Vec<Vec<u8>> = if ctx.is_avro {
        let jsons = payload
            .iter()
            .map(|raw_line| data::parse_json(raw_line))
            .collect::<serde_json::Result<Vec<JsonValue>>>()?;
        let (schema_id, schema) = avro::get_schema(ctx)?;
        let avros = jsons
            .iter()
            .map(|json| avro::to_avro(json, &schema))
            .collect::<Result<Vec<AvroValue>, ()>>()?; //TODO: err type
        avros
            .into_iter()
            .map(|avro| avro::encode(avro, &schema, schema_id))
            .collect::<Result<Vec<Vec<u8>>, avro_rs::Error>>()?
    } else {
        payload.into_iter().map(|s| s.into_bytes()).collect()
    };

    Producer::produce(&ctx.kafka_ctx, encoded).map_err(|e| e.into())
}

fn match_args() -> ArgMatches {
    App::new("Kafka Avro CLI")
        .about("Produces/consumes Avro serialized messages into Kafka")
        .setting(AppSettings::NoBinaryName)
        .setting(AppSettings::SubcommandRequired)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            App::new("produce")
                .about("Produces a Kafka message")
                .arg(
                    Arg::new("text")
                        .about("Message input is just a plain text. (JSON by default)")
                        .long("text")
                        .short('T')
                        .required(false),
                )
                .arg(
                    Arg::new("hosts")
                        .about("Kafka hosts")
                        .short('h')
                        .long("hosts")
                        .takes_value(true)
                        .value_name("host:port[,host:port[...]]")
                        .required(true),
                )
                .arg(
                    Arg::new("topic")
                        .about("Topic name")
                        .short('t')
                        .long("topic")
                        .takes_value(true)
                        .value_name("TOPIC")
                        .required(true),
                )
                .arg(
                    Arg::new("payload")
                        .about("Message payload. JSON expected if '--text' flag is not present")
                        .short('p')
                        .long("payload")
                        .multiple_values(false)
                        .value_name("JSON")
                        .required_unless_present("text")
                        .conflicts_with("payload-file")
                )
                .arg(
                    Arg::new("payload-file")
                        .about("New-line delimited file. Each row is a message payload. (JSON or plain text in respect with `text`)")
                        .long("payload-file")
                        .multiple_values(false)
                        .value_name("PATH")
                        .required_unless_present("text")
                        .conflicts_with("payload")
                )
                .arg(
                    Arg::new("register-schema")
                        .about("Register the Avro schema if it wasn't found")
                        .long("register-schema")
                        .requires("registry-url")
                        .conflicts_with("text")
                )
                .arg(
                    Arg::new("schema")
                        .about("Avro schema used to serialize payload")
                        .short('s')
                        .long("schema")
                        .multiple_values(false)
                        .value_name("SCHEMA JSON")
                        .required_unless_present("text")
                        .conflicts_with("schema-file")
                )
                .arg(
                    Arg::new("schema-file")
                        .about("File containing the Avro schema used to serialize payload")
                        .long("schema")
                        .multiple_values(false)
                        .value_name("PATH")
                        .required_unless_present("text")
                        .conflicts_with("schema"),
                )
                .arg(
                    Arg::new("registry-url")
                        .about("Schema-registry url")
                        .long("registry-url")
                        .multiple_values(false)
                        .value_name("http[s]://host:port"),
                ),
        )
        .subcommand(App::new("consume").about("Consumes Kafka messages. UNIMPLEMENTED"))
        .get_matches()
}
