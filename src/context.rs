use crate::error::CliError;
use clap::ArgMatches;
use std::fs::read_to_string;

pub struct SslCtx {
    pub enabled: bool,
    pub cert_validate: bool,
    pub host_validate: bool,
    pub key_location: Option<String>,
    pub key_password: Option<String>,
    pub cert_location: Option<String>,
    pub ca_location: Option<String>,
    pub keystore_location: Option<String>,
    pub keystore_password: Option<String>,
}

pub struct KafkaCtx {
    pub hosts: String,
    pub topic: String,
    pub ssl: SslCtx,
}

pub struct AvroCtx {
    pub registry_url: Option<String>,
    pub schema: Option<String>,
}

pub enum AppCmd {
    Produce,
    Consume,
}

pub struct AppCtx {
    pub command: AppCmd,
    pub is_avro: bool,
    pub payload: Option<String>,
    pub payload_file: Option<String>,
    pub kafka_ctx: KafkaCtx,
    pub avro_ctx: AvroCtx,
}

pub fn parse_app_ctx(arg_matches: &ArgMatches) -> Result<AppCtx, CliError> {
    // parse command
    let (subcommand_args, command) = arg_matches
        .subcommand_matches("produce")
        .map(|a| (a, AppCmd::Produce))
        .or_else(|| {
            arg_matches
                .subcommand_matches("consume")
                .map(|a| (a, AppCmd::Consume))
        })
        .expect("subcommand expected");

    let hosts: String = subcommand_args
        .value_of("hosts")
        .expect("hosts expected")
        .to_owned();
    let topic: String = subcommand_args
        .value_of("topic")
        .expect("topic expected")
        .to_owned();
    let is_json = !subcommand_args.is_present("text");

    let payload = subcommand_args.value_of("payload").map(|s| s.to_owned());
    let payload_file = subcommand_args
        .value_of("payload-file")
        .map(|s| s.to_owned());
    if payload.is_none() && payload_file.is_none() {
        panic!("payload expected")
    }

    let ssl = parse_ssl_ctx(subcommand_args)?;

    parse_avro_ctx(subcommand_args).map(|avro_ctx| AppCtx {
        command,
        is_avro: is_json,
        payload,
        payload_file,
        kafka_ctx: KafkaCtx { hosts, topic, ssl },
        avro_ctx,
    })
}

fn parse_avro_ctx(arg_matches: &ArgMatches) -> Result<AvroCtx, CliError> {
    let schema = arg_matches.value_of("schema").map(|s| s.to_owned());
    // try to read schema from file if path was passed as an arg
    let schema_file = arg_matches
        .value_of("schema-file")
        .map(read_to_string)
        .transpose()?;

    Ok(AvroCtx {
        registry_url: arg_matches.value_of("registry-url").map(|s| s.to_owned()),
        schema: schema.or(schema_file),
    })
}

fn parse_ssl_ctx(arg_matches: &ArgMatches) -> Result<SslCtx, CliError> {
    let key_location = arg_matches
        .value_of("ssl-key-location")
        .map(|s| s.to_owned());
    let key_password = arg_matches
        .value_of("ssl-key-password")
        .map(|s| s.to_owned());
    let cert_location = arg_matches
        .value_of("ssl-cert-location")
        .map(|s| s.to_owned());
    let ca_location = arg_matches
        .value_of("ssl-ca-location")
        .map(|s| s.to_owned());
    let keystore_location = arg_matches
        .value_of("ssl-keystore-location")
        .map(|s| s.to_owned());
    let keystore_password = arg_matches
        .value_of("ssl-keystore-password")
        .map(|s| s.to_owned());

    Ok(SslCtx {
        enabled: arg_matches.is_present("ssl-enabled"),
        cert_validate: !arg_matches.is_present("ssl-disable-validate"),
        host_validate: arg_matches.is_present("ssl-host-validate"),
        key_location,
        key_password,
        cert_location,
        ca_location,
        keystore_location,
        keystore_password,
    })
}
