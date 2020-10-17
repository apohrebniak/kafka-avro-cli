// hosts +
// topic +
// payload
// schema +
// schema-registry-url +
// plain-text? +
// ssl opts (multiple) +
// payload-file-path
// schema-file-path

use clap::ArgMatches;

pub struct KafkaCtx {
    //TODO: SSL
    pub hosts: String,
    pub topic: String,
}

pub struct AvroCtx {
    pub registry_url: Option<String>,
    pub schema: Option<String>,
    pub schema_file: Option<String>,
    pub register_schema: Option<bool>,
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

pub fn parse_app_ctx(arg_matches: &ArgMatches) -> AppCtx {
    // parse command
    let (args, command) = arg_matches
        .subcommand_matches("produce")
        .map(|a| (a, AppCmd::Produce))
        .or(arg_matches
            .subcommand_matches("produce")
            .map(|a| (a, AppCmd::Consume)))
        .expect("subcommand expected");

    let hosts: String = args.value_of("hosts").expect("hosts expected").to_owned();
    let topic: String = args.value_of("topic").expect("topic expected").to_owned();
    let is_json = !args.is_present("text");

    let payload = args.value_of("payload").map(|s| s.to_owned());
    let payload_file = args.value_of("payload-file").map(|s| s.to_owned());
    if payload.is_none() && payload_file.is_none() {
        panic!("payload expected")
    }

    AppCtx {
        command,
        is_avro: is_json,
        payload,
        payload_file,
        kafka_ctx: KafkaCtx { hosts, topic },
        avro_ctx: parse_avro_ctx(args),
    }
}

fn parse_avro_ctx(arg_matches: &ArgMatches) -> AvroCtx {
    let schema = arg_matches.value_of("schema").map(|s| s.to_owned());
    let schema_file = arg_matches.value_of("schema").map(|s| s.to_owned());

    AvroCtx {
        registry_url: arg_matches.value_of("registry-url").map(|s| s.to_owned()),
        schema,
        schema_file,
        register_schema: None, //TODO
    }
}
