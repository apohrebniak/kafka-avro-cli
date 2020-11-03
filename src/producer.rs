use crate::context::KafkaCtx;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::error::KafkaError;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{BaseRecord, ProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, ClientContext};
use std::sync::mpsc::{channel, Sender};
use std::sync::Mutex;

const PRODUCER_MAX_RETRIES: &str = "0";

pub struct Producer;

impl Producer {
    pub fn produce(kafka_ctx: &KafkaCtx, payloads: Vec<Vec<u8>>) -> Result<(), KafkaError> {
        //config
        let client_cfg = build_kafka_config(kafka_ctx);

        //context
        let (ctx_sender, ctx_receiver) = channel::<Result<(), KafkaError>>();
        let context = BlockingProducerContext::new(ctx_sender);

        //producer
        let prod = ThreadedProducer::from_config_and_context(&client_cfg, context)?;

        for payload in &payloads {
            //actual send
            prod.send(BaseRecord::<(), [u8]>::to(&kafka_ctx.topic).payload(payload.as_slice()))
                .map_err(|(kafka_err, _)| kafka_err)?;
        }

        // wait for send confirmation by librdkafka
        (0..payloads.len())
            .map(|_| ctx_receiver.recv().unwrap())
            .collect()
    }
}

fn build_kafka_config(kafka_ctx: &KafkaCtx) -> ClientConfig {
    let mut client_cfg = ClientConfig::new();
    client_cfg.set("bootstrap.servers", &kafka_ctx.hosts);
    client_cfg.set("retries", PRODUCER_MAX_RETRIES);

    if kafka_ctx.ssl.enabled {
        client_cfg.set("security.protocol", "ssl");
        client_cfg.set(
            "enable.ssl.certificate.verification",
            if kafka_ctx.ssl.cert_validate {
                "true"
            } else {
                "false"
            },
        );
        client_cfg.set(
            "ssl.endpoint.identification.algorithm",
            if kafka_ctx.ssl.host_validate {
                "https"
            } else {
                "none"
            },
        );
        if let Some(ref path) = kafka_ctx.ssl.key_location {
            client_cfg.set("ssl.key.location", &path);
        }
        if let Some(ref pass) = kafka_ctx.ssl.key_password {
            client_cfg.set("ssl.key.password", &pass);
        }
        if let Some(ref path) = kafka_ctx.ssl.cert_location {
            client_cfg.set("ssl.certificate.location", &path);
        }
        if let Some(ref path) = kafka_ctx.ssl.ca_location {
            client_cfg.set("ssl.ca.location", &path);
        }
        if let Some(ref path) = kafka_ctx.ssl.keystore_location {
            client_cfg.set("ssl.keystore.location", &path);
        }
        if let Some(ref pass) = kafka_ctx.ssl.keystore_password {
            client_cfg.set("ssl.keystore.password", &pass);
        }
    }

    client_cfg
}

struct BlockingProducerContext {
    sender: Mutex<Sender<Result<(), KafkaError>>>,
}

impl BlockingProducerContext {
    fn new(sender: Sender<Result<(), KafkaError>>) -> BlockingProducerContext {
        BlockingProducerContext {
            sender: Mutex::new(sender),
        }
    }
}

impl ClientContext for BlockingProducerContext {}

impl ProducerContext for BlockingProducerContext {
    type DeliveryOpaque = ();

    fn delivery<'a>(
        &self,
        delivery_result: &DeliveryResult<'a>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        self.sender
            .lock()
            .unwrap()
            .send(
                delivery_result
                    .as_ref()
                    .map_err(|(kafka_err, _)| kafka_err.clone())
                    .map(|_| ()),
            )
            .unwrap();
    }
}
