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
        let mut client_cfg = ClientConfig::new();
        client_cfg.set("bootstrap.servers", &kafka_ctx.hosts);
        client_cfg.set("retries", PRODUCER_MAX_RETRIES);

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
