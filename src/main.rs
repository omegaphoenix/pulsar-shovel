mod config;
use pulsar::{
    consumer::{ConsumerOptions, InitialPosition},
    proto::MessageIdData,
    reader::Reader,
    Authentication, Pulsar, TokioExecutor,
};
use serde::Deserialize;
use std::time::Duration;

pub async fn delay_ms(ms: usize) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub src_pulsar: PulsarConfig,
    pub dest_pulsar: PulsarConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PulsarConfig {
    pub hostname: String,
    pub port: u16,
    pub tenant: String,
    pub namespace: String,
    pub topic: String,
    pub token: Option<String>,
}

async fn get_pulsar_client(config: PulsarConfig) -> Result<Pulsar<TokioExecutor>, pulsar::Error> {
    let addr = format!("pulsar+ssl://{}:{}", config.hostname, config.port);
    let mut builder = Pulsar::builder(addr, TokioExecutor);

    let authentication = Authentication {
        name: "token".to_string(),
        data: config.token.unwrap().into_bytes(),
    };
    builder = builder.with_auth(authentication);

    builder.build().await
}

async fn get_pulsar_reader(
    pulsar: Pulsar<TokioExecutor>,
    full_topic_name: &str,
    initial_position: Option<MessageIdData>,
) -> Reader<String, TokioExecutor> {
    pulsar
        .reader()
        .with_topic(full_topic_name)
        .with_consumer_name("test_reader")
        .with_options(
            if let Some(pos) = initial_position {
                log::warn!("Reconnecting reader starting from {:?}", pos);
                ConsumerOptions::default().starting_on_message(pos)
            } else {
                ConsumerOptions::default().with_initial_position(InitialPosition::Earliest)
            }
            .durable(false),
        )
        .into_reader()
        .await
        .unwrap()
}

const RECONNECT_DELAY: usize = 100; // wait 100 ms before trying to reconnect

const BUFFER_SIZE: usize = 1000;
#[tokio::main]
async fn main() {
    env_logger::init();

    let Config {
        src_pulsar,
        dest_pulsar,
    } = config::load().expect("Unable to load config");
    tokio::spawn(async move {
        let namespace = src_pulsar.namespace.clone();
        let topic = src_pulsar.topic.clone();
        let pulsar = get_pulsar_client(src_pulsar)
            .await
            .expect("Failed to build pulsar client");
        let full_topic_name = format!("persistent://public/{}/{}", namespace, &topic);

        let reader = get_pulsar_reader(pulsar.clone(), &full_topic_name, None).await;

        delay_ms(RECONNECT_DELAY).await;
    });
}
