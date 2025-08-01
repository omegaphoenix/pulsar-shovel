mod config;
use futures::FutureExt;
use futures::StreamExt;
use pulsar::{
    authentication::oauth2::{OAuth2Authentication, OAuth2Params},
    consumer::{ConsumerOptions, InitialPosition},
    producer::ProducerOptions,
    proto::MessageIdData,
    reader::Reader,
    Authentication as TokenAuthentication, Pulsar, TokioExecutor,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub async fn delay_ms(ms: usize) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub src_pulsar: PulsarConfig,
    pub dest_pulsar: PulsarConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OAuth {
    pub client_id: String,
    pub client_secret: String,
    pub client_email: String,
    pub issuer_url: String,
    pub audience: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PulsarConfig {
    pub hostname: String,
    pub port: u16,
    pub tenant: String,
    pub namespace: String,
    pub topic: String,
    pub token: Option<String>,
    pub oauth: Option<OAuth>,
}

async fn get_pulsar_client(config: PulsarConfig) -> Result<Pulsar<TokioExecutor>, pulsar::Error> {
    let addr = format!("pulsar+ssl://{}:{}", config.hostname, config.port);
    let mut builder = Pulsar::builder(addr, TokioExecutor);

    if let Some(token) = config.token {
        let authentication = TokenAuthentication {
            name: "token".to_string(),
            data: token.into_bytes(),
        };
        builder = builder.with_auth(authentication);
    }

    if let Some(oauth) = config.oauth {
        let credentials = serde_json::to_string(&oauth).unwrap();

        builder =
            builder.with_auth_provider(OAuth2Authentication::client_credentials(OAuth2Params {
                issuer_url: oauth.issuer_url.clone(),
                credentials_url: format!("data:application/json;,{}", credentials),
                audience: Some(oauth.audience),
                scope: None,
            }));
    }

    builder.build().await
}

async fn get_pulsar_reader(
    pulsar: Pulsar<TokioExecutor>,
    full_topic_name: &str,
    initial_position: Option<MessageIdData>,
) -> Result<Reader<Vec<u8>, TokioExecutor>, pulsar::Error> {
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
}

const RECONNECT_DELAY: usize = 100; // wait 100 ms before trying to reconnect
const CHECK_CONNECTION_TIMEOUT: usize = 30_000;
const LOG_FREQUENCY: usize = 10;
pub async fn read_topic(config: PulsarConfig, output: Sender<Vec<u8>>) {
    let tenant = config.tenant.clone();
    let namespace = config.namespace.clone();
    let topic = config.topic.clone();
    let pulsar = get_pulsar_client(config)
        .await
        .expect("Failed to build pulsar client");
    let full_topic_name = format!("persistent://{}/{}/{}", tenant, namespace, &topic);

    let mut counter = 0_usize;
    let mut last_position: Option<MessageIdData> = None;
    loop {
        let mut reader = match get_pulsar_reader(
            pulsar.clone(),
            &full_topic_name,
            last_position.clone(),
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                log::error!(
                    "Failed to create reader on {} with error {}. Retrying...",
                    &full_topic_name,
                    e
                );
                delay_ms(RECONNECT_DELAY).await;
                continue;
            }
        }
        .fuse();

        let check_connection_timer = delay_ms(CHECK_CONNECTION_TIMEOUT).fuse();
        futures::pin_mut!(check_connection_timer);

        'inner: loop {
            futures::select! {
                    // This is necessary due to a bug in our Pulsar broker
                    // When the broker recovers the offloaded data from s3, it sometimes fails and hangs
                    _ = check_connection_timer => {
                        let connection_result = reader.get_mut().check_connection().await;

                        if let Err(e) = connection_result {
                            log::error!("Check connection failed, attempting to reconnect... {}", e);
                        }
                        break 'inner;
                    }

                    reader_message = reader.next() => {
                        if let Some(msg) = reader_message {
                            check_connection_timer.set(delay_ms(CHECK_CONNECTION_TIMEOUT).fuse());

                            let msg = msg.expect("Failed to read message");
                            let message_id = msg.message_id();

                            // Necessary to skip repeats due to reconnecting from last position
                            if last_position == Some(message_id.clone()) {
                                log::warn!("Skipping repeated message");
                                continue;
                            }
                            last_position = Some(message_id.clone());

                            if let Err(err) = output.send(msg.payload.data).await {
                                log::error!("failed to send to receiver {}", err);
                            }

                            counter += 1;
                            if counter % LOG_FREQUENCY == 0 {
                                log::info!("{} got {} messages", &topic, counter);
                            }
                        }
                    }
            }
        }
    }
}

async fn write_topic(config: PulsarConfig, mut input: Receiver<Vec<u8>>) {
    let namespace = config.namespace.clone();
    let topic = config.topic.clone();
    let pulsar_client = get_pulsar_client(config)
        .await
        .expect("Failed to build pulsar client");

    let full_topic_name = format!("persistent://public/{}/{}", namespace, &topic);

    let mut producer = pulsar_client
        .producer()
        .with_topic(full_topic_name)
        .with_name("test_producer")
        .with_options(ProducerOptions::default())
        .build()
        .await
        .expect("Failed to create producer");

    let mut counter = 0_usize;
    while let Some(message) = input.recv().await {
        producer
            .send_non_blocking(message)
            .await
            .expect("Failed to send message");

        counter += 1;
        if counter % LOG_FREQUENCY == 0 {
            log::info!("sent {} messages to {}", counter, &topic);
        }
    }
}

const BUFFER_SIZE: usize = 1000;
#[tokio::main]
async fn main() {
    env_logger::init();

    let Config {
        src_pulsar,
        dest_pulsar,
    } = config::load().expect("Unable to load config");
    let (tx, rx) = channel(BUFFER_SIZE);
    tokio::spawn(async move { write_topic(dest_pulsar, rx).await });
    read_topic(src_pulsar, tx).await;
}
