mod config;
mod retention;
mod types;
use futures::FutureExt;
use futures::StreamExt;
use pulsar::compression::Compression;
use pulsar::compression::CompressionZstd;
use pulsar::{
    Authentication as TokenAuthentication, Pulsar, TokioExecutor,
    authentication::oauth2::{OAuth2Authentication, OAuth2Params},
    consumer::{ConsumerOptions, InitialPosition},
    producer::ProducerOptions,
    proto::MessageIdData,
    reader::Reader,
};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use types::PulsarConfig;

pub async fn delay_ms(ms: usize) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

/// Check if we've reached the end of the topic by comparing the essential message position fields
/// Ignores batch_index differences which can vary between current and last message IDs
fn is_at_end_of_topic(current_id: &MessageIdData, last_id: &MessageIdData) -> bool {
    // Compare the core position identifiers that matter for topic end detection
    current_id.ledger_id == last_id.ledger_id
        && current_id.entry_id == last_id.entry_id
        && current_id.partition == last_id.partition
}

#[derive(Clone, Deserialize)]
pub struct Config {
    /// If true, await the send receipt before sending the next message
    pub await_send: bool,
    /// If false, exit after the last message is published
    pub is_live: bool,

    pub src_pulsar: PulsarConfig,
    pub dest_pulsar: PulsarConfig,
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
                credentials_url: format!("data:application/json;,{credentials}"),
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
                log::warn!("Reconnecting reader starting from {pos:?}");
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
const LOG_FREQUENCY: usize = 100;
pub async fn read_topic(
    config: PulsarConfig,
    topic: String,
    is_live: bool,
    output: Sender<(u64, Vec<u8>)>,
) {
    let tenant = config.tenant.clone();
    let namespace = config.namespace.clone();
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
        let last_message_id = reader
            .get_mut()
            .get_last_message_id()
            .await
            .expect("failed to get last message id");

        'inner: loop {
            futures::select! {
                    // This is necessary due to a bug in our Pulsar broker
                    // When the broker recovers the offloaded data from s3, it sometimes fails and hangs
                    _ = check_connection_timer => {
                        if let Some(ref message_id) = last_position {
                            if !is_live && is_at_end_of_topic(message_id, &last_message_id) {
                                log::info!("{topic}: Reached end of topic after {counter} messages");
                                return;
                            }
                        }


                        let connection_result = reader.get_mut().check_connection().await;

                        if let Err(e) = connection_result {
                            log::error!("Check connection failed, attempting to reconnect... {e}");
                        }
                        break 'inner;
                    }

                    reader_message = reader.next() => {
                        if let Some(msg) = reader_message {
                            check_connection_timer.set(delay_ms(CHECK_CONNECTION_TIMEOUT).fuse());

                            let msg = msg.expect("Failed to read message");
                            let message_id = msg.message_id().clone();

                            // Necessary to skip repeats due to reconnecting from last position
                            if last_position == Some(message_id.clone()) {
                                log::warn!("Skipping repeated message");
                                continue;
                            }
                            last_position = Some(message_id.clone());

                            let event_time = msg.payload.metadata.event_time.unwrap_or(msg.payload.metadata.publish_time);
                            if let Err(err) = output.send((event_time, msg.payload.data)).await {
                                log::error!("failed to send to receiver {err}");
                            }

                            counter += 1;
                            if counter % LOG_FREQUENCY == 1 {
                                log::info!("{topic} got {counter} messages");
                            }

                        }
                    }
            }
        }
    }
}

const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF_MS: u64 = 100;
async fn write_topic(
    await_send: bool,
    config: PulsarConfig,
    topic: String,
    mut input: Receiver<(u64, Vec<u8>)>,
) {
    let tenant = config.tenant.clone();
    let namespace = config.namespace.clone();
    let pulsar_client = get_pulsar_client(config.clone())
        .await
        .expect("Failed to build pulsar client");

    let full_topic_name = format!("persistent://{}/{}/{}", tenant, namespace, &topic);

    let mut producer = pulsar_client
        .producer()
        .with_topic(full_topic_name)
        .with_name("test_producer")
        .with_options(ProducerOptions {
            compression: Some(Compression::Zstd(CompressionZstd { level: 6 })),
            ..Default::default()
        })
        .build()
        .await
        .expect("Failed to create producer");

    let mut counter = 0_usize;
    let mut retention_set = false;

    while let Some((event_time, data)) = input.recv().await {
        let send_future = producer
            .create_message()
            .with_content(data.clone())
            .event_time(event_time)
            .send_non_blocking()
            .await
            .expect("Failed to create send future");

        if await_send {
            let mut retry_count = 0;
            let mut backoff_ms = INITIAL_BACKOFF_MS;
            let mut result = send_future.await;

            while let Err(e) = result {
                retry_count += 1;
                if retry_count > MAX_RETRIES {
                    log::error!("Failed to send message after {MAX_RETRIES} retries: {e}");
                    break;
                }

                log::warn!(
                    "Failed to send message (attempt {retry_count}/{MAX_RETRIES}): {e}, retrying in {backoff_ms}ms",
                );

                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2; // Exponential backoff

                let retry_future = producer
                    .create_message()
                    .with_content(data.clone())
                    .event_time(event_time)
                    .send_non_blocking()
                    .await
                    .expect("Failed to create send future");
                result = retry_future.await;
            }
        }

        counter += 1;

        // Set retention policy after first successful message
        if !retention_set {
            if let Err(err) = retention::retain_topic(config.clone(), &topic).await {
                log::warn!("Failed to set retention policy for {}: {}", topic, err);
            } else {
                log::info!("Retention policy set for topic: {}", topic);
            }
            retention_set = true;
        }

        if counter % LOG_FREQUENCY == 1 {
            log::info!("sent {} messages to {}", counter, &topic);
        }
    }
}

const BUFFER_SIZE: usize = 1000;
#[tokio::main]
async fn main() {
    env_logger::init();

    let Config {
        await_send,
        is_live,
        src_pulsar,
        dest_pulsar,
    } = config::load().expect("Unable to load config");

    // Ensure both configs have the same topics
    if src_pulsar.topics.len() != dest_pulsar.topics.len() {
        panic!("Source and destination must have the same number of topics");
    }

    let mut handles = vec![];

    // Spawn a read/write pair for each topic
    for (src_topic, dest_topic) in src_pulsar.topics.iter().zip(dest_pulsar.topics.iter()) {
        let (tx, rx) = channel(BUFFER_SIZE);
        let src_config = src_pulsar.clone();
        let dest_config = dest_pulsar.clone();
        let src_topic_clone = src_topic.clone();
        let dest_topic_clone = dest_topic.clone();

        // Spawn writer task
        let writer_handle = tokio::spawn(async move {
            write_topic(await_send, dest_config, dest_topic_clone, rx).await
        });

        // Spawn reader task
        let reader_handle =
            tokio::spawn(async move { read_topic(src_config, src_topic_clone, is_live, tx).await });

        handles.push((format!("writer for {dest_topic}"), writer_handle));
        handles.push((format!("reader for {src_topic}"), reader_handle));
    }

    // Wait for all tasks to complete
    for (task_description, handle) in handles {
        if let Err(e) = handle.await {
            panic!("Task failed - {task_description}: {e}");
        }
    }
}
