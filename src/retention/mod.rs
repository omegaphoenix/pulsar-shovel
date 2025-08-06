mod auth;

use crate::types::PulsarConfig;
use auth::get_auth_token;
use reqwest::{Client, Method, Response};
use serde::{Deserialize, Serialize};

const INFINITE_RETENTION_POLICY: &RetentionPolicy = &RetentionPolicy {
    retention_size: -1,
    retention_time: -1,
};

lazy_static::lazy_static! {
    static ref INFINITE_RETENTION_POLICY_BODY: Vec<u8> = {
        serde_json::to_vec(INFINITE_RETENTION_POLICY).unwrap()
    };
}

pub async fn retain_topic(
    config: PulsarConfig,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic = format!("{}/{}/{}", config.tenant, config.namespace, topic);
    let PulsarConfig {
        hostname, oauth, ..
    } = config;
    let oauth = oauth.ok_or("No OAuth credentials provided")?;
    let client = Client::new();
    let auth_token = get_auth_token(&client, oauth.clone()).await;

    log::info!("Setting retention policies in {hostname} / {topic}");

    if let Err(err) = perform_request(&client, &auth_token, &hostname, &topic).await {
        log::error!("❌ Request failed: {err}");
    }
    Ok(())
}

/// Performs the given operation for the specified topic by making a request to the Pulsar admin API
async fn perform_request(
    client: &Client,
    auth_token: &str,
    hostname: &str,
    topic: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("https://{hostname}/admin/v2/persistent/{topic}/retention");

    let req = client
        .request(Method::POST, url)
        .body(INFINITE_RETENTION_POLICY_BODY.as_slice());

    let response = req
        .header("Authorization", format!("Bearer {auth_token}"))
        .header("Content-Type", "application/json")
        .send()
        .await?;

    validate_response(response).await?;
    Ok(())
}

/// Validate the response for a given request
async fn validate_response(response: Response) -> Result<(), Box<dyn std::error::Error>> {
    if response.status().is_success() {
        log::info!("✅ Update successful");
        Ok(())
    } else {
        let status = response.status();
        log::error!("❌ Request failed with code {status}");
        Err(format!("Request failed with status: {status}").into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RetentionPolicy {
    #[serde(rename = "retentionSizeInMB")]
    retention_size: i64,
    #[serde(rename = "retentionTimeInMinutes")]
    retention_time: i64,
}
