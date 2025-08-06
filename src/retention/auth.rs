use reqwest::Client;
use serde::{Deserialize, Serialize};
use crate::types::OAuth;

const OAUTH_URL: &str = "https://auth.streamnative.cloud/oauth/token";
const GRANT_TYPE: &str = "client_credentials";

pub async fn get_auth_token(client: &Client, oauth: OAuth) -> String {
    let payload: AuthRequestPayload = oauth.into();
    let payload =
        serde_json::to_vec(&payload).expect("Failed to serialize auth request payload to JSON");

    client
        .post(OAUTH_URL)
        .header("content-type", "application/json")
        .body(payload)
        .send()
        .await
        .expect("Failed to get auth token")
        .json::<AuthResponse>()
        .await
        .expect("Failed to parse auth token response as JSON")
        .access_token
}

#[derive(Clone, Serialize)]
struct AuthRequestPayload {
    r#type: String,
    client_id: String,
    client_secret: String,
    client_email: String,
    issuer_url: String,
    grant_type: String,
    audience: String,
}

impl From<OAuth> for AuthRequestPayload {
    fn from(oauth: OAuth) -> Self {
        let OAuth {
            client_id,
            client_secret,
            client_email,
            issuer_url,
            audience,
        } = oauth;
        Self {
            r#type: "sn_service_account".into(),
            client_id,
            client_secret,
            client_email,
            issuer_url,
            grant_type: GRANT_TYPE.into(),
            audience,
        }
    }
}

#[derive(Clone, Deserialize)]
struct AuthResponse {
    access_token: String,
}
