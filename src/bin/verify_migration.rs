use serde::{Deserialize, Serialize};
use tokio::time::Duration;

// Inline the required types since we can't import from the main crate
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OAuth {
    pub client_id: String,
    pub client_secret: String,
    pub client_email: String,
    pub issuer_url: String,
    pub audience: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PulsarConfig {
    pub hostname: String,
    pub port: u16,
    pub tenant: String,
    pub namespace: String,
    pub topics: Vec<String>,
    pub oauth: Option<OAuth>,
}

#[derive(Clone, Deserialize)]
pub struct Config {
    pub await_send: bool,
    pub is_live: bool,
    pub src_pulsar: PulsarConfig,
    pub dest_pulsar: PulsarConfig,
}

async fn get_topic_message_count(
    hostname: &str,
    oauth: &OAuth,
    tenant: &str,
    namespace: &str,
    topic: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let full_topic_name = format!("persistent://{}/{}/{}", tenant, namespace, topic);

    println!("📊 Checking topic: {}", full_topic_name);

    // Get OAuth token for admin API
    let client = reqwest::Client::new();
    let auth_token = get_auth_token(&client, oauth.clone()).await?;
    // println!("   📊 Auth token: {}", auth_token);

    // Try internal-stats first for accurate message count
    let internal_stats_url = format!(
        "https://{hostname}/admin/v2/persistent/{tenant}/{namespace}/{topic}/internalStats"
    );

    println!("   🌐 Calling internal stats API: {}", internal_stats_url);

    let response = client
        .get(&internal_stats_url)
        .header("Authorization", format!("Bearer {}", auth_token))
        .header("Accept", "application/json")
        .timeout(Duration::from_secs(180))
        .send()
        .await?;

    let stats: serde_json::Value = if response.status().is_success() {
        response.json().await?
    } else if response.status() == reqwest::StatusCode::NOT_FOUND {
        // Try partitioned-internalStats as fallback for partitioned topics
        println!("   ⚠️  Regular internal stats returned 404, trying partitioned-internalStats...");

        let partitioned_internal_stats_url = format!(
            "https://{hostname}/admin/v2/persistent/{tenant}/{namespace}/{topic}/partitioned-internalStats"
        );

        println!(
            "   🌐 Calling partitioned internal stats API: {}",
            partitioned_internal_stats_url
        );

        let partitioned_response = client
            .get(&partitioned_internal_stats_url)
            .header("Authorization", format!("Bearer {}", auth_token))
            .header("Accept", "application/json")
            .timeout(Duration::from_secs(180))
            .send()
            .await?;

        if !partitioned_response.status().is_success() {
            let status = partitioned_response.status();
            let error_text = partitioned_response.text().await.unwrap_or_default();
            return Err(format!(
                "Both internalStats and partitioned-internalStats failed. Partitioned-internalStats status {}: {}",
                status, error_text
            )
            .into());
        }

        partitioned_response.json().await?
    } else {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        return Err(format!(
            "Internal stats API failed with status {}: {}",
            status, error_text
        )
        .into());
    };

    // Log key internal stats fields for debugging
    println!("   📊 Key internal stats fields:");
    if let Some(number_of_entries) = stats.get("numberOfEntries") {
        println!("      numberOfEntries: {}", number_of_entries);
    }
    if let Some(total_size) = stats.get("totalSize") {
        println!("      totalSize: {}", total_size);
    }
    if let Some(entries_added_counter) = stats.get("entriesAddedCounter") {
        println!("      entriesAddedCounter: {}", entries_added_counter);
    }
    if let Some(current_ledger_entries) = stats.get("currentLedgerEntries") {
        println!("      currentLedgerEntries: {}", current_ledger_entries);
    }
    if let Some(partitions) = stats.get("partitions") {
        println!(
            "      partitions: {} (this is a partitioned topic)",
            partitions
        );
    }

    // For migration verification, numberOfEntries is the most reliable indicator
    // Handle both regular and partitioned topic structures
    let (number_of_entries, total_size) = if let Some(partitions) = stats.get("partitions") {
        // This is a partitioned topic - aggregate from all partitions
        println!("   📊 Detected partitioned topic, aggregating from partitions...");
        let mut total_entries = 0u64;
        let mut total_bytes = 0u64;

        if let Some(partitions_obj) = partitions.as_object() {
            for (partition_name, partition_stats) in partitions_obj {
                let entries = partition_stats
                    .get("numberOfEntries")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let size = partition_stats
                    .get("totalSize")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                println!(
                    "      Partition {}: {} entries, {} bytes",
                    partition_name, entries, size
                );
                total_entries += entries;
                total_bytes += size;
            }
        }

        (total_entries, total_bytes)
    } else {
        // Regular topic - get values directly from root
        let entries = stats
            .get("numberOfEntries")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let size = stats.get("totalSize").and_then(|v| v.as_u64()).unwrap_or(0);

        (entries, size)
    };

    println!(
        "   ✅ Topic has {} messages, total size: {} bytes",
        number_of_entries, total_size
    );

    Ok(number_of_entries)
}

// OAuth token helper function
async fn get_auth_token(
    client: &reqwest::Client,
    oauth: OAuth,
) -> Result<String, Box<dyn std::error::Error>> {
    let payload = serde_json::json!({
        "type": "sn_service_account",
        "client_id": oauth.client_id,
        "client_secret": oauth.client_secret,
        "client_email": oauth.client_email,
        "issuer_url": oauth.issuer_url,
        "grant_type": "client_credentials",
        "audience": oauth.audience
    });

    let response = client
        .post("https://auth.streamnative.cloud/oauth/token")
        .header("content-type", "application/json")
        .json(&payload)
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("OAuth failed with status: {}", response.status()).into());
    }

    let auth_response: serde_json::Value = response.json().await?;
    let access_token = auth_response
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or("No access_token in OAuth response")?;

    Ok(access_token.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Load config from command line argument or default
    let config_file = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let config_content = std::fs::read_to_string(&config_file)
        .map_err(|e| format!("Failed to read config file {}: {}", config_file, e))?;
    let config: Config = toml::from_str(&config_content)
        .map_err(|e| format!("Failed to parse config file {}: {}", config_file, e))?;

    println!("🔍 Pulsar Migration Verification Tool");
    println!("=====================================");

    println!("📋 Configuration loaded:");
    println!(
        "   Source: {}:{}/{}/{}",
        config.src_pulsar.hostname,
        config.src_pulsar.port,
        config.src_pulsar.tenant,
        config.src_pulsar.namespace
    );
    println!(
        "   Destination: {}:{}/{}/{}",
        config.dest_pulsar.hostname,
        config.dest_pulsar.port,
        config.dest_pulsar.tenant,
        config.dest_pulsar.namespace
    );
    println!("   Topics to verify: {}", config.src_pulsar.topics.len());
    println!();

    // No need to connect to Pulsar clusters - using REST API instead
    println!("🌐 Using Pulsar Admin REST API for fast verification");
    println!();

    let mut results = Vec::new();
    let mut total_src_messages = 0u64;
    let mut total_dest_messages = 0u64;
    let mut mismatched_topics = Vec::new();

    // Verify each topic pair
    for (i, topic) in config.src_pulsar.topics.iter().enumerate() {
        println!(
            "🔍 Verifying topic {}/{}: {}",
            i + 1,
            config.src_pulsar.topics.len(),
            topic
        );

        // Get source message count using REST API
        let src_count = match get_topic_message_count(
            &config.src_pulsar.hostname,
            config
                .src_pulsar
                .oauth
                .as_ref()
                .ok_or("Source OAuth not configured")?,
            &config.src_pulsar.tenant,
            &config.src_pulsar.namespace,
            topic,
        )
        .await
        {
            Ok(count) => count,
            Err(e) => {
                println!("❌ Failed to get source count for {}: {}", topic, e);
                0
            }
        };

        // Get destination message count using REST API
        let dest_count = match get_topic_message_count(
            &config.dest_pulsar.hostname,
            config
                .dest_pulsar
                .oauth
                .as_ref()
                .ok_or("Destination OAuth not configured")?,
            &config.dest_pulsar.tenant,
            &config.dest_pulsar.namespace,
            topic,
        )
        .await
        {
            Ok(count) => count,
            Err(e) => {
                println!("❌ Failed to get destination count for {}: {}", topic, e);
                0
            }
        };

        total_src_messages += src_count;
        total_dest_messages += dest_count;

        let status = if src_count == dest_count {
            "✅ MATCH"
        } else {
            mismatched_topics.push(topic.clone());
            "❌ MISMATCH"
        };

        println!(
            "   {} Source: {}, Destination: {}",
            status, src_count, dest_count
        );

        results.push((topic.clone(), src_count, dest_count));
        println!();
    }

    // Print summary
    println!("📊 MIGRATION VERIFICATION SUMMARY");
    println!("==================================");
    println!("Total topics verified: {}", results.len());
    println!("Total source messages: {}", total_src_messages);
    println!("Total destination messages: {}", total_dest_messages);
    println!("Topics with mismatched counts: {}", mismatched_topics.len());
    println!();

    if mismatched_topics.is_empty() {
        println!("🎉 SUCCESS: All topics have matching message counts!");
    } else {
        println!("⚠️  ISSUES FOUND: The following topics have mismatched counts:");
        for topic in &mismatched_topics {
            if let Some((_, src, dest)) = results.iter().find(|(t, _, _)| t == topic) {
                println!(
                    "   - {}: {} → {} (diff: {})",
                    topic,
                    src,
                    dest,
                    (*src as i64) - (*dest as i64)
                );
            }
        }
        println!();
        println!("🔧 Consider re-running the shovel for mismatched topics.");
    }

    // Detailed results table
    println!("📋 DETAILED RESULTS:");
    println!(
        "{:<60} {:>12} {:>12} {:>8}",
        "Topic", "Source", "Dest", "Status"
    );
    println!("{}", "-".repeat(100));

    for (topic, src_count, dest_count) in results {
        let status = if src_count == dest_count {
            "✅"
        } else {
            "❌"
        };
        println!(
            "{:<60} {:>12} {:>12} {:>8}",
            topic, src_count, dest_count, status
        );
    }

    Ok(())
}
