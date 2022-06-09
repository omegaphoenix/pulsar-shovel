# pulsar-shovel

This script is for copying data from one Pulsar host/namespace/topic to another. Using a Pulsar function would probably be faster but we were blocked on uploading the function.

## To use:
1. Create config.toml
`cp config-sample.toml config.toml`
2. Fill out config.toml with hostname, tenant, namespace, topic, and tokens.
3. `RUST_LOG=info cargo run --release`