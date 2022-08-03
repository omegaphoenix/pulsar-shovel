use serde::de::DeserializeOwned;
use serde_json::Error as SerdeJSONError;
use serde_yaml::Error as SerdeYAMLError;
use std::fs;
use std::io;
use std::path::Path;
use thiserror::Error;
use toml::de::Error as SerdeTOMLError;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("I/O Error reading file")]
    IoError(#[from] io::Error),

    #[error("Failed deserialization of JSON")]
    SerdeJSONError(#[from] SerdeJSONError),

    #[error("Failed deserialization of TOML")]
    SerdeTOMLError(#[from] SerdeTOMLError),

    #[error("Failed deserialization of YAML")]
    SerdeYAMLError(#[from] SerdeYAMLError),

    #[error("Neither config.toml or config.json or config.yaml was found")]
    NeitherFound,
}

fn load_json<P, T>(filepath: P) -> Result<T, ConfigError>
where
    P: AsRef<Path>,
    T: DeserializeOwned,
{
    let contents = fs::read_to_string(filepath)?;
    let data: T = serde_json::from_str(&contents)?;

    Ok(data)
}

fn load_toml<P, T>(filepath: P) -> Result<T, ConfigError>
where
    P: AsRef<Path>,
    T: DeserializeOwned,
{
    let contents = fs::read_to_string(filepath)?;
    let data: T = toml::from_str(&contents)?;

    Ok(data)
}

fn load_yaml<P, T>(filepath: P) -> Result<T, ConfigError>
where
    P: AsRef<Path>,
    T: DeserializeOwned,
{
    let contents = fs::read_to_string(filepath)?;
    let data: T = serde_yaml::from_str(&contents)?;

    Ok(data)
}

fn exists<T>(config: &Result<T, ConfigError>) -> bool {
    if let Err(ConfigError::IoError(e)) = config {
        return e.kind() != io::ErrorKind::NotFound;
    }

    true
}

/// Attempt to load config from `config.toml`, then `config.json`, then `config.yaml`.
/// in the current working directory. Deserialize it into type `T`.
pub fn load<T>() -> Result<T, ConfigError>
where
    T: DeserializeOwned,
{
    let toml: Result<T, ConfigError> = load_toml("config.toml");
    if exists(&toml) {
        return toml;
    }

    let json: Result<T, ConfigError> = load_json("config.json");
    if exists(&json) {
        return json;
    }

    let yaml: Result<T, ConfigError> = load_yaml("config.yaml");
    if exists(&yaml) {
        return yaml;
    }

    Err(ConfigError::NeitherFound)
}
