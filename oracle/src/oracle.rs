use crate::{
    AddEventEntry, CreateOracleEventData, EventData, OracleEventData, WeatherData, WeatherEntry,
};
use anyhow::anyhow;
use base64::{engine::general_purpose, Engine};
use dlctix::{
    bitcoin::key::Secp256k1,
    musig2::secp256k1::{rand, PublicKey, SecretKey},
    secp::Point,
};
use nostr::{key::Keys, nips::nip19::ToBech32};
use pem_rfc7468::{decode_vec, encode_string};
use std::{
    fs::{metadata, File},
    io::{Read, Write},
    path::Path,
    sync::Arc,
};
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Error, Debug, ToSchema)]
pub enum OracleError {
    #[error("No event found with that ID: {0}")]
    EventNotFound(String),
    #[error("Failed to get private key: {0}")]
    PrivateKey(#[from] anyhow::Error),
    #[error("Must have at least one outcome: {0}")]
    MinOutcome(String),
    #[error("Event maturity epoch must be in the future: {0}")]
    EventMaturity(String),
    #[error("Failed to convert private key into nostr keys: {0}")]
    ConvertKey(#[from] nostr::key::Error),
    #[error("Failed to convert public key into nostr base32 format: {0}")]
    Base32Key(#[from] nostr::nips::nip19::Error),
    #[error("Failed to query datasource: {0}")]
    DataQuery(#[from] duckdb::Error),
    #[error("Pubkeys in DB doesn't match with .pem")]
    MismatchPubkey(String),
}

pub struct Oracle {
    event_data: Arc<EventData>,
    weather_data: Arc<WeatherData>,
    private_key: SecretKey,
    public_key: PublicKey,
}

impl Oracle {
    pub async fn new(
        event_data: Arc<EventData>,
        weather_data: Arc<WeatherData>,
        private_key_file_path: &String,
    ) -> Result<Self, OracleError> {
        let secret_key = get_key(private_key_file_path)?;
        let secp = Secp256k1::new();
        let public_key = secret_key.public_key(&secp);
        let oracle = Self {
            event_data,
            weather_data,
            private_key: secret_key,
            public_key,
        };
        oracle.validate_oracle_metadata().await?;
        Ok(oracle)
    }

    pub async fn validate_oracle_metadata(&self) -> Result<(), OracleError> {
        let stored_public_key = match self.event_data.get_stored_public_key().await {
            Ok(key) => key,
            Err(duckdb::Error::QueryReturnedNoRows) => {
                self.add_meta_data().await?;
                return Ok(());
            }
            Err(e) => return Err(OracleError::DataQuery(e)),
        };
        if stored_public_key != self.public_key.x_only_public_key().0 {
            return Err(OracleError::MismatchPubkey(format!(
                "stored_pubkey: {:?} pem_pubkey: {:?}",
                stored_public_key,
                self.public_key()
            )));
        }
        Ok(())
    }

    async fn add_meta_data(&self) -> Result<(), OracleError> {
        self.event_data
            .add_oracle_metadata(self.public_key.x_only_public_key().0)
            .await
            .map_err(OracleError::DataQuery)
    }

    pub fn public_key(&self) -> String {
        let key = Point::from(self.public_key).serialize();
        general_purpose::STANDARD.encode(key)
    }

    pub fn npub(&self) -> Result<String, OracleError> {
        let secret_key = self.private_key.display_secret().to_string();
        let keys = Keys::parse(secret_key)?;

        Ok(keys.public_key().to_bech32()?)
    }

    pub async fn list_events(&self) -> Result<Vec<OracleEventData>, OracleError> {
        // TODO: add filter/pagination etc.
        // filter on active event/completed event/time range of event
        // if we're not careful, this endpoint might bring down the whole server
        // just due to the amount of data that can come out of it
        self.event_data
            .list_events()
            .await
            .map_err(OracleError::DataQuery)
    }

    pub async fn get_event(&self, id: &Uuid) -> Result<OracleEventData, OracleError> {
        self.event_data
            .get_oracle_event(id)
            .await
            .map_err(OracleError::DataQuery)
    }

    pub async fn create_event(
        &self,
        event: CreateOracleEventData,
    ) -> Result<OracleEventData, OracleError> {
        self.event_data
            .add_event(event)
            .await
            .map_err(OracleError::DataQuery)
    }

    pub async fn add_event_entry(
        &self,
        event_entry: AddEventEntry,
    ) -> Result<WeatherEntry, OracleError> {
        self.event_data
            .add_event_entry(event_entry)
            .await
            .map_err(OracleError::DataQuery)
    }
}

fn get_key(file_path: &String) -> Result<SecretKey, anyhow::Error> {
    if !is_pem_file(file_path) {
        return Err(anyhow!("not a '.pem' file extension"));
    }

    if metadata(file_path).is_ok() {
        read_key(file_path)
    } else {
        let key = generate_new_key();
        save_key(file_path, key)?;
        Ok(key)
    }
}

fn generate_new_key() -> SecretKey {
    SecretKey::new(&mut rand::thread_rng())
}

fn is_pem_file(file_path: &String) -> bool {
    Path::new(file_path)
        .extension()
        .and_then(|s| s.to_str())
        .map_or(false, |ext| ext == "pem")
}

fn read_key(file_path: &String) -> Result<SecretKey, anyhow::Error> {
    let mut file = File::open(file_path)?;
    let mut pem_data = String::new();
    file.read_to_string(&mut pem_data)?;

    // Decode the PEM content
    let (label, decoded_key) = decode_vec(pem_data.as_bytes()).map_err(|e| anyhow!(e))?;

    // Verify the label
    if label != "EC PRIVATE KEY" {
        return Err(anyhow!("Invalid key format"));
    }

    // Parse the private key
    let secret_key = SecretKey::from_slice(&decoded_key)?;
    Ok(secret_key)
}

fn save_key(file_path: &String, key: SecretKey) -> Result<(), anyhow::Error> {
    let pem = encode_string(
        "EC PRIVATE KEY",
        pem_rfc7468::LineEnding::LF,
        &key.secret_bytes(),
    )
    .map_err(|e| anyhow!("Failed to encode key: {}", e))?;

    // Private key file path needs to end in ".pem"
    let mut file = File::create(file_path)?;
    file.write_all(pem.as_bytes())?;
    Ok(())
}
