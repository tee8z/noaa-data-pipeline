use crate::{EventData, WeatherData};
use anyhow::anyhow;
use dlctix::{
    bitcoin::{key::Secp256k1, XOnlyPublicKey},
    musig2::secp256k1::{rand, PublicKey, SecretKey},
};
use pem_rfc7468::{decode_vec, encode_string};
use serde::{Deserialize, Serialize};
use std::{
    fs::{metadata, File},
    io::{Read, Write},
    path::Path,
    sync::Arc,
};
use thiserror::Error;
use time::OffsetDateTime;
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
}

pub struct Oracle {
    event_data: Arc<EventData>,
    weather_data: Arc<WeatherData>,
    private_key: SecretKey,
    public_key: PublicKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OracleEventData {}

//TODO: make the outcomes possible winning scores
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateEvent {
    pub event_id: String,
    pub outcomes: Vec<Vec<u8>>,
    pub event_maturity_epoch: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SignEvent {
    pub id: u32,
    pub outcome: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OracleAttestation {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OracleAnnouncement {}

impl Oracle {
    pub fn new(
        event_data: Arc<EventData>,
        weather_data: Arc<WeatherData>,
        private_key_file_path: &String,
    ) -> Result<Self, OracleError> {
        let secret_key = get_key(private_key_file_path)?;
        let secp = Secp256k1::new();
        let public_key = secret_key.public_key(&secp);
        Ok(Self {
            event_data,
            weather_data,
            private_key: secret_key,
            public_key,
        })
    }

    pub fn public_key(&self) -> XOnlyPublicKey {
        let (key, _) = self.public_key.x_only_public_key();
        key
    }

    pub async fn list_events(&self) -> Result<Vec<OracleEventData>, OracleError> {
        Ok(vec![])
    }

    pub async fn get_event(&self, id: &Uuid) -> Result<OracleEventData, OracleError> {
        Ok(OracleEventData {})
    }

    pub async fn create_event(
        &self,
        create_event: CreateEvent,
    ) -> Result<OracleAnnouncement, OracleError> {
        //TODO: move validation into struct itself
        if create_event.outcomes.is_empty() {
            return Err(OracleError::MinOutcome(format!(
                "event_id: {}",
                create_event.event_id,
            )));
        }

        if create_event.event_maturity_epoch < now() {
            return Err(OracleError::EventMaturity(format!(
                "event_id: {}, maturity: {}",
                create_event.event_id, create_event.event_maturity_epoch,
            )));
        }
        Ok(OracleAnnouncement {})
    }

    pub async fn sign_event(
        &self,
        sign_event: SignEvent,
    ) -> Result<OracleAttestation, OracleError> {
        Ok(OracleAttestation {})
    }
}

fn now() -> u32 {
    OffsetDateTime::now_utc().unix_timestamp() as u32
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
