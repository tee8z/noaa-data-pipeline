use anyhow::{anyhow, Error};
use kormir::{
    bitcoin::{secp256k1::SecretKey, XOnlyPublicKey},
    storage::{OracleEventData, Storage},
    Oracle,
};
use std::{fs::File, io::Read, str::FromStr, sync::Arc};

use crate::{DbManager, OracleStorage};

#[derive(Clone)]
pub struct OracleService {
    oracle: Arc<Oracle<OracleStorage>>,
}

impl OracleService {
    // TODO (@tee8z): switch to return an error instead of panic type when failing to get the signing key from a file
    pub fn new(db: DbManager, private_key_path: String) -> Self {
        let mut file = File::open(private_key_path).unwrap();
        let mut raw_private_key = String::new();
        file.read_to_string(&mut raw_private_key).unwrap();

        let signing_key = SecretKey::from_str(&raw_private_key).unwrap();
        let oracle_store = OracleStorage::new(db);
        let oracle = Oracle::from_signing_key(oracle_store, signing_key).unwrap();

        Self {
            oracle: Arc::new(oracle),
        }
    }

    pub fn get_pubkey(&self) -> XOnlyPublicKey {
        self.oracle.public_key()
    }

    pub async fn list_events(&self) -> Result<Vec<OracleEventData>, Error> {
        self.oracle.storage.list_events().await
    }

    pub async fn get_event(&self, id: u32) -> Result<Option<OracleEventData>, Error> {
        self.oracle
            .storage
            .get_event(id)
            .await
            .map_err(|e| anyhow!("error getting an oracle event: {}", e))
    }

    pub async fn create_event(&self) -> Result<(), Error> {
        let (id, ann) = self
            .oracle
            .create_enum_event(body.event_id, body.outcomes, body.event_maturity_epoch)
            .await?;
        let hex = ann.encode().to_hex();

        log::info!("Created enum event: {hex}");

        let relays = self
            .client
            .relays()
            .await
            .keys()
            .map(|x| x.to_string())
            .collect::<Vec<_>>();

        let event = kormir::nostr_events::create_announcement_event(
            &self.oracle.nostr_keys(),
            &ann,
            &relays,
        )?;

        self
            .oracle
            .storage
            .add_announcement_event_id(id, event.id)
            .await?;

        // broadcast event through nostr relay
        //self.client.send_event(event).await?;

        Ok(hex)
    }
}
