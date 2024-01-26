use std::collections::HashMap;
use kormir::{
    error::Error,
    storage::{OracleEventData, Storage},
    OracleAnnouncement, Signature,
};

use crate::DbManager;

pub struct OracleStorage {
    db_manager: DbManager,
}

impl OracleStorage {
    pub fn new(db: DbManager) -> Self {
        Self { db_manager: db }
    }
}

impl Storage for OracleStorage {
    async fn get_next_nonce_indexes(&self, num: usize) -> Result<Vec<u32>, Error> {
        todo!()
    }

    async fn save_announcement(
        &self,
        announcement: OracleAnnouncement,
        indexes: Vec<u32>,
    ) -> Result<u32, Error> {
        todo!()
    }

    async fn save_signatures(
        &self,
        id: u32,
        sigs: HashMap<String, Signature>,
    ) -> Result<OracleEventData, Error> {
        todo!()
    }

    async fn get_event(&self, id: u32) -> Result<Option<OracleEventData>, Error> {
        todo!()
    }
}
