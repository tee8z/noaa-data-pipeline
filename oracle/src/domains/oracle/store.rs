use anyhow::Error;
use kormir::{
    storage::{OracleEventData, Storage},
    OracleAnnouncement, Signature,
};
use std::collections::HashMap;

use crate::{DbManager, Event};

pub struct OracleStorage {
    db_manager: DbManager,
}

impl OracleStorage {
    pub fn new(db: DbManager) -> Self {
        Self { db_manager: db }
    }

    pub async fn list_events(&self) -> Result<Vec<OracleEventData>, Error> {
        let conn_lock = self.db_manager.get_conn();
        let conn = conn_lock.lock().await;
        let mut stmt = conn.prepare("SELECT id, name, is_enum, created_at, announcement_signature, oracle_event, updated_at, announcement_event_id, attestation_event_id FROM events")?;
        let event_itr = stmt.query_map([], |row| {
            Ok(OracleEventData {
                id: row.get(0)?,
                name: row.get(1)?,
                is_enum: row.get(2)?,
                created_at: row.get(3)?,
                announcement_signature: row.get(4)?,
                oracle_event: row.get(5)?,
                updated_at: row.get(6)?,
                announcement_event_id: row.get(7)?,
                attestation_event_id: row.get(8)?,
            })
        })?;

        let mut events = Vec::new();
        for event_res in event_itr {
            events.push(event_res?);
        }

        Ok(events)
    }
}

impl Storage for OracleStorage {
    async fn get_next_nonce_indexes(&self, num: usize) -> Result<Vec<u32>, kormir::error::Error> {
        todo!()
    }

    async fn save_announcement(
        &self,
        announcement: OracleAnnouncement,
        indexes: Vec<u32>,
    ) -> Result<u32, kormir::error::Error> {
        todo!()
    }

    async fn save_signatures(
        &self,
        id: u32,
        sigs: HashMap<String, Signature>,
    ) -> Result<OracleEventData, kormir::error::Error> {
        todo!()
    }

    async fn get_event(&self, id: u32) -> Result<Option<OracleEventData>, kormir::error::Error> {
        todo!()
    }
}
