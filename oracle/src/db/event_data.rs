use dlctix::bitcoin::XOnlyPublicKey;
use duckdb::Connection;
use regex::Regex;
use scooby::postgres::{select, Select};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::run_migrations;

pub struct EventData {
    // TODO: see if a read/write lock makes more sense here (careful of writer starvation) and be aware of bottleneck around locking that may occur under heavy loads
    // eventually we may need to come up with a pool or other non-locking approach for grabbing the connection, but that shouldn't appear until we hit a decent usage level
    conn: Arc<Mutex<Connection>>,
}

impl EventData {
    pub fn new(path: &str) -> Result<Self, duckdb::Error> {
        let mut conn = Connection::open(format!("{}/events.db3", path))?;
        run_migrations(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn get_stored_public_key(&self) -> Result<XOnlyPublicKey, duckdb::Error> {
        let select = select("pubkey").from("oracle_metadata");
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&select.to_string())?;
        let key: Vec<u8> = stmt.query_row([], |row| row.get(0))?;
        //TODO: use a custom error here so we don't need to panic
        let converted_key = XOnlyPublicKey::from_slice(&key).expect("invalid pubkey");
        Ok(converted_key)
    }

    pub async fn add_oracle_metadata(&self, pubkey: XOnlyPublicKey) -> Result<(), duckdb::Error> {
        let pubkey_raw = pubkey.serialize().to_vec();
        //TODO: Add the ability to change the name via config
        let name = String::from("4casttruth");
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("INSERT INTO oracle_metadata (pubkey,name) VALUES(?,?)")?;
        stmt.execute([pubkey_raw, name.into()])?;
        Ok(())
    }

    async fn prepare_query(&self, select: Select) -> Result<String, duckdb::Error> {
        let re = Regex::new(r"\$(\d+)").unwrap();
        let binding = select.to_string();
        let fixed_params = re.replace_all(&binding, "?");
        Ok(fixed_params.to_string())
    }
}
