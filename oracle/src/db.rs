use duckdb::{AccessMode, Config, Connection, DefaultNullOrder, DefaultOrder};
use std::sync::Arc;
use tokio::sync::Mutex;

// Note: if we end up doing lots of right we may need to create a queue mechanism to write to the DB file or switch to postgres
// currently system is being built under the assumptions there will be far more reads than writes
#[derive(Clone)]
pub struct DbManager {
    conn: Arc<Mutex<Connection>>,
}

impl DbManager {
    pub fn new(db_file: &str) -> duckdb::Result<Self> {
        let config = Config::default()
            .access_mode(AccessMode::ReadWrite)?
            .default_null_order(DefaultNullOrder::NullsLast)?
            .default_order(DefaultOrder::Desc)?
            .enable_external_access(true)?
            .enable_autoload_extension(true)?
            .threads(2)?;

        let conn = Connection::open_with_flags(db_file, config)?;
        if let Err(_) = conn.query_row(r"SELECT Count(*) FROM oracle_metadata", [], |_| Ok(())) {
            create_db_tables(&conn)?;
        }
        Ok(DbManager {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn get_conn(&self) -> Arc<Mutex<Connection>> {
        self.conn.clone()
    }
}

// migrations will need to be done carefully, the tooling isn't great just yet for duckdb with these in mind
// additionally, a mutex lock needs to be around the db connection as duckdb is a single file so multiple rights at the same time isn't really supported
fn create_db_tables(conn: &Connection) -> Result<(), duckdb::Error> {
    conn.execute_batch(
        r" CREATE SEQUENCE seq;
        CREATE TABLE oracle_metadata (
            id                  INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
            singleton_constant  BOOL NOT NULL,
            created_at          TIMESTAMPTZ NOT NULL,
            pubkey              BYTEA NOT NULL,
            name                TEXT NOT NULL,
            updated_at          TIMESTAMPTZ,
        );
        CREATE TABLE events (
            id                     INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
            is_enum                BOOL NOT NULL,
            created_at             TIMESTAMPTZ NOT NULL,
            announcement_signature BYTEA NOT NULL,
            oracle_event           BYTEA NOT NULL,
            name                   TEXT NOT NULL,
            updated_at             TIMESTAMPTZ,
            announcement_event_id  BYTEA,
            attestation_event_id   BYTEA,
        );
        CREATE TABLE event_nonces (
            id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
            event_id        INTEGER REFERENCES events(id) NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL,
            index           INTEGER NOT NULL,
            nonce           BYTEA NOT NULL,
            updated_at      TIMESTAMPTZ,
            signature       BYTEA,
            outcome         TEXT,

        );
    ",
    )
}