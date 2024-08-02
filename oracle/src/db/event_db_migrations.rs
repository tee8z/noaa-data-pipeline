use duckdb::Connection;
use log::info;

pub fn run_migrations(conn: &mut Connection) -> Result<(), duckdb::Error> {
    create_version_table(conn)?;
    let mut stmt = conn.prepare("SELECT version FROM db_version")?;
    let mut rows = stmt.query([])?;

    let current_version = if let Some(row) = rows.next()? {
        row.get(0)?
    } else {
        0
    };

    match current_version {
        0 => {
            create_initial_schema(conn)?;
        }
        /*1 => {
        migrate_to_version_2(conn)?;
        }*/
        _ => info!("database is up-to-date."),
    }

    Ok(())
}

pub fn create_version_table(conn: &mut Connection) -> Result<(), duckdb::Error> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS db_version ( version INTEGER PRIMARY KEY);",
        [],
    )?;
    Ok(())
}

pub fn create_initial_schema(conn: &mut Connection) -> Result<(), duckdb::Error> {
    let initial_schema = r#"
    -- Table of information about the oracle, mostly to prevent multiple keys from being used with the same database
    -- singleton_constant is a dummy column to ensure there is only one row
    CREATE TABLE oracle_metadata
    (
            pubkey             BLOB     NOT NULL UNIQUE PRIMARY KEY,
            name               TEXT      NOT NULL UNIQUE,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            singleton_constant BOOLEAN   NOT NULL DEFAULT TRUE, -- make sure there is only one row
            CONSTRAINT one_row_check UNIQUE (singleton_constant)
    );

    -- Primary table containing information about events,
    -- contains a broken up oracle announcement, excluding the oracle pubkey which is in memory
    CREATE TABLE IF NOT EXISTS events (
        id UUID PRIMARY KEY,
        announcement_signature BLOB NOT NULL,
        oracle_event BLOB NOT NULL,
        name TEXT NOT NULL UNIQUE,
        announcement_event_id BLOB UNIQUE,
        attestation_event_id BLOB UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());

    CREATE UNIQUE INDEX event_name_index ON events (name);

    -- Table for storing the nonces for each event
    -- The signature and outcome are optional, and are only filled in when the event is completed
    CREATE TABLE event_nonces
    (
        id UUID PRIMARY KEY,
        event_id   UUID   NOT NULL REFERENCES events (id),
        index      INTEGER   NOT NULL,
        nonce      BLOB     NOT NULL UNIQUE,
        signature  BLOB,
        outcome    TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX event_nonces_event_id_index ON event_nonces (event_id);

    INSERT INTO db_version (version) VALUES (1);
    "#;
    conn.execute_batch(initial_schema)?;
    Ok(())
}

/* how to add the next sql migration:
pub fn migrate_to_version_2(conn: &mut Connection) -> Result<(), duckdb::Error> {
    let migration_2 = r#"
    UPDATE db_version SET version = 2;"#;"
    conn.execute_batch(migration_2)?;
    Ok(())
}
*/
