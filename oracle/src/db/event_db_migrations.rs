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

    CREATE TABLE IF NOT EXISTS events (
        id UUID PRIMARY KEY,
        total_allowed_entries INTEGER NOT NULL,
        number_of_winners INTEGER NOT NULL,
        signing_date TIMESTAMPTZ NOT NULL,
        observation_date  TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        nonce BLOB NOT NULL,
        attestation_signature BLOB,
        -- Predefined IDs to be used for all possible entries in the event
        entry_ids UUID[],
        locations TEXT[] NOT NULL,
        locking_points BLOB[] NOT NULL
    );

    CREATE TYPE options AS ENUM ('over', 'par', 'under');

    CREATE TABLE events_entries
    (
        id UUID PRIMARY KEY,
        event_id UUID NOT NULL REFERENCES events (id),
        expected_observations STRUCT(station TEXT NOT NULL, temp_low options, temp_high options, wind_speed options)[],
        score INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );


    CREATE TABLE events_weather
    (
        id UUID PRIMARY KEY,
        event_id UUID NOT NULL REFERENCES events (id),
        weather_id UUID NOT NULL REFERENCES weather (id),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE weather
    (
        id UUID PRIMARY KEY,
        station_id TEXT NOT NULL,
        date TIMESTAMPTZ NOT NULL,
        event_id UUID NOT NULL REFERENCES events (id),
        observed STRUCT(temp_low INTEGER NOT NULL, temp_high INTEGER NOT NULL, wind_speed INTEGER NOT NULL),
        forecasted STRUCT(temp_low INTEGER NOT NULL, temp_high INTEGER NOT NULL, wind_speed INTEGER NOT NULL),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

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
