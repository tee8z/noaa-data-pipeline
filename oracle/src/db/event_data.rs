use duckdb::{arrow::array::RecordBatch, params_from_iter, Connection};
use regex::Regex;
use scooby::postgres::Select;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct EventData {
    // TODO: see if a read/write lock makes more sense here (careful of writer starvation) and be aware of bottleneck around locking that may occur under heavy loads
    // eventually we may need to come up with a pool or other non-locking approach for grabbing the connection, but that shouldn't appear until we hit a decent usage level
    conn: Arc<Mutex<Connection>>,
}

impl EventData {
    pub fn new(path: &str) -> Result<Self, duckdb::Error> {
        let conn = Connection::open(format!("{}/events.db3", path))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn query(
        &self,
        select: Select,
        params: Vec<String>,
    ) -> Result<Vec<RecordBatch>, duckdb::Error> {
        let re = Regex::new(r"\$(\d+)").unwrap();
        let binding = select.to_string();
        let fixed_params = re.replace_all(&binding, "?");
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&fixed_params)?;
        let sql_params = params_from_iter(params.iter());
        Ok(stmt.query_arrow(sql_params)?.collect())
    }
}
