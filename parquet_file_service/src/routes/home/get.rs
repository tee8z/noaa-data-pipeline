use std::sync::Arc;

use axum::{extract::State, response::Html};
use tokio::fs;

use crate::AppState;

pub async fn index_handler(State(state): State<Arc<AppState>>) -> Html<String> {
    Html(index(&state.server_address, &state.ui_dir).await)
}

pub async fn index(server_address: &str, ui_dir: &str) -> String {
    let file_content = fs::read_to_string(ui_dir)
        .await
        .expect("Unable to read index.html");
    file_content.replace("{SERVER_ADDRESS}", &format!("http://{}", server_address))
}
