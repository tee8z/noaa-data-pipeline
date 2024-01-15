use axum::{
    response::Html,
};
use tokio::{
    fs
};
pub async fn index(server_address: String, ui_path:String) -> String {
    let file_content = fs::read_to_string(&ui_path)
        .await
        .expect("Unable to read index.html");
    file_content.replace("{SERVER_ADDRESS}", &format!("http://{}", server_address))
}

pub async fn index_handler(server_address: String, ui_path:String) -> Html<String> {
    Html(index(server_address.clone(),ui_path).await)
}