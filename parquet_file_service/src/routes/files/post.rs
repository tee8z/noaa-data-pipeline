use axum::{
    extract::{Multipart, Path},
    http::StatusCode,
};
use tokio::{
    fs::File,
    io::AsyncWriteExt,
};

pub async fn upload(
    Path(file_name): Path<String>,
    mut multipart: Multipart,
) -> Result<(), (StatusCode, String)> {
    //TODO: make this configuerable, pull from context
    let UPLOADS_DIRECTORY = "test";
    if !path_is_valid(&file_name) {
        return Err((StatusCode::BAD_REQUEST, "Invalid file".to_owned()));
    }
    while let Some(field) = multipart.next_field().await.unwrap() {
        let name = field.name().unwrap().to_string();
        let data = field.bytes().await.unwrap();

        println!("Length of `{}` is {} bytes", name, data.len());
        let path = std::path::Path::new(UPLOADS_DIRECTORY).join(&file_name);
        // Create a new file and write the data to it
        let mut file = File::create(&path).await.expect("Failed to create file");
        file.write_all(&data)
            .await
            .expect("Failed to write to file");
    }
    Ok(())
}

// to prevent directory traversal attacks we ensure the path consists of exactly one normal component
fn path_is_valid(path: &str) -> bool {
    let path = std::path::Path::new(path);

    let mut components = path.components().peekable();

    if let Some(first) = components.peek() {
        if !matches!(first, std::path::Component::Normal(_)) {
            return false;
        }
    }

    components.count() == 1 && is_parquet_file(path)
}

fn is_parquet_file(path: &std::path::Path) -> bool {
    if let Some(extenstion) = path.extension() {
        extenstion == "parquet"
    } else {
        false
    }
}
