use std::{fs, path::Path};
use slog::{error, info, Logger};

pub fn create_folder(logger: &Logger, root_path: &str) {
    let path = Path::new(root_path);

    if !path.exists() || !path.is_dir() {
        // Create the folder if it doesn't exist
        if let Err(err) = fs::create_dir(path) {
            error!(logger, "error creating folder: {}", err);
        } else {
            info!(logger, "folder created: {}", root_path);
        }
    } else {
        info!(logger, "folder already exists: {}", root_path);
    }
}

pub fn path_exists(path: &str) -> bool {
    fs::metadata(path).is_ok()
}