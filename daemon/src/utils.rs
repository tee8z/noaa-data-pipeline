use std::{env, fs::File, io::Write};

use anyhow::{anyhow, Error};
use clap::Parser;
use futures::StreamExt;
use reqwest::Client;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use slog::{debug, o, Drain, Level, Logger};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Set the log level
    #[arg(short, long)]
    level: Option<String>,
}

pub fn setup_logger(cli: &Cli) -> Logger {
    let log_level = if cli.level.is_some() {
        let level = cli.level.as_ref().unwrap();
        match level.as_ref() {
            "trace" => Level::Trace,
            "debug" => Level::Debug,
            "info" => Level::Info,
            "warn" => Level::Warning,
            "error" => Level::Error,
            _ => Level::Info,
        }
    } else {
        let rust_log = env::var("RUST_LOG").unwrap_or_else(|_| String::from(""));
        match rust_log.to_lowercase().as_str() {
            "trace" => Level::Trace,
            "debug" => Level::Debug,
            "info" => Level::Info,
            "warn" => Level::Warning,
            "error" => Level::Error,
            _ => Level::Info,
        }
    };

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = drain.filter_level(log_level).fuse();
    slog::Logger::root(drain, o!("version" => "0.5"))
}

pub async fn fetch_xml(logger: &Logger, url: &str) -> Result<String, Error> {
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(Client::builder().user_agent("fetching_data/1.0").build()?)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    debug!(logger.clone(), "requesting: {}", url);
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| anyhow!("error sending request: {}", e))?;
    match response.text().await {
        Ok(xml_content) => Ok(xml_content),
        Err(e) => Err(anyhow!("error parsing body of request: {}", e)),
    }
}

pub async fn fetch_xml_zip(logger: &Logger, url: &str) -> Result<File, Error> {
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(Client::builder().user_agent("fetching_data/1.0").build()?)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    debug!(logger.clone(), "requesting: {}", url);
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| anyhow!("error sending request: {}", e))?;
    if !response.status().is_success() {
        return Err(anyhow!("error response from request"));
    }

    let mut temp_file = tempfile::tempfile().unwrap();
    let mut body = response.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        temp_file.write_all(&chunk)?;
    }

    temp_file.sync_all()?;

    Ok(temp_file)
}

pub fn get_full_path(relative_path: String) -> String {
    let mut current_dir = env::current_dir().expect("Failed to get current directory");

    // Append the relative path to the current working directory
    current_dir.push(relative_path);

    // Convert the `PathBuf` to a `String` if needed
    current_dir.to_string_lossy().to_string()
}
