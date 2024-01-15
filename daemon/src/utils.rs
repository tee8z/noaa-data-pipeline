use anyhow::{anyhow, Error};
use clap::Parser;
use futures::StreamExt;
use reqwest::Client;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use slog::{debug, error, info, o, Drain, Level, Logger};
use std::{
    env,
    fs::{self, File},
    io::Write,
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
#[derive(Parser, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Set the log level
    #[arg(short, long)]
    pub level: Option<String>,

    #[arg(short, long)]
    pub base_url: Option<String>,
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

pub struct RateLimiter {
    capacity: usize,
    tokens: f64,
    last_refill: Instant,
    refill_rate: f64,
}

impl RateLimiter {
    pub fn new(capacity: usize, refill_rate: f64) -> Self {
        RateLimiter {
            capacity,
            tokens: capacity as f64,
            last_refill: Instant::now(),
            refill_rate,
        }
    }

    fn refill_tokens(&mut self) {
        let now = Instant::now();
        let elapsed_time = now.duration_since(self.last_refill).as_secs_f64();
        let tokens_to_add = elapsed_time * self.refill_rate;

        self.tokens += tokens_to_add.min(self.capacity as f64);
        self.last_refill = now;
    }

    fn try_acquire(&mut self, tokens: f64) -> bool {
        let mut retries = 0;

        loop {
            self.refill_tokens();

            if tokens <= self.tokens {
                self.tokens -= tokens;
                return true;
            } else {
                if retries >= 3 {
                    // Maximum number of retries reached
                    return false;
                }

                retries += 1;
                thread::sleep(Duration::from_secs(20));
            }
        }
    }
}

pub async fn fetch_xml(
    logger: &Logger,
    url: &str,
    rate_limiter: Arc<Mutex<RateLimiter>>,
) -> Result<String, Error> {
    let mut limiter = rate_limiter.lock().await;
    if !limiter.try_acquire(1.0) {
        // This happens after waitin and trying 3 times
        return Err(anyhow!("Rate limit exceeded after retries"));
    }

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

pub async fn fetch_xml_zip(
    logger: &Logger,
    url: &str,
    rate_limiter: Arc<Mutex<RateLimiter>>,
) -> Result<File, Error> {
    let mut limiter = rate_limiter.lock().await;
    if !limiter.try_acquire(1.0) {
        // This happens after waitin and trying 3 times
        return Err(anyhow!("Rate limit exceeded after retries"));
    }
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

pub fn create_folder(root_path: &str, logger: &Logger) {
    let path = Path::new(root_path);

    if !path.exists() || !path.is_dir() {
        // Create the folder if it doesn't exist
        if let Err(err) = fs::create_dir(path) {
            error!(logger, "Error creating folder: {}", err);
            // Handle the error as needed
        } else {
            info!(logger, "Folder created: {}", root_path);
        }
    } else {
        info!(logger, "Folder already exists: {}", root_path);
    }
}
