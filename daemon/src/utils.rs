use anyhow::{anyhow,Error};
use reqwest::Client;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use slog::{Logger, error,debug};

pub async fn fetch_xml(logger: &Logger, url: &str) -> Result<String, Error>
{
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(Client::builder().user_agent("fetching_data/1.0").build()?)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    debug!(logger.clone(), "requesting: {}", url);
    let response = client.get(url).send().await.map_err(|e| {
        error!(logger.clone(), "error sending request: {}", e);
        anyhow!("error sending request: {}", e)
    })?;
    match response.text().await {
        Ok(xml_content) => Ok(xml_content),
        Err(e) => {
            error!(logger.clone(), "error sending request: {}", e);
            Err(anyhow!("error parsing body of request: {}", e))
        }
    }
}