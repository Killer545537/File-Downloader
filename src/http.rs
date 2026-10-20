use anyhow::{Context, Ok, Result};
use std::time::Duration;
use url::Url;

use reqwest::{Client, StatusCode, header, redirect::Policy};

pub struct HttpClient {
    inner: Client,
}

impl HttpClient {
    pub fn build(redirect_limit: usize) -> Result<Self> {
        let inner = Client::builder()
            .redirect(Policy::limited(redirect_limit))
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(60 * 60))
            .build()?;

        Ok(Self { inner })
    }

    pub fn client(&self) -> &Client {
        &self.inner
    }

    pub async fn head_size(&self, url: &Url) -> Result<Option<u64>> {
        let response = self
            .inner
            .head(url.clone())
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await?;

        if !response.status().is_success() {
            return Ok(None);
        }

        // HEAD has no response body, so reqwest's body length is always zero.
        // Read the advertised resource size from the header instead.
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .map(|value| {
                value
                    .to_str()
                    .context("invalid Content-Length header")?
                    .parse::<u64>()
                    .context("invalid Content-Length header")
            })
            .transpose()
    }

    pub async fn supports_ranges(&self, url: &Url, total: u64) -> Result<bool> {
        if total == 0 {
            return Ok(false);
        }

        let response = self
            .inner
            .get(url.clone())
            .header(header::ACCEPT_ENCODING, "identity")
            .header(header::RANGE, "bytes=0-0")
            .send()
            .await?;

        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Ok(false);
        }

        let expected = format!("bytes 0-0/{total}");

        let valid = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == expected);

        let _ = response.bytes().await?;

        Ok(valid)
    }
}
