mod file_writer;
mod parallel;
mod sequential;

#[cfg(test)]
mod tests;
use std::path::Path;

use anyhow::Result;
use tracing::{info, warn};

use crate::{config::Config, http::HttpClient};

pub struct Downloader {
    http: HttpClient,
    config: Config,
}

impl Downloader {
    pub fn new(http: HttpClient, config: Config) -> Self {
        Self { http, config }
    }

    pub async fn download(&self, output: &Path) -> Result<()> {
        let total = self.http.head_size(&self.config.url).await?;

        if let Some(0) = total {
            tokio::fs::write(output, []).await?;
            info!(path=%&output.display(), "downloaded empty file");
            return Ok(());
        }

        if let Some(total) = total {
            match self.http.supports_ranges(&self.config.url, total).await {
                Ok(true) => {
                    info!(total_bytes = total, "using parallel range download");

                    return parallel::download(
                        self.http.client(),
                        &self.config.url,
                        output,
                        total,
                        self.config.parallel,
                    )
                    .await;
                }
                Ok(false) => {
                    warn!(
                        "byte ranges unsupported; \
                        falling back to sequential download"
                    );
                }
                Err(error) => {
                    warn!(
                        %error,
                        "range probe failed; \
                        falling back to sequential download
                        "
                    )
                }
            }
        } else {
            warn!("file size unavailable; using sequential download");
        }

        sequential::download(self.http.client(), &self.config.url, output).await
    }
}
