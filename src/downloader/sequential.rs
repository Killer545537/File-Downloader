use super::file_writer::FileWriter;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::{Client, Url, header::ACCEPT_ENCODING};
use std::path::Path;
use tracing::info;

pub async fn download(client: &Client, url: &Url, output: &Path) -> Result<()> {
    let response = client
        .get(url.clone())
        .header(ACCEPT_ENCODING, "identity")
        .send()
        .await
        .context("GET request failed")?
        .error_for_status()
        .context("server returned an unsuccessful status")?;

    let writer = FileWriter::create(output).await?;
    let mut stream = response.bytes_stream();
    let mut total = 0_u64;

    while let Some(item) = stream.next().await {
        let bytes = item.context("failed while reading response body")?;

        writer.append(&bytes).await?;
        total += bytes.len() as u64;
    }

    writer.flush().await?;

    info!(total_bytes = total, "sequential download completed");

    Ok(())
}
