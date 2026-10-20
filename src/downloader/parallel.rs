use super::file_writer::FileWriter;
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode, Url,
    header::{ACCEPT_ENCODING, CONTENT_RANGE, RANGE},
};
use std::path::Path;
use tokio::task::JoinSet;
use tracing::debug;

pub async fn download(
    client: &Client,
    url: &Url,
    output: &Path,
    total: u64,
    parallelism: usize,
) -> Result<()> {
    if total == 0 {
        tokio::fs::write(output, []).await?;
        return Ok(());
    }

    let writer = FileWriter::create_sized(output, total).await?;

    let chunk_count = total.min(parallelism as u64) as usize;
    let base_len = total / chunk_count as u64;
    let remainder = total % chunk_count as u64;

    let mut ranges = Vec::with_capacity(chunk_count);
    let mut start = 0_u64;

    for index in 0..chunk_count {
        let extra = if (index as u64) < remainder { 1 } else { 0 };
        let length = base_len + extra;
        let end = start + length - 1;

        ranges.push((start, end));
        start = end + 1;
    }

    let mut tasks = JoinSet::new();

    for (start, end) in ranges {
        let client = client.clone();
        let url = url.clone();
        let writer = writer.clone();

        tasks.spawn(async move { download_chunk(&client, &url, &writer, start, end, total).await });
    }

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}

            Ok(Err(error)) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(error);
            }

            Err(error) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(error).context("download task failed");
            }
        }
    }

    writer.flush().await?;
    debug!(total_bytes = total, "all chunks written");

    Ok(())
}

async fn download_chunk(
    client: &Client,
    url: &Url,
    writer: &FileWriter,
    start: u64,
    end: u64,
    total: u64,
) -> Result<()> {
    let response = client
        .get(url.clone())
        .header(ACCEPT_ENCODING, "identity")
        .header(RANGE, format!("bytes={start}-{end}"))
        .send()
        .await
        .with_context(|| format!("request for bytes={start}-{end} failed"))?;

    if response.status() != StatusCode::PARTIAL_CONTENT {
        bail!(
            "server returned {} for range {start}-{end}",
            response.status()
        );
    }

    let expected_range = format!("bytes {start}-{end}/{total}");

    let actual_range = response
        .headers()
        .get(CONTENT_RANGE)
        .context("missing Content-Range header")?
        .to_str()?;

    if actual_range != expected_range {
        bail!(
            "expected Content-Range '{expected_range}', \
             got '{actual_range}'"
        );
    }

    let mut stream = response.bytes_stream();
    let mut written = 0_u64;
    let expected_len = end - start + 1;

    while let Some(item) = stream.next().await {
        let bytes = item.context("failed while reading response body")?;
        let next_written = written + bytes.len() as u64;

        if next_written > expected_len {
            bail!("server sent more bytes than requested for range {start}-{end}");
        }

        writer.write_at(start + written, &bytes).await?;
        written = next_written;
    }

    if written != expected_len {
        bail!(
            "range {start}-{end}: expected {expected_len} bytes, \
             received {written}"
        );
    }

    debug!(start, end, written, "chunk completed");

    Ok(())
}
