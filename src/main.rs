mod cli;
mod config;
mod downloader;
mod http;
mod utils;

use anyhow::{Context, Ok, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::{cli::Cli, config::Config, downloader::Downloader, http::HttpClient};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = Config::try_from(cli)?;

    let client = HttpClient::build(config.redirect_limit)?;
    let downloader = Downloader::new(client, config.clone());

    downloader
        .download(&config.output)
        .await
        .with_context(|| format!("download failed: {}", config.url))?;

    println!("Saved: {}", config.output.display());

    Ok(())
}
