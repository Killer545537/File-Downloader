use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "parallel-downloader",
    version,
    about = "Download files over HTTP(S)"
)]
pub struct Cli {
    /// URL to download
    pub url: String,

    /// Output file path (defaults to the URL path's filename)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Maximum number of concurrent range requests
    #[arg(short = 'j', long, default_value_t = 8)]
    pub parallel: usize,

    /// Maximum number of HTTP redirects to follow
    #[arg(long, default_value_t = 8)]
    pub redirect_limit: usize,
}
