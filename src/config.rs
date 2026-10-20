use std::path::PathBuf;

use anyhow::bail;
use url::Url;

use crate::{cli::Cli, utils::filename_from_url};

#[derive(Clone, Debug)]
pub struct Config {
    pub url: Url,
    pub output: PathBuf,
    pub parallel: usize,
    pub redirect_limit: usize,
}

impl TryFrom<Cli> for Config {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let url = Url::parse(&cli.url)?;

        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            bail!("URL must be an HTTP or HTTPS URL with a hostname");
        }

        if cli.parallel == 0 {
            bail!("--parallel must be at least 1");
        }

        let output = cli.output.unwrap_or_else(|| filename_from_url(&url));

        Ok(Self {
            url,
            output,
            parallel: cli.parallel,
            redirect_limit: cli.redirect_limit,
        })
    }
}
