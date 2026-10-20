use reqwest::Url;
use std::path::PathBuf;

pub fn filename_from_url(url: &Url) -> PathBuf {
    url.path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("download"))
}
