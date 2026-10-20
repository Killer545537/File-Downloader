use super::Downloader;
use crate::{config::Config, http::HttpClient};
use std::{
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use url::Url;

#[derive(Clone, Copy)]
enum Head {
    Size,
    MissingSize,
    Unsupported(u16),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Request {
    method: String,
    range: Option<String>,
}

struct Server {
    url: Url,
    requests: Arc<Mutex<Vec<Request>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<io::Result<()>>>,
}

impl Server {
    fn start(body: Vec<u8>, head: Head, ranges: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = Url::parse(&format!("http://{}/file", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let recorded = Arc::clone(&requests);
        let stopped = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            while !stopped.load(Ordering::Relaxed) && Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false)?;
                        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
                        let request = read_request(&mut stream)?;
                        recorded.lock().unwrap().push(request.clone());
                        if let Err(error) = respond(&mut stream, &request, &body, head, ranges) {
                            // A range probe may discard an ignored-range response body.
                            if !matches!(
                                error.kind(),
                                io::ErrorKind::BrokenPipe
                                    | io::ErrorKind::ConnectionReset
                                    | io::ErrorKind::ConnectionAborted
                            ) {
                                return Err(error);
                            }
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        });
        Self {
            url,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    fn finish(&mut self) -> Vec<Request> {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap().unwrap();
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn read_request(stream: &mut TcpStream) -> io::Result<Request> {
    let mut bytes = Vec::new();
    let mut byte = [0];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte)?;
        bytes.push(byte[0]);
        if bytes.len() > 16 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized request",
            ));
        }
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut lines = text.split("\r\n");
    let method = lines
        .next()
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing method"))?
        .to_owned();
    let range = lines.find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("range")
            .then(|| value.trim().to_owned())
    });
    Ok(Request { method, range })
}

fn respond(
    stream: &mut TcpStream,
    request: &Request,
    body: &[u8],
    head: Head,
    ranges: bool,
) -> io::Result<()> {
    if request.method == "HEAD" {
        // HEAD advertises the representation length but never sends its body.
        return match head {
            Head::Size => write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            ),
            Head::MissingSize => stream.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n"),
            Head::Unsupported(status) => write!(
                stream,
                "HTTP/1.1 {status} Unsupported\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        };
    }
    assert_eq!(request.method, "GET");
    if let Some(range) = request.range.as_deref().filter(|_| ranges) {
        let (start, end) = range
            .strip_prefix("bytes=")
            .and_then(|value| value.split_once('-'))
            .expect("expected a single bounded byte range");
        let start: usize = start.parse().unwrap();
        let end: usize = end.parse().unwrap();
        assert!(start <= end && end < body.len(), "invalid range: {range}");
        let chunk = &body[start..=end];
        write!(
            stream,
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nConnection: close\r\n\r\n",
            chunk.len(),
            body.len()
        )?;
        stream.write_all(chunk)
    } else {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )?;
        stream.write_all(body)
    }
}

struct Output {
    directory: PathBuf,
    path: PathBuf,
}

impl Output {
    fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        loop {
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "parallel-downloader-test-{}-{id}",
                std::process::id()
            ));
            match fs::create_dir(&directory) {
                Ok(()) => {
                    let path = directory.join("output.bin");
                    return Self { directory, path };
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("cannot create test output directory: {error}"),
            }
        }
    }
}

impl Drop for Output {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn payload(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index * 31 + index / 251) % 256) as u8)
        .collect()
}

fn downloader(server: &Server, output: &Output, parallel: usize) -> Downloader {
    let config = Config {
        url: server.url.clone(),
        output: output.path.clone(),
        parallel,
        redirect_limit: 3,
    };
    Downloader::new(HttpClient::build(config.redirect_limit).unwrap(), config)
}

fn request(method: &str, range: Option<&str>) -> Request {
    Request {
        method: method.to_owned(),
        range: range.map(str::to_owned),
    }
}

#[tokio::test]
async fn head_representation_size_drives_real_parallel_ranges_with_uneven_splits() {
    let body = payload(10_485_760);
    let mut server = Server::start(body.clone(), Head::Size, true);
    let output = Output::new();
    let downloader = downloader(&server, &output, 3);

    assert_eq!(
        downloader.http.head_size(&server.url).await.unwrap(),
        Some(10_485_760),
        "HEAD size must come from Content-Length, not the empty HEAD body"
    );
    let result = downloader.download(&output.path).await;
    let requests = server.finish();
    result.unwrap();
    assert_eq!(fs::read(&output.path).unwrap(), body);
    assert_eq!(
        &requests[..3],
        &[
            request("HEAD", None),
            request("HEAD", None),
            request("GET", Some("bytes=0-0")),
        ]
    );
    let mut chunks = requests[3..].to_vec();
    chunks.sort_by(|left, right| left.range.cmp(&right.range));
    assert_eq!(
        chunks,
        vec![
            request("GET", Some("bytes=0-3495253")),
            request("GET", Some("bytes=3495254-6990506")),
            request("GET", Some("bytes=6990507-10485759")),
        ]
    );
}

#[tokio::test]
async fn ignored_ranges_fall_back_to_a_complete_sequential_download() {
    let body = payload(1003);
    let mut server = Server::start(body.clone(), Head::Size, false);
    let output = Output::new();
    downloader(&server, &output, 4)
        .download(&output.path)
        .await
        .unwrap();
    assert_eq!(fs::read(&output.path).unwrap(), body);
    assert_eq!(
        server.finish(),
        vec![
            request("HEAD", None),
            request("GET", Some("bytes=0-0")),
            request("GET", None),
        ]
    );
}

async fn assert_head_falls_back(head: Head) {
    let body = payload(1003);
    let mut server = Server::start(body.clone(), head, true);
    let output = Output::new();
    downloader(&server, &output, 4)
        .download(&output.path)
        .await
        .unwrap();
    assert_eq!(fs::read(&output.path).unwrap(), body);
    assert_eq!(
        server.finish(),
        vec![request("HEAD", None), request("GET", None),]
    );
}

#[tokio::test]
async fn missing_head_size_falls_back_to_sequential_without_a_range_probe() {
    assert_head_falls_back(Head::MissingSize).await;
}

#[tokio::test]
async fn unsupported_head_falls_back_to_sequential_without_a_range_probe() {
    for status in [405, 501] {
        assert_head_falls_back(Head::Unsupported(status)).await;
    }
}

#[tokio::test]
async fn legitimate_zero_length_file_truncates_output_without_getting_a_body() {
    let mut server = Server::start(Vec::new(), Head::Size, true);
    let output = Output::new();
    fs::write(&output.path, b"stale output must be truncated").unwrap();
    downloader(&server, &output, 4)
        .download(&output.path)
        .await
        .unwrap();
    assert_eq!(fs::read(&output.path).unwrap(), Vec::<u8>::new());
    assert_eq!(server.finish(), vec![request("HEAD", None)]);
}
