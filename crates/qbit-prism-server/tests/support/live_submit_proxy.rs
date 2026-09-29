//! An HTTP/1.1 JSON-RPC proxy in front of a live fixture's node that
//! records every `submitblock` and, when armed, holds the next one until the
//! test releases it, while every other call passes (#575). A held call keeps
//! a found block mid-landing, its claim live, for as long as a scenario
//! needs.
use super::*;
use qbit_prism_server::codec::{double_sha256, hash_display};
use std::sync::Arc;
use tokio::{
    io::AsyncReadExt,
    net::TcpStream,
    sync::{oneshot, watch},
};

/// A held `submitblock`: its block and the release.
pub(crate) struct Hit {
    pub block: String,
    pub release: oneshot::Sender<()>,
}

struct ProxyState {
    upstream: u16,
    submitted: Mutex<Vec<String>>,
    trap: Mutex<Option<oneshot::Sender<Hit>>>,
    /// Flipped to release every held call at cleanup.
    released: watch::Sender<bool>,
}

/// An HTTP/1.1 JSON-RPC proxy in front of the node that records every
/// `submitblock` and, when armed, holds the next one until its release while
/// every other call passes.
pub(crate) struct SubmitProxy {
    pub port: u16,
    state: Arc<ProxyState>,
    task: tokio::task::JoinHandle<()>,
}

impl SubmitProxy {
    pub(crate) async fn start(upstream: u16) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let state = Arc::new(ProxyState {
            upstream,
            submitted: Mutex::new(Vec::new()),
            trap: Mutex::new(None),
            released: watch::Sender::new(false),
        });
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((downstream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let _ = relay(state, downstream).await;
                });
            }
        });
        Ok(Self { port, state, task })
    }

    /// Hold the next `submitblock`.
    pub(crate) fn arm(&self) -> oneshot::Receiver<Hit> {
        let (sender, receiver) = oneshot::channel();
        *self.state.trap.lock().unwrap() = Some(sender);
        receiver
    }

    /// How many `submitblock` calls for `block` reached the proxy.
    pub(crate) fn offers(&self, block: &str) -> usize {
        self.state
            .submitted
            .lock()
            .unwrap()
            .iter()
            .filter(|submitted| *submitted == block)
            .count()
    }

    pub(crate) fn release_all(&self) {
        self.state.released.send_replace(true);
    }
}

impl Drop for SubmitProxy {
    fn drop(&mut self) {
        self.release_all();
        self.task.abort();
    }
}

/// Relays one server connection, one request and reply at a time. Each
/// request goes to the node on a new connection, opened after any hold: a
/// held call can outlast qbitd's idle timeout, and must not be written into
/// a connection the node has closed meanwhile.
async fn relay(state: Arc<ProxyState>, downstream: TcpStream) -> Result<()> {
    let (down_read, mut down_write) = downstream.into_split();
    let mut down_read = BufReader::new(down_read);
    loop {
        let Some((request, body)) = read_http(&mut down_read).await? else {
            return Ok(());
        };
        if let Some(block) = submitted_block(&body) {
            state.submitted.lock().unwrap().push(block.clone());
            let trap = state.trap.lock().unwrap().take();
            if let Some(sender) = trap {
                let (release, released) = oneshot::channel();
                let mut cleanup = state.released.subscribe();
                if sender.send(Hit { block, release }).is_ok() {
                    tokio::select! {
                        _ = released => {}
                        _ = cleanup.wait_for(|released| *released) => {}
                    }
                }
            }
        }
        let upstream = TcpStream::connect(("127.0.0.1", state.upstream)).await?;
        let (up_read, mut up_write) = upstream.into_split();
        up_write.write_all(&request).await?;
        let Some((reply, _)) = read_http(&mut BufReader::new(up_read)).await? else {
            return Ok(());
        };
        down_write.write_all(&reply).await?;
    }
}

/// The block hash of a `submitblock` request body, if it is one.
fn submitted_block(body: &[u8]) -> Option<String> {
    let request: Value = serde_json::from_slice(body).ok()?;
    if request["method"] != "submitblock" {
        return None;
    }
    let block = hex::decode(request["params"][0].as_str()?).ok()?;
    Some(hash_display(&double_sha256(block.get(..80)?)))
}

/// One HTTP/1.1 message framed by `Content-Length`, as qbitd and the
/// servers' client frame every message: the raw bytes and the body.
async fn read_http<R>(reader: &mut BufReader<R>) -> Result<Option<(Vec<u8>, Vec<u8>)>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut raw = Vec::new();
    let mut length = None;
    loop {
        let start = raw.len();
        if reader.read_until(b'\n', &mut raw).await? == 0 {
            ensure!(raw.is_empty(), "connection closed inside an HTTP header");
            return Ok(None);
        }
        let line = std::str::from_utf8(&raw[start..])?.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = Some(value.trim().parse::<usize>()?);
            }
        }
    }
    let mut body = vec![0; length.context("HTTP message without Content-Length")?];
    reader.read_exact(&mut body).await?;
    raw.extend_from_slice(&body);
    Ok(Some((raw, body)))
}
