//! The readiness-only HTTP endpoint the Hashbalancer checks (3.1, decision
//! D-7): `GET /readyz` carrying the `X-Qbit-Healthcheck-Token` header answers
//! `200 ready` while this frontend admits miners and `503 not ready`
//! otherwise. It listens on its own port (`PRISM_READINESS_PORT`), meant for
//! a node's public address, so it says nothing else: no health payload, no
//! metrics, no operator route, and a request without the token learns only
//! that it was refused.
//!
//! Every connection has a two-second deadline, twice the balancer's check
//! timeout, carries one request and is closed after its answer. At most 256
//! are served at once, and at most 8 from one source address (an IPv6 source
//! counts by its /64), so one client cannot hold the slots the balancer's
//! checks need. A connection beyond either cap is closed at once, unanswered,
//! which a checker counts as a failed probe.
use super::admission::AdmissionSignal;
use crate::{config, metrics::Metrics, metrics::ReadinessAnswer};
use anyhow::{ensure, Context, Result};
use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderValue, Method, Request, Response, StatusCode},
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    convert::Infallible,
    net::{IpAddr, Ipv6Addr},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{watch, Semaphore},
    task::JoinSet,
};

/// The header the Hashbalancer's HTTP checks carry (its `HEALTH_HEADER`).
pub const TOKEN_HEADER: &str = "x-qbit-healthcheck-token";
pub const PATH: &str = "/readyz";
/// How long one connection, and every slot it holds, may last.
const CONNECTION_DEADLINE: Duration = Duration::from_secs(2);
const MAX_CONNECTIONS: usize = 256;
const MAX_CONNECTIONS_PER_SOURCE: usize = 8;
/// hyper's smallest read buffer; a request line and headers must fit.
const MAX_REQUEST_BYTES: usize = 8192;

/// `PRISM_READINESS_BIND`, `PRISM_READINESS_PORT` and the token.
#[derive(Clone)]
pub struct EndpointConfig {
    pub bind: String,
    pub port: u16,
    token: Arc<str>,
}

impl std::fmt::Debug for EndpointConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndpointConfig")
            .field("bind", &self.bind)
            .field("port", &self.port)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl EndpointConfig {
    /// `None` while `PRISM_READINESS_PORT` is unset or 0, the default.
    pub fn from_env() -> Result<Option<Self>> {
        let port = config::number("PRISM_READINESS_PORT", 0u16)?;
        if port == 0 {
            return Ok(None);
        }
        let token = config::secret("PRISM_READINESS_TOKEN")?.context(
            "PRISM_READINESS_TOKEN or PRISM_READINESS_TOKEN_FILE is required when \
             PRISM_READINESS_PORT is set: the readiness endpoint answers only that token",
        )?;
        Ok(Some(Self::new(
            config::value("PRISM_READINESS_BIND", "127.0.0.1"),
            port,
            &token,
        )?))
    }

    pub fn new(bind: String, port: u16, token: &str) -> Result<Self> {
        // Never echo the value: the error names the setting only.
        ensure!(
            token.len() >= 16 && token.bytes().all(|byte| byte.is_ascii_graphic()),
            "PRISM_READINESS_TOKEN must be at least 16 printable ASCII characters, without spaces"
        );
        Ok(Self {
            bind,
            port,
            token: token.into(),
        })
    }
}

/// What the endpoint answers from: the health publisher's latest admission
/// decision, aged against the health freshness budget.
#[derive(Clone)]
pub struct Endpoint {
    token: Arc<str>,
    admission: watch::Receiver<AdmissionSignal>,
    stale_after: Duration,
    metrics: Arc<Metrics>,
}

impl Endpoint {
    pub fn new(
        config: &EndpointConfig,
        admission: watch::Receiver<AdmissionSignal>,
        stale_after: Duration,
        metrics: Arc<Metrics>,
    ) -> Self {
        metrics.enable_readiness_endpoint();
        Self {
            token: config.token.clone(),
            admission,
            stale_after,
            metrics,
        }
    }

    /// The token is checked before anything else, so a request without it
    /// cannot tell which paths exist or what the frontend's state is.
    fn answer(&self, method: &Method, path: &str, headers: &HeaderMap) -> Response<Body> {
        let (status, body, answer) = if !authorized(headers, &self.token) {
            (
                StatusCode::UNAUTHORIZED,
                "unauthorized\n",
                ReadinessAnswer::Unauthorized,
            )
        } else if path != PATH || !matches!(*method, Method::GET | Method::HEAD) {
            (
                StatusCode::NOT_FOUND,
                "not found\n",
                ReadinessAnswer::NotFound,
            )
        } else if self
            .admission
            .borrow()
            .admits_at(tokio::time::Instant::now(), self.stale_after)
        {
            (StatusCode::OK, "ready\n", ReadinessAnswer::Ready)
        } else {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "not ready\n",
                ReadinessAnswer::NotReady,
            )
        };
        self.metrics.record_readiness_request(answer);
        let mut response = Response::new(if *method == Method::HEAD {
            Body::empty()
        } else {
            Body::from(body)
        });
        *response.status_mut() = status;
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("close"));
        response
    }
}

/// Exactly one token header equal to the configured token. Both sides are
/// digested first, so the comparison reveals neither a matching prefix nor
/// the configured length.
fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let mut values = headers.get_all(TOKEN_HEADER).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    let presented = Sha256::digest(value.as_bytes());
    let expected = Sha256::digest(expected.as_bytes());
    presented
        .iter()
        .zip(expected.iter())
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

/// What one listener allows: the connection deadline and both caps.
#[derive(Clone, Copy, Debug)]
struct Limits {
    deadline: Duration,
    connections: usize,
    per_source: usize,
}

impl Limits {
    const DEFAULT: Self = Self {
        deadline: CONNECTION_DEADLINE,
        connections: MAX_CONNECTIONS,
        per_source: MAX_CONNECTIONS_PER_SOURCE,
    };
}

/// The address a source's connections are counted under: an IPv4 address
/// (an IPv4-mapped IPv6 one included) as itself, an IPv6 address by its /64,
/// the smallest block one client is usually given.
fn source_key(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) => match address.to_ipv4_mapped() {
            Some(address) => IpAddr::V4(address),
            None => IpAddr::V6(Ipv6Addr::from(u128::from(address) & !(u64::MAX as u128))),
        },
        address => address,
    }
}

/// Open connections per source address.
#[derive(Default)]
struct Sources(Mutex<HashMap<IpAddr, usize>>);

/// One connection's place in its source's share, given back when it ends.
struct SourceSlot {
    sources: Arc<Sources>,
    key: IpAddr,
}

impl Sources {
    fn acquire(self: &Arc<Self>, address: IpAddr, cap: usize) -> Option<SourceSlot> {
        let key = source_key(address);
        let mut open = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count = open.entry(key).or_default();
        if *count >= cap {
            return None;
        }
        *count += 1;
        Some(SourceSlot {
            sources: self.clone(),
            key,
        })
    }
}

impl Drop for SourceSlot {
    fn drop(&mut self) {
        let mut open = self
            .sources
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = open.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                open.remove(&self.key);
            }
        }
    }
}

/// Serve the endpoint on `listener` until `shutdown`. An accept error is
/// logged and retried after a pause, never fatal: the endpoint is a probe
/// target, and losing it reads as not ready, which is the safe failure.
pub async fn serve(
    listener: TcpListener,
    endpoint: Endpoint,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    serve_with(listener, endpoint, shutdown, Limits::DEFAULT).await
}

async fn serve_with(
    listener: TcpListener,
    endpoint: Endpoint,
    mut shutdown: watch::Receiver<bool>,
    limits: Limits,
) -> Result<()> {
    let permits = Arc::new(Semaphore::new(limits.connections));
    let sources = Arc::new(Sources::default());
    let mut connections = JoinSet::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            _ = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        tracing::warn!(%error, "readiness endpoint accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                // Beyond either cap the connection is closed unanswered.
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let Some(slot) = sources.acquire(peer.ip(), limits.per_source) else {
                    drop(stream);
                    continue;
                };
                let endpoint = endpoint.clone();
                connections.spawn(async move {
                    let _held = (permit, slot);
                    let service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                        let response = endpoint.answer(request.method(), request.uri().path(), request.headers());
                        async move { Ok::<_, Infallible>(response) }
                    });
                    let connection = hyper::server::conn::http1::Builder::new()
                        .keep_alive(false)
                        .max_buf_size(MAX_REQUEST_BYTES)
                        .timer(hyper_util::rt::TokioTimer::new())
                        .header_read_timeout(limits.deadline)
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
                    let _ = tokio::time::timeout(limits.deadline, connection).await;
                });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const TOKEN: &str = "readiness-token-0123456789";
    const BUDGET: Duration = Duration::from_secs(15);

    fn endpoint() -> (Endpoint, watch::Sender<AdmissionSignal>, Arc<Metrics>) {
        let config = EndpointConfig::new("127.0.0.1".into(), 9084, TOKEN).unwrap();
        let (decisions, admission) = watch::channel(AdmissionSignal::UNDECIDED);
        let metrics = Arc::new(Metrics::default());
        (
            Endpoint::new(&config, admission, BUDGET, metrics.clone()),
            decisions,
            metrics,
        )
    }

    fn token(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.append(TOKEN_HEADER, HeaderValue::from_str(value).unwrap());
        headers
    }

    #[test]
    fn short_or_spaced_tokens_are_refused_without_echoing_them() {
        for bad in [
            "short-token",
            "a token with spaces",
            "tab\ttoken-0123456789",
            "",
        ] {
            let error = EndpointConfig::new("0.0.0.0".into(), 9084, bad)
                .unwrap_err()
                .to_string();
            assert!(error.contains("PRISM_READINESS_TOKEN"), "{error}");
            assert!(bad.is_empty() || !error.contains(bad), "{error}");
        }
        let config = EndpointConfig::new("0.0.0.0".into(), 9084, TOKEN).unwrap();
        assert!(!format!("{config:?}").contains(TOKEN));
    }

    #[test]
    fn only_one_exact_token_header_is_accepted() {
        assert!(authorized(&token(TOKEN), TOKEN));
        let mut repeated = token(TOKEN);
        repeated.append(TOKEN_HEADER, HeaderValue::from_static(TOKEN));
        for rejected in [
            HeaderMap::new(),
            token(&TOKEN[..TOKEN.len() - 1]),
            token(&format!("{TOKEN}x")),
            token(&TOKEN.to_uppercase()),
            repeated,
        ] {
            assert!(!authorized(&rejected, TOKEN), "{rejected:?}");
        }
    }

    #[test]
    fn answers_follow_the_token_the_path_and_a_fresh_admission() {
        let (endpoint, decisions, metrics) = endpoint();
        let get =
            |path: &str, headers: &HeaderMap| endpoint.answer(&Method::GET, path, headers).status();
        // Refused before the path is even looked at.
        assert_eq!(get("/nowhere", &HeaderMap::new()), StatusCode::UNAUTHORIZED);
        assert_eq!(
            get(PATH, &token("wrong-token-0123456789")),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(get("/healthz", &token(TOKEN)), StatusCode::NOT_FOUND);
        assert_eq!(
            endpoint.answer(&Method::POST, PATH, &token(TOKEN)).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(get(PATH, &token(TOKEN)), StatusCode::SERVICE_UNAVAILABLE);
        let now = tokio::time::Instant::now();
        decisions.send_replace(AdmissionSignal::decided(true, now));
        assert_eq!(get(PATH, &token(TOKEN)), StatusCode::OK);
        assert_eq!(
            endpoint.answer(&Method::HEAD, PATH, &token(TOKEN)).status(),
            StatusCode::OK
        );
        // A decision older than the freshness budget admits nothing.
        decisions.send_replace(AdmissionSignal::decided(
            true,
            now - BUDGET - Duration::from_secs(1),
        ));
        assert_eq!(get(PATH, &token(TOKEN)), StatusCode::SERVICE_UNAVAILABLE);
        decisions.send_replace(AdmissionSignal::decided(false, now));
        assert_eq!(get(PATH, &token(TOKEN)), StatusCode::SERVICE_UNAVAILABLE);
        let body = metrics.render();
        for (answer, count) in [
            ("ready", 2),
            ("not_ready", 3),
            ("unauthorized", 2),
            ("not_found", 2),
        ] {
            let line =
                format!("qbit_prism_readiness_requests_total{{result=\"{answer}\"}} {count}");
            assert!(body.lines().any(|l| l == line), "missing {line}");
        }
    }

    #[test]
    fn a_source_is_counted_by_its_address_or_its_ipv6_64() {
        let loopback = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        assert_eq!(source_key(loopback), loopback);
        assert_eq!(
            source_key(IpAddr::V6(std::net::Ipv4Addr::LOCALHOST.to_ipv6_mapped())),
            loopback
        );
        // ::1 is counted under ::/64, with every address of that block.
        assert_eq!(
            source_key(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        );
        let sources = Arc::new(Sources::default());
        let mut slots: Vec<_> = (0..MAX_CONNECTIONS_PER_SOURCE)
            .map(|_| {
                sources
                    .acquire(loopback, MAX_CONNECTIONS_PER_SOURCE)
                    .unwrap()
            })
            .collect();
        assert!(sources
            .acquire(loopback, MAX_CONNECTIONS_PER_SOURCE)
            .is_none());
        let other = IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 2));
        let other_slot = sources.acquire(other, MAX_CONNECTIONS_PER_SOURCE).unwrap();
        slots.pop();
        let again = sources
            .acquire(loopback, MAX_CONNECTIONS_PER_SOURCE)
            .unwrap();
        drop((slots, other_slot, again));
        assert!(
            sources.0.lock().unwrap().is_empty(),
            "a slot was not given back"
        );
    }

    async fn exchange(address: std::net::SocketAddr, request: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        response
    }

    /// Over a real socket, as HAProxy's `http-check` sends it (HTTP/1.0 by
    /// default): one answer, then the connection closes.
    #[tokio::test]
    async fn the_endpoint_answers_haproxy_style_checks_over_a_socket_and_closes() {
        let (endpoint, decisions, _metrics) = endpoint();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = watch::channel(false);
        let server = tokio::spawn(serve(listener, endpoint, shutdown));
        let check = format!("GET /readyz HTTP/1.0\r\n{TOKEN_HEADER}: {TOKEN}\r\n\r\n");
        let response = exchange(address, &check).await;
        assert!(response.starts_with("HTTP/1.0 503"), "{response}");
        assert!(response.ends_with("not ready\n"), "{response}");
        decisions.send_replace(AdmissionSignal::decided(true, tokio::time::Instant::now()));
        let response = exchange(address, &check).await;
        assert!(response.starts_with("HTTP/1.0 200"), "{response}");
        assert!(response
            .to_ascii_lowercase()
            .contains("cache-control: no-store"));
        assert!(response.ends_with("ready\n"), "{response}");
        let response = exchange(
            address,
            "GET /readyz HTTP/1.1\r\nHost: a\r\nConnection: keep-alive\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 401"), "{response}");
        assert!(!response.contains("ready"), "{response}");
        stop.send_replace(true);
        server.await.unwrap().unwrap();
    }

    /// What a client gets over one connection, if anything: a connection
    /// closed unanswered reads as empty, whether by a FIN or a reset.
    async fn exchange_or_closed(address: std::net::SocketAddr, request: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let _ = stream.write_all(request.as_bytes()).await;
        let mut response = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("the connection was neither answered nor closed");
        String::from_utf8_lossy(&response).into_owned()
    }

    /// Past the per-source cap, or the global one, a connection is closed at
    /// once and unanswered, and a slot is given back when its connection
    /// ends: a client holding connections open shuts nobody else out, and
    /// shuts itself out only for as long as it holds them.
    #[tokio::test]
    async fn connections_past_a_cap_are_closed_unanswered_until_a_slot_frees() {
        let long = Duration::from_secs(60);
        for limits in [
            Limits {
                deadline: long,
                connections: MAX_CONNECTIONS,
                per_source: 2,
            },
            Limits {
                deadline: long,
                connections: 2,
                per_source: MAX_CONNECTIONS_PER_SOURCE,
            },
        ] {
            let (endpoint, _decisions, _metrics) = endpoint();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (stop, shutdown) = watch::channel(false);
            let server = tokio::spawn(serve_with(listener, endpoint, shutdown, limits));
            let check = format!("GET /readyz HTTP/1.0\r\n{TOKEN_HEADER}: {TOKEN}\r\n\r\n");
            // Two unfinished requests hold both slots: accepted in order,
            // they are served before the check behind them.
            let mut holders = Vec::new();
            for _ in 0..2 {
                let mut holder = tokio::net::TcpStream::connect(address).await.unwrap();
                holder.write_all(b"GET /readyz HTTP/1.1\r\n").await.unwrap();
                holders.push(holder);
            }
            let response = exchange_or_closed(address, &check).await;
            assert!(
                response.is_empty(),
                "{limits:?}: answered past the cap: {response}"
            );
            drop(holders.pop());
            let freed = std::time::Instant::now();
            loop {
                let response = exchange_or_closed(address, &check).await;
                if response.starts_with("HTTP/1.0 503") {
                    break;
                }
                assert!(response.is_empty(), "{limits:?}: {response}");
                assert!(
                    freed.elapsed() < Duration::from_secs(10),
                    "{limits:?}: the freed slot was never given back"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            stop.send_replace(true);
            server.await.unwrap().unwrap();
        }
    }

    /// A client that never finishes its request is cut off at the deadline,
    /// so it cannot hold one of the endpoint's connection slots.
    #[tokio::test(start_paused = true)]
    async fn a_silent_connection_is_closed_at_the_deadline() {
        let (endpoint, _decisions, _metrics) = endpoint();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, shutdown) = watch::channel(false);
        let server = tokio::spawn(serve(listener, endpoint, shutdown));
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(b"GET /readyz HTTP/1.1\r\n").await.unwrap();
        let mut response = Vec::new();
        let read = tokio::time::timeout(
            CONNECTION_DEADLINE + Duration::from_secs(1),
            stream.read_to_end(&mut response),
        )
        .await;
        assert!(read.is_ok(), "the connection outlived its deadline");
        assert!(!String::from_utf8_lossy(&response).contains("200"));
        stop.send_replace(true);
        server.await.unwrap().unwrap();
    }
}
