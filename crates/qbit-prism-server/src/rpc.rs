use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, PoisonError,
    },
    task::Poll,
    time::Duration,
};

/// The node answered a call with a JSON-RPC error object: the request reached
/// the node and the node refused it, as distinct from a transport or protocol
/// failure. The message is the one every caller always saw; what this adds is
/// a type a caller can downcast to when "no such block" must be told apart
/// from "the node is unreachable", as the operator recovery command must.
#[derive(Clone, Debug, thiserror::Error)]
#[error("qbit RPC {method}: {error}")]
pub struct RpcReplyError {
    pub method: String,
    pub error: Value,
}

/// The JSON-RPC error code qbitd answers every call with while it is still
/// warming up after a start (`RPC_IN_WARMUP`).
pub const RPC_IN_WARMUP: i64 = -28;

impl RpcReplyError {
    /// The node's JSON-RPC error code, when the error object carries an
    /// integer one.
    pub fn code(&self) -> Option<i64> {
        self.error["code"].as_i64()
    }

    /// The node answered that it is still warming up. qbitd (like the Bitcoin
    /// Core it derives from) checks its warmup flag first in
    /// `CRPCTable::execute`, before it looks the method up, and nothing else
    /// raises this code; the flag is cleared once, at the end of startup. So
    /// this reply proves that the node did not run the call (#526). That
    /// holds for the node the call was sent to: the RPC URL names one node
    /// (a path-routing reverse proxy is fine). An endpoint that retries a
    /// failed POST on another backend is not supported; it could double-submit
    /// a block on its own, with or without this rule.
    pub fn in_warmup(&self) -> bool {
        self.code() == Some(RPC_IN_WARMUP)
    }
}

/// A call that provably never reached the node: the client failed to
/// establish the connection the request would have been written to (the
/// connection was refused, the name did not resolve, no route, the connect
/// timeout, a relaying call's connect deadline (#762), or a TLS or proxy
/// handshake failure), so not one byte of the request left this process for
/// the node (#522).
///
/// The boundary is reqwest's `is_connect()`, which holds exactly when hyper's
/// client reports its `Connect` error kind. hyper raises that kind only while
/// obtaining a connection, from the connector or the pool checkout, and a
/// request is handed to a connection only after one was obtained. hyper
/// retries on another connection only a request its dispatcher returned
/// unwritten, so a connect error after such a retry still proves that no
/// attempt wrote the request. A failure on an established connection, a reset
/// or a closed connection after the write, a lost reply, the request timeout
/// and every HTTP or JSON-RPC error are not this type: the request may have
/// reached the node. That includes a request timeout that expires while
/// connecting, which only a call without a connect deadline can meet: the
/// timeout cannot tell a connect from a request written to a pooled
/// connection. The message keeps the transport-failure wording every caller
/// already matched on.
#[derive(Clone, Debug, thiserror::Error)]
#[error("qbit RPC {method} transport failed before the request was sent: the connection could not be established ({cause})")]
pub struct RpcNotSentError {
    pub method: String,
    /// The operating system's reason, when the failure carries one (such as
    /// "connection refused"); for a connect cut at its call's connect
    /// deadline, that budget and the call's deadline (#762); else "connect
    /// failed". Never the URL.
    pub cause: String,
}

impl RpcNotSentError {
    fn from_connect(method: &str, error: &reqwest::Error) -> Self {
        let mut source = std::error::Error::source(error);
        let mut cause = None;
        while let Some(error) = source {
            if let Some(cut) = error.downcast_ref::<ConnectCut>() {
                cause = Some(cut.to_string());
                break;
            }
            if let Some(io) = error.downcast_ref::<std::io::Error>() {
                cause = Some(io.kind().to_string());
            }
            source = error.source();
        }
        Self {
            method: method.to_owned(),
            cause: cause.unwrap_or_else(|| "connect failed".to_owned()),
        }
    }
}

/// The calls that hand the node a block or a transaction to relay to the
/// network. PRISM makes the first three; the wallet's own sends are listed
/// so that no later caller can relay through one either (#291).
pub const RELAY_METHODS: &[&str] = &[
    "submitblock",
    "sendrawtransaction",
    "submitpackage",
    "sendtoaddress",
    "sendmany",
    "send",
    "sendall",
    "bumpfee",
];

/// A [`RELAY_METHODS`] call that a client built [`Rpc::without_relay`]
/// refused (#291). As with [`RpcNotSentError`], no byte of the request left
/// this process.
#[derive(Clone, Debug, thiserror::Error)]
#[error("qbit RPC {method} refused before it was sent: block submission is disabled by PRISM_BLOCK_SUBMIT_ENABLED")]
pub struct RpcRelayRefused {
    pub method: String,
}

/// qbitd's default `-rpcservertimeout`: how long the node keeps an idle,
/// kept-alive connection open before it closes it.
pub const QBITD_DEFAULT_RPCSERVERTIMEOUT: Duration = Duration::from_secs(30);

/// How long the client keeps an idle connection to the node for reuse (#759).
/// It stays below the node's own idle close, [`QBITD_DEFAULT_RPCSERVERTIMEOUT`]
/// unless the node sets another. A request written into a connection just as
/// the node closes it is never read, and fails after the connection existed.
/// With the bound below the node's, the client always drops an idle
/// connection first.
///
/// [`RELAY_METHODS`] calls share the bounded pool. A new connection for each
/// would put a connect inside every `submitblock`'s short deadline, and a
/// connect that outlasts its share of it returns the block unsent (#762).
pub const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
const _: () = assert!(POOL_IDLE_TIMEOUT.as_secs() < QBITD_DEFAULT_RPCSERVERTIMEOUT.as_secs());

/// How long [`Rpc`]'s client gives a new connection before its connect fails.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A relaying call's connect budget ends at least this long before
/// [`CONNECT_TIMEOUT`] would. So a call whose connect outlasts its budget
/// always reports the cut, with its reason, never the plain connect timeout,
/// whatever its deadline (#762).
const CONNECT_BUDGET_MARGIN: Duration = Duration::from_millis(100);

/// A [`RELAY_METHODS`] call with its own deadline may spend the first
/// `1 / RELAY_CONNECT_DIVISOR` of it, the first half, obtaining its
/// connection (#762); see [`relay_connect_budget`].
pub const RELAY_CONNECT_DIVISOR: u32 = 2;

/// How long a [`RELAY_METHODS`] call with its own `deadline` may spend
/// obtaining a connection before it fails as [`RpcNotSentError`] (#762): the
/// first `1 / RELAY_CONNECT_DIVISOR` of it, and never longer than 4.9 s, just
/// under the client's 5 s connect timeout. The rest of the deadline is the
/// request's.
pub fn relay_connect_budget(deadline: Duration) -> Duration {
    (deadline / RELAY_CONNECT_DIVISOR).min(CONNECT_TIMEOUT.saturating_sub(CONNECT_BUDGET_MARGIN))
}

/// The connect deadline of the relaying call a task is sending (#762).
#[derive(Clone, Copy)]
struct CallConnectDeadline {
    at: tokio::time::Instant,
    /// The call's whole deadline, named by the reason of a cut connect.
    deadline: Duration,
}

tokio::task_local! {
    /// Read by [`ConnectBound`].
    static CONNECT_DEADLINE: CallConnectDeadline;
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The reason of a connect [`ConnectBound`] stopped waiting for at its call's
/// connect deadline (#762). [`RpcNotSentError`] carries it, so that the
/// offer's `last_error` tells it apart from every other connect failure.
#[derive(Debug, thiserror::Error)]
#[error("no connection within {budget:?}, its share of the call's {deadline:?} deadline")]
struct ConnectCut {
    budget: Duration,
    deadline: Duration,
}

/// [`Rpc`]'s connector layer (#762). A relaying call with its own deadline
/// waits for a new connection only until its [`CONNECT_DEADLINE`]; it then
/// fails as a connect error. hyper reports a failed connect as its `Connect`
/// error kind only to a request that has not been handed a connection, so the
/// call fails as [`RpcNotSentError`]. A request handed a pooled connection
/// meanwhile goes on, and its outcome is that request's.
///
/// The connect itself is not abandoned. It runs in its own task, bounded by
/// the client's [`CONNECT_TIMEOUT`]. One established after its call stopped
/// waiting, whether the call was cut or hyper moved the connect to the
/// background when the request took a pooled connection, is kept as the
/// spare: the next connect the client starts takes it at once, if it is
/// younger than the client's idle bound, which stays under the node's idle
/// close, and the kernel says the node has neither closed it nor sent on it
/// ([`KernelSocket::idle_and_open`]). So the offer that follows a cut, or any
/// call before it, finds the connection ready. Every connect of an [`Rpc`]
/// client is to its one node (a wallet client changes only the path), so the
/// spare serves any of them. Every other connect runs as before, unless it can
/// take the spare.
///
/// The node can still close the spare between that check and the request's
/// write, as it can a pooled connection. A request written into the close
/// fails after the write, and its outcome is unknown: hyper-util knows that
/// such a request on a new connection came back unwritten, but it does not
/// say so (its error kind for it, `Canceled`, is private), so the call cannot
/// prove it unsent.
struct ConnectBoundLayer<Target> {
    idle_timeout: Duration,
    target: std::marker::PhantomData<fn(Target)>,
}

impl<Target> ConnectBoundLayer<Target> {
    fn new(idle_timeout: Duration) -> Self {
        Self {
            idle_timeout,
            target: std::marker::PhantomData,
        }
    }
}

impl<Target> Clone for ConnectBoundLayer<Target> {
    fn clone(&self) -> Self {
        Self::new(self.idle_timeout)
    }
}

impl<S, Target> tower::Layer<S> for ConnectBoundLayer<Target>
where
    S: tower::Service<Target>,
{
    type Service = ConnectBound<S, S::Response>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectBound {
            inner,
            idle_timeout: self.idle_timeout,
            spare: Arc::new(Mutex::new(None)),
        }
    }
}

/// The connection a connect established after its call stopped waiting.
struct Spare<C> {
    conn: C,
    socket: KernelSocket,
    parked_at: tokio::time::Instant,
}

struct ConnectBound<S, C> {
    inner: S,
    idle_timeout: Duration,
    /// Shared by every clone: hyper clones the connector for each connect.
    spare: Arc<Mutex<Option<Spare<C>>>>,
}

impl<S: Clone, C> Clone for ConnectBound<S, C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            idle_timeout: self.idle_timeout,
            spare: self.spare.clone(),
        }
    }
}

impl<S, C> ConnectBound<S, C>
where
    C: hyper_util::client::legacy::connect::Connection + Send + 'static,
{
    fn take_spare(&self) -> Option<C> {
        let spare = self
            .spare
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()?;
        (spare.parked_at.elapsed() < self.idle_timeout && spare.socket.idle_and_open())
            .then_some(spare.conn)
    }

    /// Keep `conn` as the spare, in place of an older one. A connection whose
    /// socket the kernel cannot be asked about is not kept.
    fn park(spare: &Mutex<Option<Spare<C>>>, conn: C) {
        let mut extras = hyper::http::Extensions::new();
        conn.connected().get_extras(&mut extras);
        let Some(socket) = extras
            .get::<hyper_util::client::legacy::connect::HttpInfo>()
            .and_then(|info| KernelSocket::find(info.local_addr(), info.remote_addr()))
        else {
            return;
        };
        let older = spare
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(Spare {
                conn,
                socket,
                parked_at: tokio::time::Instant::now(),
            });
        drop(older);
    }
}

/// This process's TCP socket under a spare, below any TLS layer, which the
/// kernel answers for (#762).
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
struct KernelSocket {
    fd: std::os::fd::RawFd,
    local: std::net::SocketAddr,
    remote: std::net::SocketAddr,
}

#[cfg(target_os = "linux")]
impl KernelSocket {
    /// The socket connected from `local` to `remote`, if this process holds
    /// it: the two addresses name one connected TCP socket.
    fn find(local: std::net::SocketAddr, remote: std::net::SocketAddr) -> Option<Self> {
        std::fs::read_dir("/proc/self/fd")
            .ok()?
            .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
            .map(|fd| Self { fd, local, remote })
            .find(Self::holds_it)
    }

    /// Whether the descriptor still holds this socket.
    fn holds_it(&self) -> bool {
        socket_type(self.fd) == Some(libc::SOCK_STREAM)
            && socket_address(self.fd, libc::getsockname) == Some(self.local)
            && socket_address(self.fd, libc::getpeername) == Some(self.remote)
    }

    /// Whether the connection is open with nothing to read: a non-blocking
    /// peek finds no byte yet. The kernel answers it, whatever the runtime has
    /// seen of the socket so far. The node's close (a read of zero bytes), a
    /// byte it sent unasked, an error, or a descriptor that no longer holds
    /// this socket all mean that the connection must carry no request: one
    /// written to a connection the node closed fails after the write, with
    /// its outcome unknown.
    fn idle_and_open(&self) -> bool {
        if !self.holds_it() {
            return false;
        }
        let mut byte = 0u8;
        // SAFETY: `byte` is valid for the one byte asked for.
        let read = unsafe {
            libc::recv(
                self.fd,
                (&mut byte as *mut u8).cast(),
                1,
                libc::MSG_PEEK | libc::MSG_DONTWAIT,
            )
        };
        read < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::WouldBlock
    }
}

#[cfg(target_os = "linux")]
fn socket_type(fd: std::os::fd::RawFd) -> Option<libc::c_int> {
    let mut kind: libc::c_int = 0;
    let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `kind` is valid for `length` bytes.
    let read = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut kind as *mut libc::c_int).cast(),
            &mut length,
        )
    };
    (read == 0).then_some(kind)
}

#[cfg(target_os = "linux")]
type SocketAddressCall =
    unsafe extern "C" fn(libc::c_int, *mut libc::sockaddr, *mut libc::socklen_t) -> libc::c_int;

/// `fd`'s own (`getsockname`) or peer (`getpeername`) address.
#[cfg(target_os = "linux")]
fn socket_address(
    fd: std::os::fd::RawFd,
    address_of: SocketAddressCall,
) -> Option<std::net::SocketAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
    // SAFETY: all zeroes is a valid `sockaddr_storage`.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: `storage` is valid for `length` bytes, room for any address.
    let read = unsafe {
        address_of(
            fd,
            (&mut storage as *mut libc::sockaddr_storage).cast(),
            &mut length,
        )
    };
    if read != 0 {
        return None;
    }
    match libc::c_int::from(storage.ss_family) {
        libc::AF_INET => {
            // SAFETY: the family says that `storage` holds a `sockaddr_in`.
            let v4 = unsafe {
                &*(&storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in>()
            };
            Some(SocketAddr::from((
                Ipv4Addr::from(u32::from_be(v4.sin_addr.s_addr)),
                u16::from_be(v4.sin_port),
            )))
        }
        libc::AF_INET6 => {
            // SAFETY: the family says that `storage` holds a `sockaddr_in6`.
            let v6 = unsafe {
                &*(&storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>()
            };
            Some(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(v6.sin6_addr.s6_addr),
                u16::from_be(v6.sin6_port),
                v6.sin6_flowinfo,
                v6.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

/// Elsewhere the kernel is not asked, and no connection is kept as a spare.
#[cfg(not(target_os = "linux"))]
#[derive(Clone, Copy)]
struct KernelSocket;

#[cfg(not(target_os = "linux"))]
impl KernelSocket {
    fn find(_local: std::net::SocketAddr, _remote: std::net::SocketAddr) -> Option<Self> {
        None
    }

    fn idle_and_open(&self) -> bool {
        false
    }
}

impl<S, Target, C> tower::Service<Target> for ConnectBound<S, C>
where
    S: tower::Service<Target, Response = C, Error = BoxError>,
    S::Future: Send + 'static,
    C: hyper_util::client::legacy::connect::Connection + Send + 'static,
{
    type Response = C;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<C, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), BoxError>> {
        self.inner.poll_ready(cx)
    }

    /// hyper starts a connect from the task of the request that needs it, so
    /// the deadline read here is that request's call's.
    fn call(&mut self, target: Target) -> Self::Future {
        if let Some(conn) = self.take_spare() {
            return Box::pin(std::future::ready(Ok(conn)));
        }
        let connecting = self.inner.call(target);
        let Ok(bound) = CONNECT_DEADLINE.try_with(|bound| *bound) else {
            return Box::pin(connecting);
        };
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        let spare = self.spare.clone();
        tokio::spawn({
            let spare = spare.clone();
            async move {
                // The budget ends first (`CONNECT_BUDGET_MARGIN`): this bound
                // only ends a connect nothing waits for any more.
                let connected = tokio::time::timeout(CONNECT_TIMEOUT, connecting)
                    .await
                    .unwrap_or_else(|_| Err(tower::timeout::error::Elapsed::new().into()));
                if let Err(Ok(conn)) = sender.send(connected) {
                    Self::park(&spare, conn);
                }
            }
        });
        Box::pin(async move {
            match tokio::time::timeout_at(bound.at, &mut receiver).await {
                Ok(Ok(connected)) => connected,
                Ok(Err(_)) => Err("the connect task ended without a result".into()),
                Err(_) => {
                    // A connect that finished at the deadline is kept too.
                    receiver.close();
                    if let Ok(Ok(conn)) = receiver.try_recv() {
                        Self::park(&spare, conn);
                    }
                    Err(ConnectCut {
                        budget: relay_connect_budget(bound.deadline),
                        deadline: bound.deadline,
                    }
                    .into())
                }
            }
        })
    }
}

/// An HTTP client builder for a node's RPC endpoint, keeping idle connections
/// for at most [`POOL_IDLE_TIMEOUT`]. Every node client starts from it:
/// [`Rpc`]'s, the public API's and the load harness's.
pub fn node_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder().pool_idle_timeout(POOL_IDLE_TIMEOUT)
}

/// Reusable, deadline-bound HTTP connections. Mutating calls are never retried
/// blindly: callers reconcile their durable outbox against chain state first.
#[derive(Clone)]
pub struct Rpc {
    client: reqwest::Client,
    url: String,
    user: String,
    password: String,
    next_id: Arc<AtomicU64>,
    /// Whether [`RELAY_METHODS`] calls are sent; inherited by wallet clients.
    relay: bool,
}

impl Rpc {
    pub fn wallet(&self, name: &str) -> Result<Self> {
        let mut rpc = self.clone();
        let mut url = url::Url::parse(&rpc.url)?;
        // A reverse proxy may route this node below a path prefix. Retain
        // that prefix and replace only an existing terminal wallet selection.
        let segments: Vec<_> = url
            .path_segments()
            .context("invalid wallet RPC URL")?
            .collect();
        let trimmed = segments.strip_suffix(&[""]).unwrap_or(&segments);
        let replace_wallet = trimmed.len() >= 2 && trimmed[trimmed.len() - 2] == "wallet";
        let empty_wallet = segments.len() >= 2
            && segments[segments.len() - 2] == "wallet"
            && segments.last() == Some(&"");
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("invalid wallet RPC URL"))?;
            path.pop_if_empty();
            if replace_wallet {
                path.pop().pop();
            } else if empty_wallet {
                path.pop();
            }
            path.push("wallet").push(name);
        }
        rpc.url = url.into();
        Ok(rpc)
    }
    pub fn new(url: String, user: String, password: String, timeout: Duration) -> Result<Self> {
        Self::with_pool_idle_timeout(url, user, password, timeout, POOL_IDLE_TIMEOUT)
    }

    /// [`Rpc::new`] with another bound on how long an idle pooled connection
    /// is kept: for a node whose idle close is shorter than qbitd's default,
    /// and for tests. Keep it below the node's idle close (#759).
    pub fn with_pool_idle_timeout(
        url: String,
        user: String,
        password: String,
        timeout: Duration,
        pool_idle_timeout: Duration,
    ) -> Result<Self> {
        let parsed = url::Url::parse(&url).context("invalid QBIT RPC URL")?;
        anyhow::ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "QBIT RPC URL must use http(s)"
        );
        let client = node_client_builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(timeout)
            .pool_idle_timeout(pool_idle_timeout)
            .connector_layer(ConnectBoundLayer::new(pool_idle_timeout))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            url,
            user,
            password,
            next_id: Arc::new(AtomicU64::new(1)),
            relay: true,
        })
    }

    /// This client, and every wallet client made from it, refusing each
    /// [`RELAY_METHODS`] call before it is sent: a frontend with
    /// `PRISM_BLOCK_SUBMIT_ENABLED` off talks to its node through one (#291).
    pub fn without_relay(mut self) -> Self {
        self.relay = false;
        self
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.call_timeout(method, params, None).await
    }

    /// [`Rpc::call`] with its own deadline, `timeout`, in place of the
    /// client's. The deadline bounds the whole call, its connect included.
    /// A [`RELAY_METHODS`] call spends at most [`relay_connect_budget`] of it
    /// obtaining a connection (#762): still connecting then, it fails as
    /// [`RpcNotSentError`], whereas the deadline itself cannot tell a connect
    /// from a request already written, and leaves the outcome unknown. A
    /// relaying call without its own deadline keeps the client's.
    pub async fn call_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Option<Duration>,
    ) -> Result<Value> {
        if !self.relay && RELAY_METHODS.contains(&method) {
            return Err(RpcRelayRefused {
                method: method.to_owned(),
            }
            .into());
        }
        let connect_deadline =
            timeout
                .filter(|_| RELAY_METHODS.contains(&method))
                .map(|deadline| CallConnectDeadline {
                    at: tokio::time::Instant::now() + relay_connect_budget(deadline),
                    deadline,
                });
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut request = self
            .client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.password))
            .json(&json!({"jsonrpc":"1.0","id":id,"method":method,"params":params}));
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        // Sent from inside the scope, so that every connect it starts reads
        // the deadline.
        let send = async move { request.send().await };
        let sent = match connect_deadline {
            Some(deadline) => CONNECT_DEADLINE.scope(deadline, send).await,
            None => send.await,
        };
        // Do not include the request URL or credentials in diagnostics.
        let response = sent.map_err(|error| {
            if error.is_connect() {
                RpcNotSentError::from_connect(method, &error).into()
            } else {
                anyhow::anyhow!("qbit RPC {method} transport failed")
            }
        })?;
        let status = response.status();
        let value: Value = response.json().await.map_err(|_| {
            anyhow::anyhow!("qbit RPC {method} returned invalid JSON (HTTP {status})")
        })?;
        if !value["error"].is_null() {
            return Err(RpcReplyError {
                method: method.to_owned(),
                error: value["error"].clone(),
            }
            .into());
        }
        anyhow::ensure!(
            status.is_success(),
            "qbit RPC {method} failed with HTTP {status}"
        );
        anyhow::ensure!(value["id"] == id, "qbit RPC {method} response ID mismatch");
        value
            .get("result")
            .cloned()
            .context("qbit RPC missing result")
    }
}
