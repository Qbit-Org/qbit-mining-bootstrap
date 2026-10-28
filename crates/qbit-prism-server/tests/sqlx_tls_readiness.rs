//! sqlx-core's TLS socket wakes for whichever direction can make progress
//! (#771): the launchbadge/sqlx#4251 backport in vendor/sqlx-core.
//!
//! When rustls's `complete_io` finds both its write and its read blocked,
//! sqlx-core 0.8.6 waited for write readiness and then for read readiness, so
//! it could park on one direction while only the other could ever become
//! ready. These tests run sqlx-core's rustls client against a rustls server
//! over an in-memory socket whose readiness they control, and poll by hand so
//! each interleaving is deterministic. A client left pending with no wakeup
//! that can still arrive is a stall; the unpatched crate stalls in both.
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ServerConfig, ServerConnection};
use sqlx_core::bytes::BufMut;
use sqlx_core::io::ReadBuf;
use sqlx_core::net::tls::{self, TlsConfig};
use sqlx_core::net::{Socket, SocketIntoBox};
use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, Read, Write};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

/// The client's send buffer: far smaller than a request, so every request
/// blocks the client's writes many times over.
const WIRE_CAPACITY: usize = 4096;
const REQUEST_LEN: usize = 256 * 1024;
const RESPONSE: &[u8] = b"request received";
/// Bounds the client's socket calls in a test (a passing run makes under
/// 500), so a client that spins inside one poll fails instead of hanging.
const MAX_SOCKET_CALLS: usize = 100_000;

#[derive(Default)]
struct Wire {
    /// Bytes the client wrote that the server has not taken yet.
    to_server: Vec<u8>,
    /// Bytes the server took off the wire and has not processed yet.
    server_inbox: Vec<u8>,
    to_client: VecDeque<u8>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    /// Frees the client's send buffer when the client tries a read after its
    /// write blocked, the way a peer draining the socket between the two
    /// does. Like a readiness event whose waker was already spent, it wakes
    /// nobody, so only a client that polls write readiness again sees it.
    drain_on_blocked_read: bool,
    drains_on_blocked_read: usize,
    socket_calls: usize,
}

impl Wire {
    fn count_call(&mut self) {
        self.socket_calls += 1;
        assert!(
            self.socket_calls < MAX_SOCKET_CALLS,
            "the client is spinning on its socket"
        );
    }

    /// Moves the client's unsent bytes to the server, waking a client that
    /// waits to write.
    fn take_to_server(&mut self) -> bool {
        if self.to_server.is_empty() {
            return false;
        }
        let taken = std::mem::take(&mut self.to_server);
        self.server_inbox.extend(taken);
        if let Some(waker) = self.write_waker.take() {
            waker.wake();
        }
        true
    }
}

struct ClientSocket(Arc<Mutex<Wire>>);

impl Socket for ClientSocket {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        let mut wire = self.0.lock().unwrap();
        wire.count_call();
        if wire.to_client.is_empty() {
            if wire.drain_on_blocked_read && wire.to_server.len() == WIRE_CAPACITY {
                assert!(wire.write_waker.is_none(), "the drain must wake nobody");
                wire.drains_on_blocked_read += 1;
                wire.take_to_server();
            }
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let len = buf.remaining_mut().min(wire.to_client.len());
        let bytes: Vec<u8> = wire.to_client.drain(..len).collect();
        buf.put_slice(&bytes);
        Ok(len)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut wire = self.0.lock().unwrap();
        wire.count_call();
        let len = buf.len().min(WIRE_CAPACITY - wire.to_server.len());
        if len == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        wire.to_server.extend_from_slice(&buf[..len]);
        Ok(len)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut wire = self.0.lock().unwrap();
        wire.count_call();
        if wire.to_client.is_empty() {
            wire.read_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut wire = self.0.lock().unwrap();
        wire.count_call();
        if wire.to_server.len() == WIRE_CAPACITY {
            wire.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A server that answers once it has the whole request. Like a peer blocked
/// in its own write, it takes nothing off the wire while the client has
/// unread bytes.
struct Server {
    wire: Arc<Mutex<Wire>>,
    tls: ServerConnection,
    request: Vec<u8>,
    /// Sent once the client's request has filled the wire.
    interjection: Option<&'static [u8]>,
    responded: bool,
}

impl Server {
    /// Does everything the server can before it needs the client again.
    fn step(&mut self) {
        let mut wire = self.wire.lock().unwrap();
        if wire.to_server.len() == WIRE_CAPACITY && !self.tls.is_handshaking() {
            if let Some(bytes) = self.interjection.take() {
                self.tls.writer().write_all(bytes).unwrap();
                send(&mut self.tls, &mut wire);
            }
        }
        if wire.to_client.is_empty() {
            wire.take_to_server();
        }
        let inbox = std::mem::take(&mut wire.server_inbox);
        let mut inbox = inbox.as_slice();
        while !inbox.is_empty() {
            self.tls.read_tls(&mut inbox).unwrap();
            self.tls.process_new_packets().unwrap();
            match self.tls.reader().read_to_end(&mut self.request) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                other => panic!("the client closed the session: {other:?}"),
            }
        }
        if !self.responded && self.request.len() == REQUEST_LEN {
            self.tls.writer().write_all(RESPONSE).unwrap();
            self.responded = true;
        }
        send(&mut self.tls, &mut wire);
    }

    fn stall(&self) -> String {
        let wire = self.wire.lock().unwrap();
        format!(
            "the client is parked with no wakeup to come: {} bytes unsent, {} unread, \
             read waker {}, write waker {}, {} of {REQUEST_LEN} request bytes received",
            wire.to_server.len(),
            wire.to_client.len(),
            wire.read_waker.is_some(),
            wire.write_waker.is_some(),
            self.request.len(),
        )
    }
}

/// Puts the server's pending records on the wire, waking a client that
/// waits to read.
fn send(tls: &mut ServerConnection, wire: &mut Wire) {
    let mut sent = Vec::new();
    while tls.wants_write() {
        tls.write_tls(&mut sent).unwrap();
    }
    if !sent.is_empty() {
        wire.to_client.extend(sent);
        if let Some(waker) = wire.read_waker.take() {
            waker.wake();
        }
    }
}

#[derive(Default)]
struct Woken(AtomicBool);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Polls `future` to completion, running the server whenever the client waits
/// on it, or reports the stall when nothing can wake the client again.
fn drive<F: Future>(future: F, server: &mut Server) -> Result<F::Output, String> {
    let woken = Arc::new(Woken::default());
    let waker = Waker::from(woken.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return Ok(output);
        }
        if woken.0.swap(false, Ordering::SeqCst) {
            continue;
        }
        server.step();
        if !woken.0.swap(false, Ordering::SeqCst) {
            return Err(server.stall());
        }
    }
}

fn der(tag: u8, parts: &[&[u8]]) -> Vec<u8> {
    let len: usize = parts.iter().map(|part| part.len()).sum();
    let [high, low] = u16::try_from(len).unwrap().to_be_bytes();
    let mut out = vec![tag];
    match (high, low) {
        (0, short) if short < 0x80 => out.push(short),
        (0, low) => out.extend([0x81, low]),
        (high, low) => out.extend([0x82, high, low]),
    }
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

/// A self-signed Ed25519 certificate for `spki`. Its own signature is filler:
/// the client accepts any certificate, as `sslmode=require` does, and checks
/// only the handshake signature made with the key.
fn certificate(spki: &[u8]) -> Vec<u8> {
    let ed25519 = der(0x30, &[&der(0x06, &[&[0x2b, 0x65, 0x70]])]);
    let common_name = der(0x06, &[&[0x55, 0x04, 0x03]]);
    let name = der(
        0x30,
        &[&der(
            0x31,
            &[&der(0x30, &[&common_name, &der(0x0c, &[b"localhost"])])],
        )],
    );
    let validity = der(
        0x30,
        &[
            &der(0x17, &[b"000101000000Z"]),
            &der(0x17, &[b"491231235959Z"]),
        ],
    );
    let tbs = der(
        0x30,
        &[
            &der(0xa0, &[&der(0x02, &[&[2]])]),
            &der(0x02, &[&[1]]),
            &ed25519,
            &name,
            &validity,
            &name,
            spki,
        ],
    );
    der(0x30, &[&tbs, &ed25519, &der(0x03, &[&[0], &[0; 64]])])
}

fn server_config() -> Arc<ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    // PKCS#8 v1 for a fixed Ed25519 seed: a test key that guards nothing.
    let pkcs8 = [
        &[
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70,
        ][..],
        &[0x04, 0x22, 0x04, 0x20],
        &[7; 32],
    ]
    .concat();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8));
    let signing_key = provider
        .key_provider
        .load_private_key(key.clone_key())
        .unwrap();
    let spki = signing_key.public_key().unwrap();
    let cert = CertificateDer::from(certificate(spki.as_ref()));
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
    // No session tickets: the server sends nothing the tests do not choose.
    config.send_tls13_tickets = 0;
    Arc::new(config)
}

fn connect(interjection: Option<&'static [u8]>) -> (Server, Box<dyn Socket>) {
    let wire = Arc::new(Mutex::new(Wire::default()));
    let mut server = Server {
        wire: wire.clone(),
        tls: ServerConnection::new(server_config()).unwrap(),
        request: Vec::new(),
        interjection,
        responded: false,
    };
    let config = TlsConfig {
        accept_invalid_certs: true,
        accept_invalid_hostnames: true,
        hostname: "localhost",
        root_cert_path: None,
        client_cert_path: None,
        client_key_path: None,
    };
    let handshake = tls::handshake(ClientSocket(wire), config, SocketIntoBox);
    let socket = drive(handshake, &mut server)
        .expect("the handshake stalled")
        .expect("the handshake failed");
    (server, socket)
}

fn request() -> Vec<u8> {
    (0..REQUEST_LEN).map(|i| (i % 251) as u8).collect()
}

/// Sends the request, then reads `reply_len` bytes back.
async fn exchange(
    socket: &mut Box<dyn Socket>,
    request: &[u8],
    reply_len: usize,
) -> io::Result<Vec<u8>> {
    let mut unsent = request;
    while !unsent.is_empty() {
        let written = socket.write(unsent).await?;
        unsent = &unsent[written..];
    }
    socket.flush().await?;
    let mut reply = Vec::new();
    while reply.len() < reply_len {
        let mut chunk = [0; 1024];
        let mut buf = &mut chunk[..];
        let read = socket.read(&mut buf).await?;
        assert_ne!(read, 0, "the server closed the session");
        reply.extend_from_slice(&chunk[..read]);
    }
    Ok(reply)
}

/// Write readiness that arrives between a blocked write and the readiness poll
/// must send the client back to writing; sqlx-core 0.8.6 spent it and parked on
/// read readiness while the server waited for the rest of the request.
#[test]
fn write_readiness_after_a_blocked_write_resumes_the_write() {
    let (mut server, mut socket) = connect(None);
    server.wire.lock().unwrap().drain_on_blocked_read = true;
    let request = request();

    let reply = drive(exchange(&mut socket, &request, RESPONSE.len()), &mut server)
        .unwrap_or_else(|stall| panic!("{stall}"))
        .unwrap();

    assert!(server.wire.lock().unwrap().drains_on_blocked_read > 0);
    assert!(
        server.request == request,
        "the server got a different request"
    );
    assert_eq!(reply, RESPONSE);
}

/// Read readiness that arrives while the client waits to write must wake it:
/// sqlx-core 0.8.6 waited on write readiness alone, and the server would not
/// take more of the request until the client read what it had sent.
#[test]
fn read_readiness_while_the_write_is_blocked_wakes_the_client() {
    const INTERJECTION: &[u8] = b"read me before I read more";
    let (mut server, mut socket) = connect(Some(INTERJECTION));
    let request = request();

    let reply = drive(
        exchange(&mut socket, &request, INTERJECTION.len() + RESPONSE.len()),
        &mut server,
    )
    .unwrap_or_else(|stall| panic!("{stall}"))
    .unwrap();

    assert!(server.interjection.is_none());
    assert!(
        server.request == request,
        "the server got a different request"
    );
    assert_eq!(reply, [INTERJECTION, RESPONSE].concat());
}
