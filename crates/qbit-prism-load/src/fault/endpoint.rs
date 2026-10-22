//! A TCP relay the harness can fence and move (#554): the stable writer
//! endpoint in front of the PostgreSQL primary, and the standby's
//! replication link.
//!
//! A run that injects a database failover gives its frontends (through the
//! delay proxy) and its own side pool the writer endpoint's address instead
//! of the primary's, as an operator gives them a virtual IP or a DNS name:
//! after a promotion the endpoint is moved to the new primary and every
//! client reconnects to the same address, with no restart and no new pool.
//! The standby streams through a second endpoint, which a fault fences to
//! cut replication while the primary keeps committing.
//!
//! [`Endpoint::fence`] cuts every live connection and refuses new ones (each
//! is accepted and closed at once), as a fenced writer address or a
//! partitioned link does. [`Endpoint::route_to`] points the endpoint at
//! another port on the loopback and lifts the fence; connections still open
//! to the old upstream are cut, since that server is gone or fenced.

use anyhow::{Context, Result};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
};

struct Shared {
    upstream_port: AtomicU16,
    fenced: AtomicBool,
    /// Bumped to cut every live connection.
    generation: watch::Sender<u64>,
    live: AtomicUsize,
    refused: AtomicUsize,
}

pub struct Endpoint {
    pub local: SocketAddr,
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.task.abort();
        self.cut();
    }
}

impl Endpoint {
    /// Listen on a free loopback port, forwarding to `127.0.0.1:upstream_port`.
    pub async fn open(upstream_port: u16) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding a fault endpoint")?;
        let local = listener.local_addr()?;
        let (generation, _) = watch::channel(0);
        let shared = Arc::new(Shared {
            upstream_port: AtomicU16::new(upstream_port),
            fenced: AtomicBool::new(false),
            generation,
            live: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
        });
        let task = {
            let shared = shared.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((client, _)) = listener.accept().await else {
                        break;
                    };
                    if shared.fenced.load(Ordering::SeqCst) {
                        shared.refused.fetch_add(1, Ordering::Relaxed);
                        drop(client);
                        continue;
                    }
                    let shared = shared.clone();
                    tokio::spawn(async move {
                        let mut cut = shared.generation.subscribe();
                        // A fence between the accept and the subscription
                        // above would otherwise let this one through.
                        if shared.fenced.load(Ordering::SeqCst) {
                            shared.refused.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        let port = shared.upstream_port.load(Ordering::SeqCst);
                        let Ok(mut server) = TcpStream::connect(("127.0.0.1", port)).await else {
                            return;
                        };
                        let mut client = client;
                        let _ = client.set_nodelay(true);
                        let _ = server.set_nodelay(true);
                        shared.live.fetch_add(1, Ordering::Relaxed);
                        tokio::select! {
                            _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                            _ = cut.changed() => {}
                        }
                        shared.live.fetch_sub(1, Ordering::Relaxed);
                    });
                }
            })
        };
        Ok(Self {
            local,
            shared,
            task,
        })
    }

    pub fn port(&self) -> u16 {
        self.local.port()
    }

    /// The port connections are forwarded to now.
    pub fn upstream_port(&self) -> u16 {
        self.shared.upstream_port.load(Ordering::SeqCst)
    }

    fn cut(&self) {
        self.shared
            .generation
            .send_modify(|generation| *generation += 1);
    }

    /// Cut every live connection and refuse new ones until the next
    /// [`route_to`](Self::route_to).
    pub fn fence(&self) {
        self.shared.fenced.store(true, Ordering::SeqCst);
        self.cut();
    }

    /// Forward new connections to `127.0.0.1:port` and lift any fence.
    pub fn route_to(&self, port: u16) {
        self.shared.upstream_port.store(port, Ordering::SeqCst);
        self.cut();
        self.shared.fenced.store(false, Ordering::SeqCst);
    }

    pub fn is_fenced(&self) -> bool {
        self.shared.fenced.load(Ordering::SeqCst)
    }

    /// Connections the fence refused so far.
    pub fn refused(&self) -> usize {
        self.shared.refused.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn echo() -> (u16, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buffer = [0u8; 64];
                    while let Ok(read) = socket.read(&mut buffer).await {
                        if read == 0 || socket.write_all(&buffer[..read]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        (port, task)
    }

    async fn round_trip(port: u16) -> bool {
        let Ok(mut socket) = TcpStream::connect(("127.0.0.1", port)).await else {
            return false;
        };
        if socket.write_all(b"ping").await.is_err() {
            return false;
        }
        let mut buffer = [0u8; 4];
        matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                socket.read_exact(&mut buffer)
            )
            .await,
            Ok(Ok(_))
        ) && &buffer == b"ping"
    }

    #[tokio::test]
    async fn a_fence_cuts_and_refuses_until_the_endpoint_is_moved() {
        let (first, _a) = echo().await;
        let (second, _b) = echo().await;
        let endpoint = Endpoint::open(first).await.unwrap();
        assert!(round_trip(endpoint.port()).await);
        // A live connection is cut by the fence.
        let mut live = TcpStream::connect(("127.0.0.1", endpoint.port()))
            .await
            .unwrap();
        live.write_all(b"ping").await.unwrap();
        let mut buffer = [0u8; 4];
        live.read_exact(&mut buffer).await.unwrap();
        endpoint.fence();
        let mut rest = Vec::new();
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            live.read_to_end(&mut rest),
        )
        .await;
        assert!(matches!(read, Ok(Ok(0)) | Ok(Err(_))), "{read:?}");
        assert!(
            !round_trip(endpoint.port()).await,
            "a fenced endpoint refuses"
        );
        assert!(endpoint.refused() >= 1);
        endpoint.route_to(second);
        assert_eq!(endpoint.upstream_port(), second);
        assert!(!endpoint.is_fenced());
        assert!(round_trip(endpoint.port()).await);
    }
}
