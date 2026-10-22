//! TCP listeners with an explicit accept-queue length.
//!
//! `tokio::net::TcpListener::bind` listens with mio's fixed backlog of 128,
//! the standard library's value. Miners that reconnect together (a cutover
//! that re-points every miner, a frontend restart, an LB failover) can
//! outrun the accept loop by more than that. The kernel then drops their
//! SYNs (`TcpExtListenOverflows`), and an LB that observes layer-4 errors
//! marks the frontend down: on the pair, 400 simultaneous connects through
//! HAProxy overflowed a 128 queue 175 times. Whatever is asked for, the
//! kernel caps the backlog at `net.core.somaxconn`.
use std::{io, net::SocketAddr};
use tokio::net::{lookup_host, TcpListener, TcpSocket, ToSocketAddrs};

/// The HTTP listeners' backlog (the audit and operator API, and the public
/// read service). Their clients are health checks, scrapes and API readers,
/// not miners reconnecting at once; 1024 was the 2.x.x runtime's listen
/// default.
pub const HTTP_LISTEN_BACKLOG: u32 = 1024;

/// The largest backlog `listen(2)` takes: its argument is a C `int`.
pub const MAX_LISTEN_BACKLOG: u32 = i32::MAX as u32;

/// The calling process's network namespace's `net.core.somaxconn`, the cap
/// the kernel puts on every listen backlog, or `None` where it can't be read.
pub fn somaxconn() -> Option<u32> {
    std::fs::read_to_string("/proc/sys/net/core/somaxconn")
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// [`TcpListener::bind`] with `backlog` in place of mio's 128. Every resolved
/// address is tried in order: the first that listens wins, otherwise the last
/// error is returned, as `TcpListener::bind` does. `SO_REUSEADDR` is set, as
/// mio sets it, and `IPV6_V6ONLY` keeps the system default, as mio keeps it.
pub async fn bind_listener(addr: impl ToSocketAddrs, backlog: u32) -> io::Result<TcpListener> {
    let mut last = None;
    for addr in lookup_host(addr).await? {
        match listen_on(addr, backlog) {
            Ok(listener) => return Ok(listener),
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve to any address",
        )
    }))
}

fn listen_on(addr: SocketAddr, backlog: u32) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(backlog)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::{net::TcpStream, task::JoinSet};

    /// Nothing accepts, so every completed handshake waits in the accept
    /// queue, and once that queue is full the kernel drops further SYNs: a
    /// client's connect completes only while the queue has room. With mio's
    /// 128 about 129 of these connects complete and the rest time out.
    #[tokio::test]
    async fn a_burst_beyond_mios_128_fits_the_configured_backlog() {
        const BACKLOG: u32 = 512;
        const CONNECTS: usize = 300;
        // Below the test's backlog the kernel's cap, not the helper, would set
        // the queue, and a pass would prove nothing: fail visibly instead.
        let cap = somaxconn().expect("net.core.somaxconn is readable on Linux");
        assert!(
            cap >= BACKLOG,
            "net.core.somaxconn is {cap}, below this test's backlog of {BACKLOG}; raise it to run the test"
        );
        let listener = bind_listener(("127.0.0.1", 0), BACKLOG).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut connects = JoinSet::new();
        for _ in 0..CONNECTS {
            connects.spawn(tokio::time::timeout(
                Duration::from_secs(2),
                TcpStream::connect(addr),
            ));
        }
        let mut streams = Vec::with_capacity(CONNECTS);
        while let Some(joined) = connects.join_next().await {
            if let Ok(Ok(stream)) = joined.unwrap() {
                streams.push(stream);
            }
        }
        assert_eq!(
            streams.len(),
            CONNECTS,
            "only {} of {CONNECTS} connects completed against a backlog of {BACKLOG}",
            streams.len()
        );
        drop(listener);
    }

    #[tokio::test]
    async fn an_address_that_cannot_be_bound_reports_the_bind_error() {
        let held = bind_listener(("127.0.0.1", 0), 16).await.unwrap();
        let port = held.local_addr().unwrap().port();
        let error = bind_listener(("127.0.0.1", port), 16).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }
}
