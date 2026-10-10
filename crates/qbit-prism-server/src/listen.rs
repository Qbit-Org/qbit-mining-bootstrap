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
use std::{
    io,
    net::{IpAddr, SocketAddr},
};
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
    bind_on(addr)?.listen(backlog)
}

fn bind_on(addr: SocketAddr) -> io::Result<TcpSocket> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    Ok(socket)
}

/// Whether two bound addresses could never both listen: one port, on one
/// address, or on an address the other's unspecified one covers. An IPv4-mapped
/// IPv6 address is that IPv4 address, and IPv6's unspecified address covers
/// IPv4 too (no socket here is IPv6-only). With `SO_REUSEADDR` both bind while
/// neither listens, as [`ReservedAddress`]es do; the second `listen` fails.
pub fn listen_conflict(a: SocketAddr, b: SocketAddr) -> bool {
    if a.port() != b.port() {
        return false;
    }
    match (a.ip().to_canonical(), b.ip().to_canonical()) {
        (IpAddr::V4(x), IpAddr::V4(y)) => x == y || x.is_unspecified() || y.is_unspecified(),
        (IpAddr::V6(x), IpAddr::V6(y)) => x == y || x.is_unspecified() || y.is_unspecified(),
        (IpAddr::V6(v6), IpAddr::V4(_)) | (IpAddr::V4(_), IpAddr::V6(v6)) => v6.is_unspecified(),
    }
}

/// A Stratum address bound but not listening (3.1 dual-writer mode). The
/// bind holds the port from startup, so a restart that loses the bind race to
/// a listening predecessor still fails before the coordinator starts, while
/// the kernel refuses every connection (RST) until [`Self::listen`]: no
/// handshake completes, so no check, balancer or miner sees a frontend that
/// is not ready as up. Dropping the listener and [`Self::reserve`] refuse
/// again, resetting any connection still queued unaccepted.
///
/// `SO_REUSEADDR` is set, as on every listener here, so the address binds
/// again at once while closed sessions linger in `TIME_WAIT`. Linux lets two
/// such sockets bind one address while neither listens, so a predecessor that
/// is itself reserved does not fail this bind; whichever listens second
/// fails then. The frontend refuses its own addresses that would clash so at
/// startup ([`listen_conflict`]).
#[derive(Debug)]
pub struct ReservedAddress {
    addr: SocketAddr,
    socket: Option<TcpSocket>,
}

/// Bind `addr` without listening. Every resolved address is tried in order,
/// as [`bind_listener`] does; the first that binds is held. A zero port is
/// assigned now and kept, so a later [`ReservedAddress::reserve`] binds the
/// same port again.
pub async fn reserve_address(addr: impl ToSocketAddrs) -> io::Result<ReservedAddress> {
    let mut last = None;
    for addr in lookup_host(addr).await? {
        match bind_on(addr) {
            Ok(socket) => {
                return Ok(ReservedAddress {
                    addr: socket.local_addr()?,
                    socket: Some(socket),
                })
            }
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

impl ReservedAddress {
    /// The bound address, with its assigned port.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Start listening with `backlog`. The listener owns the bound socket:
    /// once it is dropped, [`Self::reserve`] binds the address again.
    pub fn listen(&mut self, backlog: u32) -> io::Result<TcpListener> {
        let socket = match self.socket.take() {
            Some(socket) => socket,
            None => bind_on(self.addr)?,
        };
        socket.listen(backlog)
    }

    /// Hold the address again, not listening, after the listener [`Self::listen`]
    /// returned has been dropped.
    pub fn reserve(&mut self) -> io::Result<()> {
        if self.socket.is_none() {
            self.socket = Some(bind_on(self.addr)?);
        }
        Ok(())
    }
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

    async fn refused(addr: SocketAddr) -> bool {
        match tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await {
            Ok(Err(error)) => error.kind() == io::ErrorKind::ConnectionRefused,
            Ok(Ok(_)) => false,
            Err(_) => panic!("a connect to {addr} neither completed nor was refused"),
        }
    }

    /// Two reservations of one address both bind, and only the first
    /// listens: what `listen_conflict` names, which the kernel agrees with
    /// for the pairs it can bind here.
    #[tokio::test]
    async fn reservations_that_cannot_both_listen_are_named_as_conflicting() {
        let mut first = reserve_address(("127.0.0.1", 0)).await.unwrap();
        let port = first.local_addr().port();
        let mut same = reserve_address(("127.0.0.1", port)).await.unwrap();
        let mut wildcard = reserve_address(("0.0.0.0", port)).await.unwrap();
        let mut other = reserve_address(("127.0.0.2", port)).await.unwrap();
        assert!(listen_conflict(first.local_addr(), same.local_addr()));
        assert!(listen_conflict(first.local_addr(), wildcard.local_addr()));
        assert!(!listen_conflict(first.local_addr(), other.local_addr()));
        let _listening = first.listen(16).unwrap();
        assert_eq!(
            same.listen(16).unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
        assert_eq!(
            wildcard.listen(16).unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
        let _other = other.listen(16).unwrap();
        // Ports apart never conflict; IPv6's unspecified address covers IPv4,
        // a mapped IPv4 address is that address, and IPv4's unspecified one
        // leaves IPv6 alone.
        let addr = |text: &str| text.parse::<SocketAddr>().unwrap();
        assert!(!listen_conflict(addr("127.0.0.1:1"), addr("127.0.0.1:2")));
        assert!(listen_conflict(addr("[::]:1"), addr("127.0.0.1:1")));
        assert!(listen_conflict(
            addr("[::ffff:127.0.0.1]:1"),
            addr("127.0.0.1:1")
        ));
        assert!(listen_conflict(
            addr("[::ffff:127.0.0.1]:1"),
            addr("0.0.0.0:1")
        ));
        assert!(listen_conflict(
            addr("[::ffff:0.0.0.0]:1"),
            addr("[::ffff:127.0.0.1]:1")
        ));
        assert!(!listen_conflict(
            addr("[::ffff:127.0.0.1]:1"),
            addr("[::1]:1")
        ));
        assert!(!listen_conflict(addr("0.0.0.0:1"), addr("[::1]:1")));
        assert!(listen_conflict(addr("[::]:1"), addr("[::1]:1")));
        assert!(!listen_conflict(addr("[::1]:1"), addr("[::2]:1")));
    }

    #[tokio::test]
    async fn a_listening_predecessor_fails_the_reservation_as_it_fails_a_bind() {
        let predecessor = bind_listener(("127.0.0.1", 0), 16).await.unwrap();
        let error = reserve_address(predecessor.local_addr().unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    /// The dual-writer gate's socket cycle: refused while reserved, accepted
    /// while listening, refused again, on the same port, and a connection
    /// left unaccepted in the queue is reset when the listener closes.
    #[tokio::test]
    async fn a_reserved_address_refuses_until_it_listens_and_again_after() {
        let mut reserved = reserve_address(("127.0.0.1", 0)).await.unwrap();
        let addr = reserved.local_addr();
        assert_ne!(addr.port(), 0, "the zero port is assigned at the bind");
        assert!(refused(addr).await, "a reserved address must refuse");

        let listener = reserved.listen(16).unwrap();
        let mut accepted = TcpStream::connect(addr).await.unwrap();
        let (_server, _) = listener.accept().await.unwrap();
        let queued = TcpStream::connect(addr).await.unwrap();
        drop(listener);
        reserved.reserve().unwrap();
        assert_eq!(reserved.local_addr(), addr);
        assert!(refused(addr).await, "a closed listener must refuse again");
        // The queued handshake completed but was never accepted: closing the
        // listener resets it, so its miner reconnects elsewhere at once.
        let mut queued = queued;
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::io::AsyncReadExt::read(&mut queued, &mut byte),
        )
        .await
        .unwrap();
        assert!(
            matches!(&read, Err(error) if error.kind() == io::ErrorKind::ConnectionReset)
                || matches!(read, Ok(0)),
            "the unaccepted connection must be closed: {read:?}"
        );
        // An accepted session is not the listener's: it outlives the close.
        tokio::io::AsyncWriteExt::write_all(&mut accepted, b"x")
            .await
            .unwrap();

        let listener = reserved.listen(16).unwrap();
        TcpStream::connect(addr).await.unwrap();
        assert_eq!(listener.local_addr().unwrap(), addr);
    }
}
