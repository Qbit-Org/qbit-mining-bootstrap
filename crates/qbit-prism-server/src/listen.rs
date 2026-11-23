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
use anyhow::Context;
use std::{
    io,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio::{
    net::{lookup_host, TcpListener, TcpSocket, TcpStream, ToSocketAddrs},
    time::Instant,
};

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

/// How long a Stratum listener waits after an `accept()` failure it rides
/// out before it tries again, as the readiness endpoint does. A failure for
/// want of a descriptor or of kernel memory leaves the connection queued, and
/// the socket stays readable, so an accept tried at once fails at once: this
/// pause is what keeps the loop from spinning.
pub const ACCEPT_RETRY_PAUSE: Duration = Duration::from_millis(100);

/// At most one line per this interval about a listener's failed accepts.
const ACCEPT_FAILURE_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// What a Stratum listener does about one `accept()` error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcceptFailure {
    /// The listening socket itself cannot accept, now or on any retry: the
    /// listener ends, as on every error before 3.1.2.
    Fatal,
    /// Nothing about the socket or the queue stands in the way: the next
    /// accept is tried at once. It cannot spin: each such failure used up a
    /// queued connection, or a signal.
    Retry,
    /// The next accept waits [`ACCEPT_RETRY_PAUSE`].
    Pause,
}

/// One `accept()` error: its errno's name, for the log, and what to do.
///
/// - Fatal: `EBADF` and `ENOTSOCK` (not an open socket), `EINVAL` (not
///   listening), `EFAULT` (a bad address buffer, a bug), and `EPERM` and
///   `EACCES`: Linux fails an accept so only when a security module refuses
///   the listening socket's accept permission, which it refuses again on every
///   retry (netfilter never fails an accept).
/// - Retried at once: `ECONNABORTED`, which Linux returns only after taking
///   the connection off the queue (its peer address could not be read), and
///   `EINTR` (a signal; near impossible on a non-blocking socket).
/// - Paused: `EMFILE` and `ENFILE` (no descriptor), `ENOBUFS` and `ENOMEM` (no
///   kernel memory), the network errors accept(2) says Linux may pass
///   on for the connection being accepted (`EPROTO`, `ENOPROTOOPT`, `ENETDOWN`,
///   `ENETUNREACH`, `EHOSTDOWN`, `EHOSTUNREACH`, `ENONET`, `EOPNOTSUPP`,
///   `ETIMEDOUT`; Linux's TCP accept returns none of them today, so the pause
///   costs nothing and keeps the loop from spinning if one ever repeats), and
///   any error not named here, an error with no errno included.
fn accept_error(error: &io::Error) -> (&'static str, AcceptFailure) {
    use AcceptFailure::{Fatal, Pause, Retry};
    let Some(errno) = error.raw_os_error() else {
        return ("other", Pause);
    };
    #[cfg(target_os = "linux")]
    {
        match errno {
            libc::EBADF => ("EBADF", Fatal),
            libc::ENOTSOCK => ("ENOTSOCK", Fatal),
            libc::EINVAL => ("EINVAL", Fatal),
            libc::EFAULT => ("EFAULT", Fatal),
            libc::EPERM => ("EPERM", Fatal),
            libc::EACCES => ("EACCES", Fatal),
            libc::ECONNABORTED => ("ECONNABORTED", Retry),
            libc::EINTR => ("EINTR", Retry),
            libc::EMFILE => ("EMFILE", Pause),
            libc::ENFILE => ("ENFILE", Pause),
            libc::ENOBUFS => ("ENOBUFS", Pause),
            libc::ENOMEM => ("ENOMEM", Pause),
            libc::EPROTO => ("EPROTO", Pause),
            libc::ENOPROTOOPT => ("ENOPROTOOPT", Pause),
            libc::ENETDOWN => ("ENETDOWN", Pause),
            libc::ENETUNREACH => ("ENETUNREACH", Pause),
            libc::EHOSTDOWN => ("EHOSTDOWN", Pause),
            libc::EHOSTUNREACH => ("EHOSTUNREACH", Pause),
            libc::ENONET => ("ENONET", Pause),
            libc::EOPNOTSUPP => ("EOPNOTSUPP", Pause),
            libc::ETIMEDOUT => ("ETIMEDOUT", Pause),
            _ => ("other", Pause),
        }
    }
    // Off Linux (development builds only) errno has no names here; the
    // error kinds carry the same decisions where std names them.
    #[cfg(not(target_os = "linux"))]
    {
        let _ = errno;
        match error.kind() {
            io::ErrorKind::InvalidInput | io::ErrorKind::PermissionDenied => ("other", Fatal),
            io::ErrorKind::ConnectionAborted | io::ErrorKind::Interrupted => ("other", Retry),
            _ => ("other", Pause),
        }
    }
}

/// A Stratum listener's accepts. An error `accept_error` rides out is
/// logged and the next accept waits [`ACCEPT_RETRY_PAUSE`], unless it may be
/// tried at once. At most one line per `ACCEPT_FAILURE_REPORT_INTERVAL`
/// reports failed accepts: a warning at a failure; at the first accept after
/// a warned run, a line saying it is accepting again; otherwise at the first
/// accept once the interval has passed. Each line's `failed_accepts` counts
/// the failures since the previous line, its own included, so the lines add
/// up to every failure. Only an error that ends the listener is returned.
#[derive(Debug)]
pub(crate) struct Acceptor {
    listener: String,
    /// The pause after the last failure ridden out: no accept before it.
    resume_at: Option<Instant>,
    /// When the last line about failed accepts was logged.
    reported_at: Option<Instant>,
    /// Failed accepts no line has reported yet.
    unreported: u64,
    /// The failures since the last accept that succeeded, on this listening
    /// socket: since when, and whether a warning named them.
    run: Option<FailedRun>,
}

#[derive(Debug)]
struct FailedRun {
    since: Instant,
    warned: bool,
}

impl Acceptor {
    /// The accepts of the Stratum listener named `listener`, for its log.
    pub(crate) fn new(listener: impl Into<String>) -> Self {
        Self {
            listener: listener.into(),
            resume_at: None,
            reported_at: None,
            unreported: 0,
            run: None,
        }
    }

    /// The next connection on `listener`. Cancel-safe, as `accept` is: a
    /// pause cut short by another branch of a `select!` resumes, to the same
    /// instant, at the next call.
    pub(crate) async fn accept(
        &mut self,
        listener: &TcpListener,
    ) -> anyhow::Result<(TcpStream, SocketAddr)> {
        loop {
            if let Some(resume_at) = self.resume_at {
                tokio::time::sleep_until(resume_at).await;
                self.resume_at = None;
            }
            match listener.accept().await {
                Ok(accepted) => {
                    self.accepted();
                    return Ok(accepted);
                }
                Err(error) => self
                    .failed(error)
                    .with_context(|| format!("accept on the {} Stratum listener", self.listener))?,
            }
        }
    }

    /// The listening socket was closed (a dual-writer withdrawal). A run of
    /// failures ends with it, so a later listener's first accept does not
    /// report the time it was closed as failing; failures no line reported
    /// wait for the next one.
    pub(crate) fn closed(&mut self) {
        self.run = None;
        self.resume_at = None;
    }

    fn report_due(&self, now: Instant) -> bool {
        self.reported_at
            .is_none_or(|at| now.duration_since(at) >= ACCEPT_FAILURE_REPORT_INTERVAL)
    }

    fn reported(&mut self, now: Instant) {
        self.reported_at = Some(now);
        self.unreported = 0;
    }

    fn failed(&mut self, error: io::Error) -> io::Result<()> {
        let (kind, failure) = accept_error(&error);
        self.unreported += 1;
        let pause = match failure {
            AcceptFailure::Fatal => {
                tracing::error!(
                    listener = %self.listener,
                    kind,
                    %error,
                    failed_accepts = self.unreported,
                    "Stratum listener accept failed on the listening socket itself: the listener stops"
                );
                return Err(error);
            }
            AcceptFailure::Retry => Duration::ZERO,
            AcceptFailure::Pause => ACCEPT_RETRY_PAUSE,
        };
        let now = Instant::now();
        if !pause.is_zero() {
            self.resume_at = Some(now + pause);
        }
        let warn = self.report_due(now);
        let run = self.run.get_or_insert(FailedRun {
            since: now,
            warned: false,
        });
        if warn {
            run.warned = true;
            tracing::warn!(
                listener = %self.listener,
                kind,
                %error,
                failed_accepts = self.unreported,
                pause_ms = pause.as_millis() as u64,
                "Stratum listener accept failed: the listener keeps its sessions and accepts again"
            );
            self.reported(now);
        }
        Ok(())
    }

    fn accepted(&mut self) {
        if self.run.is_none() && self.unreported == 0 {
            return;
        }
        let now = Instant::now();
        match self.run.take() {
            Some(run) if run.warned => {
                tracing::info!(
                    listener = %self.listener,
                    failed_accepts = self.unreported,
                    failing_ms = now.duration_since(run.since).as_millis() as u64,
                    "Stratum listener accepting again after failed accepts"
                );
                self.reported(now);
            }
            _ if self.unreported > 0 && self.report_due(now) => {
                tracing::info!(
                    listener = %self.listener,
                    failed_accepts = self.unreported,
                    "Stratum listener accepting; failed accepts since the last report"
                );
                self.reported(now);
            }
            _ => {}
        }
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

    /// Only an error that says the listening socket itself cannot accept
    /// ends a Stratum listener; a connection the kernel already dequeued, or
    /// a signal, is retried at once; running out of descriptors or kernel memory, and
    /// anything unnamed, is paused and ridden out.
    #[test]
    fn only_errors_of_the_listening_socket_itself_end_the_listener() {
        use AcceptFailure::{Fatal, Pause, Retry};
        let errno = io::Error::from_raw_os_error;
        for (code, name, failure) in [
            (libc::EBADF, "EBADF", Fatal),
            (libc::ENOTSOCK, "ENOTSOCK", Fatal),
            (libc::EINVAL, "EINVAL", Fatal),
            (libc::EFAULT, "EFAULT", Fatal),
            (libc::EPERM, "EPERM", Fatal),
            (libc::EACCES, "EACCES", Fatal),
            (libc::ECONNABORTED, "ECONNABORTED", Retry),
            (libc::EINTR, "EINTR", Retry),
            (libc::EMFILE, "EMFILE", Pause),
            (libc::ENFILE, "ENFILE", Pause),
            (libc::ENOBUFS, "ENOBUFS", Pause),
            (libc::ENOMEM, "ENOMEM", Pause),
            (libc::EPROTO, "EPROTO", Pause),
            (libc::EOPNOTSUPP, "EOPNOTSUPP", Pause),
            (libc::EXDEV, "other", Pause),
        ] {
            assert_eq!(accept_error(&errno(code)), (name, failure), "{name}");
        }
        assert_eq!(
            accept_error(&io::Error::other("not an OS error")),
            ("other", Pause)
        );
    }

    /// A failure ridden out holds the next accept back for the whole pause,
    /// although a connection is waiting, and a pause cut short by a
    /// `select!` resumes to the same instant; a connection already dequeued
    /// is followed at once; a fatal error is returned.
    #[tokio::test(start_paused = true)]
    async fn a_failure_ridden_out_pauses_the_next_accept() {
        let listener = bind_listener(("127.0.0.1", 0), 16).await.unwrap();
        let address = listener.local_addr().unwrap();
        let _first = TcpStream::connect(address).await.unwrap();
        let mut acceptor = Acceptor::new("test");
        let failed_at = Instant::now();
        acceptor
            .failed(io::Error::from_raw_os_error(libc::EMFILE))
            .unwrap();
        let early = ACCEPT_RETRY_PAUSE - Duration::from_millis(1);
        assert!(
            tokio::time::timeout(early, acceptor.accept(&listener))
                .await
                .is_err(),
            "an accept was tried within the pause"
        );
        acceptor.accept(&listener).await.unwrap();
        assert_eq!(Instant::now(), failed_at + ACCEPT_RETRY_PAUSE);
        assert!(acceptor.run.is_none() && acceptor.resume_at.is_none());

        let _second = TcpStream::connect(address).await.unwrap();
        let failed_at = Instant::now();
        acceptor
            .failed(io::Error::from_raw_os_error(libc::ECONNABORTED))
            .unwrap();
        assert!(acceptor.resume_at.is_none());
        acceptor.accept(&listener).await.unwrap();
        assert_eq!(Instant::now(), failed_at, "a dequeued connection paused");

        let error = acceptor
            .failed(io::Error::from_raw_os_error(libc::EBADF))
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));
    }

    /// A run of failures ends with its listening socket: a gated listener
    /// withdrawn mid-run starts its next socket with no pause and no run.
    #[test]
    fn a_closed_listener_ends_the_run_of_failures() {
        let mut acceptor = Acceptor::new("test");
        acceptor
            .failed(io::Error::from_raw_os_error(libc::EMFILE))
            .unwrap();
        assert!(acceptor.run.as_ref().is_some_and(|run| run.warned));
        acceptor.closed();
        assert!(acceptor.run.is_none() && acceptor.resume_at.is_none());
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
