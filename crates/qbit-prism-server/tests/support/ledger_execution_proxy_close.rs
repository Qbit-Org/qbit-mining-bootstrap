//! Deterministic transport scheduling at the proxy's actual pump boundary.
use super::*;
use std::io;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll, Waker};
use tokio::io::ReadBuf;

#[derive(Default)]
struct CloseGate {
    closed: AtomicBool,
    reader: Mutex<Option<Waker>>,
}

impl CloseGate {
    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(waker) = self.reader.lock().unwrap().take() {
            waker.wake();
        }
    }
}

enum ClientEnd {
    Eof,
    Reset,
    Pending,
}

struct ClosingClient {
    frames: std::io::Cursor<Vec<u8>>,
    after_close: std::io::Cursor<Vec<u8>>,
    gate: Arc<CloseGate>,
    end: ClientEnd,
}

impl AsyncRead for ClosingClient {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.frames.position() < self.frames.get_ref().len() as u64 {
            return Pin::new(&mut self.frames).poll_read(cx, buf);
        }
        *self.gate.reader.lock().unwrap() = Some(cx.waker().clone());
        if !self.gate.closed.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        if self.after_close.position() < self.after_close.get_ref().len() as u64 {
            return Pin::new(&mut self.after_close).poll_read(cx, buf);
        }
        match self.end {
            ClientEnd::Eof => Poll::Ready(Ok(())),
            ClientEnd::Reset => Poll::Ready(Err(io::ErrorKind::ConnectionReset.into())),
            ClientEnd::Pending => Poll::Pending,
        }
    }
}

struct BrokenClientWriter {
    gate: Arc<CloseGate>,
    error: io::ErrorKind,
    /// Accept only this many bytes before failing, including partial frames.
    remaining: usize,
}

struct PauseAtReadyDelivery {
    socket: TcpStream,
    remaining: Option<usize>,
    release: std::sync::mpsc::Receiver<()>,
}

impl AsyncWrite for PauseAtReadyDelivery {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.remaining.is_none() && bytes.first() == Some(&b'Z') {
            self.remaining = Some(bytes.len());
        }
        let result = Pin::new(&mut self.socket).poll_write(cx, bytes);
        if let Poll::Ready(Ok(written)) = result {
            if let Some(remaining) = &mut self.remaining {
                *remaining -= written;
                if *remaining == 0 {
                    self.remaining = None;
                    // Test-only scheduler barrier: the real TCP write has
                    // completed, but its proxy worker cannot publish yet.
                    // Another worker reads the reply and inspects the observer.
                    if let Err(error) = self.release.recv_timeout(std::time::Duration::from_secs(5))
                    {
                        return Poll::Ready(Err(io::Error::other(error)));
                    }
                }
            }
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

impl AsyncWrite for BrokenClientWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.remaining > 0 {
            let written = self.remaining.min(bytes.len());
            self.remaining -= written;
            return Poll::Ready(Ok(written));
        }
        // Client EOF becomes readable in the same poll that its write fails.
        // The server pump therefore wins select before the client is repolled.
        self.gate.close();
        Poll::Ready(Err(self.error.into()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn observations() -> Arc<Shared> {
    Arc::new(Shared {
        seq: AtomicU64::new(0),
        connections: AtomicU64::new(0),
        state: Mutex::default(),
    })
}

async fn ordered_close(
    requests: Vec<u8>,
    after_close: Vec<u8>,
    responses: Vec<u8>,
    end: ClientEnd,
    write_error: io::ErrorKind,
    accepted_bytes: usize,
) -> (Result<()>, Arc<Shared>) {
    let shared = observations();
    let connection = Arc::new(Mutex::new(Connection::default()));
    let gate = Arc::new(CloseGate::default());
    let client = pump_client(
        ClosingClient {
            frames: io::Cursor::new(requests),
            after_close: io::Cursor::new(after_close),
            gate: gate.clone(),
            end,
        },
        tokio::io::sink(),
        connection.clone(),
        shared.clone(),
        1,
    );
    tokio::pin!(client);
    // Requests are observed and forwarded before the reply; EOF is not yet
    // readable. This fixes the order without sleeps or scheduler assumptions.
    assert!(futures_util::poll!(&mut client).is_pending());
    let server = pump_server(
        responses.as_slice(),
        BrokenClientWriter {
            gate,
            error: write_error,
            remaining: accepted_bytes,
        },
        connection,
        shared.clone(),
        1,
    );
    (relay(client, server).await, shared)
}

#[tokio::test]
async fn client_eof_after_commit_write_failure_keeps_rollback_pending() -> Result<()> {
    let (result, shared) = ordered_close(
        [frame(b'Q', b"COMMIT\0")?, frame(b'Q', b"ROLLBACK\0")?].concat(),
        frame(b'X', b"")?,
        frame(b'C', b"COMMIT\0")?,
        ClientEnd::Eof,
        io::ErrorKind::BrokenPipe,
        0,
    )
    .await;
    result?;
    let state = shared.state.lock().unwrap();
    assert_eq!(state.executions.len(), 2);
    assert_eq!(state.executions[0].sql, "COMMIT");
    assert_eq!(
        state.executions[0].outcome,
        Outcome::Completed {
            tag: "COMMIT".into(),
            delivered: false
        }
    );
    assert!(!state.executions[0].complete_response());
    assert_eq!(state.executions[1].sql, "ROLLBACK");
    assert_eq!(state.executions[1].outcome, Outcome::Pending);
    Ok(())
}

#[tokio::test]
async fn buffered_rollback_before_eof_is_counted_without_claiming_it_completed() -> Result<()> {
    let (result, shared) = ordered_close(
        frame(b'Q', b"COMMIT\0")?,
        [frame(b'Q', b"ROLLBACK\0")?, frame(b'X', b"")?].concat(),
        frame(b'C', b"COMMIT\0")?,
        ClientEnd::Eof,
        io::ErrorKind::BrokenPipe,
        0,
    )
    .await;
    result?;
    let state = shared.state.lock().unwrap();
    assert_eq!(state.executions.len(), 2);
    assert_eq!(state.executions[1].sql, "ROLLBACK");
    assert_eq!(state.executions[1].outcome, Outcome::Pending);
    assert!(!state.executions[0].delivered());
    Ok(())
}

#[tokio::test]
async fn eof_without_a_commit_acknowledgement_keeps_both_controls_pending() -> Result<()> {
    let (result, shared) = ordered_close(
        [frame(b'Q', b"COMMIT\0")?, frame(b'Q', b"ROLLBACK\0")?].concat(),
        vec![],
        frame(b'N', b"Mclosing\0\0")?,
        ClientEnd::Eof,
        io::ErrorKind::BrokenPipe,
        0,
    )
    .await;
    result?;
    let state = shared.state.lock().unwrap();
    assert_eq!(state.executions.len(), 2);
    for execution in &state.executions {
        assert_eq!(execution.outcome, Outcome::Pending);
        assert!(!execution.complete_response());
        assert!(!execution.is_commit());
    }
    Ok(())
}

#[tokio::test]
async fn client_eof_winning_first_keeps_unobserved_commit_and_rollback_pending() -> Result<()> {
    let shared = observations();
    let connection = Arc::new(Mutex::new(Connection::default()));
    let gate = Arc::new(CloseGate::default());
    let client = pump_client(
        ClosingClient {
            frames: io::Cursor::new(
                [frame(b'Q', b"COMMIT\0")?, frame(b'Q', b"ROLLBACK\0")?].concat(),
            ),
            after_close: io::Cursor::new(vec![]),
            gate: gate.clone(),
            end: ClientEnd::Eof,
        },
        tokio::io::sink(),
        connection,
        shared.clone(),
        1,
    );
    tokio::pin!(client);
    assert!(futures_util::poll!(&mut client).is_pending());
    gate.close();
    relay(client, std::future::pending()).await?;
    let state = shared.state.lock().unwrap();
    assert_eq!(state.executions.len(), 2);
    for execution in &state.executions {
        assert_eq!(execution.outcome, Outcome::Pending);
        assert!(!execution.complete_response());
    }
    Ok(())
}

#[tokio::test]
async fn broken_pipe_without_observed_eof_remains_an_error() -> Result<()> {
    let (result, _) = ordered_close(
        frame(b'Q', b"COMMIT\0")?,
        vec![],
        frame(b'C', b"COMMIT\0")?,
        ClientEnd::Pending,
        io::ErrorKind::BrokenPipe,
        0,
    )
    .await;
    assert_eq!(
        result
            .unwrap_err()
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    Ok(())
}

#[tokio::test]
async fn unrelated_client_read_and_write_resets_remain_errors() -> Result<()> {
    for (end, write_error) in [
        (ClientEnd::Reset, io::ErrorKind::BrokenPipe),
        (ClientEnd::Eof, io::ErrorKind::ConnectionReset),
    ] {
        let (result, _) = ordered_close(
            frame(b'Q', b"COMMIT\0")?,
            vec![],
            frame(b'C', b"COMMIT\0")?,
            end,
            write_error,
            0,
        )
        .await;
        assert_eq!(
            result
                .unwrap_err()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::ConnectionReset
        );
    }
    Ok(())
}

#[tokio::test]
async fn truncated_and_malformed_client_frames_cannot_prove_clean_close() -> Result<()> {
    for tail in [
        vec![b'Q', 0, 0],
        vec![b'Q', 0, 0, 0, 7, b'x'],
        frame(b'Q', b"unterminated")?,
        frame(b'X', b"invalid")?,
    ] {
        let (result, _) = ordered_close(
            frame(b'Q', b"COMMIT\0")?,
            tail,
            frame(b'C', b"COMMIT\0")?,
            ClientEnd::Eof,
            io::ErrorKind::BrokenPipe,
            0,
        )
        .await;
        assert!(result.is_err(), "malformed client frame was accepted");
    }
    Ok(())
}

#[tokio::test]
async fn malformed_server_frames_remain_errors() -> Result<()> {
    for response in [
        vec![b'C', 0, 0, 0, 3],
        vec![b'C', 0, 0, 0, 7, b'x'],
        frame(b'C', b"COMMIT")?,
        frame(b'N', b"Munterminated")?,
    ] {
        let (result, _) = ordered_close(
            frame(b'Q', b"COMMIT\0")?,
            vec![],
            response,
            ClientEnd::Eof,
            io::ErrorKind::BrokenPipe,
            0,
        )
        .await;
        assert!(result.is_err(), "malformed server frame was accepted");
    }
    Ok(())
}

#[tokio::test]
async fn malformed_ready_for_query_cannot_complete_a_select() -> Result<()> {
    for body in [b"".as_slice(), b"?", b"II"] {
        let (result, shared) = ordered_close(
            frame(b'Q', b"SELECT 1 WHERE false\0")?,
            vec![],
            [frame(b'C', b"SELECT 0\0")?, frame(b'Z', body)?].concat(),
            ClientEnd::Pending,
            io::ErrorKind::BrokenPipe,
            usize::MAX,
        )
        .await;
        assert!(result.is_err(), "malformed ReadyForQuery was accepted");
        assert!(shared.state.lock().unwrap().executions[0]
            .returned_rows()
            .is_err());
    }
    Ok(())
}

#[tokio::test]
async fn unknown_frontend_frame_cannot_qualify_broken_pipe_recovery() -> Result<()> {
    let (result, _) = ordered_close(
        frame(b'Q', b"COMMIT\0")?,
        frame(b'?', b"")?,
        frame(b'C', b"COMMIT\0")?,
        ClientEnd::Eof,
        io::ErrorKind::BrokenPipe,
        0,
    )
    .await;
    assert!(result.is_err(), "unknown frontend frame was accepted");
    Ok(())
}

#[tokio::test]
async fn lost_or_partial_completion_and_ready_frames_keep_select_incomplete() -> Result<()> {
    let completion = frame(b'C', b"SELECT 0\0")?;
    let response = [completion.clone(), frame(b'Z', b"I")?].concat();
    for accepted in [0, 1, completion.len(), completion.len() + 1] {
        let (result, shared) = ordered_close(
            frame(b'Q', b"SELECT 1 WHERE false\0")?,
            vec![],
            response.clone(),
            ClientEnd::Eof,
            io::ErrorKind::BrokenPipe,
            accepted,
        )
        .await;
        result?;
        let state = shared.state.lock().unwrap();
        assert_eq!(state.executions.len(), 1);
        let execution = &state.executions[0];
        assert_eq!(execution.delivered(), accepted >= completion.len());
        assert!(!execution.complete_response());
        assert!(execution.returned_rows().is_err());
    }
    Ok(())
}

#[tokio::test]
async fn successful_zero_and_incorrect_row_counts_remain_distinct() -> Result<()> {
    for count in [0, 1] {
        let (result, shared) = ordered_close(
            frame(b'Q', b"SELECT 1 WHERE false\0")?,
            vec![],
            [
                frame(b'C', format!("SELECT {count}\0").as_bytes())?,
                frame(b'Z', b"I")?,
            ]
            .concat(),
            ClientEnd::Pending,
            io::ErrorKind::BrokenPipe,
            usize::MAX,
        )
        .await;
        result?;
        let state = shared.state.lock().unwrap();
        assert_eq!(state.executions.len(), 1);
        let execution = &state.executions[0];
        assert!(execution.complete_response());
        if count == 0 {
            assert_eq!(execution.returned_rows()?, 0);
        } else {
            assert!(execution.returned_rows().is_err());
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledgement_delivery_cannot_expose_an_incomplete_observer_snapshot() -> Result<()> {
    let shared = observations();
    let connection = Arc::new(Mutex::new(Connection::default()));
    record(
        &shared,
        &connection,
        1,
        Protocol::Simple,
        "SELECT 1 WHERE false".into(),
    );
    let proxy = observer(shared.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (client, server) = tokio::join!(
        TcpStream::connect(listener.local_addr()?),
        listener.accept()
    );
    let mut client = BufReader::new(client?);
    let (release, pause) = std::sync::mpsc::channel();
    let replies = [frame(b'C', b"SELECT 0\0")?, frame(b'Z', b"I")?].concat();
    let mut tasks = JoinSet::new();
    tasks.spawn(pump_server(
        io::Cursor::new(replies),
        PauseAtReadyDelivery {
            socket: server?.0,
            remaining: None,
            release: pause,
        },
        connection,
        shared.clone(),
        1,
    ));
    let received = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        ensure!(read_frame(&mut client).await? == Some((b'C', b"SELECT 0\0".to_vec())));
        ensure!(read_frame(&mut client).await? == Some((b'Z', b"I".to_vec())));
        Ok::<_, anyhow::Error>(())
    })
    .await;
    // Inspect through the public observer while the forwarding worker is
    // paused. If publication holds its lock, readers must wait for it; if it
    // does not, an actual snapshot here must already report completion.
    let readable = shared.state.try_lock().is_ok();
    let during_delivery = readable.then(|| proxy.executions_since(0));
    let released = release.send(());
    let pumped = tasks.join_next().await.context("missing server pump")?;
    received??;
    released?;
    pumped??;
    if let Some(snapshot) = during_delivery {
        assert!(
            snapshot?[0].complete_response(),
            "client-visible acknowledgement raced observer publication"
        );
    }
    assert_eq!(
        shared.state.lock().unwrap().executions[0].returned_rows()?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn undelivered_error_remains_a_server_observation_not_a_complete_response() -> Result<()> {
    let (result, shared) = ordered_close(
        frame(b'Q', b"COMMIT\0")?,
        frame(b'X', b"")?,
        frame(b'E', b"C40001\0Mserialization failure\0\0")?,
        ClientEnd::Eof,
        io::ErrorKind::BrokenPipe,
        0,
    )
    .await;
    result?;
    let state = shared.state.lock().unwrap();
    assert_eq!(state.executions.len(), 1);
    let execution = &state.executions[0];
    assert_eq!(execution.sqlstate(), Some("40001"));
    assert!(!execution.delivered());
    assert!(!execution.complete_response());
    assert!(execution.returned_rows().is_err());
    assert_eq!(state.rejections.len(), 1);
    assert_eq!(
        state.rejections[0].frame,
        RejectedFrame::Execution(execution.seq)
    );
    Ok(())
}

fn observer(shared: Arc<Shared>) -> ExecutionProxy {
    ExecutionProxy {
        addr: "127.0.0.1:0".parse().unwrap(),
        shared,
        accept: Mutex::default(),
        tasks: Arc::default(),
        failure: Mutex::default(),
    }
}

#[tokio::test]
async fn executions_reject_unknown_frames_and_keep_reported_task_failures() -> Result<()> {
    let (result, shared) = ordered_close(
        frame(b'E', b"unbound\0\0\0\0\0")?,
        vec![],
        frame(b'C', b"SELECT 0\0")?,
        ClientEnd::Eof,
        io::ErrorKind::BrokenPipe,
        0,
    )
    .await;
    result?;
    let proxy = observer(shared);
    assert!(proxy
        .executions_since(0)
        .unwrap_err()
        .to_string()
        .contains("could not identify"));
    proxy.finish().await?;

    let proxy = observer(observations());
    let task = proxy
        .tasks
        .lock()
        .unwrap()
        .spawn(async { bail!("unrelated connection failure") });
    // Await this exact task's completion without draining the registry that
    // check() must inspect. No timing delay stands in for observing it.
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    for _ in 0..2 {
        assert!(proxy
            .executions_since(0)
            .unwrap_err()
            .to_string()
            .contains("failed"));
    }
    // Cleanup joins resources; check() retains the already-reported failure.
    proxy.finish().await?;
    assert!(proxy.tasks.lock().unwrap().is_empty());
    assert!(proxy.executions_since(0).is_err());
    Ok(())
}
