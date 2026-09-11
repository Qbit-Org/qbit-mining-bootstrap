//! Delay proxy between every frontend and PostgreSQL.
//!
//! One TCP listener fronts the primary. Each forwarded chunk is held for the
//! configured one-way delay before it is written on, so a round trip pays the
//! delay twice. The delay is a shared atomic, flipped per phase; it is 0
//! everywhere except the `slow_database` phase.

use anyhow::{Context, Result};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

/// What `database_delay_milliseconds` in the artifact refers to.
pub const DELAY_SEMANTICS: &str =
    "one-way per-chunk delay in milliseconds; a database round trip pays it twice";

pub struct DelayProxy {
    pub local: SocketAddr,
    delay_micros: Arc<AtomicU64>,
    connections: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for DelayProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl DelayProxy {
    pub async fn open(upstream: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind database delay proxy")?;
        let local = listener.local_addr()?;
        let delay_micros = Arc::new(AtomicU64::new(0));
        let connections = Arc::new(AtomicUsize::new(0));
        let task = {
            let delay = delay_micros.clone();
            let counter = connections.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((client, _)) = listener.accept().await else {
                        break;
                    };
                    let delay = delay.clone();
                    let counter = counter.clone();
                    counter.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        if let Ok(server) = TcpStream::connect(upstream).await {
                            let _ = client.set_nodelay(true);
                            let _ = server.set_nodelay(true);
                            pump(client, server, delay).await;
                        }
                        counter.fetch_sub(1, Ordering::Relaxed);
                    });
                }
            })
        };
        Ok(Self {
            local,
            delay_micros,
            connections,
            task,
        })
    }

    pub fn url_host(&self) -> String {
        format!("{}", self.local)
    }

    pub fn set_delay_millis(&self, millis: u64) {
        self.delay_micros
            .store(millis.saturating_mul(1000), Ordering::SeqCst);
    }

    pub fn delay_millis(&self) -> u64 {
        self.delay_micros.load(Ordering::SeqCst) / 1000
    }

    pub fn open_connections(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }
}

async fn pump(client: TcpStream, server: TcpStream, delay: Arc<AtomicU64>) {
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    let up = tokio::spawn(copy_delayed(client_read, server_write, delay.clone()));
    let down = tokio::spawn(copy_delayed(server_read, client_write, delay));
    let _ = up.await;
    let _ = down.await;
}

async fn copy_delayed<R, W>(mut reader: R, mut writer: W, delay: Arc<AtomicU64>)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let micros = delay.load(Ordering::Relaxed);
        if micros > 0 {
            tokio::time::sleep(Duration::from_micros(micros)).await;
        }
        if writer.write_all(&buffer[..read]).await.is_err() {
            break;
        }
        if writer.flush().await.is_err() {
            break;
        }
    }
    let _ = writer.shutdown().await;
}

/// Median wall-clock cost of a trivial round trip, used to turn the configured
/// delay into an observed one.
pub async fn measure_select1_millis(url: &str, samples: usize) -> Result<f64> {
    use sqlx::Connection;
    let mut connection = sqlx::PgConnection::connect(url)
        .await
        .with_context(|| format!("connect for round-trip measurement: {url}"))?;
    // Warm up: the first statement pays parse and plan costs.
    for _ in 0..3 {
        sqlx::query("SELECT 1").execute(&mut connection).await?;
    }
    let mut timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        sqlx::query("SELECT 1").execute(&mut connection).await?;
        timings.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    let _ = connection.close().await;
    timings.sort_by(f64::total_cmp);
    Ok(timings[timings.len() / 2])
}
