//! Small, rate-limited SHA256 Stratum miner for reproducible regtest exercises.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use num_bigint::BigUint;
use qbit_prism_server::codec::{
    difficulty_target, double_sha256, parse_u32_hex, target_from_compact,
};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::mpsc,
    time::timeout,
};

#[derive(Parser, Debug)]
#[command(about = "Constrained CPU Stratum miner for PRISM testing")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:3340")]
    address: String,
    #[arg(long, alias = "user")]
    username: String,
    #[arg(long, default_value = "x")]
    password: String,
    #[arg(long, default_value_t = 1)]
    threads: usize,
    /// Aggregate hash-rate budget across every worker thread.
    #[arg(long, default_value_t = 100)]
    hashes_per_second: u64,
    #[arg(long, default_value_t = 30)]
    duration_seconds: u64,
    /// Stop after this many accepted shares; zero runs for the full duration.
    #[arg(long, default_value_t = 0)]
    max_shares: u64,
    #[arg(long, default_value_t = 250)]
    pause_after_share_ms: u64,
    /// Exercise the high-difficulty listener's block-below-share-floor path.
    #[arg(long)]
    submit_blocks_below_share_target: bool,
}

#[derive(Clone)]
struct Work {
    id: String,
    coinb1: Vec<u8>,
    coinb2: Vec<u8>,
    extranonce1: Vec<u8>,
    extranonce2_size: usize,
    branch: Vec<[u8; 32]>,
    previous: [u8; 32],
    version: u32,
    nbits: u32,
    ntime: u32,
    share_target: BigUint,
    network_target: BigUint,
}

impl Work {
    fn from_notify(
        value: &Value,
        extranonce1: &[u8],
        extranonce2_size: usize,
        difficulty: f64,
    ) -> Result<Self> {
        let p = value["params"]
            .as_array()
            .context("notify params missing")?;
        ensure!(p.len() == 9, "invalid notify params");
        let field = |index: usize| p[index].as_str().context("notify field must be a string");
        let mut previous = hex::decode(field(1)?)?;
        ensure!(previous.len() == 32, "invalid prevhash length");
        for word in previous.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let branch = p[4]
            .as_array()
            .context("merkle branch missing")?
            .iter()
            .map(|v| {
                let bytes = hex::decode(v.as_str().context("invalid merkle sibling")?)?;
                bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid merkle sibling length"))
            })
            .collect::<Result<Vec<_>>>()?;
        let nbits = parse_u32_hex(field(6)?)?;
        Ok(Self {
            id: field(0)?.into(),
            coinb1: hex::decode(field(2)?)?,
            coinb2: hex::decode(field(3)?)?,
            extranonce1: extranonce1.to_vec(),
            extranonce2_size,
            branch,
            previous: previous.try_into().unwrap(),
            version: parse_u32_hex(field(5)?)?,
            nbits,
            ntime: parse_u32_hex(field(7)?)?,
            share_target: difficulty_target(difficulty)?,
            network_target: target_from_compact(nbits)?,
        })
    }
    fn header(&self, extra: u64) -> ([u8; 80], String) {
        let mut extranonce2 = vec![0; self.extranonce2_size];
        let width = self.extranonce2_size.min(8);
        extranonce2[..width].copy_from_slice(&extra.to_le_bytes()[..width]);
        let coinbase = [
            self.coinb1.as_slice(),
            self.extranonce1.as_slice(),
            extranonce2.as_slice(),
            self.coinb2.as_slice(),
        ]
        .concat();
        let mut merkle = double_sha256(&coinbase);
        for sibling in &self.branch {
            merkle = double_sha256(&[merkle.as_slice(), sibling.as_slice()].concat());
        }
        let mut header = [0; 80];
        header[..4].copy_from_slice(&self.version.to_le_bytes());
        header[4..36].copy_from_slice(&self.previous);
        header[36..68].copy_from_slice(&merkle);
        header[68..72].copy_from_slice(&self.ntime.to_le_bytes());
        header[72..76].copy_from_slice(&self.nbits.to_le_bytes());
        (header, hex::encode(extranonce2))
    }
}

struct Solution {
    job_id: String,
    extranonce2: String,
    ntime: u32,
    nonce: u32,
    block: bool,
}

struct MiningBudget {
    threads: usize,
    rate: u64,
    below_floor: bool,
}

fn mine(
    worker: usize,
    budget: MiningBudget,
    work: Arc<RwLock<Option<Arc<Work>>>>,
    stop: Arc<AtomicBool>,
    hashes: Arc<AtomicU64>,
    solutions: mpsc::Sender<Solution>,
) {
    let MiningBudget {
        threads,
        rate,
        below_floor,
    } = budget;
    let rate = rate as f64 / threads as f64;
    let batch = (rate / 10.0).clamp(1.0, 1000.0) as u32;
    let mut current = None::<Arc<Work>>;
    let mut header = [0; 80];
    let mut extranonce2 = String::new();
    let mut extra = worker as u64;
    let mut nonce = 0u32;
    while !stop.load(Ordering::Relaxed) {
        let started = Instant::now();
        let published = work.read().unwrap().clone();
        let Some(published) = published else {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        };
        if current.as_ref().is_none_or(|old| old.id != published.id) {
            extra = worker as u64;
            nonce = 0;
            (header, extranonce2) = published.header(extra);
            current = Some(published);
        }
        let current = current.as_ref().unwrap();
        let mut attempted = 0;
        for _ in 0..batch {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            header[76..80].copy_from_slice(&nonce.to_le_bytes());
            let hash = BigUint::from_bytes_le(&double_sha256(&header));
            let block = hash <= current.network_target;
            if hash <= current.share_target || (below_floor && block) {
                let _ = solutions.try_send(Solution {
                    job_id: current.id.clone(),
                    extranonce2: extranonce2.clone(),
                    ntime: current.ntime,
                    nonce,
                    block,
                });
            }
            attempted += 1;
            nonce = nonce.wrapping_add(1);
            if nonce == 0 {
                extra = extra.wrapping_add(threads as u64);
                (header, extranonce2) = current.header(extra);
            }
        }
        hashes.fetch_add(attempted, Ordering::Relaxed);
        let remaining =
            Duration::from_secs_f64(attempted as f64 / rate).saturating_sub(started.elapsed());
        // Keep shutdown responsive even for a one-hash-per-second test.
        let deadline = Instant::now() + remaining;
        while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
            std::thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(20)),
            );
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        (1..=64).contains(&args.threads),
        "--threads must be between 1 and 64"
    );
    ensure!(
        (1..=10_000_000).contains(&args.hashes_per_second),
        "hash-rate budget must be between 1 and 10000000"
    );
    ensure!(
        args.hashes_per_second >= args.threads as u64,
        "hash-rate budget must be at least the thread count"
    );
    ensure!(args.duration_seconds > 0, "duration must be positive");
    let address = args
        .address
        .strip_prefix("stratum+tcp://")
        .unwrap_or(&args.address);
    let stream = timeout(Duration::from_secs(10), TcpStream::connect(address)).await??;
    stream.set_nodelay(true)?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    for request in [
        json!({"id":1,"method":"mining.subscribe","params":["qbit-prism-miner/2.0"]}),
        json!({"id":2,"method":"mining.authorize","params":[args.username,args.password]}),
    ] {
        writer.write_all(format!("{request}\n").as_bytes()).await?;
    }
    let work = Arc::new(RwLock::new(None));
    let stop = Arc::new(AtomicBool::new(false));
    let hashes = Arc::new(AtomicU64::new(0));
    let (solutions_tx, mut solutions) = mpsc::channel(32);
    let mut workers = Vec::new();
    for worker in 0..args.threads {
        let (work, stop, hashes, solutions) = (
            work.clone(),
            stop.clone(),
            hashes.clone(),
            solutions_tx.clone(),
        );
        let (threads, rate, below) = (
            args.threads,
            args.hashes_per_second,
            args.submit_blocks_below_share_target,
        );
        workers.push(std::thread::spawn(move || {
            mine(
                worker,
                MiningBudget {
                    threads,
                    rate,
                    below_floor: below,
                },
                work,
                stop,
                hashes,
                solutions,
            )
        }));
    }
    drop(solutions_tx);
    let started = Instant::now();
    let (mut accepted, mut rejected, mut submitted, mut job_count) = (0u64, 0u64, 0u64, 0u64);
    let run=async {
        let mut extranonce1=Vec::new();
        let mut extranonce2_size=0usize;
        let mut difficulty=1.0;
        let mut next_id=10u64;
        let mut pending=HashSet::new();
        let mut last_submit=None::<Instant>;
        let mut line=Vec::new();
        let mut tick=tokio::time::interval(Duration::from_millis(20));
        loop {
            if started.elapsed()>=Duration::from_secs(args.duration_seconds) || (args.max_shares>0 && accepted>=args.max_shares) {break;}
            let mut bounded_reader=(&mut reader).take((8*1024*1024+1-line.len()) as u64);
            tokio::select! {
                _=tokio::signal::ctrl_c()=>break,
                _=tick.tick()=>{},
                read=bounded_reader.read_until(b'\n',&mut line)=>{
                    ensure!(read?>0,"Stratum server disconnected");
                    ensure!(line.len()<=8*1024*1024,"oversized server response");
                    let message:Value=serde_json::from_slice(&line)?;line.clear();
                    match message.get("method").and_then(Value::as_str) {
                        Some("mining.set_difficulty")=>{
                            difficulty=message["params"][0].as_f64().context("invalid difficulty")?;
                            ensure!(difficulty.is_finite() && difficulty>0.0,"invalid difficulty");
                        },
                        Some("mining.notify")=>{
                            ensure!(!extranonce1.is_empty() && extranonce2_size>0,"notify before subscription");
                            let next=Work::from_notify(&message,&extranonce1,extranonce2_size,difficulty)?;
                            println!("{}",json!({"event":"job","job_id":next.id,"difficulty":difficulty,"clean_jobs":message["params"][8]}));
                            *work.write().unwrap()=Some(Arc::new(next));job_count+=1;
                        },
                        _=>{
                            match message["id"].as_u64() {
                                Some(1)=>{
                                    ensure!(message["error"].is_null(),"subscribe rejected: {}",message["error"]);
                                    extranonce1=hex::decode(message["result"][1].as_str().context("missing extranonce1")?)?;
                                    extranonce2_size=message["result"][2].as_u64().context("missing extranonce2 size")?.try_into()?;
                                    ensure!((1..=32).contains(&extranonce2_size),"unsupported extranonce2 size");
                                },
                                Some(2)=>ensure!(message["result"]==true,"authorization rejected: {}",message["error"]),
                                Some(id) if pending.remove(&id)=>{
                                    let ok=message["result"]==true;
                                    if ok {accepted+=1;} else {rejected+=1;}
                                    println!("{}",json!({"event":"share","accepted":ok,"response":message}));
                                },
                                _=>{},
                            }
                        }
                    }
                },
                Some(solution)=solutions.recv(),if pending.is_empty() && last_submit.is_none_or(|at|at.elapsed()>=Duration::from_millis(args.pause_after_share_ms))=>{
                    if work.read().unwrap().as_ref().is_none_or(|current|current.id!=solution.job_id) {continue;}
                    let request=json!({"id":next_id,"method":"mining.submit","params":[args.username,solution.job_id,solution.extranonce2,
                        format!("{:08x}",solution.ntime),format!("{:08x}",solution.nonce)]});
                    timeout(Duration::from_secs(5),writer.write_all(format!("{request}\n").as_bytes())).await??;
                    pending.insert(next_id);next_id+=1;submitted+=1;last_submit=Some(Instant::now());
                    println!("{}",json!({"event":"submit","block_target_met":solution.block,"request_id":next_id-1}));
                }
            }
        }
        Ok::<(),anyhow::Error>(())
    }.await;
    stop.store(true, Ordering::Relaxed);
    for worker in workers {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("miner worker panicked"))?;
    }
    println!(
        "{}",
        json!({"event":"summary","address":address,"username":args.username,"threads":args.threads,"hashes_per_second_budget":args.hashes_per_second,
        "hashes":hashes.load(Ordering::Relaxed),"elapsed_seconds":started.elapsed().as_secs_f64(),"jobs":job_count,"submitted":submitted,"accepted":accepted,"rejected":rejected})
    );
    run?;
    ensure!(accepted > 0, "no accepted shares during mining run");
    Ok(())
}
