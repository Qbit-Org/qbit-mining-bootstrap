//! A Stratum v1 client for the live suite that solves the current job in the
//! test and records exactly what the server answered to each submission, so a
//! scenario can compare every acknowledged share with the ledger (#575).
//!
//! Regtest's block target (`207fffff`) is easier than a difficulty-1 share, so
//! a server left at its default share difficulty turns every share into a
//! block. A scenario that needs shares that are not blocks runs its servers
//! with [`SHARE_ONLY_SETTINGS`]: vardiff off and a share target above the
//! block target, so about half of all hashes are shares that miss the block
//! target and the client can choose which kind it submits.
use super::*;
use num_bigint::BigUint;
use qbit_prism_server::codec::{
    difficulty_target, double_sha256, hash_display, parse_u32_hex, target_from_compact,
};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

/// Server settings under which a share can miss the regtest block target:
/// a fixed share difficulty of about 2^-32, whose target is above regtest's.
pub(crate) const SHARE_ONLY_SETTINGS: [(&str, &str); 2] = [
    ("PRISM_STRATUM_VARDIFF", "0"),
    ("PRISM_STRATUM_SHARE_DIFF", "0.00000000025"),
];

/// How long a submission waits for its answer: well past the server's own
/// bound, the share commit timeout plus grace (15 s by default), which also
/// bounds a block whose answer waits for its landing (#577).
const ANSWER_SECONDS: u64 = 70;

/// Start `servers` with [`SHARE_ONLY_SETTINGS`] and each one's `overrides`,
/// and wait until every one is ready.
pub(crate) async fn start_share_only_servers(
    f: &mut Fixture,
    servers: &[(usize, Vec<(&str, String)>)],
) -> Result<()> {
    f.server_env = SHARE_ONLY_SETTINGS
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    for (index, overrides) in servers {
        let process = f.start_server_with(*index, None, overrides)?;
        f.servers.push(process);
    }
    for (index, _) in servers {
        until(&format!("server {index} readiness"), 60, || async {
            Ok(f.client
                .get(format!("http://127.0.0.1:{}/healthz", f.api[*index]))
                .send()
                .await?
                .status()
                .is_success())
        })
        .await?;
    }
    Ok(())
}

/// Which target a submitted header meets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Proof {
    /// The share target but not the block target: a share and nothing else.
    Share,
    /// The block target (and so the share target too).
    Block,
}

/// What the server did with one submission.
#[derive(Clone, Debug)]
pub(crate) enum Answer {
    Accepted,
    /// A JSON-RPC error or a `false` result: the full response.
    Rejected(Value),
    /// The connection closed, or no answer came within [`ANSWER_SECONDS`].
    Unanswered(String),
}

impl Answer {
    pub(crate) fn accepted(&self) -> bool {
        matches!(self, Answer::Accepted)
    }

    /// The server's reason for a rejection: the `reason_id` of its
    /// `[code, message, {"reason_id": ...}]` error when present, else the
    /// whole error.
    pub(crate) fn reason(&self) -> String {
        match self {
            Answer::Accepted => "accepted".into(),
            Answer::Rejected(response) => {
                let error = &response["error"];
                error[2]["reason_id"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| error.to_string())
            }
            Answer::Unanswered(why) => format!("unanswered: {why}"),
        }
    }
}

/// One submission and its answer. `share_id` is the ledger's identity for
/// it: the worker name and the header hash in display order.
#[derive(Clone, Debug)]
pub(crate) struct Submitted {
    pub share_id: String,
    pub hash: String,
    pub answer: Answer,
}

/// A solved header for one job.
pub(crate) struct Solution {
    pub extranonce2: String,
    pub ntime: String,
    pub nonce: String,
    /// The rolled version bits, as `mining.submit`'s sixth field, when the
    /// job was solved with version rolling.
    pub version_bits: Option<String>,
    /// Display-order header hash.
    pub hash: String,
}

/// A share that is not a block when the job's share target allows one; a
/// block otherwise (a share difficulty at or above the network's, as a
/// miner's `mining.suggest_difficulty` can ask for on regtest).
pub(crate) fn easiest_proof(notify: &Value, difficulty: f64) -> Result<Proof> {
    let bits = notify["params"][6]
        .as_str()
        .context("notify bits missing")?;
    let block = target_from_compact(parse_u32_hex(bits)?)?;
    Ok(if difficulty_target(difficulty)? > block {
        Proof::Share
    } else {
        Proof::Block
    })
}

/// Solve `notify` for `proof`. `counter` picks the extranonce2 (and so the
/// merkle root); `version_mask`, when nonzero, rolls the lowest bit of the
/// mask into the version, as a version-rolling miner does.
pub(crate) fn solve(
    notify: &Value,
    extranonce1: &str,
    extranonce2_size: usize,
    difficulty: f64,
    version_mask: u32,
    counter: u64,
    proof: Proof,
) -> Result<Solution> {
    let params = notify["params"].as_array().context("notify missing")?;
    let field = |index: usize| {
        params
            .get(index)
            .and_then(Value::as_str)
            .with_context(|| format!("notify field {index} missing"))
    };
    let extranonce2 = format!("{:0width$x}", counter, width = extranonce2_size * 2);
    ensure!(
        extranonce2.len() == extranonce2_size * 2,
        "extranonce2 counter {counter} exceeds {extranonce2_size} bytes"
    );
    let coinbase = hex::decode(format!(
        "{}{extranonce1}{extranonce2}{}",
        field(2)?,
        field(3)?
    ))?;
    let mut merkle = double_sha256(&coinbase);
    for sibling in params
        .get(4)
        .and_then(Value::as_array)
        .context("merkle branch missing")?
    {
        let sibling = hex::decode(sibling.as_str().context("invalid sibling")?)?;
        merkle = double_sha256(&[merkle.as_slice(), sibling.as_slice()].concat());
    }
    let mut previous = hex::decode(field(1)?)?;
    for word in previous.as_chunks_mut::<4>().0 {
        word.reverse();
    }
    let bits = parse_u32_hex(field(6)?)?;
    let block_target = target_from_compact(bits)?;
    let share_target = difficulty_target(difficulty)?;
    let ntime = parse_u32_hex(field(7)?)?;
    let base_version = parse_u32_hex(field(5)?)?;
    let (version, version_bits) = if version_mask == 0 {
        (base_version, None)
    } else {
        let rolled = version_mask.isolate_lowest_one();
        let version = (base_version & !version_mask) | ((base_version ^ rolled) & version_mask);
        (version, Some(format!("{:08x}", version & version_mask)))
    };
    for nonce in 0..1_000_000u32 {
        let header = [
            version.to_le_bytes().as_slice(),
            previous.as_slice(),
            merkle.as_slice(),
            ntime.to_le_bytes().as_slice(),
            bits.to_le_bytes().as_slice(),
            nonce.to_le_bytes().as_slice(),
        ]
        .concat();
        let hash = double_sha256(&header);
        let value = BigUint::from_bytes_le(&hash);
        let found = match proof {
            Proof::Share => value <= share_target && value > block_target,
            Proof::Block => value <= block_target && value <= share_target,
        };
        if found {
            return Ok(Solution {
                extranonce2,
                ntime: format!("{ntime:08x}"),
                nonce: format!("{nonce:08x}"),
                version_bits,
                hash: hash_display(&hash),
            });
        }
    }
    bail!("no {proof:?} solution in the nonce budget (share difficulty {difficulty})")
}

/// A subscribed and authorized session with the latest job and difficulty.
pub(crate) struct ShareClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    pub username: String,
    extranonce1: String,
    extranonce2_size: usize,
    difficulty: Option<f64>,
    notify: Value,
    next_id: u64,
    counter: u64,
}

impl ShareClient {
    /// Subscribe and authorize `username` on `port`, and wait for work.
    pub(crate) async fn connect(port: u16, username: &str) -> Result<Self> {
        let stream = tokio::time::timeout(
            Duration::from_secs(10),
            TcpStream::connect(("127.0.0.1", port)),
        )
        .await
        .context("Stratum connect timed out")??;
        let (read, writer) = stream.into_split();
        let mut client = Self {
            reader: BufReader::new(read),
            writer,
            username: username.into(),
            extranonce1: String::new(),
            extranonce2_size: 0,
            difficulty: None,
            notify: Value::Null,
            next_id: 10,
            counter: 0,
        };
        client
            .send(&json!({"id":1,"method":"mining.subscribe","params":["prism-live-share-client"]}))
            .await?;
        let subscribed = client.response(1).await?;
        client.extranonce1 = subscribed["result"][1]
            .as_str()
            .with_context(|| format!("subscribe: {subscribed}"))?
            .into();
        client.extranonce2_size = subscribed["result"][2]
            .as_u64()
            .context("extranonce2 size missing")?
            .try_into()?;
        client
            .send(&json!({"id":2,"method":"mining.authorize","params":[client.username,"x"]}))
            .await?;
        let authorized = client.response(2).await?;
        ensure!(authorized["result"] == true, "authorize: {authorized}");
        while client.notify.is_null() || client.difficulty.is_none() {
            client.read(Duration::from_secs(35)).await?;
        }
        Ok(client)
    }

    async fn send(&mut self, payload: &Value) -> Result<()> {
        self.writer
            .write_all(format!("{payload}\n").as_bytes())
            .await?;
        Ok(())
    }

    async fn read(&mut self, limit: Duration) -> Result<Value> {
        let mut line = String::new();
        let read = tokio::time::timeout(limit, self.reader.read_line(&mut line))
            .await
            .with_context(|| format!("no Stratum message within {limit:?}"))??;
        ensure!(read > 0, "Stratum connection closed");
        let message: Value = serde_json::from_str(&line)?;
        if message["method"] == "mining.set_difficulty" {
            self.difficulty = message["params"][0].as_f64();
        }
        if message["method"] == "mining.notify" {
            self.notify = message.clone();
        }
        Ok(message)
    }

    async fn response(&mut self, id: u64) -> Result<Value> {
        loop {
            let message = self.read(Duration::from_secs(35)).await?;
            if message["id"] == id {
                return Ok(message);
            }
        }
    }

    /// Until the latest job builds on `parent` (display order).
    pub(crate) async fn work_on(&mut self, parent: &str, limit: Duration) -> Result<()> {
        let mut wire = hex::decode(parent)?;
        wire.reverse();
        for word in wire.as_chunks_mut::<4>().0 {
            word.reverse();
        }
        let expected = hex::encode(wire);
        let deadline = Instant::now() + limit;
        while self.notify["params"][1].as_str() != Some(expected.as_str()) {
            let left = deadline.saturating_duration_since(Instant::now());
            ensure!(!left.is_zero(), "no work on {parent} within {limit:?}");
            self.read(left).await?;
        }
        Ok(())
    }

    /// Solve the latest job for `proof`, submit it, and wait for the answer.
    /// An error means nothing was submitted; a lost connection after the
    /// submission is [`Answer::Unanswered`].
    pub(crate) async fn submit(&mut self, proof: Proof) -> Result<Submitted> {
        self.counter += 1;
        let solution = solve(
            &self.notify,
            &self.extranonce1,
            self.extranonce2_size,
            self.difficulty.context("share difficulty missing")?,
            0,
            self.counter,
            proof,
        )?;
        let job = self.notify["params"][0]
            .as_str()
            .context("job id missing")?
            .to_owned();
        self.next_id += 1;
        let id = self.next_id;
        let share_id = format!("{}:{}", self.username, solution.hash);
        let submitted = |answer| Submitted {
            share_id: share_id.clone(),
            hash: solution.hash.clone(),
            answer,
        };
        let request = json!({"id":id,"method":"mining.submit","params":[
            self.username, job, solution.extranonce2, solution.ntime, solution.nonce]});
        if let Err(error) = self.send(&request).await {
            return Ok(submitted(Answer::Unanswered(format!("write: {error}"))));
        }
        let deadline = Instant::now() + Duration::from_secs(ANSWER_SECONDS);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let message = match self.read(left).await {
                Ok(message) => message,
                Err(error) => return Ok(submitted(Answer::Unanswered(format!("{error:#}")))),
            };
            if message["id"] != id {
                continue;
            }
            let answer = if message["result"] == true && message["error"].is_null() {
                Answer::Accepted
            } else {
                Answer::Rejected(message)
            };
            return Ok(submitted(answer));
        }
    }
}
