//! Stratum v1 clients for the live suite that solve work in the test and
//! record exactly what the server answered (#575).
//!
//! [`StratumSession`] is one connection and what the server has told it:
//! extranonce, difficulty, latest job, version mask, and every notification
//! in order, each checked for the shape miners parse. [`ShareClient`] is a
//! scripted miner on top of it that submits shares or blocks and records the
//! answer to each, so a scenario can compare every acknowledged share with
//! the ledger. The transcript replay drives a session directly.
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
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    time::error::Elapsed,
};

/// Server settings under which a share can miss the regtest block target:
/// a fixed share difficulty of about 2^-32, whose target is above regtest's.
pub(crate) const SHARE_ONLY_SETTINGS: [(&str, &str); 2] = [
    ("PRISM_STRATUM_VARDIFF", "0"),
    ("PRISM_STRATUM_SHARE_DIFF", "0.00000000025"),
];

/// How long a request waits for its answer: well past the server's own
/// bound, the share commit timeout plus grace (15 s by default), which also
/// bounds a block whose answer waits for its landing (#577).
pub(crate) const ANSWER: Duration = Duration::from_secs(70);

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
    /// No answer within [`ANSWER`].
    TimedOut,
    /// The connection failed before the answer.
    Lost(String),
}

impl Answer {
    pub(crate) fn accepted(&self) -> bool {
        matches!(self, Answer::Accepted)
    }

    /// The `reason_id` of a rejection's `[code, message, {"reason_id": ...}]`.
    pub(crate) fn reason_id(&self) -> Option<&str> {
        match self {
            Answer::Rejected(response) => reason_id(response),
            _ => None,
        }
    }
}

impl std::fmt::Display for Answer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Answer::Accepted => f.write_str("accepted"),
            Answer::Rejected(response) => match reason_id(response) {
                Some(reason) => f.write_str(reason),
                None => write!(f, "rejected {}", response["error"]),
            },
            Answer::TimedOut => f.write_str("unanswered: timed out"),
            Answer::Lost(why) => write!(f, "unanswered: connection lost ({why})"),
        }
    }
}

/// The `reason_id` of a response's `[code, message, {"reason_id": ...}]` error.
pub(crate) fn reason_id(response: &Value) -> Option<&str> {
    response["error"][2]["reason_id"].as_str()
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
fn easiest_proof(notify: &Value, difficulty: f64) -> Result<Proof> {
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
fn solve(
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

/// Solve `notify` for a share that misses its block target, without
/// version rolling: the session load's proof (#553).
pub(crate) fn solve_share(
    notify: &Value,
    extranonce1: &str,
    extranonce2_size: usize,
    difficulty: f64,
    counter: u64,
) -> Result<Solution> {
    solve(
        notify,
        extranonce1,
        extranonce2_size,
        difficulty,
        0,
        counter,
        Proof::Share,
    )
}

/// A hex string of even length, of `length` digits when given.
pub(crate) fn is_hex(value: &Value, length: Option<usize>) -> bool {
    value.as_str().is_some_and(|text| {
        text.len() % 2 == 0
            && length.is_none_or(|length| text.len() == length)
            && text.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

/// A notification's parameters have the shape miners parse.
fn check_notification(message: &Value) -> Result<()> {
    let params = message["params"].as_array();
    let method = message["method"].as_str().unwrap_or_default();
    let valid = match method {
        "mining.notify" => params.is_some_and(|p| {
            p.len() == 9
                && p[0].as_str().is_some_and(|job| !job.is_empty())
                && is_hex(&p[1], Some(64))
                && is_hex(&p[2], None)
                && is_hex(&p[3], None)
                && p[4]
                    .as_array()
                    .is_some_and(|branch| branch.iter().all(|node| is_hex(node, Some(64))))
                && (5..=7).all(|index| is_hex(&p[index], Some(8)))
                && p[8].is_boolean()
        }),
        "mining.set_difficulty" => {
            params.is_some_and(|p| p.len() == 1 && p[0].as_f64().is_some_and(|d| d > 0.0))
        }
        "mining.set_version_mask" => params.is_some_and(|p| p.len() == 1 && is_hex(&p[0], Some(8))),
        _ => true,
    };
    ensure!(valid, "malformed {method}: {message}");
    Ok(())
}

/// One Stratum connection and what the server has told it so far.
pub(crate) struct StratumSession {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    pub extranonce1: Option<String>,
    pub extranonce2_size: Option<usize>,
    pub difficulty: Option<f64>,
    /// The latest `mining.notify`.
    pub notify: Option<Value>,
    /// The version-rolling mask granted by `mining.configure` or the latest
    /// `mining.set_version_mask`; zero when none.
    pub version_mask: u32,
    /// Every notification method, in arrival order.
    pub notifications: Vec<String>,
    counter: u64,
}

impl StratumSession {
    pub(crate) async fn open(port: u16) -> Result<Self> {
        let stream = tokio::time::timeout(
            Duration::from_secs(10),
            TcpStream::connect(("127.0.0.1", port)),
        )
        .await
        .context("Stratum connect timed out")??;
        let (read, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(read),
            writer,
            extranonce1: None,
            extranonce2_size: None,
            difficulty: None,
            notify: None,
            version_mask: 0,
            notifications: Vec::new(),
            counter: 0,
        })
    }

    pub(crate) async fn send(&mut self, message: &Value) -> Result<()> {
        self.writer
            .write_all(format!("{message}\n").as_bytes())
            .await?;
        Ok(())
    }

    /// The next message within `limit`, keeping and checking every
    /// notification. A timeout is an error that downcasts to [`Elapsed`].
    async fn next(&mut self, limit: Duration) -> Result<Value> {
        let mut line = String::new();
        let read = tokio::time::timeout(limit, self.reader.read_line(&mut line))
            .await
            .map_err(anyhow::Error::new)
            .with_context(|| format!("no Stratum message within {limit:?}"))??;
        ensure!(read > 0, "Stratum connection closed");
        let message: Value = serde_json::from_str(&line)?;
        if let Some(method) = message["method"].as_str() {
            check_notification(&message)?;
            self.notifications.push(method.to_owned());
            match method {
                "mining.set_difficulty" => self.difficulty = message["params"][0].as_f64(),
                "mining.notify" => self.notify = Some(message.clone()),
                "mining.set_version_mask" => {
                    self.version_mask =
                        parse_u32_hex(message["params"][0].as_str().context("mask missing")?)?;
                }
                _ => {}
            }
        }
        Ok(message)
    }

    /// The response to request `id`, within `limit`.
    pub(crate) async fn answer(&mut self, id: &Value, limit: Duration) -> Result<Value> {
        let deadline = Instant::now() + limit;
        loop {
            let message = self
                .next(deadline.saturating_duration_since(Instant::now()))
                .await?;
            if message.get("method").is_none() && &message["id"] == id {
                return Ok(message);
            }
        }
    }

    /// Send `request` and wait for its response; a subscribe or configure
    /// response also updates the session.
    pub(crate) async fn request(&mut self, request: &Value) -> Result<Value> {
        self.send(request).await?;
        let answer = self.answer(&request["id"], ANSWER).await?;
        self.learn(request["method"].as_str().unwrap_or_default(), &answer)?;
        Ok(answer)
    }

    /// Keep what a response to `method` tells the session.
    pub(crate) fn learn(&mut self, method: &str, answer: &Value) -> Result<()> {
        match method {
            "mining.subscribe" => {
                self.extranonce1 = answer["result"][1].as_str().map(str::to_owned);
                self.extranonce2_size = answer["result"][2].as_u64().map(|size| size as usize);
            }
            "mining.configure" => {
                if let Some(mask) = answer["result"]["version-rolling.mask"].as_str() {
                    self.version_mask = parse_u32_hex(mask)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Until a job and a share difficulty have arrived.
    pub(crate) async fn work(&mut self) -> Result<()> {
        while self.notify.is_none() || self.difficulty.is_none() {
            self.next(ANSWER).await?;
        }
        Ok(())
    }

    /// Until a job other than the latest one arrives.
    pub(crate) async fn newer_work(&mut self) -> Result<()> {
        let job = |session: &Self| {
            session
                .notify
                .as_ref()
                .map(|notify| notify["params"][0].clone())
        };
        let latest = job(self);
        while job(self) == latest {
            self.next(ANSWER).await?;
        }
        Ok(())
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
        while self
            .notify
            .as_ref()
            .and_then(|notify| notify["params"][1].as_str())
            != Some(expected.as_str())
        {
            let left = deadline.saturating_duration_since(Instant::now());
            ensure!(!left.is_zero(), "no work on {parent} within {limit:?}");
            self.next(left).await?;
        }
        Ok(())
    }

    /// Solve the latest job as `username`: for `proof`, or the easiest proof
    /// the job allows when `None`; with version rolling when `rolls`. Returns
    /// the `mining.submit` params and the ledger's share id.
    pub(crate) async fn solve_latest(
        &mut self,
        username: &str,
        proof: Option<Proof>,
        rolls: bool,
    ) -> Result<(Value, Submitted)> {
        self.work().await?;
        ensure!(
            !rolls || self.version_mask != 0,
            "version rolling needs a granted mask"
        );
        self.counter += 1;
        let notify = self.notify.as_ref().context("no job")?;
        let difficulty = self.difficulty.context("no difficulty")?;
        let proof = match proof {
            Some(proof) => proof,
            None => easiest_proof(notify, difficulty)?,
        };
        let solution = solve(
            notify,
            self.extranonce1.as_deref().context("no extranonce1")?,
            self.extranonce2_size.context("no extranonce2 size")?,
            difficulty,
            if rolls { self.version_mask } else { 0 },
            self.counter,
            proof,
        )?;
        let mut params = vec![
            json!(username),
            notify["params"][0].clone(),
            json!(solution.extranonce2),
            json!(solution.ntime),
            json!(solution.nonce),
        ];
        if rolls {
            params.push(json!(solution.version_bits.context("no version bits")?));
        }
        let submitted = Submitted {
            share_id: format!("{username}:{}", solution.hash),
            hash: solution.hash,
            answer: Answer::TimedOut,
        };
        Ok((Value::Array(params), submitted))
    }
}

/// A subscribed and authorized miner that submits one proof at a time.
pub(crate) struct ShareClient {
    session: StratumSession,
    pub username: String,
    next_id: u64,
}

impl ShareClient {
    /// Subscribe and authorize `username` on `port`, and wait for work.
    pub(crate) async fn connect(port: u16, username: &str) -> Result<Self> {
        let mut session = StratumSession::open(port).await?;
        let subscribed = session
            .request(
                &json!({"id":1,"method":"mining.subscribe","params":["prism-live-share-client"]}),
            )
            .await?;
        ensure!(
            session.extranonce1.is_some() && session.extranonce2_size.is_some(),
            "subscribe: {subscribed}"
        );
        let authorized = session
            .request(&json!({"id":2,"method":"mining.authorize","params":[username,"x"]}))
            .await?;
        ensure!(authorized["result"] == true, "authorize: {authorized}");
        session.work().await?;
        Ok(Self {
            session,
            username: username.into(),
            next_id: 10,
        })
    }

    /// Until the latest job builds on `parent` (display order).
    pub(crate) async fn work_on(&mut self, parent: &str, limit: Duration) -> Result<()> {
        self.session.work_on(parent, limit).await
    }

    /// Until a job other than the latest one read so far arrives.
    pub(crate) async fn newer_work(&mut self) -> Result<()> {
        self.session.newer_work().await
    }

    /// The id of the latest job read so far, the one [`ShareClient::solve`]
    /// solves next; reads nothing more from the server.
    pub(crate) fn job_id(&self) -> Option<&str> {
        self.session
            .notify
            .as_ref()
            .and_then(|notify| notify["params"][0].as_str())
    }

    /// Solve the latest job for `proof`, submit it, and wait for the answer.
    /// An error means nothing was submitted; a failure after the submission
    /// is [`Answer::TimedOut`] or [`Answer::Lost`].
    pub(crate) async fn submit(&mut self, proof: Proof) -> Result<Submitted> {
        let (params, submitted) = self.solve(proof).await?;
        Ok(self.submit_solved(params, submitted).await)
    }

    /// Solve the latest job for `proof` without submitting it: work a miner
    /// keeps, to submit later with [`ShareClient::submit_solved`] (#474).
    pub(crate) async fn solve(&mut self, proof: Proof) -> Result<(Value, Submitted)> {
        self.session
            .solve_latest(&self.username, Some(proof), false)
            .await
    }

    /// Submit `params`, solved earlier, and wait for the answer, recorded in
    /// `submitted`.
    pub(crate) async fn submit_solved(
        &mut self,
        params: Value,
        mut submitted: Submitted,
    ) -> Submitted {
        self.next_id += 1;
        let id = json!(self.next_id);
        let request = json!({"id":id,"method":"mining.submit","params":params});
        submitted.answer = match self.session.send(&request).await {
            Err(error) => Answer::Lost(format!("write: {error}")),
            Ok(()) => match self.session.answer(&id, ANSWER).await {
                Ok(message) if message["result"] == true && message["error"].is_null() => {
                    Answer::Accepted
                }
                Ok(message) => Answer::Rejected(message),
                Err(error) if error.downcast_ref::<Elapsed>().is_some() => Answer::TimedOut,
                Err(error) => Answer::Lost(format!("{error:#}")),
            },
        };
        submitted
    }
}
