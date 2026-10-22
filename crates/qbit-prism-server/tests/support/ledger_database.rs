//! One PostgreSQL database per ledger test fixture.
//!
//! PostgreSQL scopes advisory locks to a database, not a schema, so fixtures
//! that shared one database queued on each other's migration, order and
//! settlement keys. A fixture here owns a generated database, keeps its own
//! schema on `search_path` inside it, and drops the database however the test
//! ends: `close`, a failed setup, an early return, a panic or a cancellation.
use anyhow::{anyhow, ensure, Context, Result};
use percent_encoding::percent_decode_str;
use sqlx::{postgres::PgPoolOptions, Connection, PgConnection, PgPool};
use std::io::Write;
use std::time::Duration;

/// Bounds every cleanup that runs from a fresh session.
const CLEANUP_WAIT: Duration = Duration::from_secs(10);
/// How long cleanup waits for the fixture's old admin session to end.
const SESSION_END_MS: i64 = 5_000;

/// The admin session that sent CREATE DATABASE, as the server identifies it,
/// so cleanup ends exactly that session and never one that reused its PID.
#[derive(Clone)]
struct Session {
    pid: i32,
    started: String,
}

pub struct FixtureDatabase {
    /// One connection to the maintenance database named by the gate's URL:
    /// it creates and drops the fixture database, and tests may read
    /// cluster-wide catalogs through it.
    pub admin: PgPool,
    /// The gate's URL. It may hold a password, so no message prints it.
    raw: String,
    session: Session,
    name: String,
    pub schema: String,
    /// The fixture database, with `search_path` set to `schema`.
    pub url: String,
    /// Whether the database may exist and has not been cleaned up yet.
    armed: bool,
    /// CREATE DATABASE failed without a server error, so its reply was lost
    /// and `session` may still be running it. A drop from any other session
    /// could run before that CREATE commits and miss the database.
    creation_unknown: bool,
}

/// A generated identifier: a lowercase letter, then lowercase letters, digits
/// and `_`, at most 63 bytes. PostgreSQL never truncates it, and it can be
/// double-quoted without escaping.
pub fn identifier(name: &str) -> Result<&str> {
    let mut bytes = name.bytes();
    ensure!(
        name.len() <= 63
            && bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
            && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "{name:?} is not a generated PostgreSQL identifier"
    );
    Ok(name)
}

/// The database and schema names for a new fixture.
pub fn generated_names(schema_prefix: &str) -> (String, String) {
    let id = uuid::Uuid::new_v4().simple();
    (
        format!("prism_fixture_{id}"),
        format!("{schema_prefix}{id}"),
    )
}

/// Validates the gate's URL and derives the fixture's from it: the same
/// server, credentials and connection options, the fixture database as the
/// path, and `search_path` appended last so it overrides any earlier
/// `options` value.
pub fn fixture_url(raw: &str, database: &str, schema: &str) -> Result<String> {
    // url's parse errors never quote the input.
    let mut url = url::Url::parse(raw)
        .map_err(|error| anyhow!("the test database URL is not a valid URL: {error}"))?;
    ensure!(
        matches!(url.scheme(), "postgres" | "postgresql") && !url.cannot_be_a_base(),
        "the test database URL must be a postgres:// or postgresql:// URL"
    );
    // sqlx lets a dbname parameter override the path, which would put every
    // fixture back in the maintenance database.
    ensure!(
        !url.query_pairs().any(|(key, _)| key == "dbname"),
        "the test database URL must name its database in the path, not with dbname="
    );
    let maintenance = percent_decode_str(url.path().trim_start_matches('/'))
        .decode_utf8()
        .context("the test database URL's database name is not UTF-8")?;
    ensure!(
        !is_template(&maintenance),
        "the test database URL names {maintenance}; fixtures copy template1, which no other session may be using, so connect through a maintenance database such as postgres"
    );
    url.set_path(&format!("/{}", identifier(database)?));
    url.query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={}", identifier(schema)?));
    Ok(url.into())
}

fn is_template(database: &str) -> bool {
    matches!(database, "template0" | "template1")
}

fn drop_statement(name: &str) -> String {
    // FORCE ends sessions a test left in this database; it touches no other.
    format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)")
}

impl FixtureDatabase {
    /// Creates a fixture database and a `schema_prefix` schema inside it.
    /// `raw` is the URL the calling test's gate returned.
    pub async fn open(raw: &str, schema_prefix: &str) -> Result<Self> {
        let (name, schema) = generated_names(schema_prefix);
        Self::open_named(raw, &name, &schema).await
    }

    /// `open` with a given schema, so a regression can make setup fail after
    /// the database exists. `name` must have the generated fixture form, and a
    /// name that is already taken is refused without dropping anything.
    pub async fn open_named(raw: &str, name: &str, schema: &str) -> Result<Self> {
        ensure!(
            name.strip_prefix("prism_fixture_").is_some_and(|id| {
                id.len() == 32
                    && id
                        .bytes()
                        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            }),
            "{name:?} is not a generated fixture database name"
        );
        let url = fixture_url(raw, name, schema)?;
        let admin = PgPoolOptions::new().max_connections(1).connect(raw).await?;
        let mut connection = match admin.acquire().await {
            Ok(connection) => connection,
            Err(error) => {
                admin.close().await;
                return Err(error.into());
            }
        };
        let probe = sqlx::query_as::<_, (i32, String, String, bool)>(
            "SELECT a.pid, a.backend_start::text, d.datname::text, d.datistemplate FROM pg_stat_activity a, pg_database d WHERE a.pid=pg_backend_pid() AND d.datname=current_database()",
        )
        .fetch_one(&mut *connection)
        .await;
        // The URL check cannot see a database the environment chose.
        let session = match probe {
            Ok((pid, started, maintenance, template))
                if !template && !is_template(&maintenance) =>
            {
                Session { pid, started }
            }
            outcome => {
                drop(connection);
                admin.close().await;
                return Err(match outcome {
                    Ok((_, _, maintenance, _)) => anyhow!(
                        "the test database URL reaches template database {maintenance}; connect through a maintenance database such as postgres"
                    ),
                    Err(error) => error.into(),
                });
            }
        };
        // Armed before CREATE DATABASE is sent: if this future is dropped or
        // the reply is lost, the database may exist, and Drop reconciles it.
        let mut fixture = Self {
            admin,
            raw: raw.to_owned(),
            session,
            name: name.to_owned(),
            schema: schema.to_owned(),
            url,
            armed: true,
            creation_unknown: false,
        };
        let created = sqlx::raw_sql(&format!("CREATE DATABASE \"{name}\""))
            .execute(&mut *connection)
            .await;
        // Return the pool's only connection before any cleanup needs it.
        drop(connection);
        if let Err(error) = created {
            let code = error
                .as_database_error()
                .and_then(|error| error.code())
                .map(|code| code.into_owned());
            let error = anyhow::Error::from(error);
            let error = match code.as_deref() {
                // The name is taken, so this fixture created nothing and must
                // not drop a database it does not own.
                Some("42P04") => {
                    fixture.armed = false;
                    fixture.admin.close().await;
                    return Err(error.context(format!(
                        "fixture database {name} already exists; it was left untouched"
                    )));
                }
                Some("42501") => error.context(
                    "ledger fixtures create one database each, so the test database role needs CREATEDB",
                ),
                Some("55006") => error.context(
                    "CREATE DATABASE copies template1, so no other session may be connected to it",
                ),
                // The server answered, so the CREATE rolled back.
                Some(_) => error.context(format!("creating fixture database {name}")),
                // No server answer: the CREATE may still commit.
                None => {
                    fixture.creation_unknown = true;
                    error.context(format!(
                        "creating fixture database {name} ended without a server reply, so whether it exists is unknown"
                    ))
                }
            };
            return Err(fixture.abandon(error).await);
        }
        let schema_created = async {
            let mut connection = PgConnection::connect(&fixture.url).await?;
            let created = sqlx::raw_sql(&format!("CREATE SCHEMA \"{schema}\""))
                .execute(&mut connection)
                .await;
            let closed = connection.close().await;
            created?;
            closed?;
            anyhow::Ok(())
        }
        .await;
        match schema_created {
            Ok(()) => Ok(fixture),
            Err(error) => Err(fixture
                .abandon(error.context(format!("creating schema {schema}")))
                .await),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Drops the database. Callers close the pools they own first; FORCE ends
    /// any session a test left behind. A test error wins over a cleanup
    /// error, which is then attached to it as context.
    pub async fn close(self, result: Result<()>) -> Result<()> {
        match result {
            Ok(()) => {
                let mut fixture = self;
                fixture.drop_database().await
            }
            Err(error) => Err(self.abandon(error).await),
        }
    }

    /// Drops the database after `error` and returns `error`, with any
    /// cleanup failure attached as context.
    pub async fn abandon(mut self, error: anyhow::Error) -> anyhow::Error {
        match self.drop_database().await {
            Ok(()) => error,
            Err(cleanup) => error.context(format!(
                "removal of fixture database {} could not be confirmed: {cleanup:#}",
                self.name
            )),
        }
    }

    async fn drop_database(&mut self) -> Result<()> {
        let outcome = if self.creation_unknown {
            // The admin pool may already hold a new session whose drop would
            // find nothing while the original CREATE is still running, so end
            // that session before looking for the database.
            drop_from_fresh_session(&self.raw, &self.name, &self.session).await
        } else {
            match sqlx::raw_sql(&drop_statement(&self.name))
                .execute(&self.admin)
                .await
            {
                Ok(_) => Ok(()),
                // The admin session may have been lost mid-statement. The drop
                // is idempotent and names only this fixture's database, so
                // repeat it from a fresh session.
                Err(error) => drop_from_fresh_session(&self.raw, &self.name, &self.session)
                    .await
                    .map_err(|retry| {
                        anyhow::Error::from(error).context(format!(
                            "the retry from a fresh session also failed: {retry:#}"
                        ))
                    }),
            }
        };
        // Disarm only once the database is gone: a failed or cancelled cleanup
        // leaves Drop its bounded fallback.
        if outcome.is_ok() {
            self.armed = false;
        }
        self.admin.close().await;
        outcome
    }
}

/// Drops `name` over a new connection, after ending the fixture's admin
/// session: a CREATE DATABASE whose reply was lost may still be running there,
/// and a drop that ran before it committed would miss the database.
async fn drop_from_fresh_session(raw: &str, name: &str, session: &Session) -> Result<()> {
    tokio::time::timeout(CLEANUP_WAIT, async {
        let mut connection = PgConnection::connect(raw).await?;
        let ended: Option<bool> = sqlx::query_scalar(
            "SELECT pg_terminate_backend(pid, $3) FROM pg_stat_activity WHERE pid=$1 AND backend_start::text=$2",
        )
        .bind(session.pid)
        .bind(&session.started)
        .bind(SESSION_END_MS)
        .fetch_optional(&mut connection)
        .await?;
        ensure!(
            ended != Some(false),
            "the fixture's admin session did not end within {SESSION_END_MS} ms"
        );
        sqlx::raw_sql(&drop_statement(name))
            .execute(&mut connection)
            .await?;
        connection.close().await?;
        Ok(())
    })
    .await
    .with_context(|| {
        format!("cleanup did not finish within {CLEANUP_WAIT:?}; the server may still complete it")
    })?
}

impl Drop for FixtureDatabase {
    /// The fallback for a fixture that was never closed. It must not rely on
    /// the test's runtime, which may be shutting down, so it runs on its own
    /// thread and runtime, and it never panics.
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (raw, name, session) = (self.raw.clone(), self.name.clone(), self.session.clone());
        let fallback = move || -> Result<()> {
            let thread = std::thread::Builder::new()
                .name("fixture-database-drop".into())
                .spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?
                        .block_on(drop_from_fresh_session(&raw, &name, &session))
                })?;
            thread
                .join()
                .map_err(|_| anyhow!("the drop thread panicked"))?
        };
        // A multi-thread runtime hands this worker's other tasks elsewhere
        // while it waits; a current-thread runtime has nothing else to run.
        let outcome = match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(fallback)
            }
            _ => fallback(),
        };
        if let Err(error) = outcome {
            let _ = writeln!(
                std::io::stderr(),
                "ledger fixture: removal of database {} could not be confirmed: {error:#}",
                self.name
            );
        }
    }
}
