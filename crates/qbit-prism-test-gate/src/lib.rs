//! One integration gate for every test whose execution depends on an
//! environment input (#286).
//!
//! A gated test asks the gate for the inputs it needs at the top of its body,
//! from the thread libtest runs the test on. The gate reads them, applies one
//! decision table, records the decision in the execution manifest, and then
//! hands the values back, fails the test, or prints one skip line and tells
//! the test to return.
//!
//! | input set and non-empty | switch | result |
//! | --- | --- | --- |
//! | yes | -- | run |
//! | no | `PRISM_TEST_REQUIRE_INTEGRATION=1` | fail, naming the missing variable and the test |
//! | no | `GITHUB_JOB=prism-native-postgres` | fail, same |
//! | no | -- | print one skip line with a fixed, greppable prefix, and return |
//!
//! The three variables play different roles. This repository sets the inputs
//! itself, in the `prism-native-postgres` job of `.github/workflows/ci.yml`
//! and in `test/prism-native-tests.sh`. GitHub sets `GITHUB_JOB` to the
//! running job's id, so matching it means a database outage in that job
//! surfaces as a failure instead of a silent pass even if the job's own
//! environment were edited. `PRISM_TEST_REQUIRE_INTEGRATION=1` is the opt-in
//! switch for any run that wants every integration test to be mandatory.
//!
//! Keying on `CI` would be wrong: GitHub sets `CI=true` in every job,
//! including `rust-tests`, which runs the whole workspace with no database.
//!
//! A variable whose value is not valid Unicode is an error, never "absent":
//! treating it as unset would let a deliberately configured run skip a test
//! without a word. An empty or whitespace-only value counts as unset. A
//! non-empty but malformed value is not second-guessed here; it reaches the
//! test, which fails with the connection or spawn error an operator needs.
//!
//! When `PRISM_TEST_GATE_MANIFEST` names a file, every decision appends one
//! line to it, `executed <id>`, `skipped <id>` or `failed <id>`, where the id
//! is `<package>::<binary>::<test path>`. Each line is one `O_APPEND` write,
//! so parallel test threads and several test binaries can share the file.
//! `scripts/check_gate_manifest.py` compares it with the checked-in list of
//! gated tests in `test/prism-gated-tests.txt`.
//!
//! `scripts/check_gate_env_reads.py` fails when any Rust file outside this
//! crate names one of the gate variables in a string literal, so a new gated
//! test cannot read them directly and bypass the table.

use std::env::VarError;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

/// The opt-in switch: `1` makes every missing input a failure.
pub const SWITCH_VAR: &str = "PRISM_TEST_REQUIRE_INTEGRATION";
/// Set by GitHub to the id of the running job.
pub const JOB_VAR: &str = "GITHUB_JOB";
/// The job whose id demands the integration suite.
pub const REQUIRED_JOB: &str = "prism-native-postgres";
/// Path of the execution manifest; unset means no manifest.
pub const MANIFEST_VAR: &str = "PRISM_TEST_GATE_MANIFEST";
/// Fixed prefix of every skip line the gate prints.
pub const SKIP_PREFIX: &str = "[prism-test-gate] skipped";

/// An environment input a gated test can depend on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Input {
    /// `PRISM_TEST_DATABASE_URL`: a disposable PostgreSQL the test may write to.
    DatabaseUrl,
    /// `PRISM_TEST_PG_BIN_DIR`: a directory with `initdb`, `pg_ctl` and
    /// `pg_basebackup`, for tests that run their own clusters.
    PgBinDir,
    /// `QBITD_BIN`: a `qbitd` executable for regtest.
    QbitdBin,
}

impl Input {
    /// Every input, in the order the manifest and messages list them.
    pub const ALL: [Input; 3] = [Input::DatabaseUrl, Input::PgBinDir, Input::QbitdBin];

    /// The environment variable that carries the input.
    pub const fn name(self) -> &'static str {
        match self {
            Input::DatabaseUrl => "PRISM_TEST_DATABASE_URL",
            Input::PgBinDir => "PRISM_TEST_PG_BIN_DIR",
            Input::QbitdBin => "QBITD_BIN",
        }
    }

    fn read(self) -> Raw {
        std::env::var(self.name())
    }
}

impl fmt::Display for Input {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A raw environment read, exactly as `std::env::var` reports it.
pub type Raw = Result<String, VarError>;

/// What made a missing input a failure rather than a skip.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Demand {
    /// `PRISM_TEST_REQUIRE_INTEGRATION=1`.
    Switch,
    /// `GITHUB_JOB=prism-native-postgres`.
    Job,
    /// The test was selected explicitly (`#[ignore]`, run with `--ignored`),
    /// so a vacuous pass would be exactly what the selection tried to avoid.
    Explicit,
}

impl fmt::Display for Demand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Demand::Switch => write!(f, "{SWITCH_VAR}=1"),
            Demand::Job => write!(f, "{JOB_VAR}={REQUIRED_JOB}"),
            Demand::Explicit => f.write_str("the explicit selection of this test"),
        }
    }
}

/// The outcome of the decision table for one test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Every input is present; the values, trimmed, in the order requested.
    Run(Vec<String>),
    /// At least one input is missing and something demands the suite.
    Fail { missing: Vec<Input>, demand: Demand },
    /// At least one input is missing and nothing demands the suite.
    Skip { missing: Vec<Input> },
}

/// Why the gate could not give a test its inputs.
#[derive(Debug)]
pub enum Error {
    /// A variable is set to a value that is not valid Unicode.
    Unreadable {
        variable: &'static str,
        detail: String,
    },
    /// The switch holds something other than `1`, `0` or nothing.
    BadSwitch { value: String },
    /// An input is missing and something demands it.
    Required {
        test: String,
        missing: Vec<Input>,
        demand: Demand,
    },
    /// The gate was called from a thread libtest did not name after a test.
    UnnamedThread { thread: String },
    /// The manifest could not be appended to.
    Manifest {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Unreadable { variable, detail } => {
                write!(f, "{variable} is set but not readable: {detail}")
            }
            Error::BadSwitch { value } => write!(
                f,
                "{SWITCH_VAR}={value:?} is not recognised; set it to 1 to require every \
                 integration test, or leave it unset"
            ),
            Error::Required {
                test,
                missing,
                demand,
            } => write!(
                f,
                "{test} requires {}: {} while {demand} demands the integration suite",
                join(missing, ", "),
                unset(missing),
            ),
            Error::UnnamedThread { thread } => write!(
                f,
                "the integration gate must be called from the test's own thread (libtest names \
                 it after the test), not from {thread:?}; call it at the top of the test body"
            ),
            Error::Manifest { path, source } => {
                write!(
                    f,
                    "cannot append to {MANIFEST_VAR}={}: {source}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Manifest { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn join(inputs: &[Input], separator: &str) -> String {
    inputs
        .iter()
        .map(|input| input.name())
        .collect::<Vec<_>>()
        .join(separator)
}

fn unset(inputs: &[Input]) -> String {
    match inputs {
        [one] => format!("{one} is unset or empty"),
        many => format!("{} are unset or empty", join(many, " and ")),
    }
}

/// Reads one variable, treating a non-Unicode value as an error.
fn readable(variable: &'static str, raw: &Raw) -> Result<Option<String>, Error> {
    match raw {
        Ok(value) => Ok(Some(value.clone())),
        Err(VarError::NotPresent) => Ok(None),
        Err(error) => Err(Error::Unreadable {
            variable,
            detail: error.to_string(),
        }),
    }
}

/// The decision table over injected values, with no environment access.
///
/// `values[i]` is the raw read of `inputs[i]`. The switch is consulted before
/// the job id, so a run that sets both is reported as demanded by the switch.
pub fn decide(
    inputs: &[Input],
    values: &[Raw],
    switch: &Raw,
    job: &Raw,
) -> Result<Decision, Error> {
    assert_eq!(
        inputs.len(),
        values.len(),
        "one raw value per requested input"
    );
    let mut present = Vec::with_capacity(inputs.len());
    let mut missing = Vec::new();
    for (input, raw) in inputs.iter().zip(values) {
        match readable(input.name(), raw)? {
            Some(value) if !value.trim().is_empty() => present.push(value.trim().to_owned()),
            _ => missing.push(*input),
        }
    }
    let switch_on = match readable(SWITCH_VAR, switch)?.as_deref().map(str::trim) {
        None | Some("") | Some("0") => false,
        Some("1") => true,
        Some(other) => {
            return Err(Error::BadSwitch {
                value: other.to_owned(),
            })
        }
    };
    let job_demands = readable(JOB_VAR, job)?.as_deref() == Some(REQUIRED_JOB);
    if missing.is_empty() {
        Ok(Decision::Run(present))
    } else if switch_on {
        Ok(Decision::Fail {
            missing,
            demand: Demand::Switch,
        })
    } else if job_demands {
        Ok(Decision::Fail {
            missing,
            demand: Demand::Job,
        })
    } else {
        Ok(Decision::Skip { missing })
    }
}

/// Where a gated test lives: the Cargo package and the test binary. Build it
/// with [`site!`] so both names come from the calling crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Site {
    /// `CARGO_PKG_NAME` of the crate holding the test.
    pub package: &'static str,
    /// `CARGO_CRATE_NAME` of the test binary: the integration test's file
    /// stem, or the library's crate name for `#[cfg(test)]` modules.
    pub binary: &'static str,
}

/// The [`Site`] of the calling crate and test binary.
#[macro_export]
macro_rules! site {
    () => {
        $crate::Site {
            package: env!("CARGO_PKG_NAME"),
            binary: env!("CARGO_CRATE_NAME"),
        }
    };
}

impl Site {
    /// `<package>::<binary>::<test path>`, where the test path is the name
    /// libtest gave the thread it runs the test on (`module::test_fn`).
    pub fn test_id(self) -> Result<String, Error> {
        let thread = std::thread::current();
        let name = match thread.name() {
            Some(name) if !name.is_empty() && name != "main" && !name.starts_with("tokio-") => name,
            other => {
                return Err(Error::UnnamedThread {
                    thread: other.unwrap_or("<unnamed>").to_owned(),
                })
            }
        };
        Ok(format!("{}::{}::{name}", self.package, self.binary))
    }
}

/// Appends `<kind> <id>` to the manifest at `path` as one `O_APPEND` write.
///
/// The whole line is built first and handed to a single `write`, never
/// `write_all`, which could split it across two writes after a short write and
/// let another thread's line land in between. A short write is an error.
pub fn record_to(path: &std::path::Path, kind: &str, id: &str) -> Result<(), Error> {
    let line = format!("{kind} {id}\n");
    let manifest = |source| Error::Manifest {
        path: path.to_path_buf(),
        source,
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(manifest)?;
    let written = file.write(line.as_bytes()).map_err(manifest)?;
    if written != line.len() {
        return Err(manifest(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            format!(
                "short write: {written} of {} bytes; the manifest line may be torn",
                line.len()
            ),
        )));
    }
    Ok(())
}

/// Records the decision in the manifest named by `PRISM_TEST_GATE_MANIFEST`,
/// if any.
fn record(kind: &str, id: &str) -> Result<(), Error> {
    match readable(MANIFEST_VAR, &std::env::var(MANIFEST_VAR))? {
        Some(path) if !path.trim().is_empty() => record_to(&PathBuf::from(path.trim()), kind, id),
        _ => Ok(()),
    }
}

/// Prints one line on the process's real stderr, past libtest's capture, so
/// a skip is visible without `--nocapture`. The line goes out in a single
/// `write`, so parallel test threads cannot interleave their lines; a short
/// write is not retried, since a retry would be the second write.
fn announce(line: &str) {
    let line = format!("{line}\n");
    #[cfg(unix)]
    {
        use std::os::fd::AsFd;
        if let Ok(fd) = std::io::stderr().as_fd().try_clone_to_owned() {
            let mut stderr = std::fs::File::from(fd);
            let _ = stderr.write(line.as_bytes());
            return;
        }
    }
    eprint!("{line}");
}

/// Applies the table to the process environment for `inputs`.
///
/// Returns the values in the order requested, or `None` after printing the
/// skip line. A demanded but missing input is an error, as is an unreadable
/// variable or an unwritable manifest.
pub fn inputs(site: Site, inputs: &[Input]) -> Result<Option<Vec<String>>, Error> {
    settle(site, inputs, false)
}

/// Like [`inputs`], for a test that is selected explicitly (`#[ignore]`, run
/// with `--ignored`): a missing input always fails, since nothing else stops
/// the explicit selection from passing without running. Every `#[ignore]`
/// gated test in the workspace uses this entry point.
pub fn required_inputs(site: Site, inputs: &[Input]) -> Result<Vec<String>, Error> {
    settle(site, inputs, true).map(|values| values.expect("explicit tests never skip"))
}

fn settle(site: Site, inputs: &[Input], explicit: bool) -> Result<Option<Vec<String>>, Error> {
    let id = site.test_id()?;
    let values: Vec<Raw> = inputs.iter().map(|input| input.read()).collect();
    let decision = decide(
        inputs,
        &values,
        &std::env::var(SWITCH_VAR),
        &std::env::var(JOB_VAR),
    )?;
    let (decision, demand) = match decision {
        Decision::Skip { missing } if explicit => (
            Decision::Fail {
                missing,
                demand: Demand::Explicit,
            },
            Some(Demand::Explicit),
        ),
        Decision::Fail { missing, demand } => (Decision::Fail { missing, demand }, Some(demand)),
        other => (other, None),
    };
    match decision {
        Decision::Run(values) => {
            record("executed", &id)?;
            Ok(Some(values))
        }
        Decision::Fail { missing, .. } => {
            record("failed", &id)?;
            Err(Error::Required {
                test: id,
                missing,
                demand: demand.expect("a failing decision names its demand"),
            })
        }
        Decision::Skip { missing } => {
            record("skipped", &id)?;
            announce(&format!(
                "{SKIP_PREFIX} {id}: {}; set {} to run it, or {SWITCH_VAR}=1 to fail instead",
                unset(&missing),
                join(&missing, " and "),
            ));
            Ok(None)
        }
    }
}

/// `PRISM_TEST_DATABASE_URL`, or `None` after the skip line.
pub fn database_url(site: Site) -> Result<Option<String>, Error> {
    Ok(inputs(site, &[Input::DatabaseUrl])?.map(|mut values| values.remove(0)))
}

/// `PRISM_TEST_DATABASE_URL` for an explicitly selected test; never skips.
pub fn required_database_url(site: Site) -> Result<String, Error> {
    Ok(required_inputs(site, &[Input::DatabaseUrl])?.remove(0))
}

/// `PRISM_TEST_PG_BIN_DIR`, or `None` after the skip line.
pub fn pg_bin_dir(site: Site) -> Result<Option<String>, Error> {
    Ok(inputs(site, &[Input::PgBinDir])?.map(|mut values| values.remove(0)))
}

/// `(QBITD_BIN, PRISM_TEST_DATABASE_URL)`, or `None` after the skip line.
pub fn qbitd_and_database_url(site: Site) -> Result<Option<(String, String)>, Error> {
    Ok(
        inputs(site, &[Input::QbitdBin, Input::DatabaseUrl])?.map(|mut values| {
            let database = values.remove(1);
            (values.remove(0), database)
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    fn set(value: &str) -> Raw {
        Ok(value.to_owned())
    }

    fn unset() -> Raw {
        Err(VarError::NotPresent)
    }

    #[cfg(unix)]
    fn unreadable() -> Raw {
        use std::os::unix::ffi::OsStringExt;
        Err(VarError::NotUnicode(OsString::from_vec(vec![0xff, 0xfe])))
    }

    #[cfg(not(unix))]
    fn unreadable() -> Raw {
        Err(VarError::NotUnicode(OsString::from("\u{FFFD}")))
    }

    const DB: &[Input] = &[Input::DatabaseUrl];
    const LIVE: &[Input] = &[Input::QbitdBin, Input::DatabaseUrl];

    #[test]
    fn present_inputs_run_whatever_the_switch_and_job_say() {
        for switch in [unset(), set("0"), set("1"), set("")] {
            for job in [unset(), set("rust-tests"), set(REQUIRED_JOB)] {
                let decision = decide(DB, &[set(" postgres://x/y ")], &switch, &job).unwrap();
                assert_eq!(decision, Decision::Run(vec!["postgres://x/y".into()]));
            }
        }
        let decision = decide(
            LIVE,
            &[set("/bin/qbitd"), set("postgres://x/y")],
            &unset(),
            &unset(),
        );
        assert_eq!(
            decision.unwrap(),
            Decision::Run(vec!["/bin/qbitd".into(), "postgres://x/y".into()])
        );
    }

    #[test]
    fn a_missing_input_fails_when_the_switch_is_on() {
        for job in [unset(), set("rust-tests"), set(REQUIRED_JOB)] {
            let decision = decide(DB, &[unset()], &set("1"), &job).unwrap();
            assert_eq!(
                decision,
                Decision::Fail {
                    missing: vec![Input::DatabaseUrl],
                    demand: Demand::Switch,
                }
            );
        }
    }

    #[test]
    fn a_missing_input_fails_in_the_native_job() {
        for switch in [unset(), set(""), set("0")] {
            let decision = decide(DB, &[unset()], &switch, &set(REQUIRED_JOB)).unwrap();
            assert_eq!(
                decision,
                Decision::Fail {
                    missing: vec![Input::DatabaseUrl],
                    demand: Demand::Job,
                }
            );
        }
    }

    #[test]
    fn a_missing_input_skips_when_nothing_demands_it() {
        for switch in [unset(), set(""), set("0")] {
            for job in [unset(), set("rust-tests"), set("")] {
                let decision = decide(DB, &[unset()], &switch, &job).unwrap();
                assert_eq!(
                    decision,
                    Decision::Skip {
                        missing: vec![Input::DatabaseUrl]
                    }
                );
            }
        }
    }

    #[test]
    fn empty_and_whitespace_values_count_as_unset() {
        for value in ["", " ", "\t\n"] {
            let decision = decide(DB, &[set(value)], &unset(), &unset()).unwrap();
            assert_eq!(
                decision,
                Decision::Skip {
                    missing: vec![Input::DatabaseUrl]
                }
            );
        }
    }

    #[test]
    fn every_missing_input_is_named_in_request_order() {
        let decision = decide(LIVE, &[unset(), set(" ")], &set("1"), &unset()).unwrap();
        assert_eq!(
            decision,
            Decision::Fail {
                missing: vec![Input::QbitdBin, Input::DatabaseUrl],
                demand: Demand::Switch,
            }
        );
        let decision = decide(LIVE, &[set("/bin/qbitd"), unset()], &unset(), &unset()).unwrap();
        assert_eq!(
            decision,
            Decision::Skip {
                missing: vec![Input::DatabaseUrl]
            }
        );
    }

    #[test]
    fn an_unreadable_variable_is_an_error_never_absent() {
        let error = decide(DB, &[unreadable()], &unset(), &unset()).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("PRISM_TEST_DATABASE_URL is set but not readable"),
            "{error}"
        );
        let error = decide(DB, &[set("postgres://x/y")], &unreadable(), &unset()).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("PRISM_TEST_REQUIRE_INTEGRATION is set but not readable"),
            "{error}"
        );
        let error = decide(DB, &[unset()], &unset(), &unreadable()).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("GITHUB_JOB is set but not readable"),
            "{error}"
        );
        // Even a present input does not excuse an unreadable sibling.
        let error = decide(LIVE, &[set("/bin/qbitd"), unreadable()], &unset(), &unset());
        assert!(error.is_err());
    }

    #[test]
    fn an_unrecognised_switch_value_is_an_error() {
        for value in ["yes", "true", "2", "on"] {
            let error = decide(DB, &[unset()], &set(value), &unset()).unwrap_err();
            assert!(
                matches!(&error, Error::BadSwitch { value: got } if got == value),
                "{error}"
            );
            // Also when the inputs are present: a typo must not pass silently.
            assert!(decide(DB, &[set("postgres://x/y")], &set(value), &unset()).is_err());
        }
        assert!(decide(DB, &[unset()], &set(" 1 "), &unset()).is_ok());
    }

    #[test]
    fn the_switch_is_named_before_the_job_when_both_demand() {
        let decision = decide(DB, &[unset()], &set("1"), &set(REQUIRED_JOB)).unwrap();
        assert!(matches!(
            decision,
            Decision::Fail {
                demand: Demand::Switch,
                ..
            }
        ));
    }

    #[test]
    fn failure_messages_name_the_test_the_variables_and_the_demand() {
        let error = Error::Required {
            test: "qbit-prism-server::live_regtest::real_two_server_mining".into(),
            missing: vec![Input::QbitdBin, Input::DatabaseUrl],
            demand: Demand::Job,
        };
        assert_eq!(
            error.to_string(),
            "qbit-prism-server::live_regtest::real_two_server_mining requires QBITD_BIN, \
             PRISM_TEST_DATABASE_URL: QBITD_BIN and PRISM_TEST_DATABASE_URL are unset or empty \
             while GITHUB_JOB=prism-native-postgres demands the integration suite"
        );
        let error = Error::Required {
            test: "t".into(),
            missing: vec![Input::PgBinDir],
            demand: Demand::Explicit,
        };
        assert_eq!(
            error.to_string(),
            "t requires PRISM_TEST_PG_BIN_DIR: PRISM_TEST_PG_BIN_DIR is unset or empty while the \
             explicit selection of this test demands the integration suite"
        );
    }

    #[test]
    fn the_test_id_comes_from_the_test_thread() {
        let site = Site {
            package: "pkg",
            binary: "bin",
        };
        assert_eq!(
            site.test_id().unwrap(),
            "pkg::bin::tests::the_test_id_comes_from_the_test_thread"
        );
        let off_thread = std::thread::spawn(move || site.test_id()).join().unwrap();
        assert!(
            matches!(off_thread, Err(Error::UnnamedThread { .. })),
            "{off_thread:?}"
        );
        let worker = std::thread::Builder::new()
            .name("tokio-runtime-worker".into())
            .spawn(move || site.test_id())
            .unwrap()
            .join()
            .unwrap();
        assert!(matches!(worker, Err(Error::UnnamedThread { .. })));
    }

    #[test]
    fn manifest_lines_append_one_per_decision() {
        let dir = std::env::temp_dir().join(format!(
            "prism-test-gate-{}-{}",
            std::process::id(),
            line!()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("manifest.txt");
        record_to(&path, "executed", "a::b::c").unwrap();
        record_to(&path, "skipped", "a::b::d").unwrap();
        record_to(&path, "failed", "a::b::e").unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "executed a::b::c\nskipped a::b::d\nfailed a::b::e\n"
        );
        let error =
            record_to(&dir.join("missing").join("manifest.txt"), "executed", "x").unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("cannot append to PRISM_TEST_GATE_MANIFEST="),
            "{error}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_skip_prefix_is_stable() {
        // CI greps the test log for this exact text; change it together with
        // scripts/check_gate_manifest.py and docs/prism-integration-test-gate.md.
        assert_eq!(SKIP_PREFIX, "[prism-test-gate] skipped");
    }
}
