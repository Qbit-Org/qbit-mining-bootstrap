//! The runtime path end to end: a child copy of this binary runs one gated
//! probe under a controlled environment, and the parent checks what the gate
//! did to the exit status, the manifest and stderr.

use qbit_prism_test_gate::{self as gate, Input, SKIP_PREFIX};
use std::path::PathBuf;
use std::process::Command;

const PROBE: &str = "child_probe";
const REQUIRED_PROBE: &str = "child_required_probe";

/// Runs in the child only. Explicitly selected so the parent's run does not
/// count it; the parent runs it with `--ignored --exact`.
#[test]
#[ignore = "driven by the parent tests below through a child process"]
fn child_probe() {
    match gate::database_url(gate::site!()) {
        Ok(Some(url)) => println!("probe ran with {url}"),
        Ok(None) => println!("probe skipped"),
        Err(error) => panic!("{error}"),
    }
}

#[test]
#[ignore = "driven by the parent tests below through a child process"]
fn child_required_probe() {
    match gate::required_database_url(gate::site!()) {
        Ok(url) => println!("probe ran with {url}"),
        Err(error) => panic!("{error}"),
    }
}

struct Run {
    success: bool,
    stdout: String,
    stderr: String,
    manifest: String,
}

fn run_probe(probe: &str, env: &[(&str, &str)]) -> Run {
    let dir = std::env::temp_dir().join(format!(
        "prism-test-gate-process-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("t")
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let manifest: PathBuf = dir.join("manifest.txt");
    let _ = std::fs::remove_file(&manifest);
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--ignored", "--exact", probe, "--nocapture"])
        .env_clear()
        .env(gate::MANIFEST_VAR, &manifest);
    for (name, value) in env {
        command.env(name, value);
    }
    let output = command.output().unwrap();
    let run = Run {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        manifest: std::fs::read_to_string(&manifest).unwrap_or_default(),
    };
    std::fs::remove_dir_all(&dir).unwrap();
    run
}

fn id(probe: &str) -> String {
    format!("qbit-prism-test-gate::process::{probe}")
}

#[test]
fn a_present_input_runs_and_is_recorded_as_executed() {
    let run = run_probe(PROBE, &[(Input::DatabaseUrl.name(), " postgres://x/y ")]);
    assert!(run.success, "{}{}", run.stdout, run.stderr);
    assert!(
        run.stdout.contains("probe ran with postgres://x/y"),
        "{}",
        run.stdout
    );
    assert_eq!(run.manifest, format!("executed {}\n", id(PROBE)));
    assert!(!run.stderr.contains(SKIP_PREFIX), "{}", run.stderr);
}

#[test]
fn a_missing_input_skips_with_one_prefixed_line() {
    let run = run_probe(PROBE, &[]);
    assert!(run.success, "{}{}", run.stdout, run.stderr);
    assert!(run.stdout.contains("probe skipped"), "{}", run.stdout);
    assert_eq!(run.manifest, format!("skipped {}\n", id(PROBE)));
    let skip_lines: Vec<&str> = run
        .stderr
        .lines()
        .filter(|line| line.starts_with(SKIP_PREFIX))
        .collect();
    assert_eq!(
        skip_lines,
        vec![format!(
            "{SKIP_PREFIX} {}: PRISM_TEST_DATABASE_URL is unset or empty; set \
             PRISM_TEST_DATABASE_URL to run it, or PRISM_TEST_REQUIRE_INTEGRATION=1 to fail instead",
            id(PROBE)
        )]
    );
}

#[test]
fn the_skip_line_is_visible_without_nocapture() {
    // Same as above, but with libtest capturing: the line must still reach stderr.
    let dir = std::env::temp_dir().join(format!("prism-test-gate-capture-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", PROBE])
        .env_clear()
        .output()
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(stderr.matches(SKIP_PREFIX).count(), 1, "{stderr}");
}

#[test]
fn a_missing_input_fails_when_the_switch_is_on() {
    let run = run_probe(PROBE, &[(gate::SWITCH_VAR, "1")]);
    assert!(!run.success, "{}{}", run.stdout, run.stderr);
    assert_eq!(run.manifest, format!("failed {}\n", id(PROBE)));
    let expected = format!(
        "{} requires PRISM_TEST_DATABASE_URL: PRISM_TEST_DATABASE_URL is unset or empty while \
         PRISM_TEST_REQUIRE_INTEGRATION=1 demands the integration suite",
        id(PROBE)
    );
    assert!(
        run.stdout.contains(&expected) || run.stderr.contains(&expected),
        "{}{}",
        run.stdout,
        run.stderr
    );
    assert!(!run.stderr.contains(SKIP_PREFIX), "{}", run.stderr);
}

#[test]
fn a_missing_input_fails_in_the_native_job() {
    let run = run_probe(PROBE, &[(gate::JOB_VAR, gate::REQUIRED_JOB)]);
    assert!(!run.success, "{}{}", run.stdout, run.stderr);
    assert_eq!(run.manifest, format!("failed {}\n", id(PROBE)));
    let expected = format!(
        "GITHUB_JOB={} demands the integration suite",
        gate::REQUIRED_JOB
    );
    assert!(
        run.stdout.contains(&expected) || run.stderr.contains(&expected),
        "{}{}",
        run.stdout,
        run.stderr
    );
}

#[test]
fn another_job_id_still_skips() {
    let run = run_probe(PROBE, &[(gate::JOB_VAR, "rust-tests"), ("CI", "true")]);
    assert!(run.success, "{}{}", run.stdout, run.stderr);
    assert_eq!(run.manifest, format!("skipped {}\n", id(PROBE)));
}

#[test]
fn an_explicitly_selected_test_never_skips() {
    let run = run_probe(REQUIRED_PROBE, &[]);
    assert!(!run.success, "{}{}", run.stdout, run.stderr);
    assert_eq!(run.manifest, format!("failed {}\n", id(REQUIRED_PROBE)));
    let expected = "while the explicit selection of this test demands the integration suite";
    assert!(
        run.stdout.contains(expected) || run.stderr.contains(expected),
        "{}{}",
        run.stdout,
        run.stderr
    );
    let run = run_probe(
        REQUIRED_PROBE,
        &[(Input::DatabaseUrl.name(), "postgres://x/y")],
    );
    assert!(run.success, "{}{}", run.stdout, run.stderr);
    assert_eq!(run.manifest, format!("executed {}\n", id(REQUIRED_PROBE)));
}

#[test]
fn an_unwritable_manifest_fails_the_test() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", PROBE, "--nocapture"])
        .env_clear()
        .env(Input::DatabaseUrl.name(), "postgres://x/y")
        .env(
            gate::MANIFEST_VAR,
            "/nonexistent-prism-gate-dir/manifest.txt",
        )
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.contains("cannot append to PRISM_TEST_GATE_MANIFEST="),
        "{text}"
    );
}

#[cfg(unix)]
#[test]
fn an_unreadable_input_fails_instead_of_skipping() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", PROBE, "--nocapture"])
        .env_clear()
        .env(Input::DatabaseUrl.name(), OsStr::from_bytes(&[0xff, 0xfe]))
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.contains("PRISM_TEST_DATABASE_URL is set but not readable"),
        "{text}"
    );
    assert!(!text.contains(SKIP_PREFIX), "{text}");
}
