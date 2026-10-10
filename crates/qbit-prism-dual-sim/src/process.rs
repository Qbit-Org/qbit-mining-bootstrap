//! Child processes the simulation owns: where their output goes, the signals
//! the fault injector sends them, and how they end.
//!
//! Every child writes stdout and stderr to one log file under the scenario's
//! directory. A restart appends to the same file after a marker line, so one
//! file tells a frontend's whole story across its kills and restarts.

use anyhow::{bail, Context, Result};
use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, MutexGuard, PoisonError,
    },
    time::{Duration, Instant},
};

/// One running child, its log and whether the injector froze it.
pub struct Process {
    name: String,
    child: Mutex<Child>,
    pid: u32,
    log: PathBuf,
    frozen: AtomicBool,
}

impl Process {
    /// Start `command` with its output appended to `log`, after a marker line
    /// naming the start.
    pub fn spawn(name: &str, command: &mut Command, log: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
            .with_context(|| format!("opening the log {}", log.display()))?;
        writeln!(
            file,
            "=== dual-sim: {name} started at {}",
            chrono::Utc::now().to_rfc3339()
        )?;
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::from(file.try_clone()?))
            .stderr(Stdio::from(file))
            .spawn()
            .with_context(|| format!("starting {name} ({:?})", command.get_program()))?;
        Ok(Self {
            name: name.to_owned(),
            pid: child.id(),
            child: Mutex::new(child),
            log: log.to_owned(),
            frozen: AtomicBool::new(false),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Child> {
        self.child.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn log(&self) -> &Path {
        &self.log
    }

    /// The exit status once the child has exited, reaping it; `None` while it
    /// runs or is stopped.
    pub fn exited(&self) -> Option<ExitStatus> {
        self.lock().try_wait().ok().flatten()
    }

    /// SIGKILL, then reap: a crash with no chance to flush, release a lock or
    /// close a socket cleanly. Works on a frozen child too.
    pub fn kill9(&self) -> Result<()> {
        let mut child = self.lock();
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        child
            .kill()
            .with_context(|| format!("SIGKILL {}", self.name))?;
        child.wait()?;
        self.frozen.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// SIGSTOP: the process keeps every socket and lock it holds and answers
    /// nothing, as a host wedged in the kernel or a stalled VM does.
    pub fn freeze(&self) -> Result<()> {
        signal(self.pid_i32()?, libc::SIGSTOP)?;
        self.frozen.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// SIGCONT after [`Process::freeze`].
    pub fn thaw(&self) -> Result<()> {
        signal(self.pid_i32()?, libc::SIGCONT)?;
        self.frozen.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    /// SIGTERM, then SIGKILL if the child is still running after `grace`.
    /// Returns whether it exited within the grace period.
    pub fn terminate(&self, grace: Duration) -> Result<bool> {
        if self.frozen() {
            self.thaw()?;
        }
        if self.exited().is_some() {
            return Ok(true);
        }
        signal(self.pid_i32()?, libc::SIGTERM)?;
        let deadline = Instant::now() + grace;
        while Instant::now() < deadline {
            if self.exited().is_some() {
                return Ok(true);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        self.kill9()?;
        Ok(false)
    }

    fn pid_i32(&self) -> Result<i32> {
        i32::try_from(self.pid).context("pid out of range")
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.kill9();
    }
}

/// Send `sig` to `pid`.
pub fn signal(pid: i32, sig: libc::c_int) -> Result<()> {
    // SAFETY: kill(2) takes plain integers and has no memory effects.
    let result = unsafe { libc::kill(pid, sig) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        bail!("kill({pid}, {sig}): {error}");
    }
    Ok(())
}

/// Whether a process with this pid exists (zombies included).
pub fn alive(pid: i32) -> bool {
    // SAFETY: signal 0 only checks that the process exists.
    unsafe { libc::kill(pid, 0) == 0 }
}

/// The pids whose parent is `parent`, read from `/proc`.
pub fn children(parent: i32) -> Vec<i32> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        // `pid (comm) state ppid ...`: comm may hold spaces and parentheses,
        // so the fields are counted from the last closing parenthesis.
        let Some((_, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        if rest
            .split_whitespace()
            .nth(1)
            .and_then(|ppid| ppid.parse::<i32>().ok())
            == Some(parent)
        {
            found.push(pid);
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frozen_child_answers_nothing_until_thawed_and_a_kill_reaps_it() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let log = directory.path().join("sleeper.log");
        let process = Process::spawn("sleeper", Command::new("sleep").arg("30"), &log)?;
        assert!(process.exited().is_none());
        process.freeze()?;
        // SIGSTOP is delivered asynchronously.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stat = std::fs::read_to_string(format!("/proc/{}/stat", process.pid()))?;
            let state = stat
                .rsplit_once(')')
                .and_then(|(_, rest)| rest.trim().chars().next());
            if state == Some('T') {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the frozen child is still {state:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        process.thaw()?;
        process.kill9()?;
        assert!(process.exited().is_some());
        assert!(std::fs::read_to_string(&log)?.contains("=== dual-sim: sleeper started at"));
        Ok(())
    }

    #[test]
    fn children_are_found_through_proc() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let process = Process::spawn(
            "parent",
            Command::new("sh").args(["-c", "sleep 30 & wait"]),
            &directory.path().join("parent.log"),
        )?;
        let parent = i32::try_from(process.pid())?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while children(parent).is_empty() {
            assert!(
                Instant::now() < deadline,
                "the shell never forked its child"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        for child in children(parent) {
            signal(child, libc::SIGKILL)?;
        }
        process.kill9()?;
        Ok(())
    }
}
