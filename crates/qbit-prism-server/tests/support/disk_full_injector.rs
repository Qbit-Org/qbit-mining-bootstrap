//! A disk-full fault injector for tests: a small ext4 filesystem the test
//! creates and mounts without root, a ballast file that fills it to
//! `ENOSPC` on [`DiskFullInjector::start`], and its removal on
//! [`DiskFullInjector::stop`], the operator freeing space (#575; built for
//! reuse by #554's full-disk-on-the-WAL-volume fault under load).
//!
//! Put whatever must run out of space under [`DiskFullInjector::volume`]: a
//! PostgreSQL data directory (with `pg_wal`), or only `pg_wal` through a
//! symlink. Keep logs and Unix sockets in [`DiskFullInjector::home`], which
//! is off the filesystem, so they keep working while it is full.
//!
//! The filesystem is an image file mounted by `fuse2fs -o fakeroot`
//! through the setuid `fusermount` helper, so it needs `fuse2fs`,
//! `mkfs.ext4` and `/dev/fuse`, and no privileges. Dropping the injector
//! unmounts it and removes the image; stop whatever uses the volume first.
//!
//! Any test binary can include it with `#[path]`, next to
//! `live_private_postgres.rs` as module `private_postgres` for its process
//! helpers; it needs `anyhow`, `libc` and `tempfile`.
use super::private_postgres::{program, run};
use anyhow::{bail, ensure, Context, Result};
use std::{
    ffi::CString,
    fs::File,
    io::Write,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

pub(crate) struct DiskFullInjector {
    mount: PathBuf,
    ballast: PathBuf,
    fuse: Option<Child>,
    unmount: PathBuf,
    /// Declared last, so the mount is gone before its files are removed.
    home: tempfile::TempDir,
}

impl DiskFullInjector {
    /// Create and mount an empty ext4 filesystem of `mib` MiB.
    pub(crate) fn mount(mib: u64) -> Result<Self> {
        let hint = "install fuse2fs and e2fsprogs";
        let fuse2fs = program("fuse2fs").context(hint)?;
        let mkfs = program("mkfs.ext4").context(hint)?;
        let unmount = program("fusermount3")
            .or_else(|_| program("fusermount"))
            .context("install fuse3")?;
        ensure!(
            Path::new("/dev/fuse").exists(),
            "/dev/fuse is missing: the disk-full injector needs FUSE"
        );
        // Short, so a PostgreSQL socket path in it stays within its limit.
        let home = tempfile::Builder::new().prefix("diskfull-").tempdir()?;
        let image = home.path().join("volume.img");
        File::create(&image)?.set_len(mib << 20)?;
        run(
            &mkfs,
            &["-q", "-F", "-m", "0", image.to_str().context("image path")?],
        )?;
        let mount = home.path().join("volume");
        std::fs::create_dir(&mount)?;
        let log = File::create(home.path().join("fuse2fs.log"))?;
        let fuse = Command::new(&fuse2fs)
            .arg(&image)
            .arg(&mount)
            .args(["-o", "fakeroot", "-f"])
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        let mut injector = Self {
            ballast: mount.join("ballast"),
            mount,
            fuse: Some(fuse),
            unmount,
            home,
        };
        let started = Instant::now();
        while !injector.mounted()? {
            if let Some(status) = injector.fuse.as_mut().context("fuse2fs")?.try_wait()? {
                bail!(
                    "fuse2fs exited with {status} before mounting: {}",
                    std::fs::read_to_string(injector.home.path().join("fuse2fs.log"))?
                );
            }
            ensure!(
                started.elapsed() < Duration::from_secs(15),
                "fuse2fs did not mount within 15 s"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(injector)
    }

    /// The mounted filesystem, for the data that must run out of space.
    pub(crate) fn volume(&self) -> &Path {
        &self.mount
    }

    /// A short directory off the filesystem, for logs and sockets.
    pub(crate) fn home(&self) -> &Path {
        self.home.path()
    }

    fn mounted(&self) -> Result<bool> {
        let mount = self.mount.to_str().context("mount path")?;
        Ok(std::fs::read_to_string("/proc/self/mounts")?
            .lines()
            .any(|line| line.split(' ').nth(1) == Some(mount)))
    }

    /// Bytes an unprivileged writer can still allocate.
    pub(crate) fn free_bytes(&self) -> Result<u64> {
        let path = CString::new(self.mount.as_os_str().as_bytes())?;
        // SAFETY: `statvfs` is plain data, valid when zeroed.
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: `path` is NUL-terminated and `stat` is writable.
        ensure!(
            unsafe { libc::statvfs(path.as_ptr(), &mut stat) } == 0,
            "statvfs: {}",
            std::io::Error::last_os_error()
        );
        Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
    }

    /// Start the fault: write the ballast until the filesystem refuses, in
    /// shrinking chunks so under 4 KiB is left. Returns the ballast's size.
    pub(crate) fn start(&self) -> Result<u64> {
        let mut file = File::options()
            .create(true)
            .append(true)
            .open(&self.ballast)?;
        let mut written = 0u64;
        for chunk in [1 << 20, 64 << 10, 4 << 10] {
            let zeros = vec![0u8; chunk];
            loop {
                match file.write_all(&zeros) {
                    Ok(()) => written += chunk as u64,
                    Err(error) if error.raw_os_error() == Some(libc::ENOSPC) => break,
                    Err(error) => return Err(error.into()),
                }
            }
        }
        match file.sync_all() {
            Err(error) if error.raw_os_error() != Some(libc::ENOSPC) => Err(error.into()),
            _ => Ok(written),
        }
    }

    /// Stop the fault: delete the ballast, if there is one.
    pub(crate) fn stop(&self) -> Result<()> {
        match std::fs::remove_file(&self.ballast) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }
}

impl Drop for DiskFullInjector {
    fn drop(&mut self) {
        // A file still open on the volume (a PostgreSQL that did not stop)
        // makes a plain unmount fail; detach it lazily then, so the mount
        // point is never left behind.
        let unmounted = Command::new(&self.unmount)
            .arg("-u")
            .arg(&self.mount)
            .output()
            .is_ok_and(|output| output.status.success());
        if !unmounted {
            let _ = Command::new(&self.unmount)
                .arg("-uz")
                .arg(&self.mount)
                .output();
        }
        if let Some(mut fuse) = self.fuse.take() {
            let started = Instant::now();
            while matches!(fuse.try_wait(), Ok(None)) && started.elapsed() < Duration::from_secs(10)
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = fuse.kill();
            let _ = fuse.wait();
        }
    }
}
