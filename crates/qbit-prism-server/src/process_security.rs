//! Disable Linux core dumps before any credentials or signing seeds are loaded.
//! RLIMIT_CORE alone does not stop dumps piped to a host collector; see core(5).
pub fn disable_core_dumps() -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: PR_SET_DUMPABLE takes an integer mode and no pointers. Set it
        // before spawning runtime threads, which share the process's address space.
        let result = unsafe {
            libc::prctl(
                libc::PR_SET_DUMPABLE,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        };
        anyhow::ensure!(
            result == 0,
            "cannot disable process core dumps: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    #[test]
    fn kernel_confirms_process_is_not_dumpable() {
        super::disable_core_dumps().unwrap();
        // SAFETY: PR_GET_DUMPABLE returns the integer mode and takes no pointers.
        let mode = unsafe {
            libc::prctl(
                libc::PR_GET_DUMPABLE,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        };
        assert_eq!(mode, 0);
    }
}
