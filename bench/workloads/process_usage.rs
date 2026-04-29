#[derive(Copy, Clone, Debug)]
pub(crate) struct ProcessCpuSnapshot {
    user_seconds: f64,
    system_seconds: f64,
    voluntary_context_switches: i64,
    involuntary_context_switches: i64,
    max_rss_bytes: u64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ProcessCpuUsage {
    pub(crate) user_seconds: f64,
    pub(crate) system_seconds: f64,
    pub(crate) total_seconds: f64,
    pub(crate) voluntary_context_switches: i64,
    pub(crate) involuntary_context_switches: i64,
    pub(crate) max_rss_bytes: u64,
}

impl ProcessCpuSnapshot {
    pub(crate) fn capture() -> std::io::Result<Self> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }

        let usage = unsafe { usage.assume_init() };
        Ok(Self {
            user_seconds: timeval_to_seconds(usage.ru_utime),
            system_seconds: timeval_to_seconds(usage.ru_stime),
            voluntary_context_switches: usage.ru_nvcsw,
            involuntary_context_switches: usage.ru_nivcsw,
            max_rss_bytes: max_rss_bytes(usage.ru_maxrss),
        })
    }

    pub(crate) fn elapsed_since(self, start: Self) -> ProcessCpuUsage {
        let user_seconds = (self.user_seconds - start.user_seconds).max(0.0);
        let system_seconds = (self.system_seconds - start.system_seconds).max(0.0);

        ProcessCpuUsage {
            user_seconds,
            system_seconds,
            total_seconds: user_seconds + system_seconds,
            voluntary_context_switches: self.voluntary_context_switches - start.voluntary_context_switches,
            involuntary_context_switches: self.involuntary_context_switches
                - start.involuntary_context_switches,
            max_rss_bytes: self.max_rss_bytes.max(start.max_rss_bytes),
        }
    }
}

fn timeval_to_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + (value.tv_usec as f64 / 1_000_000.0)
}

#[cfg(target_os = "linux")]
fn max_rss_bytes(value: libc::c_long) -> u64 {
    (value.max(0) as u64) * 1024
}

#[cfg(not(target_os = "linux"))]
fn max_rss_bytes(value: libc::c_long) -> u64 {
    value.max(0) as u64
}
