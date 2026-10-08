//! Small generic "poll an async producer until it yields a value" helper.

/// Call `f` immediately, then every `interval`, until it returns `Some` or
/// `timeout` elapses. Returns the first `Some`, or `None` on timeout.
pub async fn poll_until_some<F, Fut>(
    mut f: F,
    timeout: std::time::Duration,
    interval: std::time::Duration,
) -> Option<String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<String>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return Some(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(interval).await;
    }
}

/// Observe what a post-start `Running` status write needs: the container
/// statuses and the pod IP.
///
/// Upstream builds the whole API status in one pass at write time
/// (`generateAPIPodStatus`, `pkg/kubelet/kubelet_pods.go`), so nothing in it is
/// older than anything else. Here the pod IP is awaited (bounded) because CNI
/// publishes it asynchronously; the container statuses MUST be read after that
/// wait, or they are as stale as the wait is long (#2771: statuses read before a
/// 10s IP poll that timed out were persisted as `Waiting/ContainerCreating`).
pub async fn observe_running_status<T, S, SFut, I, IFut>(
    read_statuses: S,
    poll_ip: I,
    ip_timeout: std::time::Duration,
    ip_interval: std::time::Duration,
) -> (Option<T>, Option<String>)
where
    S: FnOnce() -> SFut,
    SFut: std::future::Future<Output = Option<T>>,
    I: FnMut() -> IFut,
    IFut: std::future::Future<Output = Option<String>>,
{
    let ip = poll_until_some(poll_ip, ip_timeout, ip_interval).await;
    let statuses = read_statuses().await;
    (statuses, ip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// Returns None on the first two calls, then Some — the helper must keep
    /// polling and return the value, not give up after the first miss.
    #[tokio::test(start_paused = true)]
    async fn returns_value_after_initial_nones() {
        let calls = Cell::new(0u32);
        let got = poll_until_some(
            || {
                let n = calls.get();
                calls.set(n + 1);
                async move {
                    if n >= 2 {
                        Some("10.1.2.3".to_string())
                    } else {
                        None
                    }
                }
            },
            std::time::Duration::from_secs(10),
            std::time::Duration::from_millis(150),
        )
        .await;
        assert_eq!(got, Some("10.1.2.3".to_string()));
        assert_eq!(
            calls.get(),
            3,
            "should have polled 3 times (2 None + 1 Some)"
        );
    }

    /// If the producer never yields, the helper returns None at the deadline.
    #[tokio::test(start_paused = true)]
    async fn returns_none_on_timeout() {
        let got = poll_until_some(
            || async { None },
            std::time::Duration::from_secs(1),
            std::time::Duration::from_millis(150),
        )
        .await;
        assert_eq!(got, None);
    }

    /// #2771: the statuses persisted with the Running write must be read AFTER
    /// the IP wait, so they reflect the runtime as of the write.
    #[tokio::test(start_paused = true)]
    async fn statuses_are_read_after_the_ip_wait() {
        let clock = Cell::new(0u32); // bumped on every observation
        let ip_polls = Cell::new(0u32);
        let (statuses, ip) = observe_running_status(
            || {
                clock.set(clock.get() + 1);
                let at = clock.get();
                async move { Some(at) }
            },
            || {
                clock.set(clock.get() + 1);
                ip_polls.set(ip_polls.get() + 1);
                let found = ip_polls.get() >= 3;
                async move { found.then(|| "10.1.2.3".to_string()) }
            },
            std::time::Duration::from_secs(10),
            std::time::Duration::from_millis(150),
        )
        .await;
        assert_eq!(ip.as_deref(), Some("10.1.2.3"));
        // 3 IP polls tick the clock to 3; a read taken after them is >= 4.
        assert_eq!(
            statuses,
            Some(4),
            "statuses were read before the IP wait finished"
        );
    }

    /// Even when the IP never shows up, the statuses reflect the end of the wait.
    #[tokio::test(start_paused = true)]
    async fn statuses_are_read_after_a_timed_out_ip_wait() {
        let polls = Cell::new(0u32);
        let (statuses, ip) = observe_running_status(
            || {
                let seen = polls.get();
                async move { Some(seen) }
            },
            || {
                polls.set(polls.get() + 1);
                async { None }
            },
            std::time::Duration::from_secs(1),
            std::time::Duration::from_millis(150),
        )
        .await;
        assert_eq!(ip, None);
        assert!(
            statuses.unwrap() > 1,
            "statuses were read before the IP wait ran"
        );
    }
}
