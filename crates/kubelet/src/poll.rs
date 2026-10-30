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
