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
}
