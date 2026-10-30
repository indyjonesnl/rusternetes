//! Unit tests moved from `src/poll.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::poll::*;
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
