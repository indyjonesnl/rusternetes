//! Unit tests moved from `src/flow_control_queueset.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::flow_control_integrator::Integrator;
    pub use rusternetes_api_server::flow_control_metrics as fcmetrics;
    pub use rusternetes_api_server::flow_control_queueset::shufflesharding::Dealer;
    pub use std::collections::{HashMap, HashSet, VecDeque};
    pub use std::sync::{Arc, Mutex, Weak};
    pub use std::time::Duration;
    pub use tokio::sync::Notify;
    pub use tracing::{error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::flow_control_queueset::*;

    // ---- test clock (eventclock/testing Fake) ----

    type Event = (Duration, u64, Box<dyn FnOnce() + Send>);

    struct FakeClock {
        state: Mutex<(Duration, u64, Vec<Event>)>,
    }

    impl FakeClock {
        fn new() -> Arc<FakeClock> {
            Arc::new(FakeClock {
                state: Mutex::new((Duration::ZERO, 0, Vec::new())),
            })
        }
        /// `SetTime`: run due events in order, each at its own time.
        fn advance(&self, d: Duration) {
            let target = self.state.lock().unwrap().0 + d;
            loop {
                let ev = {
                    let mut s = self.state.lock().unwrap();
                    let idx =
                        s.2.iter()
                            .enumerate()
                            .filter(|(_, e)| e.0 <= target)
                            .min_by_key(|(_, e)| (e.0, e.1))
                            .map(|(i, _)| i);
                    match idx {
                        Some(i) => {
                            let e = s.2.remove(i);
                            s.0 = s.0.max(e.0);
                            Some(e.2)
                        }
                        None => {
                            s.0 = target;
                            None
                        }
                    }
                };
                match ev {
                    Some(f) => f(),
                    None => break,
                }
            }
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Duration {
            self.state.lock().unwrap().0
        }
        fn after(&self, d: Duration, f: Box<dyn FnOnce() + Send>) {
            let mut s = self.state.lock().unwrap();
            let at = s.0 + d;
            let seq = s.1;
            s.1 += 1;
            s.2.push((at, seq, f));
        }
    }

    fn qcfg(queues: isize, qll: isize, hand: isize) -> QueuingConfig {
        QueuingConfig {
            name: "t".into(),
            desired_num_queues: queues,
            queue_length_limit: qll,
            hand_size: hand,
        }
    }

    fn we(initial: u64) -> WorkEstimate {
        WorkEstimate {
            initial_seats: initial,
            final_seats: initial,
            additional_latency: Duration::ZERO,
        }
    }

    // ---- seat_seconds_test.go-ish / types ----

    #[test]
    fn seat_seconds_scale_and_display() {
        // 1 seat * 1s == 1.0 seat-second == 1e8 units.
        assert_eq!(
            seats_times_duration(1.0, 1_000_000_000),
            SeatSeconds(100_000_000)
        );
        assert_eq!(SeatSeconds(150_000_000).to_string(), "1.50000000ss");
        assert_eq!(SeatSeconds(150_000_000).to_float(), 1.5);
        // A negative duration wraps like Go's uint64 and cancels on addition.
        let neg = seats_times_duration(2.0, -500_000_000);
        assert_eq!(
            SeatSeconds(100) + neg,
            SeatSeconds(100) - SeatSeconds(100_000_000)
        );
    }

    /// TestRequestSeats
    #[test]
    fn request_max_seats() {
        for (i, f, want) in [(3, 3, 3), (1, 3, 3), (3, 1, 3)] {
            let w = WorkEstimate {
                initial_seats: i,
                final_seats: f,
                additional_latency: Duration::ZERO,
            };
            assert_eq!(w.max_seats(), want);
        }
    }

    /// TestRequestWork
    #[test]
    fn request_total_work() {
        let mut inner = Inner::new(FakeClock::new());
        inner.estimated_service_duration_ns = 2_000_000_000;
        let c = inner.complete_work_estimate(&WorkEstimate {
            initial_seats: 3,
            final_seats: 50,
            additional_latency: Duration::from_secs(70),
        });
        assert_eq!(
            c.total_work,
            seats_times_duration(3.0, 2_000_000_000) + seats_times_duration(50.0, 70_000_000_000)
        );
    }

    // ---- shufflesharding_test.go ----

    #[test]
    fn required_entropy_bits() {
        assert_eq!(shufflesharding::required_entropy_bits(1024, 6), 60);
        assert_eq!(shufflesharding::required_entropy_bits(512, 8), 72);
    }

    #[test]
    fn new_dealer_errors_verbatim() {
        let e = |d, h| Dealer::new(d, h).err();
        assert_eq!(
            e(-100, 8).as_deref(),
            Some("deckSize -100 or handSize 8 is not positive")
        );
        assert_eq!(
            e(100, 0).as_deref(),
            Some("deckSize 100 or handSize 0 is not positive")
        );
        assert_eq!(
            e(100, 101).as_deref(),
            Some("handSize 101 is greater than deckSize 100")
        );
        assert_eq!(
            e(1 << 27, 2).as_deref(),
            Some("deckSize 134217728 is impractically large")
        );
        assert_eq!(
            e(512, 8).as_deref(),
            Some("required entropy bits of deckSize 512 and handSize 8 is greater than 60")
        );
        assert!(e(1024, 6).is_none());
    }

    /// TestCardDuplication: a dealt hand never repeats a card.
    #[test]
    fn dealt_hand_has_no_duplicates() {
        for (deck, hand) in [(5, 5), (16, 3), (64, 6), (1024, 6)] {
            let d = Dealer::new(deck, hand).unwrap();
            for h in 0..5000u64 {
                let hash = h.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 4;
                let cards = d.deal_into_hand(hash);
                let set: HashSet<_> = cards.iter().collect();
                assert_eq!(
                    set.len(),
                    cards.len(),
                    "deck {deck} hand {hand} hash {hash}"
                );
                assert!(cards.iter().all(|&c| (c as isize) < deck));
            }
        }
    }

    // ---- fifo_list_test.go: running queueSum ----

    #[test]
    fn fifo_sum_tracks_enqueue_and_remove() {
        let mut inner = Inner::new(FakeClock::new());
        inner.set_configuration(qcfg(1, 10, 1), check_config(&qcfg(1, 10, 1)).unwrap(), {
            DispatchingConfig {
                concurrency_limit: 1,
            }
        });
        let qid = inner.queues[0].id;
        let mut ids = vec![];
        for seats in [1u64, 2, 3] {
            let w = inner.complete_work_estimate(&we(seats));
            let rid = inner.new_request("fs", "", Some(qid), w, false);
            inner.enqueue_to_bound(qid, rid);
            ids.push((rid, w));
        }
        let sum = inner.queues[0].sum;
        assert_eq!(sum.max_seats_sum, 6);
        assert_eq!(sum.initial_seats_sum, 6);
        assert_eq!(
            sum.total_work_sum,
            ids.iter()
                .fold(SeatSeconds(0), |a, (_, w)| a + w.total_work)
        );
        assert!(inner.remove_waiting(qid, ids[1].0));
        assert!(
            !inner.remove_waiting(qid, ids[1].0),
            "double removal is a no-op"
        );
        assert_eq!(inner.queues[0].sum.max_seats_sum, 4);
        assert_eq!(inner.queues[0].waiting.len(), 2);
    }

    // ---- TestFindDispatchQueueLocked ----

    struct FindCase {
        concurrency_limit: usize,
        tot_seats_in_use: usize,
        /// (nextDispatchR seconds, head request initial seats)
        queues: Vec<(i64, u64)>,
        attempts: usize,
        before: Option<(usize, usize)>, // (attempt, seats in use to set)
        min_queue_expected: Vec<Option<usize>>,
        robin_expected: Vec<isize>,
    }

    fn run_find_case(c: FindCase) {
        let mut inner = Inner::new(FakeClock::new());
        inner.estimated_service_duration_ns = 3_000_000; // G = 3ms
        inner.dcfg.concurrency_limit = c.concurrency_limit;
        inner.tot_seats_in_use = c.tot_seats_in_use;
        inner.robin_index = -1;
        for (r_secs, seats) in &c.queues {
            let mut q = inner.create_queue();
            q.next_dispatch_r = seats_times_duration(1.0, r_secs * 1_000_000_000);
            inner.queues.push(q);
            let qid = inner.queues.last().unwrap().id;
            let w = inner.complete_work_estimate(&WorkEstimate {
                initial_seats: *seats,
                ..Default::default()
            });
            let rid = inner.new_request("fs", "", Some(qid), w, false);
            inner.enqueue_to_bound(qid, rid);
            // enqueue resets nextDispatchR for an empty queue; restore.
            let qpos = inner.queue_pos(qid).unwrap();
            inner.queues[qpos].next_dispatch_r = seats_times_duration(1.0, r_secs * 1_000_000_000);
        }
        let ids: Vec<u64> = inner.queues.iter().map(|q| q.id).collect();
        for i in 0..c.attempts {
            let attempt = i + 1;
            if let Some((a, in_use)) = c.before {
                if a == attempt {
                    inner.tot_seats_in_use = in_use;
                }
            }
            let got = inner.find_dispatch_queue_to_bound();
            let want = c.min_queue_expected[i].map(|ix| ids[ix]);
            assert_eq!(got.map(|(q, _)| q), want, "attempt {attempt}");
            assert_eq!(
                inner.robin_index, c.robin_expected[i],
                "robin attempt {attempt}"
            );
            // Put the request back if one was taken so the next attempt can see it.
            if let Some((qid, rid)) = got {
                let qpos = inner.queue_pos(qid).unwrap();
                inner.queues[qpos].waiting.push_front(rid);
                let w = inner.requests[&rid].work;
                inner.queues[qpos].sum.max_seats_sum += w.max_seats();
                inner.queues[qpos].sum.total_work_sum += w.total_work;
            }
        }
    }

    #[test]
    fn find_dispatch_least_virtual_start_wins() {
        run_find_case(FindCase {
            concurrency_limit: 1,
            tot_seats_in_use: 0,
            queues: vec![(200, 1), (100, 1)],
            attempts: 1,
            before: None,
            min_queue_expected: vec![Some(1)],
            robin_expected: vec![1],
        });
    }

    #[test]
    fn find_dispatch_no_seats_no_queue() {
        run_find_case(FindCase {
            concurrency_limit: 1,
            tot_seats_in_use: 1,
            queues: vec![(200, 1)],
            attempts: 1,
            before: None,
            min_queue_expected: vec![None],
            robin_expected: vec![0],
        });
    }

    #[test]
    fn find_dispatch_wide_request_with_seats_available() {
        run_find_case(FindCase {
            concurrency_limit: 50,
            tot_seats_in_use: 25,
            queues: vec![(200, 50), (100, 25)],
            attempts: 1,
            before: None,
            min_queue_expected: vec![Some(1)],
            robin_expected: vec![1],
        });
    }

    #[test]
    fn find_dispatch_wide_request_without_seats_is_not_picked_then_is() {
        run_find_case(FindCase {
            concurrency_limit: 50,
            tot_seats_in_use: 26,
            queues: vec![(200, 10), (100, 25)],
            attempts: 3,
            before: Some((3, 25)),
            min_queue_expected: vec![None, None, Some(1)],
            robin_expected: vec![1, 1, 1],
        });
    }

    // ---- TestFinishRequestLocked ----

    #[test]
    fn finish_with_additional_latency_lingers_seats() {
        let clk = FakeClock::new();
        let qs = QueueSet::new(
            clk.clone(),
            qcfg(1, 10, 1),
            DispatchingConfig {
                concurrency_limit: 20,
            },
        )
        .unwrap();
        let h = qs
            .start_request(
                &WorkEstimate {
                    initial_seats: 1,
                    final_seats: 10,
                    additional_latency: Duration::from_secs(60),
                },
                1,
                "",
                "fs",
            )
            .unwrap();
        let ex = h.try_execution().ok().unwrap();
        assert_eq!(qs.stats().seats_in_use, 10);
        drop(ex);
        // Request finished but its seats linger for AdditionalLatency.
        assert_eq!(qs.stats().executing, 0);
        assert_eq!(qs.stats().seats_in_use, 10);
        clk.advance(Duration::from_secs(59));
        assert_eq!(qs.stats().seats_in_use, 10);
        clk.advance(Duration::from_secs(1));
        assert_eq!(qs.stats().seats_in_use, 0);
        assert!(qs.is_idle());
    }

    #[test]
    fn finish_without_additional_latency_releases_immediately() {
        let clk = FakeClock::new();
        let qs = QueueSet::new(
            clk,
            qcfg(1, 10, 1),
            DispatchingConfig {
                concurrency_limit: 20,
            },
        )
        .unwrap();
        let ex = qs
            .start_request(&we(10), 1, "", "fs")
            .unwrap()
            .try_execution()
            .ok()
            .unwrap();
        assert_eq!(qs.stats().seats_in_use, 10);
        assert!(ex.finish());
        assert_eq!(qs.stats().seats_in_use, 0);
    }

    // ---- TestExampt / TestNoRestraint / baseline ----

    #[test]
    fn exempt_never_queues_or_rejects() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(-1, 0, 0),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let held: Vec<_> = (0..50)
            .map(|i| {
                qs.start_request(&we(1), i, "", "exempt")
                    .unwrap()
                    .try_execution()
                    .ok()
                    .unwrap()
            })
            .collect();
        assert_eq!(qs.stats().executing, 50);
        drop(held);
        assert!(qs.is_idle());
    }

    /// Queueless mode (`DesiredNumQueues == 0`): concurrency limit only.
    #[test]
    fn queueless_rejects_over_limit() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(0, 0, 0),
            DispatchingConfig {
                concurrency_limit: 2,
            },
        )
        .unwrap();
        let a = qs.start_request(&we(1), 1, "", "fs").unwrap();
        let _b = qs.start_request(&we(1), 2, "", "fs").unwrap();
        let rej = qs.start_request(&we(1), 3, "", "fs").err().unwrap();
        assert_eq!(rej.reason, RejectReason::ConcurrencyLimit);
        assert!(!rej.idle);
        assert_eq!(qs.stats().rejected, 1);
        drop(a.try_execution().ok().unwrap());
        assert!(qs.start_request(&we(1), 4, "", "fs").is_ok());
    }

    // ---- queue-full rejection (rejectOrEnqueueToBoundLocked) ----

    #[test]
    fn rejects_when_seats_exhausted_and_queue_full() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(1, 2, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let run = qs
            .start_request(&we(1), 1, "", "fs")
            .unwrap()
            .try_execution()
            .ok()
            .unwrap();
        let w1 = qs.start_request(&we(1), 1, "", "fs").unwrap();
        let w2 = qs.start_request(&we(1), 1, "", "fs").unwrap();
        assert_eq!(qs.stats().waiting, 2);
        let rej = qs.start_request(&we(1), 1, "", "fs").err().unwrap();
        assert_eq!(rej.reason, RejectReason::QueueFull);
        // FIFO within a queue: finishing the runner dispatches w1, not w2.
        drop(run);
        assert!(w1.is_dispatched());
        assert!(!w2.is_dispatched());
    }

    /// A full queue does not reject while seats remain (the AND in
    /// rejectOrEnqueueToBoundLocked).
    #[test]
    fn full_queue_with_free_seats_still_enqueues() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(1, 0, 1),
            DispatchingConfig {
                concurrency_limit: 3,
            },
        )
        .unwrap();
        assert!(qs.start_request(&we(1), 1, "", "fs").is_ok());
    }

    // ---- TestTooWide ----

    #[test]
    fn too_wide_runs_only_when_alone() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(4, 10, 1),
            DispatchingConfig {
                concurrency_limit: 6,
            },
        )
        .unwrap();
        let small = qs.start_request(&we(2), 1, "", "fs").unwrap();
        let wide = qs.start_request(&we(7), 2, "", "fs").unwrap();
        assert!(small.is_dispatched());
        assert!(!wide.is_dispatched(), "must wait for the executing request");
        drop(small.try_execution().ok().unwrap());
        assert!(wide.is_dispatched());
        // Nothing else may run alongside the too-wide request.
        let other = qs.start_request(&we(1), 1, "", "fs").unwrap();
        assert!(!other.is_dispatched());
    }

    // ---- seat demand (queueset.go seatDemandIntegrator) ----

    #[test]
    fn seat_demand_integrates_seats_in_use_plus_waiting() {
        let clk = FakeClock::new();
        let qs = QueueSet::new(
            clk.clone(),
            qcfg(4, 10, 1),
            DispatchingConfig {
                concurrency_limit: 2,
            },
        )
        .unwrap();
        let demand = qs.seat_demand();
        let running = qs.start_request(&we(2), 1, "", "fs").unwrap(); // 2 in use
        let waiting = qs.start_request(&we(1), 2, "", "fs").unwrap(); // +1 waiting
        clk.advance(Duration::from_secs(1));
        assert_eq!(demand.get_results().max, 3.0);
        drop(running.try_execution().ok().unwrap()); // waiting one dispatches: 1
        clk.advance(Duration::from_secs(1));
        let r = demand.reset();
        assert_eq!(r.max, 3.0);
        assert!((r.average - 2.0).abs() < 1e-9, "average {}", r.average);
        drop(waiting);
        demand.reset(); // start a fresh window with the released demand
        clk.advance(Duration::from_secs(1));
        assert_eq!(demand.reset().max, 0.0);
    }

    /// `Complete` with a larger `ConcurrencyLimit` dispatches what now fits
    /// (queueset.go `setConfiguration` -> `dispatchAsMuchAsPossibleLocked`).
    #[test]
    fn raising_the_concurrency_limit_dispatches_waiting_requests() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(4, 10, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let _a = qs.start_request(&we(1), 1, "", "fs").unwrap();
        let b = qs.start_request(&we(1), 2, "", "fs").unwrap();
        assert!(!b.is_dispatched());
        qs.set_configuration(
            qcfg(4, 10, 1),
            DispatchingConfig {
                concurrency_limit: 2,
            },
        )
        .unwrap();
        assert!(b.is_dispatched());
        assert_eq!(qs.concurrency_limit(), 2);
    }

    // ---- TestContextCancel ----

    #[test]
    fn dropping_a_waiting_handle_cancels_it() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(1, 5, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let run = qs
            .start_request(&we(1), 1, "", "fs")
            .unwrap()
            .try_execution()
            .ok()
            .unwrap();
        let waiting = qs.start_request(&we(1), 1, "", "fs").unwrap();
        let behind = qs.start_request(&we(1), 1, "", "fs").unwrap();
        assert_eq!(qs.stats().waiting, 2);
        drop(waiting);
        let s = qs.stats();
        assert_eq!(
            (s.waiting, s.seats_waiting, s.cancelled, s.rejected),
            (1, 1, 1, 1)
        );
        drop(run);
        assert!(behind.is_dispatched());
    }

    #[test]
    fn dropping_an_unobserved_dispatched_handle_returns_its_seats() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(1, 5, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let h = qs.start_request(&we(1), 1, "", "fs").unwrap();
        assert!(h.is_dispatched());
        drop(h);
        assert!(qs.is_idle());
        assert_eq!(qs.stats().seats_in_use, 0);
    }

    #[tokio::test]
    async fn wait_resolves_when_dispatched() {
        let qs = QueueSet::new(
            Arc::new(RealClock::default()),
            qcfg(1, 5, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let run = qs.start_request(&we(1), 1, "", "fs").unwrap().wait().await;
        let waiter = qs.start_request(&we(1), 1, "", "fs").unwrap();
        let task = tokio::spawn(async move {
            let ex = waiter.wait().await;
            ex.finish()
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!task.is_finished());
        drop(run);
        assert!(task.await.unwrap());
    }

    #[tokio::test]
    async fn cancelling_the_wait_future_cancels_the_request() {
        let qs = QueueSet::new(
            Arc::new(RealClock::default()),
            qcfg(1, 5, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let _run = qs.start_request(&we(1), 1, "", "fs").unwrap().wait().await;
        let waiter = qs.start_request(&we(1), 1, "", "fs").unwrap();
        let r = tokio::time::timeout(Duration::from_millis(20), waiter.wait()).await;
        assert!(r.is_err());
        assert_eq!(qs.stats().waiting, 0);
        assert_eq!(qs.stats().cancelled, 1);
    }

    // ---- setConfiguration: queues added at once, removed by attrition ----

    #[test]
    fn shrinking_queues_removes_them_by_attrition() {
        let qs = QueueSet::new(
            FakeClock::new(),
            qcfg(4, 5, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        // Occupy queue for hash 3 (deck 4, hand 1 => card = hash % 4).
        let run = qs.start_request(&we(1), 3, "", "fs").unwrap();
        qs.set_configuration(
            qcfg(2, 5, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        // Still 4 queues: the busy one drains, the empty ones go only when
        // a request finishes in them (removeQueueIfEmptyLocked).
        assert_eq!(qs.lock().queues.len(), 4);
        drop(run);
        assert_eq!(qs.lock().queues.len(), 3);
        // Growing adds queues at once.
        qs.set_configuration(
            qcfg(6, 5, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        assert_eq!(qs.lock().queues.len(), 6);
    }

    #[test]
    fn invalid_shuffle_sharding_config_is_rejected() {
        let e = QueueSet::new(
            FakeClock::new(),
            qcfg(4, 5, 9),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .err()
        .unwrap();
        assert_eq!(
            e,
            "the QueueSetConfig implies an invalid shuffle sharding config \
             (DesiredNumQueues is deckSize): handSize 9 is greater than deckSize 4"
        );
    }

    // ---- TestSeatSecondsRollover: epoch advance ----

    #[test]
    fn epoch_advances_when_r_reaches_high_r() {
        let clk = FakeClock::new();
        let qs = QueueSet::new(
            clk.clone(),
            qcfg(2, 5, 1),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let run = qs.start_request(&we(1), 0, "", "fs").unwrap();
        let waiting = qs.start_request(&we(1), 0, "", "fs").unwrap();
        {
            let mut g = qs.lock();
            g.current_r = SeatSeconds(HIGH_R.0 - 1);
            let qi = g.queues.iter().position(|q| !q.waiting.is_empty()).unwrap();
            g.queues[qi].next_dispatch_r = SeatSeconds(HIGH_R.0 - 5);
            let rid = *g.queues[qi].waiting.front().unwrap();
            g.requests.get_mut(&rid).unwrap().arrival_r = SeatSeconds(HIGH_R.0 - 3);
        }
        // One active queue with demand >= limit => R advances 1 seat-second
        // per second; that crosses HIGH_R and rolls the epoch back.
        clk.advance(Duration::from_secs(1));
        {
            let mut g = qs.lock();
            g.sync_time();
            assert!(g.current_r < HIGH_R, "R wound back below the threshold");
            assert!(g.current_r.0 > R_DECREMENT.0 / 2, "by exactly rDecrement");
            let qi = g.queues.iter().position(|q| !q.waiting.is_empty()).unwrap();
            let want = SeatSeconds(HIGH_R.0 - 5) - R_DECREMENT;
            assert_eq!(g.queues[qi].next_dispatch_r, want);
            let rid = *g.queues[qi].waiting.front().unwrap();
            assert_eq!(
                g.requests[&rid].arrival_r,
                SeatSeconds(HIGH_R.0 - 3) - R_DECREMENT
            );
        }
        drop(run);
        assert!(waiting.is_dispatched());
    }

    // ---- #2987: queueset metrics (queueset.go:478, :521, :577, :819, :842) ----

    fn named_qcfg(name: &str) -> QueuingConfig {
        QueuingConfig {
            name: name.into(),
            ..qcfg(2, 5, 1)
        }
    }

    #[test]
    fn queueset_publishes_virtual_time_and_dispatch_gauges() {
        use rusternetes_api_server::flow_control::metric_test_util::{has_series, sample};
        let clk = FakeClock::new();
        let qs = QueueSet::new(
            clk.clone(),
            named_qcfg("m-vt"),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let run = qs.start_request(&we(1), 0, "", "m-vt-fs").unwrap();
        let waiting = qs.start_request(&we(1), 0, "", "m-vt-fs").unwrap();
        clk.advance(Duration::from_secs(1));
        drop(run);
        assert!(waiting.is_dispatched());
        let ns = "apiserver_flowcontrol_";
        let pl = [("priority_level", "m-vt")];
        for g in ["current_r", "dispatch_r", "latest_s"] {
            assert!(has_series(&format!("{ns}{g}"), &pl), "{g} missing");
        }
        for g in ["next_s_bounds", "next_discounted_s_bounds"] {
            for b in ["min", "max"] {
                assert!(
                    has_series(
                        &format!("{ns}{g}"),
                        &[("priority_level", "m-vt"), ("bound", b)]
                    ),
                    "{g}{{{b}}} missing"
                );
            }
        }
        assert!(sample(&format!("{ns}current_r"), &pl) > 0.0);
    }

    #[test]
    fn queue_length_after_enqueue_is_observed_and_no_accommodation_counted() {
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        let clk = FakeClock::new();
        let qs = QueueSet::new(
            clk.clone(),
            named_qcfg("m-ql"),
            DispatchingConfig {
                concurrency_limit: 2,
            },
        )
        .unwrap();
        let l = [("priority_level", "m-ql"), ("flow_schema", "m-ql-fs")];
        let _run = qs.start_request(&we(1), 0, "", "m-ql-fs").unwrap();
        let _waiting = qs.start_request(&we(2), 0, "", "m-ql-fs").unwrap();
        // Observed once per enqueued request (queueset.go:577).
        assert_eq!(
            sample(
                "apiserver_flowcontrol_request_queue_length_after_enqueue",
                &l
            ),
            2.0
        );
        // The second request's dispatch attempt found no free seat (:819).
        assert!(
            sample(
                "apiserver_flowcontrol_request_dispatch_no_accommodation_total",
                &l
            ) >= 1.0
        );
    }

    #[test]
    fn epoch_advance_is_counted() {
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        let clk = FakeClock::new();
        let qs = QueueSet::new(
            clk.clone(),
            named_qcfg("m-ep"),
            DispatchingConfig {
                concurrency_limit: 1,
            },
        )
        .unwrap();
        let _run = qs.start_request(&we(1), 0, "", "fs").unwrap();
        let _waiting = qs.start_request(&we(1), 0, "", "fs").unwrap();
        {
            let mut g = qs.lock();
            g.current_r = SeatSeconds(HIGH_R.0 - 1);
            let qi = g.queues.iter().position(|q| !q.waiting.is_empty()).unwrap();
            g.queues[qi].next_dispatch_r = SeatSeconds(HIGH_R.0 - 5);
            let rid = *g.queues[qi].waiting.front().unwrap();
            g.requests.get_mut(&rid).unwrap().arrival_r = SeatSeconds(HIGH_R.0 - 3);
        }
        clk.advance(Duration::from_secs(1));
        qs.lock().sync_time();
        assert_eq!(
            sample(
                "apiserver_flowcontrol_epoch_advance_total",
                &[("priority_level", "m-ep"), ("success", "true")]
            ),
            1.0
        );
    }

    // ---- uniform-scenario fairness (queueset_test.go TestBaseline family) ----

    struct SimClient {
        hash: u64,
        threads: usize,
        done: usize,
    }

    /// Always-busy clients (think time 0) over `limit` seats of 1s requests,
    /// driven in 10ms ticks of a fake clock. Returns completions per client.
    fn simulate(
        queues: isize,
        qll: isize,
        hand: isize,
        limit: usize,
        clients: &mut [SimClient],
        initial_seats: Vec<u64>,
        secs: u64,
    ) -> Vec<usize> {
        let clk = FakeClock::new();
        let qs = QueueSet::new(
            clk.clone(),
            qcfg(queues, qll, hand),
            DispatchingConfig {
                concurrency_limit: limit,
            },
        )
        .unwrap();
        let mut pending: Vec<(usize, RequestHandle)> = vec![];
        // (finish_at, client, execution)
        let mut running: Vec<(Duration, usize, Execution)> = vec![];
        let start_one =
            |c: usize, clients: &[SimClient], pending: &mut Vec<(usize, RequestHandle)>| {
                let h = qs
                    .start_request(&we(initial_seats[c]), clients[c].hash, "", "fs")
                    .expect("not rejected");
                pending.push((c, h));
            };
        for c in 0..clients.len() {
            for _ in 0..clients[c].threads {
                start_one(c, clients, &mut pending);
            }
        }
        let end = Duration::from_secs(secs);
        while clk.now() < end {
            loop {
                let mut changed = false;
                let mut still = vec![];
                for (c, h) in pending.drain(..) {
                    match h.try_execution() {
                        Ok(ex) => {
                            running.push((clk.now() + Duration::from_secs(1), c, ex));
                            changed = true;
                        }
                        Err(h) => still.push((c, h)),
                    }
                }
                pending = still;
                let now = clk.now();
                let mut i = 0;
                while i < running.len() {
                    if running[i].0 <= now {
                        let (_, c, ex) = running.swap_remove(i);
                        drop(ex);
                        clients[c].done += 1;
                        start_one(c, clients, &mut pending);
                        changed = true;
                    } else {
                        i += 1;
                    }
                }
                if !changed {
                    break;
                }
            }
            clk.advance(Duration::from_millis(10));
        }
        clients.iter().map(|c| c.done).collect()
    }

    /// Two always-busy flows with very different offered load get equal
    /// shares of a saturated level (fair, not proportional to demand).
    #[test]
    fn heavy_and_light_flow_share_equally() {
        // hash % 9: 1001001001 -> queue 4, 2002002002 -> queue 8 (distinct).
        let mut clients = [
            SimClient {
                hash: 1001001001,
                threads: 8,
                done: 0,
            },
            SimClient {
                hash: 2002002002,
                threads: 2,
                done: 0,
            },
        ];
        let done = simulate(9, 20, 1, 2, &mut clients, vec![1, 1], 100);
        let (a, b) = (done[0] as f64, done[1] as f64);
        assert!(a + b >= 190.0, "work conserving: {a}+{b}");
        assert!((a - b).abs() / ((a + b) / 2.0) < 0.05, "fair: {a} vs {b}");
    }

    /// A flow offering less than its fair share is fully served and the
    /// surplus goes to the other flow (max-min fairness; TestDifferentFlowsExpectUnequal).
    #[test]
    fn unused_share_is_given_to_the_busy_flow() {
        let mut clients = [
            SimClient {
                hash: 1001001001,
                threads: 6,
                done: 0,
            },
            SimClient {
                hash: 2002002002,
                threads: 1,
                done: 0,
            },
        ];
        // 4 seats: B can use only 1; A should get the other 3.
        let done = simulate(9, 20, 1, 4, &mut clients, vec![1, 1], 100);
        assert!(done[1] >= 95, "B served at its demand: {}", done[1]);
        assert!(done[0] >= 290, "A gets the surplus: {}", done[0]);
    }

    /// Width matters: a 2-seat flow gets as many seat-seconds as a 1-seat flow
    /// (TestDifferentWidths), i.e. half the request rate.
    #[test]
    fn wider_requests_get_equal_seat_seconds() {
        let mut clients = [
            SimClient {
                hash: 1001001001,
                threads: 6,
                done: 0,
            },
            SimClient {
                hash: 2002002002,
                threads: 6,
                done: 0,
            },
        ];
        let done = simulate(9, 20, 1, 4, &mut clients, vec![1, 2], 100);
        let seat_secs = [done[0] as f64, done[1] as f64 * 2.0];
        assert!(
            (seat_secs[0] - seat_secs[1]).abs() / ((seat_secs[0] + seat_secs[1]) / 2.0) < 0.1,
            "{seat_secs:?}"
        );
    }
}
