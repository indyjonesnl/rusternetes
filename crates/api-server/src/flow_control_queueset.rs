//! API Priority and Fairness: the shuffle-sharded fair `QueueSet`.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/queueset`
//! (release-1.35), together with the pieces it is built from:
//!
//! - `queueset.go`: `StartRequest`, `shuffleShardAndRejectOrEnqueueLocked`,
//!   `rejectOrEnqueueToBoundLocked`, `dispatchLocked`,
//!   `findDispatchQueueToBoundLocked`, `finishRequestLocked`,
//!   `boundNextDispatchLocked` (anti-windup), `syncTimeLocked` /
//!   `advanceEpoch` (R rollover), `getVirtualTimeRatioLocked`,
//!   `canAccommodateSeatsLocked`, `removeQueueIfEmptyLocked`,
//!   `setConfiguration` (queues added at once, removed by attrition).
//! - `types.go`: `completedWorkEstimate`, `queueSum`.
//! - `fifo_list.go`: the per-queue FIFO with running `queueSum`.
//! - `request/seat_seconds.go`: fixed-point `SeatSeconds` (1e8 scale, u64,
//!   wrapping arithmetic exactly as Go's `uint64`).
//! - `util/shufflesharding/shufflesharding.go`: `Dealer`.
//!
//! The mechanism is ported whole: virtual-time fair dispatch (R meter,
//! per-queue `nextDispatchR`, min virtual-finish selection in round-robin
//! order), shuffle sharding with least-work-queue pick, queue length limit,
//! seat accounting incl. `FinalSeats`/`AdditionalLatency` lingering, the
//! too-wide-request rule, exemption (`DesiredNumQueues < 0`) and the
//! queueless concurrency-limit-only mode (`DesiredNumQueues == 0`).
//!
//! DELIBERATE DEVIATIONS (each is an Idiomatic-Rust expression change, not a
//! mechanism change):
//!
//! - Go's per-request `promise.WriteOnce` + `context.Context` becomes a
//!   `tokio::sync::Notify` plus "cancel on drop" of [`RequestHandle`]. Upstream
//!   `wait()` takes the lock and ejects a still-queued request when the ctx is
//!   done; here dropping the handle does the same under the lock, so the
//!   `decision.Set(decisionExecute)` returning false branch of `dispatchLocked`
//!   (a request cancelled between dispatch pick and set) cannot arise.
//! - Metrics (`metrics.*`, `RatioedGauge`, seat-demand integrator,
//!   `queueNoteFn`) and `Dump` are not ported; `ConcurrencyDenominator` is
//!   metrics-only and so omitted from [`DispatchingConfig`].
//! - `fifo` removal is O(n) in queue length (a `VecDeque`) instead of a linked
//!   list handle; queue length is bounded by `QueueLengthLimit`.
//! - Queues are identified by a stable id (Go uses `*queue` pointers) and the
//!   positional `index` is derived, which is what `removeQueueAndUpdateIndexes`
//!   maintains.
//!
//! This module is NOT yet wired into `flow_control.rs`; see the follow-up
//! issues referenced from the PR.

use crate::flow_control_integrator::Integrator;
use crate::flow_control_metrics as fcmetrics;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info};

// ---------------------------------------------------------------------------
// request/seat_seconds.go
// ---------------------------------------------------------------------------

const SS_SCALE: f64 = 1e8;

/// `SeatSeconds` (seat_seconds.go:29): fixed-point work measure; `n`
/// represents `n/1e8` seat-seconds. Arithmetic wraps like Go's `uint64`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeatSeconds(pub u64);

/// `MaxSeatSeconds` (seat_seconds.go:32).
pub const MAX_SEAT_SECONDS: SeatSeconds = SeatSeconds(u64::MAX);
/// `MinSeatSeconds` (seat_seconds.go:35).
pub const MIN_SEAT_SECONDS: SeatSeconds = SeatSeconds(0);

impl std::ops::Add for SeatSeconds {
    type Output = SeatSeconds;
    fn add(self, o: SeatSeconds) -> SeatSeconds {
        SeatSeconds(self.0.wrapping_add(o.0))
    }
}
impl std::ops::Sub for SeatSeconds {
    type Output = SeatSeconds;
    fn sub(self, o: SeatSeconds) -> SeatSeconds {
        SeatSeconds(self.0.wrapping_sub(o.0))
    }
}
impl std::ops::AddAssign for SeatSeconds {
    fn add_assign(&mut self, o: SeatSeconds) {
        *self = *self + o;
    }
}
impl std::ops::SubAssign for SeatSeconds {
    fn sub_assign(&mut self, o: SeatSeconds) {
        *self = *self - o;
    }
}

impl SeatSeconds {
    /// `ToFloat` (seat_seconds.go:46).
    pub fn to_float(self) -> f64 {
        self.0 as f64 / SS_SCALE
    }

    /// `DurationPerSeat` (seat_seconds.go:52), in nanoseconds.
    pub fn duration_per_seat(self, seats: f64) -> i64 {
        (self.0 as f64 / seats * (1e9 / SS_SCALE)) as i64
    }
}

impl std::fmt::Display for SeatSeconds {
    /// `String` (seat_seconds.go:58).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let div = SS_SCALE as u64;
        write!(f, "{}.{:08}ss", self.0 / div, self.0 % div)
    }
}

/// `SeatsTimesDuration` (seat_seconds.go:40). `nanos` is signed because
/// `finishRequestLocked` passes `estimated - actual`, which may be negative;
/// the Go `int64 -> uint64` conversion wraps and so does this.
pub fn seats_times_duration(seats: f64, nanos: i64) -> SeatSeconds {
    SeatSeconds((seats * nanos as f64 / (1e9 / SS_SCALE)).round() as i64 as u64)
}

fn nanos(d: Duration) -> i64 {
    d.as_nanos().min(i64::MAX as u128) as i64
}

// ---------------------------------------------------------------------------
// util/shufflesharding/shufflesharding.go
// ---------------------------------------------------------------------------

pub mod shufflesharding {
    /// `MaxHashBits` (shufflesharding.go:28).
    pub const MAX_HASH_BITS: usize = 60;

    /// `RequiredEntropyBits` (shufflesharding.go:35).
    pub fn required_entropy_bits(deck_size: usize, hand_size: usize) -> usize {
        ((deck_size as f64).log2() * hand_size as f64).ceil() as usize
    }

    /// `Dealer` (shufflesharding.go:42).
    #[derive(Debug, Clone)]
    pub struct Dealer {
        deck_size: usize,
        hand_size: usize,
    }

    impl Dealer {
        /// `NewDealer` (shufflesharding.go:55); error strings verbatim.
        pub fn new(deck_size: isize, hand_size: isize) -> Result<Dealer, String> {
            if deck_size <= 0 || hand_size <= 0 {
                return Err(format!(
                    "deckSize {} or handSize {} is not positive",
                    deck_size, hand_size
                ));
            }
            if hand_size > deck_size {
                return Err(format!(
                    "handSize {} is greater than deckSize {}",
                    hand_size, deck_size
                ));
            }
            if deck_size > 1 << 26 {
                return Err(format!("deckSize {} is impractically large", deck_size));
            }
            let (d, h) = (deck_size as usize, hand_size as usize);
            if required_entropy_bits(d, h) > MAX_HASH_BITS {
                return Err(format!(
                    "required entropy bits of deckSize {} and handSize {} is greater than {}",
                    deck_size, hand_size, MAX_HASH_BITS
                ));
            }
            Ok(Dealer {
                deck_size: d,
                hand_size: h,
            })
        }

        /// `Deal` (shufflesharding.go:80).
        pub fn deal(&self, mut hash_value: u64, mut pick: impl FnMut(usize)) {
            // 15 is the largest possible value of handSize (Go comment).
            let mut remainders = [0usize; 15];
            for (i, r) in remainders.iter_mut().enumerate().take(self.hand_size) {
                let divisor = (self.deck_size - i) as u64;
                let next = hash_value / divisor;
                *r = (hash_value - divisor * next) as usize;
                hash_value = next;
            }
            for i in 0..self.hand_size {
                let mut card = remainders[i];
                for j in (1..=i).rev() {
                    if card >= remainders[j - 1] {
                        card += 1;
                    }
                }
                pick(card);
            }
        }

        /// `DealIntoHand` (shufflesharding.go:103).
        pub fn deal_into_hand(&self, hash_value: u64) -> Vec<usize> {
            let mut h = Vec::with_capacity(self.hand_size);
            self.deal(hash_value, |c| h.push(c));
            h
        }
    }
}

use shufflesharding::Dealer;

// ---------------------------------------------------------------------------
// Configuration and request types (fairqueuing/interface.go, request/width.go)
// ---------------------------------------------------------------------------

/// `fq.QueuingConfig` (interface.go:113).
#[derive(Clone, Debug, Default)]
pub struct QueuingConfig {
    pub name: String,
    /// `<0`: exempt (dispatch immediately); `0`: concurrency limit only, no
    /// queues; `>0`: that many queues.
    pub desired_num_queues: isize,
    pub queue_length_limit: isize,
    pub hand_size: isize,
}

/// `fq.DispatchingConfig` (interface.go:134), minus the metrics-only
/// `ConcurrencyDenominator`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DispatchingConfig {
    pub concurrency_limit: usize,
}

/// `fcrequest.WorkEstimate` (width.go:34).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkEstimate {
    pub initial_seats: u64,
    pub final_seats: u64,
    pub additional_latency: Duration,
}

impl WorkEstimate {
    /// `MaxSeats` (width.go:52).
    pub fn max_seats(&self) -> usize {
        self.initial_seats.max(self.final_seats) as usize
    }
}

/// `completedWorkEstimate` (types.go:94).
#[derive(Clone, Copy, Debug)]
pub struct CompletedWorkEstimate {
    pub initial_seats: u64,
    pub final_seats: u64,
    pub additional_latency_ns: i64,
    pub total_work: SeatSeconds,
    pub final_work: SeatSeconds,
}

impl CompletedWorkEstimate {
    pub fn max_seats(&self) -> usize {
        self.initial_seats.max(self.final_seats) as usize
    }
}

/// `queueSum` (types.go:128): running totals over a queue's waiting requests.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueueSum {
    pub initial_seats_sum: usize,
    pub max_seats_sum: usize,
    pub total_work_sum: SeatSeconds,
}

/// Why `start_request` refused a request (the metrics `reason` label).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectReason {
    /// Queueless mode and no seats (queueset.go:226).
    ConcurrencyLimit,
    /// Seats exhausted and the shuffle-sharded queue is full (queueset.go:241).
    QueueFull,
}

/// A rejected request; `idle` is the queueset's idleness as returned by
/// upstream's `StartRequest`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rejected {
    pub reason: RejectReason,
    pub idle: bool,
}

/// Snapshot of the counters `Dump` reports (queueset.go:1000-1010).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueSetStats {
    pub waiting: usize,
    pub executing: usize,
    pub seats_in_use: usize,
    pub seats_waiting: usize,
    pub dispatched: u64,
    pub rejected: u64,
    pub timedout: u64,
    pub cancelled: u64,
}

// ---------------------------------------------------------------------------
// Clock (eventclock.Interface)
// ---------------------------------------------------------------------------

/// The slice of `eventclock.Interface` the queueset uses.
pub trait Clock: Send + Sync {
    /// Monotonic time since an arbitrary epoch.
    fn now(&self) -> Duration;
    /// `EventAfterDuration`: run `f` once after `d`.
    fn after(&self, d: Duration, f: Box<dyn FnOnce() + Send>);
}

/// Wall-clock implementation. `after` spawns a tokio task.
pub struct RealClock {
    start: std::time::Instant,
}

impl Default for RealClock {
    fn default() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }
}

impl Clock for RealClock {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }
    fn after(&self, d: Duration, f: Box<dyn FnOnce() + Send>) {
        tokio::spawn(async move {
            tokio::time::sleep(d).await;
            f();
        });
    }
}

// ---------------------------------------------------------------------------
// queueset.go
// ---------------------------------------------------------------------------

/// `rDecrement` (queueset.go:373).
pub const R_DECREMENT: SeatSeconds = SeatSeconds(u64::MAX / 2);
/// `highR` (queueset.go:376).
pub const HIGH_R: SeatSeconds = SeatSeconds(R_DECREMENT.0 + R_DECREMENT.0 / 2);
/// `estimatedServiceDuration: 3 * time.Millisecond` (queueset.go:180).
const ESTIMATED_SERVICE_DURATION_NS: i64 = 3_000_000;

#[derive(Debug)]
pub struct Request {
    pub fs_name: String,
    pub flow_distinguisher: String,
    /// `None` if the request did not go through a queue.
    pub queue_id: Option<u64>,
    pub work: CompletedWorkEstimate,
    /// `decision == decisionExecute`.
    pub dispatched: bool,
    pub notify: Arc<Notify>,
    pub arrival_time: Duration,
    /// `R(arrivalTime)`.
    pub arrival_r: SeatSeconds,
    pub start_time: Duration,
}

#[derive(Debug)]
pub struct Queue {
    pub id: u64,
    /// Requests not yet executing, oldest first (`requestsWaiting`).
    pub waiting: VecDeque<u64>,
    pub sum: QueueSum,
    pub next_dispatch_r: SeatSeconds,
    pub executing: HashSet<u64>,
    pub seats_in_use: usize,
}

impl Queue {
    fn new(id: u64) -> Queue {
        Queue {
            id,
            waiting: VecDeque::new(),
            sum: QueueSum::default(),
            next_dispatch_r: SeatSeconds(0),
            executing: HashSet::new(),
            seats_in_use: 0,
        }
    }
}

pub struct Inner {
    pub clock: Arc<dyn Clock>,
    pub estimated_service_duration_ns: i64,
    pub qcfg: QueuingConfig,
    pub dcfg: DispatchingConfig,
    pub dealer: Option<Dealer>,
    pub queues: Vec<Queue>,
    pub requests: HashMap<u64, Request>,
    /// Executing requests that went through no queue.
    pub queueless_executing: HashSet<u64>,
    pub current_r: SeatSeconds,
    pub last_real_time: Duration,
    pub robin_index: isize,
    pub tot_requests_waiting: usize,
    pub tot_requests_executing: usize,
    pub tot_seats_in_use: usize,
    pub tot_seats_waiting: usize,
    /// `qs.seatDemandIntegrator`: integrates `totSeatsInUse +
    /// totSeatsWaiting` (the borrowing adjustment reads and resets it).
    pub seat_demand: Arc<Integrator>,
    pub enqueues: usize,
    pub tot_requests_dispatched: u64,
    pub tot_requests_rejected: u64,
    pub tot_requests_timedout: u64,
    pub tot_requests_cancelled: u64,
    pub next_request_id: u64,
    pub next_queue_id: u64,
}

/// `checkConfig` (queueset.go:197).
pub fn check_config(qcfg: &QueuingConfig) -> Result<Option<Dealer>, String> {
    if qcfg.desired_num_queues <= 0 {
        return Ok(None);
    }
    Dealer::new(qcfg.desired_num_queues, qcfg.hand_size)
        .map(Some)
        .map_err(|e| {
            format!(
                "the QueueSetConfig implies an invalid shuffle sharding config \
                 (DesiredNumQueues is deckSize): {}",
                e
            )
        })
}

impl Inner {
    pub fn new(clock: Arc<dyn Clock>) -> Inner {
        let now = clock.now();
        Inner {
            seat_demand: Arc::new(Integrator::new(clock.clone())),
            clock,
            estimated_service_duration_ns: ESTIMATED_SERVICE_DURATION_NS,
            qcfg: QueuingConfig::default(),
            dcfg: DispatchingConfig::default(),
            dealer: None,
            queues: Vec::new(),
            requests: HashMap::new(),
            queueless_executing: HashSet::new(),
            current_r: SeatSeconds(0),
            last_real_time: now,
            robin_index: 0,
            tot_requests_waiting: 0,
            tot_requests_executing: 0,
            tot_seats_in_use: 0,
            tot_seats_waiting: 0,
            enqueues: 0,
            tot_requests_dispatched: 0,
            tot_requests_rejected: 0,
            tot_requests_timedout: 0,
            tot_requests_cancelled: 0,
            next_request_id: 0,
            next_queue_id: 0,
        }
    }

    pub fn create_queue(&mut self) -> Queue {
        let id = self.next_queue_id;
        self.next_queue_id += 1;
        Queue::new(id)
    }

    pub fn queue_pos(&self, id: u64) -> Option<usize> {
        self.queues.iter().position(|q| q.id == id)
    }

    /// `setConfiguration` (queueset.go:233).
    pub fn set_configuration(
        &mut self,
        mut qcfg: QueuingConfig,
        dealer: Option<Dealer>,
        dcfg: DispatchingConfig,
    ) {
        self.sync_time();
        if qcfg.desired_num_queues > 0 {
            // Adding queues is the only thing that requires immediate action;
            // removal is by attrition (removeQueueIfEmptyLocked).
            let want = qcfg.desired_num_queues as usize;
            while self.queues.len() < want {
                let q = self.create_queue();
                self.queues.push(q);
            }
        } else {
            qcfg.queue_length_limit = self.qcfg.queue_length_limit;
            qcfg.hand_size = self.qcfg.hand_size;
        }
        self.qcfg = qcfg;
        self.dcfg = dcfg;
        self.dealer = dealer;
        self.dispatch_as_much_as_possible();
    }

    fn is_idle(&self) -> bool {
        self.tot_requests_waiting == 0 && self.tot_requests_executing == 0
    }

    /// `syncTimeLocked` (queueset.go:392).
    pub fn sync_time(&mut self) {
        let real_now = self.clock.now();
        let since_last = nanos(real_now.saturating_sub(self.last_real_time));
        self.last_real_time = real_now;
        let prev_r = self.current_r;
        let incr_r = seats_times_duration(self.virtual_time_ratio(), since_last);
        self.current_r = prev_r + incr_r;
        if prev_r > self.current_r {
            error!(
                qs = %self.qcfg.name, %prev_r, %incr_r, current_r = %self.current_r,
                "queueset::currentR overflow"
            );
        } else if self.current_r >= HIGH_R {
            self.advance_epoch(incr_r);
        }
        fcmetrics::set_current_r(&self.qcfg.name, self.current_r.to_float());
    }

    /// `advanceEpoch` (queueset.go:416).
    fn advance_epoch(&mut self, incr_r: SeatSeconds) {
        let mut success = true;
        let old_r = self.current_r;
        self.current_r -= R_DECREMENT;
        info!(qs = %self.qcfg.name, %old_r, new_r = %self.current_r, %incr_r, "Advancing epoch");
        for (q_idx, queue) in self.queues.iter_mut().enumerate() {
            if queue.waiting.is_empty() && queue.executing.is_empty() {
                // Do not just decrement, the value could be quite outdated.
                queue.next_dispatch_r = SeatSeconds(0);
                continue;
            }
            let old = queue.next_dispatch_r;
            queue.next_dispatch_r -= R_DECREMENT;
            if queue.next_dispatch_r > old {
                error!(qs = %self.qcfg.name, queue = q_idx, "queue::nextDispatchR underflow");
                success = false;
            }
            for rid in &queue.waiting {
                if let Some(req) = self.requests.get_mut(rid) {
                    let old = req.arrival_r;
                    req.arrival_r -= R_DECREMENT;
                    if req.arrival_r > old {
                        error!(qs = %self.qcfg.name, queue = q_idx, "request::arrivalR underflow");
                        success = false;
                    }
                }
            }
        }
        // queueset.go:521
        fcmetrics::add_epoch_advance(&self.qcfg.name, success);
    }

    /// `qs.seatDemandIntegrator.Set(totSeatsInUse + totSeatsWaiting)`
    /// (queueset.go:438, :650, :682, :711, :728, :880).
    fn note_seat_demand(&self) {
        let demand = (self.tot_seats_in_use + self.tot_seats_waiting) as f64;
        self.seat_demand.set(demand);
        // The union gauge also feeds `demand_seats` (apf_controller.go:722).
        fcmetrics::set_seat_demand(&self.qcfg.name, demand);
    }

    /// `getVirtualTimeRatioLocked` (queueset.go:455).
    fn virtual_time_ratio(&self) -> f64 {
        let mut active_queues = 0usize;
        let mut seats_requested = 0usize;
        for queue in &self.queues {
            // The sum of the maximum width of the requests in this queue: the
            // maximum rate at which the queue could work.
            seats_requested += queue.seats_in_use + queue.sum.max_seats_sum;
            if !queue.waiting.is_empty() || !queue.executing.is_empty() {
                active_queues += 1;
            }
        }
        if active_queues == 0 {
            return 0.0;
        }
        (seats_requested.min(self.dcfg.concurrency_limit)) as f64 / active_queues as f64
    }

    /// `completeWorkEstimate` (types.go:112).
    pub fn complete_work_estimate(&self, we: &WorkEstimate) -> CompletedWorkEstimate {
        let latency = nanos(we.additional_latency);
        let final_work = seats_times_duration(we.final_seats as f64, latency);
        let initial_work =
            seats_times_duration(we.initial_seats as f64, self.estimated_service_duration_ns);
        CompletedWorkEstimate {
            initial_seats: we.initial_seats,
            final_seats: we.final_seats,
            additional_latency_ns: latency,
            total_work: initial_work + final_work,
            final_work,
        }
    }

    /// `canAccommodateSeatsLocked` (queueset.go:683).
    fn can_accommodate_seats(&self, seats: usize) -> bool {
        if self.qcfg.desired_num_queues < 0 {
            // Code for exemption from limitation.
            return true;
        }
        if seats > self.dcfg.concurrency_limit {
            // The request is wider than the whole level: it may run only when
            // nothing else is executing (upstream TODO: until borrowing).
            return self.tot_requests_executing == 0;
        }
        self.tot_seats_in_use + seats <= self.dcfg.concurrency_limit
    }

    pub fn new_request(
        &mut self,
        fs_name: &str,
        flow_distinguisher: &str,
        queue_id: Option<u64>,
        work: CompletedWorkEstimate,
        dispatched: bool,
    ) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        let now = self.clock.now();
        self.requests.insert(
            id,
            Request {
                fs_name: fs_name.to_string(),
                flow_distinguisher: flow_distinguisher.to_string(),
                queue_id,
                work,
                dispatched,
                notify: Arc::new(Notify::new()),
                arrival_time: now,
                arrival_r: self.current_r,
                start_time: if dispatched { now } else { Duration::ZERO },
            },
        );
        id
    }

    /// `shuffleShardLocked` (queueset.go:518). Returns the queue position.
    fn shuffle_shard(&mut self, hash_value: u64) -> usize {
        let dealer = self.dealer.as_ref().expect("dealer present when queuing");
        let hand = dealer.deal_into_hand(hash_value);
        let hand_size = hand.len();
        let offset = self.enqueues % hand_size;
        self.enqueues += 1;
        let mut best: Option<usize> = None;
        let mut min_seat_seconds = MAX_SEAT_SECONDS;
        for i in 0..hand_size {
            let queue_idx = hand[(offset + i) % hand_size];
            // The total work in seat-seconds of requests waiting in this queue;
            // pick the minimum.
            let this = self.queues[queue_idx].sum.total_work_sum;
            if this < min_seat_seconds {
                min_seat_seconds = this;
                best = Some(queue_idx);
            }
        }
        best.unwrap_or(hand[0])
    }

    /// `shuffleShardAndRejectOrEnqueueLocked` (queueset.go:490): the id of the
    /// enqueued request, or `None` if rejected.
    fn shuffle_shard_and_reject_or_enqueue(
        &mut self,
        we: &WorkEstimate,
        hash_value: u64,
        flow_distinguisher: &str,
        fs_name: &str,
    ) -> Option<u64> {
        let qpos = self.shuffle_shard(hash_value);
        let qid = self.queues[qpos].id;
        let work = self.complete_work_estimate(we);
        // rejectOrEnqueueToBoundLocked (queueset.go:564)
        let cur_len = self.queues[qpos].waiting.len();
        let ok = !(self.tot_seats_in_use >= self.dcfg.concurrency_limit
            && cur_len as isize >= self.qcfg.queue_length_limit);
        let rid = if ok {
            let rid = self.new_request(fs_name, flow_distinguisher, Some(qid), work, false);
            self.enqueue_to_bound(qid, rid);
            // queueset.go:577 `ObserveQueueLength`
            fcmetrics::observe_queue_length(
                &self.qcfg.name,
                fs_name,
                self.queues[qpos].waiting.len(),
            );
            Some(rid)
        } else {
            None
        };
        self.bound_next_dispatch(qid);
        rid
    }

    /// `enqueueToBoundLocked` (queueset.go:580).
    pub fn enqueue_to_bound(&mut self, qid: u64, rid: u64) {
        let qpos = self.queue_pos(qid).expect("queue exists");
        if self.queues[qpos].waiting.is_empty() && self.queues[qpos].executing.is_empty() {
            // The queue's start R is set to the virtual time.
            self.queues[qpos].next_dispatch_r = self.current_r;
        }
        let w = self.requests[&rid].work;
        let q = &mut self.queues[qpos];
        q.waiting.push_back(rid);
        q.sum.initial_seats_sum += w.initial_seats as usize;
        q.sum.max_seats_sum += w.max_seats();
        q.sum.total_work_sum += w.total_work;
        self.tot_requests_waiting += 1;
        self.tot_seats_waiting += w.max_seats();
        self.note_seat_demand();
    }

    /// The FIFO's `removeFromQueueLocked` (fifo_list.go:79): remove a
    /// specific waiting request, deducting it from the queue sum. Returns
    /// false if it was not waiting.
    pub fn remove_waiting(&mut self, qid: u64, rid: u64) -> bool {
        let Some(qpos) = self.queue_pos(qid) else {
            return false;
        };
        let q = &mut self.queues[qpos];
        let Some(i) = q.waiting.iter().position(|&r| r == rid) else {
            return false;
        };
        q.waiting.remove(i);
        let w = self.requests[&rid].work;
        let q = &mut self.queues[qpos];
        q.sum.initial_seats_sum -= w.initial_seats as usize;
        q.sum.max_seats_sum -= w.max_seats();
        q.sum.total_work_sum -= w.total_work;
        true
    }

    /// `dispatchAsMuchAsPossibleLocked` (queueset.go:612).
    fn dispatch_as_much_as_possible(&mut self) {
        while self.tot_requests_waiting != 0
            && self.tot_seats_in_use < self.dcfg.concurrency_limit
            && self.dispatch()
        {}
    }

    /// `dispatchSansQueueLocked` (queueset.go:617).
    fn dispatch_sans_queue(
        &mut self,
        we: &WorkEstimate,
        flow_distinguisher: &str,
        fs_name: &str,
    ) -> u64 {
        let work = self.complete_work_estimate(we);
        let rid = self.new_request(fs_name, flow_distinguisher, None, work, true);
        self.tot_requests_executing += 1;
        self.tot_seats_in_use += work.max_seats();
        self.note_seat_demand();
        self.queueless_executing.insert(rid);
        rid
    }

    /// `dispatchLocked` (queueset.go:650).
    fn dispatch(&mut self) -> bool {
        let Some((qid, rid)) = self.find_dispatch_queue_to_bound() else {
            return false;
        };
        let now = self.clock.now();
        let work = self.requests[&rid].work;
        self.tot_requests_waiting -= 1;
        self.tot_seats_waiting -= work.max_seats();
        {
            // Decision is set exactly once, under the lock.
            let req = self.requests.get_mut(&rid).expect("request");
            req.dispatched = true;
            req.start_time = now;
            req.notify.notify_one();
        }
        // The request leaves its queue and starts executing.
        self.tot_requests_executing += 1;
        self.tot_seats_in_use += work.max_seats();
        self.note_seat_demand();
        let qpos = self.queue_pos(qid).expect("queue");
        let queue = &mut self.queues[qpos];
        queue.executing.insert(rid);
        queue.seats_in_use += work.max_seats();
        if work.total_work > SeatSeconds(R_DECREMENT.0 / 100) {
            // A single increment should never be so big.
            error!(qs = %self.qcfg.name, "dispatching request with implausibly high work");
        }
        // When a request is dequeued for service -> nextDispatchR += G * width.
        queue.next_dispatch_r += work.total_work;
        self.bound_next_dispatch(qid);
        true
    }

    /// `findDispatchQueueToBoundLocked` (queueset.go:700). Returns the queue
    /// id and the (already removed from its queue) oldest request.
    pub fn find_dispatch_queue_to_bound(&mut self) -> Option<(u64, u64)> {
        let mut min_virtual_finish = MAX_SEAT_SECONDS;
        let mut min_queue: Option<usize> = None;
        let mut s_min = MAX_SEAT_SECONDS;
        let mut ds_min = MAX_SEAT_SECONDS;
        let mut s_max = MIN_SEAT_SECONDS;
        let mut ds_max = MIN_SEAT_SECONDS;
        let nq = self.queues.len();
        for _ in 0..nq {
            self.robin_index = (self.robin_index + 1) % nq as isize;
            let queue = &self.queues[self.robin_index as usize];
            if let Some(oldest) = queue.waiting.front() {
                let oldest_work = self.requests[oldest].work;
                s_min = s_min.min(queue.next_dispatch_r);
                s_max = s_max.max(queue.next_dispatch_r);
                let estimated_work_in_progress = seats_times_duration(
                    queue.seats_in_use as f64,
                    self.estimated_service_duration_ns,
                );
                let discounted = queue.next_dispatch_r - estimated_work_in_progress;
                ds_min = ds_min.min(discounted);
                ds_max = ds_max.max(discounted);
                let current_virtual_finish = queue.next_dispatch_r + oldest_work.total_work;
                if current_virtual_finish < min_virtual_finish {
                    min_virtual_finish = current_virtual_finish;
                    min_queue = Some(self.robin_index as usize);
                }
            }
        }
        let min_index = min_queue?;
        let qid = self.queues[min_index].id;
        let rid = *self.queues[min_index].waiting.front()?;
        let max_seats = self.requests[&rid].work.max_seats();
        if !self.can_accommodate_seats(max_seats) {
            // Not advancing the round robin index further; the head of the
            // selected queue waits for executing requests to complete.
            // queueset.go:819
            let fs = self.requests[&rid].fs_name.clone();
            fcmetrics::add_dispatch_with_no_accommodation(&self.qcfg.name, &fs);
            return None;
        }
        self.remove_waiting(qid, rid);

        // If the requested final seats exceed capacity, reduce them to the
        // limit and adjust additional latency to preserve the total work.
        let limit = self.dcfg.concurrency_limit as u64;
        let req = self.requests.get_mut(&rid).expect("request");
        if req.work.final_seats > limit {
            req.work.additional_latency_ns = req.work.final_work.duration_per_seat(limit as f64);
            req.work.final_seats = limit;
        }

        // The next round starts at the chosen queue, so non-selected queues
        // win in the case that the virtual finish times are the same.
        self.robin_index = min_index as isize;

        if self.queues[min_index].next_dispatch_r < self.requests[&rid].arrival_r {
            error!(qs = %self.qcfg.name, "dispatch before arrival");
        }
        // queueset.go:842
        fcmetrics::set_dispatch_metrics(
            &self.qcfg.name,
            self.current_r.to_float(),
            self.queues[min_index].next_dispatch_r.to_float(),
            s_min.to_float(),
            s_max.to_float(),
            ds_min.to_float(),
            ds_max.to_float(),
        );
        Some((qid, rid))
    }

    /// `finishRequestLocked` (queueset.go:812). Returns the lingering
    /// duration when the seats are to be released later (`AdditionalLatency
    /// > 0`), in which case the caller schedules `release_seats`.
    fn finish_request(&mut self, rid: u64) -> Option<Duration> {
        let now = self.clock.now();
        self.tot_requests_executing -= 1;
        let (queue_id, start, initial_seats, latency) = {
            let r = &self.requests[&rid];
            (
                r.queue_id,
                r.start_time,
                r.work.initial_seats,
                r.work.additional_latency_ns,
            )
        };
        let actual_ns = nanos(now.saturating_sub(start));
        if let Some(qid) = queue_id {
            if let Some(qpos) = self.queue_pos(qid) {
                let est = self.estimated_service_duration_ns;
                let q = &mut self.queues[qpos];
                q.executing.remove(&rid);
                // The actual service time was S: nextDispatchR -= (G - S)*width.
                q.next_dispatch_r -= seats_times_duration(initial_seats as f64, est - actual_ns);
            }
            self.bound_next_dispatch(qid);
        } else {
            self.queueless_executing.remove(&rid);
        }
        if latency <= 0 {
            self.release_seats(rid);
            None
        } else {
            Some(Duration::from_nanos(latency as u64))
        }
    }

    /// `releaseSeatsLocked` (queueset.go:836).
    fn release_seats(&mut self, rid: u64) {
        let Some(r) = self.requests.remove(&rid) else {
            return;
        };
        let max = r.work.max_seats();
        self.tot_seats_in_use -= max;
        self.note_seat_demand();
        if let Some(qid) = r.queue_id {
            if let Some(qpos) = self.queue_pos(qid) {
                self.queues[qpos].seats_in_use -= max;
            }
            self.remove_queue_if_empty(qid);
        }
    }

    /// `boundNextDispatchLocked` (queueset.go:924): the anti-windup hack.
    fn bound_next_dispatch(&mut self, qid: u64) {
        let Some(qpos) = self.queue_pos(qid) else {
            return;
        };
        let Some(oldest) = self.queues[qpos].waiting.front() else {
            return;
        };
        let bound = self.requests[oldest].arrival_r;
        let q = &mut self.queues[qpos];
        if q.next_dispatch_r < bound {
            q.next_dispatch_r = bound;
        }
    }

    /// `removeQueueIfEmptyLocked` (queueset.go:937).
    fn remove_queue_if_empty(&mut self, qid: u64) {
        let Some(qpos) = self.queue_pos(qid) else {
            return;
        };
        // If there are more queues than desired and this one has no requests
        // then remove it.
        if self.queues.len() as isize > self.qcfg.desired_num_queues
            && self.queues[qpos].waiting.is_empty()
            && self.queues[qpos].executing.is_empty()
        {
            self.queues.remove(qpos);
            // Decrement to maintain the invariant that (robinIndex+1) % numQueues
            // is the index of the next queue after the one last dispatched from.
            if self.robin_index >= qpos as isize {
                self.robin_index -= 1;
            }
        }
    }

    /// The ctx-done path of `request.wait` (queueset.go:305-329).
    fn cancel_waiting(&mut self, rid: u64) {
        let Some(r) = self.requests.get(&rid) else {
            return;
        };
        let Some(qid) = r.queue_id else {
            return;
        };
        let max = r.work.max_seats();
        if self.remove_waiting(qid, rid) {
            self.tot_requests_waiting -= 1;
            self.tot_seats_waiting -= max;
            self.note_seat_demand();
            self.tot_requests_rejected += 1;
            self.tot_requests_cancelled += 1;
            self.requests.remove(&rid);
            self.bound_next_dispatch(qid);
        }
    }
}

/// A queueset (`queueSet`, queueset.go:73). Cheap to share via `Arc`.
pub struct QueueSet {
    inner: Mutex<Inner>,
    weak_self: Weak<QueueSet>,
}

impl QueueSet {
    /// `BeginConstruction` + `Complete` (queueset.go:158-196).
    pub fn new(
        clock: Arc<dyn Clock>,
        qcfg: QueuingConfig,
        dcfg: DispatchingConfig,
    ) -> Result<Arc<QueueSet>, String> {
        let dealer = check_config(&qcfg)?;
        let qs = Arc::new_cyclic(|weak| QueueSet {
            inner: Mutex::new(Inner::new(clock)),
            weak_self: weak.clone(),
        });
        qs.lock().set_configuration(qcfg, dealer, dcfg);
        Ok(qs)
    }

    /// The validation half of `BeginConstruction` / `BeginConfigChange`
    /// (queueset.go:158-196, :197 `checkConfig`): lets a caller learn that a
    /// config is broken before committing to it.
    pub fn validate_config(qcfg: &QueuingConfig) -> Result<(), String> {
        check_config(qcfg).map(|_| ())
    }

    /// `BeginConfigChange` + `Complete` on an existing set.
    pub fn set_configuration(
        &self,
        qcfg: QueuingConfig,
        dcfg: DispatchingConfig,
    ) -> Result<(), String> {
        let dealer = check_config(&qcfg)?;
        self.lock().set_configuration(qcfg, dealer, dcfg);
        Ok(())
    }

    pub fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn is_idle(&self) -> bool {
        self.lock().is_idle()
    }

    /// The seat-demand integrator (upstream: the `seatDemandIntegrator` the
    /// priority level state owns and hands to the queueset).
    pub fn seat_demand(&self) -> Arc<Integrator> {
        self.lock().seat_demand.clone()
    }

    /// The dispatching limit currently in force.
    pub fn concurrency_limit(&self) -> usize {
        self.lock().dcfg.concurrency_limit
    }

    pub fn stats(&self) -> QueueSetStats {
        let g = self.lock();
        QueueSetStats {
            waiting: g.tot_requests_waiting,
            executing: g.tot_requests_executing,
            seats_in_use: g.tot_seats_in_use,
            seats_waiting: g.tot_seats_waiting,
            dispatched: g.tot_requests_dispatched,
            rejected: g.tot_requests_rejected,
            timedout: g.tot_requests_timedout,
            cancelled: g.tot_requests_cancelled,
        }
    }

    /// `StartRequest` (queueset.go:271). On success the request is either
    /// executing already or queued; await [`RequestHandle::wait`]. Dropping
    /// the handle while it waits cancels the request (upstream: ctx done).
    pub fn start_request(
        &self,
        we: &WorkEstimate,
        hash_value: u64,
        flow_distinguisher: &str,
        fs_name: &str,
    ) -> Result<RequestHandle, Rejected> {
        let qs = self.weak_self.upgrade().expect("QueueSet is Arc-held");
        let mut g = self.lock();
        g.sync_time();

        // Step 0: only the concurrency limit, if zero queues desired.
        if g.qcfg.desired_num_queues < 1 {
            if !g.can_accommodate_seats(we.max_seats()) {
                g.tot_requests_rejected += 1;
                return Err(Rejected {
                    reason: RejectReason::ConcurrencyLimit,
                    idle: g.is_idle(),
                });
            }
            let rid = g.dispatch_sans_queue(we, flow_distinguisher, fs_name);
            let notify = g.requests[&rid].notify.clone();
            return Ok(RequestHandle::new(qs, rid, notify));
        }

        // Step 1: shuffle shard, reject or enqueue.
        let Some(rid) =
            g.shuffle_shard_and_reject_or_enqueue(we, hash_value, flow_distinguisher, fs_name)
        else {
            g.tot_requests_rejected += 1;
            return Err(Rejected {
                reason: RejectReason::QueueFull,
                idle: g.is_idle(),
            });
        };
        let notify = g.requests[&rid].notify.clone();

        // Step 2: dequeue as much as possible.
        g.dispatch_as_much_as_possible();
        Ok(RequestHandle::new(qs, rid, notify))
    }

    /// `finishRequestAndDispatchAsMuchAsPossible` (queueset.go:787). Returns
    /// whether the set is now idle.
    fn finish_request_and_dispatch(&self, rid: u64) -> bool {
        let (linger, idle) = {
            let mut g = self.lock();
            g.sync_time();
            let linger = g.finish_request(rid);
            g.dispatch_as_much_as_possible();
            (linger, g.is_idle())
        };
        if let Some(d) = linger {
            // The seats are released after AdditionalLatency elapses; this
            // has no impact on the caller (queueset.go:866-889).
            let weak = self.weak_self.clone();
            let clock = self.lock().clock.clone();
            clock.after(
                d,
                Box::new(move || {
                    if let Some(qs) = weak.upgrade() {
                        let mut g = qs.lock();
                        g.sync_time();
                        g.release_seats(rid);
                        g.dispatch_as_much_as_possible();
                    }
                }),
            );
        }
        idle
    }

    fn is_dispatched(&self, rid: u64) -> bool {
        self.lock().requests.get(&rid).is_some_and(|r| r.dispatched)
    }

    /// Drop path of a still-waiting [`RequestHandle`].
    fn cancel(&self, rid: u64) {
        let dispatched = {
            let mut g = self.lock();
            g.sync_time();
            let dispatched = g.requests.get(&rid).is_some_and(|r| r.dispatched);
            if !dispatched {
                g.cancel_waiting(rid);
            }
            dispatched
        };
        if dispatched {
            // Dispatched but the caller went away before observing it: the
            // seats must still be returned.
            self.finish_request_and_dispatch(rid);
        }
    }

    /// `OnRequestDispatched` (queueset.go:1019).
    fn note_dispatched(&self) {
        self.lock().tot_requests_dispatched += 1;
    }
}

/// `fq.Request`: an admitted request, queued or executing.
pub struct RequestHandle {
    qs: Arc<QueueSet>,
    id: u64,
    notify: Arc<Notify>,
    consumed: bool,
}

impl RequestHandle {
    fn new(qs: Arc<QueueSet>, id: u64, notify: Arc<Notify>) -> Self {
        Self {
            qs,
            id,
            notify,
            consumed: false,
        }
    }

    /// Whether the queueset has dispatched this request.
    pub fn is_dispatched(&self) -> bool {
        self.qs.is_dispatched(self.id)
    }

    /// Wait to be dispatched. Dropping the returned future cancels the
    /// request, as ctx cancellation does upstream.
    pub async fn wait(self) -> Execution {
        loop {
            if self.is_dispatched() {
                return self.into_execution();
            }
            self.notify.notified().await;
        }
    }

    /// Non-blocking [`wait`](Self::wait): the execution if already dispatched.
    pub fn try_execution(self) -> Result<Execution, RequestHandle> {
        if self.is_dispatched() {
            Ok(self.into_execution())
        } else {
            Err(self)
        }
    }

    fn into_execution(mut self) -> Execution {
        self.consumed = true;
        Execution {
            qs: self.qs.clone(),
            id: self.id,
            finished: false,
        }
    }

    pub fn flow_distinguisher_and_schema(&self) -> Option<(String, String)> {
        let g = self.qs.lock();
        g.requests
            .get(&self.id)
            .map(|r| (r.flow_distinguisher.clone(), r.fs_name.clone()))
    }

    /// Real-time arrival, for diagnostics.
    pub fn arrival_time(&self) -> Option<Duration> {
        self.qs
            .lock()
            .requests
            .get(&self.id)
            .map(|r| r.arrival_time)
    }
}

impl Drop for RequestHandle {
    fn drop(&mut self) {
        if !self.consumed {
            self.qs.cancel(self.id);
        }
    }
}

/// A dispatched request holding its seats. Dropping (or [`finish`](Self::finish))
/// releases them and dispatches whatever can now run (`Request.Finish`,
/// queueset.go:343-357).
pub struct Execution {
    qs: Arc<QueueSet>,
    id: u64,
    finished: bool,
}

impl Execution {
    /// Release the seats; returns whether the queueset is now idle.
    pub fn finish(mut self) -> bool {
        self.finished = true;
        self.qs.finish_request_and_dispatch(self.id)
    }

    /// `OnRequestDispatched` (queueset.go:1019).
    pub fn note_dispatched(&self) {
        self.qs.note_dispatched();
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        if !self.finished {
            self.qs.finish_request_and_dispatch(self.id);
        }
    }
}
