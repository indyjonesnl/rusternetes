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
struct CompletedWorkEstimate {
    initial_seats: u64,
    final_seats: u64,
    additional_latency_ns: i64,
    total_work: SeatSeconds,
    final_work: SeatSeconds,
}

impl CompletedWorkEstimate {
    fn max_seats(&self) -> usize {
        self.initial_seats.max(self.final_seats) as usize
    }
}

/// `queueSum` (types.go:128): running totals over a queue's waiting requests.
#[derive(Clone, Copy, Debug, Default)]
struct QueueSum {
    initial_seats_sum: usize,
    max_seats_sum: usize,
    total_work_sum: SeatSeconds,
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
const R_DECREMENT: SeatSeconds = SeatSeconds(u64::MAX / 2);
/// `highR` (queueset.go:376).
const HIGH_R: SeatSeconds = SeatSeconds(R_DECREMENT.0 + R_DECREMENT.0 / 2);
/// `estimatedServiceDuration: 3 * time.Millisecond` (queueset.go:180).
const ESTIMATED_SERVICE_DURATION_NS: i64 = 3_000_000;

#[derive(Debug)]
struct Request {
    fs_name: String,
    flow_distinguisher: String,
    /// `None` if the request did not go through a queue.
    queue_id: Option<u64>,
    work: CompletedWorkEstimate,
    /// `decision == decisionExecute`.
    dispatched: bool,
    notify: Arc<Notify>,
    arrival_time: Duration,
    /// `R(arrivalTime)`.
    arrival_r: SeatSeconds,
    start_time: Duration,
}

#[derive(Debug)]
struct Queue {
    id: u64,
    /// Requests not yet executing, oldest first (`requestsWaiting`).
    waiting: VecDeque<u64>,
    sum: QueueSum,
    next_dispatch_r: SeatSeconds,
    executing: HashSet<u64>,
    seats_in_use: usize,
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

struct Inner {
    clock: Arc<dyn Clock>,
    estimated_service_duration_ns: i64,
    qcfg: QueuingConfig,
    dcfg: DispatchingConfig,
    dealer: Option<Dealer>,
    queues: Vec<Queue>,
    requests: HashMap<u64, Request>,
    /// Executing requests that went through no queue.
    queueless_executing: HashSet<u64>,
    current_r: SeatSeconds,
    last_real_time: Duration,
    robin_index: isize,
    tot_requests_waiting: usize,
    tot_requests_executing: usize,
    tot_seats_in_use: usize,
    tot_seats_waiting: usize,
    /// `qs.seatDemandIntegrator`: integrates `totSeatsInUse +
    /// totSeatsWaiting` (the borrowing adjustment reads and resets it).
    seat_demand: Arc<Integrator>,
    enqueues: usize,
    tot_requests_dispatched: u64,
    tot_requests_rejected: u64,
    tot_requests_timedout: u64,
    tot_requests_cancelled: u64,
    next_request_id: u64,
    next_queue_id: u64,
}

/// `checkConfig` (queueset.go:197).
fn check_config(qcfg: &QueuingConfig) -> Result<Option<Dealer>, String> {
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
    fn new(clock: Arc<dyn Clock>) -> Inner {
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

    fn create_queue(&mut self) -> Queue {
        let id = self.next_queue_id;
        self.next_queue_id += 1;
        Queue::new(id)
    }

    fn queue_pos(&self, id: u64) -> Option<usize> {
        self.queues.iter().position(|q| q.id == id)
    }

    /// `setConfiguration` (queueset.go:233).
    fn set_configuration(
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
    fn sync_time(&mut self) {
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
    }

    /// `advanceEpoch` (queueset.go:416).
    fn advance_epoch(&mut self, incr_r: SeatSeconds) {
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
            }
            for rid in &queue.waiting {
                if let Some(req) = self.requests.get_mut(rid) {
                    let old = req.arrival_r;
                    req.arrival_r -= R_DECREMENT;
                    if req.arrival_r > old {
                        error!(qs = %self.qcfg.name, queue = q_idx, "request::arrivalR underflow");
                    }
                }
            }
        }
    }

    /// `qs.seatDemandIntegrator.Set(totSeatsInUse + totSeatsWaiting)`
    /// (queueset.go:438, :650, :682, :711, :728, :880).
    fn note_seat_demand(&self) {
        self.seat_demand
            .set((self.tot_seats_in_use + self.tot_seats_waiting) as f64);
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
    fn complete_work_estimate(&self, we: &WorkEstimate) -> CompletedWorkEstimate {
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

    fn new_request(
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
            Some(rid)
        } else {
            None
        };
        self.bound_next_dispatch(qid);
        rid
    }

    /// `enqueueToBoundLocked` (queueset.go:580).
    fn enqueue_to_bound(&mut self, qid: u64, rid: u64) {
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
    fn remove_waiting(&mut self, qid: u64, rid: u64) -> bool {
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
    fn find_dispatch_queue_to_bound(&mut self) -> Option<(u64, u64)> {
        let mut min_virtual_finish = MAX_SEAT_SECONDS;
        let mut min_queue: Option<usize> = None;
        let nq = self.queues.len();
        for _ in 0..nq {
            self.robin_index = (self.robin_index + 1) % nq as isize;
            let queue = &self.queues[self.robin_index as usize];
            if let Some(oldest) = queue.waiting.front() {
                let oldest_work = self.requests[oldest].work;
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

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
