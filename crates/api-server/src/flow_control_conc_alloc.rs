//! Port of `conc_alloc.go` (release-1.35): `computeConcurrencyAllocation`,
//! the fair division of the server's concurrency among priority levels used
//! by the borrowing adjustment (`apf_controller.go` `updateBorrowingLocked`).

/// `allocProblemItem` (conc_alloc.go:27).
#[derive(Clone, Copy, Debug, Default)]
pub struct AllocProblemItem {
    pub target: f64,
    pub lower_bound: f64,
    pub upper_bound: f64,
}

/// `MinTarget` (conc_alloc.go:108).
pub const MIN_TARGET: f64 = 0.001;

const EPSILON: f64 = 0.0000001;

/// `relativeAllocItem` (conc_alloc.go:34): like [`AllocProblemItem`] but with
/// the target avoiding zero and the bounds divided by the target.
#[derive(Clone, Copy)]
struct RelativeAllocItem {
    target: f64,
    relative_lower_bound: f64,
    relative_upper_bound: f64,
}

/// `minMax` (conc_alloc.go:93): the minimum and maximum seen while scanning.
struct MinMax {
    min: f64,
    max: f64,
}

impl MinMax {
    fn note(&mut self, x: f64) {
        self.min = self.min.min(x);
        self.max = self.max.max(x);
    }
}

/// `relativeAllocProblem.decode` (conc_alloc.go:70): with `ascending[j] =
/// 2*n + 0` the lower bound of `items[n]`, `2*n + 1` its upper bound; returns
/// the bound, the item index and whether it is the lower bound.
fn decode(items: &[RelativeAllocItem], packed: usize) -> (f64, usize, bool) {
    let item_idx = packed / 2;
    let lower = packed == item_idx * 2;
    let bound = if lower {
        items[item_idx].relative_lower_bound
    } else {
        items[item_idx].relative_upper_bound
    };
    (bound, item_idx, lower)
}

/// `computeConcurrencyAllocation` (conc_alloc.go:123): the allocations and
/// the associated `fairProp`, or why the problem is impossible.
///
/// `allocs` sums to `required_sum`; for each class the bounds hold and the
/// allocation is either `fairProp * target`, or pinned at the lower bound
/// (when that exceeds `fairProp * target`) or the upper bound (when that is
/// below it). A target below [`MIN_TARGET`] is treated as [`MIN_TARGET`].
pub fn compute_concurrency_allocation(
    required_sum: i64,
    classes: &[AllocProblemItem],
) -> Result<(Vec<f64>, f64), String> {
    if required_sum < 0 {
        return Err("negative sums are not supported".into());
    }
    let required_sum_f = required_sum as f64;
    let (mut low_sum, mut high_sum, mut target_sum) = (0.0f64, 0.0f64, 0.0f64);
    let mut ub_range = MinMax {
        min: f32::MAX as f64,
        max: 0.0,
    };
    let mut lb_range = MinMax {
        min: f32::MAX as f64,
        max: 0.0,
    };
    let mut relative_items = Vec::with_capacity(classes.len());
    for (idx, item) in classes.iter().enumerate() {
        let mut target = item.target;
        if item.lower_bound < 0.0 {
            return Err(format!(
                "lower bound {idx} is {} but negative lower bounds are not allowed",
                item.lower_bound
            ));
        }
        if target < item.lower_bound {
            return Err(format!(
                "target {idx} is {target}, which is below its lower bound of {}",
                item.lower_bound
            ));
        }
        if item.upper_bound < item.lower_bound {
            return Err(format!(
                "upper bound {idx} is {} but should not be less than the lower bound {}",
                item.upper_bound, item.lower_bound
            ));
        }
        if target < MIN_TARGET {
            // tweak this to a non-zero value to avoid dividing by zero
            target = MIN_TARGET;
        }
        low_sum += item.lower_bound;
        high_sum += item.upper_bound;
        target_sum += target;
        let rel = RelativeAllocItem {
            target,
            relative_lower_bound: item.lower_bound / target,
            relative_upper_bound: item.upper_bound / target,
        };
        ub_range.note(rel.relative_upper_bound);
        lb_range.note(rel.relative_lower_bound);
        relative_items.push(rel);
    }
    if lb_range.max > 1.0 {
        return Err(format!(
            "lbRange.max-1={}, which is impossible because lbRange.max can not be greater than 1",
            lb_range.max - 1.0
        ));
    }
    if low_sum - required_sum_f > EPSILON {
        return Err(format!(
            "lower bounds sum to {low_sum}, which is higher than the required sum of {required_sum}"
        ));
    }
    if required_sum_f - high_sum > EPSILON {
        return Err(format!(
            "upper bounds sum to {high_sum}, which is lower than the required sum of {required_sum}"
        ));
    }
    let mut ans = vec![0.0f64; classes.len()];
    if required_sum == 0 {
        return Ok((ans, 0.0));
    }
    if low_sum - required_sum_f > -EPSILON {
        // no wiggle room, constrained from below
        for (idx, item) in classes.iter().enumerate() {
            ans[idx] = item.lower_bound;
        }
        return Ok((ans, lb_range.min));
    }
    if required_sum_f - high_sum > -EPSILON {
        // no wiggle room, constrained from above
        for (idx, item) in classes.iter().enumerate() {
            ans[idx] = item.upper_bound;
        }
        return Ok((ans, ub_range.max));
    }
    // Now we know the solution is a unique fairProp in
    // [lbRange.min, ubRange.max]. See if it runs into any bounds.
    let mut fair_prop = required_sum_f / target_sum;
    if lb_range.max <= fair_prop && fair_prop <= ub_range.min {
        for (idx, rel) in relative_items.iter().enumerate() {
            ans[idx] = rel.target * fair_prop;
        }
        return Ok((ans, fair_prop));
    }
    // Sadly, some bounds matter. Sort the bounds and consider progressively
    // higher values of fairProp, starting from lbRange.min.
    let mut ascending: Vec<usize> = (0..relative_items.len() * 2).collect();
    ascending.sort_by(|&i, &j| {
        let (bi, _, _) = decode(&relative_items, i);
        let (bj, _, _) = decode(&relative_items, j);
        bi.partial_cmp(&bj).unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut sum_so_far = low_sum;
    fair_prop = lb_range.min;
    let (mut sensitive_target_sum, mut delta_sensitive_target_sum) = (0.0f64, 0.0f64);
    let (mut num_sensitive_classes, mut delta_sensitive_classes) = (0i64, 0i64);
    let mut next_idx = 0usize;
    while sum_so_far < required_sum_f {
        // There might be more than one bound equal to the current fairProp;
        // find all of them, ending with the next bound that is not.
        let mut next_bound;
        loop {
            sensitive_target_sum += delta_sensitive_target_sum;
            num_sensitive_classes += delta_sensitive_classes;
            if next_idx >= ascending.len() {
                return Err(
                    "impossible: ran out of bounds to consider in bound-constrained problem".into(),
                );
            }
            let (bound, item_idx, lower) = decode(&relative_items, ascending[next_idx]);
            next_bound = bound;
            if lower {
                delta_sensitive_classes = 1;
                delta_sensitive_target_sum = relative_items[item_idx].target;
            } else {
                delta_sensitive_classes = -1;
                delta_sensitive_target_sum = -relative_items[item_idx].target;
            }
            next_idx += 1;
            if next_bound > fair_prop {
                break;
            }
        }
        // fairProp can increase to nextBound without passing any
        // intermediate bounds.
        if num_sensitive_classes == 0 {
            // No classes are affected by the next range; skip right past it.
            fair_prop = next_bound;
            continue;
        }
        // See whether fairProp can reach the solution before the next bound.
        let delta_fair_prop = (required_sum_f - sum_so_far) / sensitive_target_sum;
        let next_prop = fair_prop + delta_fair_prop;
        if next_prop <= next_bound {
            fair_prop = next_prop;
            break;
        }
        // No, fairProp has to increase above nextBound.
        sum_so_far += (next_bound - fair_prop) * sensitive_target_sum;
        fair_prop = next_bound;
    }
    for (idx, item) in classes.iter().enumerate() {
        ans[idx] = item
            .lower_bound
            .max(item.upper_bound.min(fair_prop * relative_items[idx].target));
    }
    Ok((ans, fair_prop))
}
