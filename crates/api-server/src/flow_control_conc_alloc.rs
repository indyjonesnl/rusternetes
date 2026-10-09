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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    const FP_SLACK: f64 = 1e-10;

    /// `partition64`: calls `consume` n times with ints [0,n) and floats that
    /// sum to x.
    fn partition64(rng: &mut StdRng, n: usize, x: f64, mut consume: impl FnMut(usize, f64)) {
        if n == 0 {
            return;
        }
        let mut divs: Vec<f64> = (0..n - 1).map(|_| rng.random::<f64>()).collect();
        divs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut last = 0.0;
        for (idx, div) in divs.iter().enumerate() {
            consume(idx, (div - last) * x);
            last = *div;
        }
        consume(n - 1, (1.0 - last) * x);
    }

    fn f64_rel_diff(a: f64, b: f64) -> f64 {
        let den = a.abs().max(b.abs());
        if den == 0.0 {
            0.0
        } else {
            (a - b).abs() / den
        }
    }

    fn test_1_conc_alloc(rng: &mut StdRng) {
        let mut prob_len = [0usize, 1, 2, 3, 4, 6, 9][rng.random_range(0..7)];
        let mut classes = vec![AllocProblemItem::default(); prob_len];
        let (mut low_sum, mut high_sum) = (0.0f64, 0.0f64);
        let mut required_sum: i64 = 0;
        let mut required_sum_f = 0.0f64;
        if prob_len > 0 {
            match rng.random_range(0..20) {
                0 => {
                    required_sum = rng.random_range(0..prob_len * 3) as i64;
                    required_sum_f = required_sum as f64;
                    let mut parts = vec![];
                    partition64(rng, prob_len, required_sum_f, |j, x| parts.push((j, x)));
                    for (j, x) in parts {
                        classes[j].lower_bound = x;
                        classes[j].target = x + 2.0 * rng.random::<f64>();
                        classes[j].upper_bound = x + 3.0 * rng.random::<f64>();
                        low_sum += classes[j].lower_bound;
                        high_sum += classes[j].upper_bound;
                    }
                }
                1 => {
                    required_sum = rng.random_range(0..prob_len * 3) as i64 + 1;
                    required_sum_f = required_sum as f64;
                    let mut parts = vec![];
                    partition64(rng, prob_len, required_sum_f, |j, x| parts.push((j, x)));
                    for (j, x) in parts {
                        classes[j].upper_bound = x;
                        classes[j].lower_bound = x * (1.25 * rng.random::<f64>() - 1.0).max(0.0);
                        classes[j].target = classes[j].lower_bound + rng.random::<f64>();
                        low_sum += classes[j].lower_bound;
                        high_sum += classes[j].upper_bound;
                    }
                }
                _ => {
                    for c in classes.iter_mut() {
                        let x = (rng.random::<f64>() * 5.0 - 1.0).max(0.0);
                        c.lower_bound = x;
                        c.target = x + 2.0 * rng.random::<f64>();
                        c.upper_bound = x + 3.0 * rng.random::<f64>();
                        low_sum += c.lower_bound;
                        high_sum += c.upper_bound;
                    }
                    required_sum_f = (low_sum + (high_sum - low_sum) * rng.random::<f64>()).round();
                    required_sum = required_sum_f as i64;
                }
            }
        }
        while rng.random::<f64>() < 0.25 {
            // Add a class with a target of zero.
            classes.push(AllocProblemItem {
                target: 0.0,
                lower_bound: 0.0,
                upper_bound: rng.random::<f64>() + 0.00001,
            });
            high_sum += classes[prob_len].upper_bound;
            if prob_len > 1 {
                let m = rng.random_range(0..prob_len);
                classes.swap(m, prob_len);
            }
            prob_len = classes.len();
        }
        let result = compute_concurrency_allocation(required_sum, &classes);
        let expect_err =
            low_sum - required_sum_f > FP_SLACK || required_sum_f - high_sum > FP_SLACK;
        let (allocs, fair_prop) = match result {
            Err(e) => {
                assert!(
                    expect_err,
                    "requiredSum={required_sum} classes={classes:?} got unexpected error {e}"
                );
                return;
            }
            Ok(x) => x,
        };
        assert!(
            !expect_err,
            "expected error from requiredSum={required_sum} classes={classes:?} but got {allocs:?}, {fair_prop}"
        );
        let actual_sum: f64 = allocs.iter().sum();
        assert!(
            f64_rel_diff(required_sum_f, actual_sum) <= FP_SLACK,
            "requiredSum={required_sum} classes={classes:?} got {allocs:?} summing to {actual_sum}"
        );
        for (idx, item) in classes.iter().enumerate() {
            let target = item.target.max(MIN_TARGET);
            let alloc = fair_prop * target;
            if alloc <= item.lower_bound {
                assert_eq!(
                    allocs[idx], item.lower_bound,
                    "item {idx} should be at its lower bound: {classes:?} {allocs:?} {fair_prop}"
                );
            } else if alloc >= item.upper_bound {
                assert_eq!(
                    allocs[idx], item.upper_bound,
                    "item {idx} should be at its upper bound: {classes:?} {allocs:?} {fair_prop}"
                );
            } else {
                assert!(
                    f64_rel_diff(alloc, allocs[idx]) <= FP_SLACK,
                    "item {idx} got {} want proportional {alloc}: {classes:?} {allocs:?} {fair_prop}",
                    allocs[idx]
                );
            }
        }
    }

    /// `TestConcAlloc` (conc_alloc_test.go:30): 10000 random cases.
    #[test]
    fn conc_alloc_random_cases() {
        let mut rng = StdRng::seed_from_u64(1234567890);
        for _ in 0..10000 {
            test_1_conc_alloc(&mut rng);
        }
    }

    #[test]
    fn conc_alloc_rejects_impossible_problems() {
        let items = [AllocProblemItem {
            target: 1.0,
            lower_bound: 1.0,
            upper_bound: 2.0,
        }];
        assert!(compute_concurrency_allocation(-1, &items).is_err());
        assert!(compute_concurrency_allocation(0, &items).is_err()); // lower bounds sum above
        assert!(compute_concurrency_allocation(3, &items).is_err()); // upper bounds sum below
    }
}
