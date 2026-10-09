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

/// `computeConcurrencyAllocation` (conc_alloc.go:123): the allocations and
/// the associated `fairProp`, or why the problem is impossible.
pub fn compute_concurrency_allocation(
    required_sum: i64,
    classes: &[AllocProblemItem],
) -> Result<(Vec<f64>, f64), String> {
    let _ = (required_sum, classes);
    todo!()
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
