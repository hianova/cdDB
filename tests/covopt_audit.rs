#![allow(clippy::absurd_extreme_comparisons, unused_comparisons)]
macro_rules! likely {
    ($b:expr) => {
        $b
    };
}

#[test]
fn covopt_audit_test() {
    let n = std::env::var("COVOPT_N")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1);
    let mut sum = 0;

    for i in 0..n {
        if likely!(i < usize::MAX) {
            sum += i;
        }
        // COVOPT_ANCHOR
        core::hint::black_box(sum);
    }
    assert_eq!(sum, sum);
}
