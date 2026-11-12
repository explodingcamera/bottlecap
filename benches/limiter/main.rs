use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};

mod scale;
mod support;

criterion_group! {
    name = benches;
    config = Criterion::default()
        .without_plots()
        .sample_size(10)
        .nresamples(1_000)
        .warm_up_time(Duration::from_millis(50))
        .measurement_time(Duration::from_millis(250));
    targets = scale::benchmarks
}
criterion_main!(benches);
