use criterion::{criterion_group, criterion_main, Criterion, black_box};
use cdDB::column::ColumnArray;
use cdDB::qsbr::WorkerState;
use std::sync::Arc;

fn memory_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Memory Ops");

    group.bench_function("ColumnArray String Allocation (1000 items)", |b| {
        let worker = Arc::new(WorkerState::new());
        let mut qsbr = cdDB::qsbr::QsbrManager::new(Arc::new(cdDB::platform::atomic::AtomicPtr::new(std::ptr::null_mut())));
        b.iter(|| {
            let col = ColumnArray::<String, 1024>::new();
            let mut next = cdDB::unsafe_core::load_clone(&col.data);
            for i in 0..1000 {
                next.push(format!("item-{}", i));
            }
            let old = cdDB::unsafe_core::swap_ptr(&col.data, next);
            qsbr.defer_free(old);
            black_box(col);
        });
    });

    group.finish();
}

criterion_group!(benches, memory_benchmark);
criterion_main!(benches);