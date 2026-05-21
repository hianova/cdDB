use criterion::{criterion_group, criterion_main, Criterion};

fn memory_benchmark(c: &mut Criterion) {
    // Memory benchmarking is usually done with external tools like heaptrack,
    // but we can measure the "allocation time" or "serialized size efficiency".
    
    let mut group = c.benchmark_group("Memory Ops");

    group.bench_function("ColumnArray String Allocation (1000 items)", |b| {
        b.iter(|| {
            let mut col = Vec::with_capacity(1000);
            for i in 0..1000 {
                col.push(Some(format!("item-{}", i)));
            }
            criterion::black_box(col);
        });
    });

    group.finish();
}

criterion_group!(benches, memory_benchmark);
criterion_main!(benches);