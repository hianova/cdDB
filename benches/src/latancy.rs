use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
use criterion::{criterion_group, criterion_main, Criterion, black_box};
use std::thread;
use std::time::Duration;

fn latency_benchmark(c: &mut Criterion) {
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(None);
    let tx = db.register_partition("bench.latency".to_string());
    
    let count = 500;
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
    }
    tx.send(WriteCommand::BatchInsert(batch)).unwrap();
    
    let route = db.get_route("bench.latency").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < count {
        thread::sleep(Duration::from_millis(10));
    }

    let query_engine = Query::new(&route);
    
    let mut group = c.benchmark_group("Access Latency");
    
    group.bench_function("Hot Path Get Int (Wait-Free RCU)", |b| {
        let mut i = 0;
        b.iter(|| {
            let result = query_engine.get_int(black_box(i % count), black_box("val"));
            black_box(result);
            i += 1;
        });
    });

    group.bench_function("Bloom Filter Miss", |b| {
        let mut i = count + 1000;
        b.iter(|| {
            let result = query_engine.get_int(black_box(i), black_box("val"));
            black_box(result);
            i += 1;
        });
    });

    group.finish();
}

criterion_group!(benches, latency_benchmark);
criterion_main!(benches);