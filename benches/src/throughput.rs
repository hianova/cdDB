use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
use criterion::{criterion_group, criterion_main, Criterion, Throughput, black_box};
use std::thread;

fn throughput_benchmark(c: &mut Criterion) {
    let mut db = CdDBDispatcher::new_std(None);
    let tx = db.register_partition("bench.throughput".to_string());
    
    // Preload 100k entities for read benchmark
    let count = 100_000;
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
    }
    tx.send(WriteCommand::BatchInsert(batch)).unwrap();
    
    let route = db.get_route("bench.throughput").unwrap().clone();
    let worker = route.register_worker();
    while route.len(&worker) < count {
        thread::sleep(std::time::Duration::from_millis(10));
    }

    // --- 1. Read Throughput (Single Thread) ---
    let mut group = c.benchmark_group("Read Throughput");
    group.throughput(Throughput::Elements(1));
    
    let query_engine = Query::new(&route);
    group.bench_function("Single Thread Get Int", |b| {
        let mut i = 0;
        b.iter(|| {
            let result = query_engine.get_int(black_box(i % count), black_box("val"));
            black_box(result);
            i += 1;
        });
    });

    // --- 2. Read Throughput (Multi-Threaded 4 Readers) ---
    // Each iter spawns 4 threads each doing 1000 reads.
    // Criterion sees 1 iter = 4000 reads. We set Throughput::Elements(4000).
    group.throughput(Throughput::Elements(4_000));
    group.bench_function("Multi-Thread (4 Readers) Stress", |b| {
        b.iter(|| {
            let num_threads = 4;
            let reads_per_thread = 1_000;
            let mut handles = vec![];
            for _ in 0..num_threads {
                let r = route.clone();
                handles.push(thread::spawn(move || {
                    let q = Query::new(&r);
                    let mut acc = 0u64;
                    for j in 0..reads_per_thread {
                        let v = q.get_int(black_box(j % count), black_box("val"));
                        acc += black_box(v).unwrap_or(0) as u64;
                    }
                    acc
                }));
            }
            let mut total = 0u64;
            for h in handles {
                total += h.join().unwrap();
            }
            black_box(total);
        });
    });
    group.finish();

    // --- 3. Write Throughput ---
    let mut group = c.benchmark_group("Write Throughput");
    group.throughput(Throughput::Elements(1000)); // Per batch
    
    group.bench_function("Batch Insert (1000 items)", |b| {
        let mut id_gen = count + 1;
        b.iter(|| {
            let mut batch = Vec::with_capacity(1000);
            for _ in 0..1000 {
                let mut attrs_int = Attributes::new();
                attrs_int.insert("val".to_string(), id_gen as u32);
                batch.push((id_gen, Attributes::new(), attrs_int, Attributes::new()));
                id_gen += 1;
            }
            tx.send(WriteCommand::BatchInsert(batch)).unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, throughput_benchmark);
criterion_main!(benches);