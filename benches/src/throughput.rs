use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::thread;

fn throughput_benchmark(c: &mut Criterion) {
    let mut db = CdDBDispatcher::new(None);
    let tx = db.register_partition("bench.throughput".to_string());
    
    // Preload 100k entities for read benchmark
    let count = 100_000;
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int));
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
            let _ = query_engine.get_int(i % count, "val");
            i += 1;
        });
    });

    // --- 2. Read Throughput (Multi-Threaded 4 Readers) ---
    // Increase work per iteration to amortize thread spawn overhead
    group.bench_function("Multi-Thread (4 Readers) Stress", |b| {
        b.iter_custom(|iters| {
            let num_threads = 4;
            let work_per_thread = 10_000; // Amortize spawn overhead
            let total_ops = iters * work_per_thread as u64;
            
            let start = std::time::Instant::now();
            let mut handles = vec![];
            for _ in 0..num_threads {
                let r = route.clone();
                handles.push(thread::spawn(move || {
                    let q = Query::new(&r);
                    // Each thread does a chunk of work proportional to iters
                    let chunk = (iters as usize * work_per_thread) / num_threads;
                    for j in 0..chunk {
                        let _ = q.get_int(j % count, "val");
                    }
                }));
            }
            
            for h in handles {
                h.join().unwrap();
            }
            let elapsed = start.elapsed();
            // We want Criterion to see 'iters' as the number of units,
            // but we did 'total_ops' work.
            // So we return 'elapsed * (iters / total_ops)'?
            // No, Criterion expects the time for 'iters'.
            // If we did 'total_ops' in 'elapsed', then 'iters' took 'elapsed * (iters / total_ops)'.
            elapsed.mul_f64(iters as f64 / total_ops as f64)
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
                batch.push((id_gen, Attributes::new(), attrs_int));
                id_gen += 1;
            }
            tx.send(WriteCommand::BatchInsert(batch)).unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, throughput_benchmark);
criterion_main!(benches);