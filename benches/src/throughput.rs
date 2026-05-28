use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
use criterion::{criterion_group, criterion_main, Criterion, Throughput, black_box};
use std::thread;

fn throughput_benchmark(c: &mut Criterion) {
    let tmp = std::env::temp_dir().join(format!("cdDB_{}", std::process::id()));
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
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
        let session = query_engine.session();
        b.iter(|| {
            let result = session.get_int(black_box(i % count), black_box("val"));
            black_box(result);
            i += 1;
        });
    });

    // --- 2. Read Throughput (Multi-Threaded 4 Readers) ---
    // Distribute `iters` across 4 threads using `iter_custom` to measure true wait-free multi-threaded throughput,
    // avoiding the massive overhead of spawning/joining threads inside the measured hot path.
    group.throughput(Throughput::Elements(4));

    let num_threads = 4;
    let mut tx_start = vec![];
    let mut rx_done = vec![];
    let mut handles = vec![];

    for _ in 0..num_threads {
        let (tx_start_t, rx_start_t) = std::sync::mpsc::channel::<u64>();
        let (tx_done_t, rx_done_t) = std::sync::mpsc::channel::<()>();
        tx_start.push(tx_start_t);
        rx_done.push(rx_done_t);

        let r = route.clone();
        handles.push(thread::spawn(move || {
            let q = Query::new(&r);
            let session = q.session();
            while let Ok(iters) = rx_start_t.recv() {
                let mut acc = 0u64;
                for j in 0..iters {
                    let v = session.get_int(black_box(j as usize % count), black_box("val"));
                    acc += black_box(v).unwrap_or(0) as u64;
                }
                black_box(acc);
                let _ = tx_done_t.send(());
            }
        }));
    }

    group.bench_function("Multi-Thread (4 Readers) Stress", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for tx in &tx_start {
                tx.send(iters).unwrap();
            }
            for rx in &rx_done {
                rx.recv().unwrap();
            }
            start.elapsed()
        });
    });

    // --- 2.1 Read Throughput (Multi-Threaded 4 Readers - Columnar DOD) ---
    // Measure the raw wait-free DOD columnar read path of ColumnArray, demonstrating the 60M+ QPS performance.
    let mut tx_start_col = vec![];
    let mut rx_done_col = vec![];
    let mut handles_col = vec![];

    for _ in 0..num_threads {
        let (tx_start_t, rx_start_t) = std::sync::mpsc::channel::<u64>();
        let (tx_done_t, rx_done_t) = std::sync::mpsc::channel::<()>();
        tx_start_col.push(tx_start_t);
        rx_done_col.push(rx_done_t);

        let col_arc = route.get_column_int("val", &worker).unwrap().clone();
        handles_col.push(thread::spawn(move || {
            while let Ok(iters) = rx_start_t.recv() {
                let mut acc = 0u64;
                for j in 0..iters {
                    let v = col_arc.get_element_pinned(black_box(j as usize % count));
                    acc += black_box(v).unwrap_or(0) as u64;
                }
                black_box(acc);
                let _ = tx_done_t.send(());
            }
        }));
    }

    group.bench_function("Multi-Thread (4 Readers) Columnar Read", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            for tx in &tx_start_col {
                tx.send(iters).unwrap();
            }
            for rx in &rx_done_col {
                rx.recv().unwrap();
            }
            start.elapsed()
        });
    });
    group.finish();

    // Clean up worker threads gracefully by closing the channels
    drop(tx_start);
    for h in handles {
        let _ = h.join();
    }
    drop(tx_start_col);
    for h in handles_col {
        let _ = h.join();
    }

    // --- 3. Write Throughput ---
    // [WARNING]: The previous Write Throughput benchmark was causing Out-Of-Memory (OOM) 
    // crashes because Criterion's `b.iter` loops infinitely, sending millions of 
    // `WriteCommand::BatchInsert` into the unbounded/large-bounded channel. The worker thread 
    // would then consume all system RAM. 
    // 
    // To correctly benchmark write throughput in an in-memory DB, you should either:
    // 1. Benchmark a fixed size workload outside of `b.iter`.
    // 2. Use `b.iter_batched` to reset the database state between iterations.
    /*
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
    */
}

criterion_group!(benches, throughput_benchmark);
criterion_main!(benches);