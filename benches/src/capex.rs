use cdDB::{CdDBDispatcher, WriteCommand, Attributes, Query};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::thread;

fn capex_benchmark(c: &mut Criterion) {
    // CAPEX here likely stands for "Capital Expenditure" efficiency: 
    // performance gained per resource unit.
    
    let mut db = CdDBDispatcher::new(None);
    let tx = db.register_partition("bench.capex".to_string());
    
    let count = 50_000;
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int));
    }
    tx.send(WriteCommand::BatchInsert(batch)).unwrap();
    
    let route = db.get_route("bench.capex").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < count {
        thread::sleep(std::time::Duration::from_millis(10));
    }

    let query_engine = Query::new(route);

    let mut group = c.benchmark_group("Efficiency Index (Throughput/Resource)");
    group.throughput(Throughput::Bytes(4)); // u32 size
    
    group.bench_function("u32 Scan Efficiency", |b| {
        b.iter(|| {
            let _ = query_engine.sum_int_range("val", 0, count);
        });
    });

    group.finish();
}

criterion_group!(benches, capex_benchmark);
criterion_main!(benches);