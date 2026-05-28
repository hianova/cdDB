use cdDB::{CdDBDispatcher, WriteCommand, Attributes};
use std::time::{Instant, Duration};
use tokio::task;

#[tokio::main]
async fn main() {
    println!("=== cdDB Anti-Pattern & Performance Test ===");

    test_pipelining_impact().await;
    test_columnar_efficiency().await;
    test_hot_key_pressure().await;
}

/// Anti-pattern 4: Serial single operations (no pipelining)
async fn test_pipelining_impact() {
    println!("\n[Test 1] Pipelining / Batching Impact");
    let tmp = std::env::temp_dir().join(format!("cdDB_serial_{}", std::process::id()));
    let mut db = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    
    // Serial Test
    let writer_tx_serial = db.register_partition("benchmark.serial".to_string());
    let count = 5000;
    let start_serial = Instant::now();
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        writer_tx_serial.send(WriteCommand::Insert {
            entity_id: i,
            attributes: Attributes::new(),
            attributes_int: attrs_int,
            attributes_blob: Attributes::new(),
        }).unwrap();
    }
    let route_serial = db.get_route("benchmark.serial").unwrap();
    while route_serial.get_snapshot().len() < count {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!("  - Serial: {} entities in {:?}", count, start_serial.elapsed());

    // Batch Test
    let writer_tx_batch = db.register_partition("benchmark.batch".to_string());
    let start_batch = Instant::now();
    let mut batch = Vec::new();
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
    }
    writer_tx_batch.send(WriteCommand::BatchInsert(batch)).unwrap();
    
    let route_batch = db.get_route("benchmark.batch").unwrap();
    while route_batch.get_snapshot().len() < count {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!("  - Batching: {} entities in {:?}", count, start_batch.elapsed());
}

/// Anti-pattern 10: Storing JSON blobs in strings
async fn test_columnar_efficiency() {
    println!("\n[Test 2] Columnar Efficiency (Anti-pattern 10)");
    let tmp = std::env::temp_dir().join(format!("cdDB_columnar_{}", std::process::id()));
    let mut db = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    let writer_tx = db.register_partition("benchmark.columnar".to_string());
    
    let count = 50000;
    let mut batch = Vec::new();
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("target".to_string(), i as u32);
        attrs_int.insert("noise1".to_string(), i as u32);
        attrs_int.insert("noise2".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
    }
    writer_tx.send(WriteCommand::BatchInsert(batch)).unwrap();

    let route = db.get_route("benchmark.columnar").unwrap();
    while route.get_snapshot().len() < count {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let start = Instant::now();
    let snapshot = route.get_snapshot();
    let col = route.get_column_int("target").unwrap();
    
    let worker = route.register_worker();
    for ptr in snapshot.values() {
        if let Some(idx) = ptr.attribute_indices.get("target") {
            if let Some(val) = col.get_element(*idx, &worker) {
                sum += val as u64;
            }
        }
    }
    let duration = start.elapsed();
    println!("  - Scanned 'target' column for {} entities: {:?}", count, duration);
    println!("  - Sum check: {}", sum);
}

/// Anti-pattern 7: Hot keys
async fn test_hot_key_pressure() {
    println!("\n[Test 3] Hot Partition Reader Pressure (Anti-pattern 7)");
    let tmp = std::env::temp_dir().join(format!("cdDB_hot_{}", std::process::id()));
    let mut db = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    let writer_tx = db.register_partition("hot.partition".to_string());
    
    let mut attrs_int = Attributes::new();
    attrs_int.insert("val".to_string(), 42);
    writer_tx.send(WriteCommand::Insert {
        entity_id: 999,
        attributes: Attributes::new(),
        attributes_int: attrs_int,
        attributes_blob: Attributes::new(),
    }).unwrap();

    let route = db.get_route("hot.partition").unwrap().clone();
    while route.get_snapshot().get(&999).is_none() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut handles = vec![];
    let start = Instant::now();
    
    for _ in 0..10 {
        let r = route.clone();
        let h = task::spawn(async move {
            let mut local_sum = 0;
            for _ in 0..100000 {
                let snapshot = r.get_snapshot();
                if let Some(ptr) = snapshot.get(&999) {
                    local_sum += ptr.entity_id;
                }
            }
            local_sum
        });
        handles.push(h);
    }

    for h in handles {
        let _ = h.await;
    }
    
    let duration = start.elapsed();
    println!("  - 10 concurrent readers performed 1,000,000 snapshot lookups total: {:?}", duration);
}
