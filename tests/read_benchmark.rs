use cdDB::{CdDBDispatcher, WriteCommand, Attributes, Query};
use std::time::{Instant, Duration};
use std::collections::HashMap;
use ahash::AHashMap;

#[derive(Clone)]
struct TraditionalStruct {
    id: usize,
    val: u32,
}

#[tokio::test]
async fn test_read_performance_benchmark() {
    println!("\n=== cdDB Read Performance Benchmark (100,000 Entities) ===");
    
    let count = 100_000;
    let scan_size = 10_000;
    
    // 1. Prepare Data in cdDB
    let mut db = CdDBDispatcher::new(None);
    let tx = db.register_partition("bench.read".to_string());
    
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int));
    }
    
    let start_insert = Instant::now();
    tx.send(WriteCommand::BatchInsert(batch)).unwrap();
    
    let route = db.get_route("bench.read").unwrap();
    while route.get_snapshot().len() < count {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("  - cdDB Data Prep (Batch Insert): {:?}", start_insert.elapsed());
    
    // 2. Prepare Data in Traditional structures
    let mut hash_map = AHashMap::with_capacity(count);
    let mut vec_struct = Vec::with_capacity(count);
    for i in 0..count {
        let s = TraditionalStruct { id: i, val: i as u32 };
        hash_map.insert(i, s.clone());
        vec_struct.push(s);
    }
    println!("  - Traditional Data Prep: Done");

    // 3. Benchmarks
    
    // Test A: cdDB Columnar Scan (Continuous Memory)
    // We get the column and iterate directly to simulate cache-friendly range scan
    let col = route.get_column_int("val").unwrap();
    let start_a = Instant::now();
    // Simulate starting from a pointer jump, then scanning 10,000 items
    let start_idx = count / 2; 
    let sum_a: u64 = col.with_data(|data| {
        data.iter().skip(start_idx).take(scan_size)
            .flatten()
            .map(|&v| v as u64)
            .sum()
    });
    let duration_a = start_a.elapsed();
    println!("  - [Test A] cdDB Columnar Scan ({} items): {:?}", scan_size, duration_a);
    
    // Test B: HashMap Lookup (Random Memory Access)
    let start_b = Instant::now();
    let mut sum_b = 0u64;
    for i in 0..scan_size {
        if let Some(s) = hash_map.get(&(start_idx + i)) {
            sum_b += s.val as u64;
        }
    }
    let duration_b = start_b.elapsed();
    println!("  - [Test B] HashMap Lookup ({} items): {:?}", scan_size, duration_b);
    
    // Test C: Vec<Struct> Scan (Continuous Memory, but Struct layout)
    let start_c = Instant::now();
    let mut sum_c = 0u64;
    for i in 0..scan_size {
        sum_c += vec_struct[start_idx + i].val as u64;
    }
    let duration_c = start_c.elapsed();
    println!("  - [Test C] Vec<Struct> Scan ({} items): {:?}", scan_size, duration_c);
    
    assert_eq!(sum_a, sum_b);
    assert_eq!(sum_b, sum_c);
    
    println!("\nConclusion:");
    println!("  cdDB is {:.2}x faster than HashMap", duration_b.as_secs_f64() / duration_a.as_secs_f64());
    println!("  cdDB is {:.2}x faster/slower than Vec<Struct>", duration_c.as_secs_f64() / duration_a.as_secs_f64());
}
