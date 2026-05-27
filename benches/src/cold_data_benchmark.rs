use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
use std::time::{Instant, Duration};
use std::thread;

#[test]
fn test_cold_data_scan_performance() {
    let base_path = std::env::current_dir().unwrap().join("test_cold_data");
    if base_path.exists() {
        let _ = std::fs::remove_dir_all(&base_path);
    }
    
    let count = 10_000;
    let scan_size = 1_000;
    let start_idx = 5_000;
    
    println!("\n=== cdDB Cold Data Performance Benchmark ({} Entities) ===", count);
    
    // 1. Initial Ingestion
    {
        let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(base_path.to_string_lossy().to_string()));
        let tx = db.register_partition("cold.bench".to_string());
        
        let mut batch = Vec::with_capacity(count);
        for i in 0..count {
            let mut attrs_int = Attributes::new();
            attrs_int.insert("val".to_string(), i as u32);
            batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
        }
        
        tx.send(WriteCommand::BatchInsert(batch)).unwrap();
        
        // Give time for persistence
        thread::sleep(Duration::from_millis(1000));
        println!("  - Data Ingested and Persisted to Disk.");
    }
    
    // 2. Cold Start & Scan
    // We recreate the DB and register the same partition
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(base_path.to_string_lossy().to_string()));
    let _tx = db.register_partition("cold.bench".to_string());
    
    let route = db.get_route("cold.bench").unwrap();
    let query = Query::new(&route);
    
    // Seed bloom filter so Query knows entities exist
    for i in start_idx..start_idx + scan_size {
        query.seed_bloom_filter(i);
    }
    
    let worker = route.register_worker();
    // Initially len is 0 because we didn't replay WAL and data is only on disk
    assert_eq!(route.len(&worker), 0);
    println!("  - Memory Cleared. System in Cold State.");
    
    // 3. Cold Range Scan (Triggers InternalLoad from Disk)
    let entity_ids: Vec<usize> = (start_idx..start_idx + scan_size).collect();
    let start_cold = Instant::now();
    let sum_cold: u64 = entity_ids.iter()
        .map(|&id| query.get_int(id, "val").unwrap_or(0) as u64)
        .sum();
    let dur_cold = start_cold.elapsed();
    
    println!("  - [First Pass] Cold Range Scan ({} items): {:?}", scan_size, dur_cold);
    println!("    - Sum: {}", sum_cold);
    
    // 4. Hot Range Scan (Should be promoted to Memory)
    let start_hot = Instant::now();
    let sum_hot: u64 = entity_ids.iter()
        .map(|&id| query.get_int(id, "val").unwrap_or(0) as u64)
        .sum();
    let dur_hot = start_hot.elapsed();
    
    println!("  - [Second Pass] Hot Range Scan ({} items): {:?}", scan_size, dur_hot);
    println!("    - Sum: {}", sum_hot);
    
    assert_eq!(sum_cold, sum_hot, "Sums must match!");
    assert!(sum_cold > 0, "Sum should be non-zero!");
    
    println!("\nConclusion:");
    if dur_hot < dur_cold {
        println!("  Memory Hit is {:.2}x faster than Cold Disk Load", dur_cold.as_secs_f64() / dur_hot.as_secs_f64());
    } else {
        println!("  Warning: Hot Scan was not faster. (Likely too small dataset or high overhead)");
    }
    
    let _ = std::fs::remove_dir_all(&base_path);
}
