use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
use std::time::{Instant, Duration};
use std::thread;
use ahash::AHashMap;

#[derive(Clone)]
struct TraditionalStruct {
    id: usize,
    val: u32,
}

#[test]
fn test_read_performance_benchmark() {
    println!("\n=== cdDB Fair Performance Audit (100,000 Entities) ===");
    
    let count = 100_000;
    let scan_size = 10_000;
    
    // 1. Prepare Data
    let mut db = CdDBDispatcher::new_std(None);
    let tx = db.register_partition("bench.read".to_string());
    
    let mut batch = Vec::with_capacity(count);
    let mut hash_map = AHashMap::with_capacity(count);
    let mut vec_struct = Vec::with_capacity(count);

    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int, Attributes::new()));

        let s = TraditionalStruct { id: i, val: i as u32 };
        hash_map.insert(i, s.clone());
        vec_struct.push(s);
    }
    
    tx.send(WriteCommand::BatchInsert(batch)).unwrap();
    let route = db.get_route("bench.read").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < count {
        thread::sleep(Duration::from_millis(50));
    }
    println!("  - Data Preparation Complete.");

    // --- SECTION 1: SCAN PERFORMANCE (Contiguous Access) ---
    println!("\n[Section 1] Scan Performance ({} items):", scan_size);
    let start_idx = count / 2;

    // Test A: cdDB Columnar Scan
    let col = route.get_column_int("val", &worker).unwrap();
    let start_a = Instant::now();
    let sum_a: u64 = col.with_data(&worker, |data| {
        data.iter().skip(start_idx).take(scan_size).flatten().map(|&v| v as u64).sum()
    });
    let dur_a = start_a.elapsed();
    println!("  - cdDB Columnar Scan:  {:?}", dur_a);

    // Test B: Vec<Struct> Scan
    let start_b = Instant::now();
    let mut sum_b = 0u64;
    for i in 0..scan_size {
        sum_b += vec_struct[start_idx + i].val as u64;
    }
    let dur_b = start_b.elapsed();
    println!("  - Vec<Struct> Scan:    {:?}", dur_b);
    assert_eq!(sum_a, sum_b);

    // --- SECTION 2: RANDOM LOOKUP PERFORMANCE ---
    println!("\n[Section 2] Random Lookup Performance ({} items):", scan_size);

    // Test C: HashMap Lookup
    let start_c = Instant::now();
    let mut sum_c = 0u64;
    for i in 0..scan_size {
        if let Some(s) = hash_map.get(&(start_idx + i)) {
            sum_c += s.val as u64;
        }
    }
    let dur_c = start_c.elapsed();
    println!("  - HashMap Lookup:      {:?}", dur_c);

    // Test D: cdDB Query API (Hot Path)
    let query = Query::new(route);
    let start_d = Instant::now();
    let mut sum_d = 0u64;
    for i in 0..scan_size {
        if let Some(v) = query.get_int(start_idx + i, "val") {
            sum_d += v as u64;
        }
    }
    let dur_d = start_d.elapsed();
    println!("  - cdDB Query API:      {:?}", dur_d);
    assert_eq!(sum_c, sum_d);

    // --- SECTION 3: CACHE PENETRATION (Bloom Filter Impact) ---
    println!("\n[Section 3] Cache Penetration ({} misses):", scan_size);
    let non_existent_start = count + 1000;

    // Test E: HashMap Missing Lookups
    let start_e = Instant::now();
    for i in 0..scan_size {
        let _ = hash_map.get(&(non_existent_start + i));
    }
    let dur_e = start_e.elapsed();
    println!("  - HashMap Misses:      {:?}", dur_e);

    // Test F: cdDB Query API Misses (Bloom Filter)
    let start_f = Instant::now();
    for i in 0..scan_size {
        let _ = query.get_int(non_existent_start + i, "val");
    }
    let dur_f = start_f.elapsed();
    println!("  - cdDB Bloom Misses:   {:?}", dur_f);

    println!("\n--- Audit Conclusion ---");
    println!("1. Scan Efficiency: cdDB is {:.2}x faster than Vec<Struct> (DOD benefit)", dur_b.as_secs_f64() / dur_a.as_secs_f64());
    println!("2. Lookup Overhead: cdDB Query API is {:.2}x slower than HashMap (Sync/Security overhead)", dur_d.as_secs_f64() / dur_c.as_secs_f64());
    println!("3. Bloom Impact:    cdDB Misses are {:.2}x slower/faster than HashMap Misses", dur_f.as_secs_f64() / dur_e.as_secs_f64());
}
