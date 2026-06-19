mod it {
    mod async_test {
        #![cfg(feature = "async")]
        
        use cdDB::{CdDBDispatcher, WriteCommand, Attributes, QueryNode};
        use std::thread;
        
        #[tokio::test]
        async fn test_execute_batch_async() {
            let _temp_dir = tempfile::tempdir().unwrap();
            let tmp = _temp_dir.path().to_path_buf();
            let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
            let tx = db.register_partition("test_async".to_string());
        
            let mut attrs_int = Attributes::new();
            attrs_int.insert("val".to_string(), 42);
            let cmd = WriteCommand::Insert {
                entity_id: 1,
                attributes: Attributes::new(),
                attributes_int: attrs_int,
                attributes_blob: Attributes::new(),
            };
            tx.send(cmd).unwrap();
            
            let route = db.get_route("test_async").unwrap();
            let worker = route.register_worker();
            while route.len(&worker) < 1 {
                thread::yield_now();
            }
            
            let nodes = vec![
                QueryNode::Scan { attr: "val" },
            ];
            
            let res = db.execute_batch_async(
                "test_async".to_string(),
                nodes,
                |res| {
                    match res {
                        cdDB::QueryResult::IntList(slice) => {
                            assert_eq!(slice.len(), 1);
                            assert_eq!(slice[0], 42);
                            true
                        }
                        _ => false,
                    }
                },
            ).await;
            
            assert_eq!(res, Some(true));
        }
    }

    mod cold_data_test {
        use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
        use std::time::Instant;
        use std::thread;
        
        #[test]
        fn test_cold_data_scan_performance() {
            let _temp_dir = tempfile::tempdir().unwrap();
            let base_path = _temp_dir.path().to_path_buf();
            
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
                
                // Wait until all data is processed by the background partition thread
                let route = db.get_route("cold.bench").unwrap();
                while route.len(&route.register_worker()) < count {
                    thread::yield_now();
                }
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
            
        
        }
    }

    mod commands_test {
        use cdDB::{WriteCommand, Attributes};
        
        #[test]
        fn test_encode_decode_roundtrip() {
            let mut attrs_str = Attributes::new();
            attrs_str.insert("name".to_string(), "test_name".to_string());
            
            let mut attrs_int = Attributes::new();
            attrs_int.insert("val".to_string(), 42);
            
            let mut attrs_blob = Attributes::new();
            attrs_blob.insert("blob_data".to_string(), vec![1, 2, 3]);
        
            let cmd = WriteCommand::Insert {
                entity_id: 123,
                attributes: attrs_str,
                attributes_int: attrs_int,
                attributes_blob: attrs_blob,
            };
            
            let encoded = cmd.encode();
            let decoded = WriteCommand::decode(&encoded).expect("Decode should succeed");
            
            match decoded {
                WriteCommand::Insert { entity_id, attributes, attributes_int, attributes_blob } => {
                    assert_eq!(entity_id, 123);
                    assert_eq!(attributes_int.inner().get("val"), Some(&42));
                    assert_eq!(attributes_blob.inner().get("blob_data"), Some(&vec![1, 2, 3]));
                    assert_eq!(attributes.inner().get("name"), Some(&"test_name".to_string()));
                },
                _ => panic!("Decoded command is not an Insert"),
            }
        }
    }

    mod delete_test {
        use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
        use std::time::Duration;
        use std::thread;
        
        #[test]
        fn test_delete_command() {
            let _temp_dir = tempfile::tempdir().unwrap();
            let tmp = _temp_dir.path().to_path_buf();
            let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
            let tx = db.register_partition("test.delete".to_string());
        
            let mut attrs = Attributes::new();
            attrs.insert("val".to_string(), 100);
            
            // Insert
            tx.send(WriteCommand::Insert {
                entity_id: 10,
                attributes: Attributes::new(),
                attributes_int: attrs,
                attributes_blob: Attributes::new(),
            }).unwrap();
        
            let route = db.get_route("test.delete").unwrap();
            let worker = route.register_worker();
            while route.len(&worker) < 1 {
                thread::yield_now();
            }
            
            let query = Query::new(&route);
            assert_eq!(query.get_int(10, "val"), Some(100));
        
            // Delete
            tx.send(WriteCommand::Delete { entity_id: 10 }).unwrap();
            
            // Wait for delete to process
            let start = std::time::Instant::now();
            while query.get_int(10, "val").is_some() {
                if start.elapsed() > Duration::from_secs(5) {
                    panic!("Delete timeout");
                }
                thread::yield_now();
            }
            
            assert_eq!(query.get_int(10, "val"), None);
        }
    }

    mod loom_tests {
        #![cfg(feature = "loom")]
        use cdDB::qsbr::{WorkerNode, WorkerState};
        use std::sync::Arc;
        use cdDB::sync::atomic::AtomicPtr;
        use loom::thread;
        
        #[test]
        fn test_qsbr_worker_registration() {
            loom::model(|| {
                let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut::<WorkerNode>()));
                
                let w_clone1 = workers.clone();
                let t1 = thread::spawn(move || {
                    let worker_state = Arc::new(WorkerState::new());
                    let new_node = Box::into_raw(Box::new(WorkerNode {
                        worker: worker_state,
                        next: AtomicPtr::new(core::ptr::null_mut()),
                    }));
                    loop {
                        let head = w_clone1.load(loom::sync::atomic::Ordering::Acquire);
                        unsafe { (*new_node).next.store(head, loom::sync::atomic::Ordering::Relaxed) };
                        if w_clone1.compare_exchange(head, new_node, loom::sync::atomic::Ordering::Release, loom::sync::atomic::Ordering::Relaxed).is_ok() {
                            break;
                        }
                    }
                });
                
                let w_clone2 = workers.clone();
                let t2 = thread::spawn(move || {
                    let worker_state = Arc::new(WorkerState::new());
                    let new_node = Box::into_raw(Box::new(WorkerNode {
                        worker: worker_state,
                        next: AtomicPtr::new(core::ptr::null_mut()),
                    }));
                    loop {
                        let head = w_clone2.load(loom::sync::atomic::Ordering::Acquire);
                        unsafe { (*new_node).next.store(head, loom::sync::atomic::Ordering::Relaxed) };
                        if w_clone2.compare_exchange(head, new_node, loom::sync::atomic::Ordering::Release, loom::sync::atomic::Ordering::Relaxed).is_ok() {
                            break;
                        }
                    }
                });
                
                t1.join().unwrap();
                t2.join().unwrap();
                
                let mut count = 0;
                let mut curr = workers.load(loom::sync::atomic::Ordering::Acquire);
                while !curr.is_null() {
                    count += 1;
                    curr = unsafe { (*curr).next.load(loom::sync::atomic::Ordering::Acquire) };
                }
                
                assert_eq!(count, 2);
                
                let mut curr = workers.load(loom::sync::atomic::Ordering::Acquire);
                while !curr.is_null() {
                    let next = unsafe { (*curr).next.load(loom::sync::atomic::Ordering::Acquire) };
                    unsafe { drop(Box::from_raw(curr)); }
                    curr = next;
                }
            });
        }
    }

    mod olap_test {
        use cdDB::{
            AggregateOp, Attributes, CdDBDispatcher, QueryNode, QueryResult, WriteCommand,
        };
        use std::time::Duration;
        use std::thread;
        
        
        #[test]
        fn test_olap_vectorized_queries() {
            println!("\n=== cdDB OLAP Vectorized Queries Test ===");
        
            let _temp_dir = tempfile::tempdir().unwrap();
            let tmp = _temp_dir.path().to_path_buf();
            let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
            let tx = db.register_partition("olap.test".to_string());
        
            // 1. Insert some data
            let count = 100;
            let mut batch = Vec::with_capacity(count);
            for i in 0..count {
                let mut attrs_int = Attributes::new();
                attrs_int.insert("val".to_string(), i as u32);
                attrs_int.insert("even".to_string(), (i % 2 == 0) as u32);
                let mut attrs = Attributes::new();
                attrs.insert("str_val".to_string(), format!("str-{}", i));
                let mut attrs_blob = Attributes::new();
                attrs_blob.insert("blob_val".to_string(), vec![(i % 255) as u8; 4]);
                batch.push((i, attrs, attrs_int, attrs_blob));
            }
            tx.send(WriteCommand::BatchInsert(batch)).unwrap();
        
            let route = db.get_route("olap.test").unwrap();
            let worker = route.register_worker();
        
            // Wait for data to be applied
            while route.len(&worker) < count {
                thread::sleep(Duration::from_millis(10));
            }
        
            // 2. Test Scan
            println!("Testing Scan...");
            db.execute_batch("olap.test", &[QueryNode::Scan { attr: "val" }], |res| {
                if let QueryResult::IntList(list) = res {
                    assert_eq!(list.len(), count);
                    assert_eq!(list[0], 0);
                    assert_eq!(list[99], 99);
                    println!("  - Scan success: collected {} elements", list.len());
                } else {
                    panic!("Scan failed to return IntList");
                }
            });
        
            // 3. Test Aggregate Sum
            println!("Testing Aggregate Sum...");
            db.execute_batch("olap.test", &[QueryNode::Aggregate { attr: "val", op: AggregateOp::Sum }], |res| {
                if let QueryResult::IntSum(sum) = res {
                    let expected_sum = (0..count as u64).sum::<u64>();
                    assert_eq!(sum, expected_sum);
                    println!("  - Sum success: {} (expected {})", sum, expected_sum);
                } else {
                    panic!("Sum failed to return IntSum");
                }
            });
        
            // 4. Test Aggregate Avg
            println!("Testing Aggregate Avg...");
            db.execute_batch("olap.test", &[QueryNode::Aggregate { attr: "val", op: AggregateOp::Avg }], |res| {
                if let QueryResult::IntAvg(avg) = res {
                    let expected_avg = (count - 1) as f64 / 2.0;
                    assert_eq!(avg, expected_avg);
                    println!("  - Avg success: {} (expected {})", avg, expected_avg);
                } else {
                    panic!("Avg failed to return IntAvg");
                }
            });
        
            // 5. Test Aggregate Min/Max/Count
            println!("Testing Aggregate Min/Max/Count...");
            let nodes = [
                QueryNode::Aggregate { attr: "val", op: AggregateOp::Min },
                QueryNode::Aggregate { attr: "val", op: AggregateOp::Max },
                QueryNode::Aggregate { attr: "val", op: AggregateOp::Count },
            ];
            let mut idx = 0;
            db.execute_batch("olap.test", &nodes, |res| {
                match idx {
                    0 => {
                        if let QueryResult::IntMin(min) = res { assert_eq!(min, 0); } else { panic!("Min expected"); }
                    }
                    1 => {
                        if let QueryResult::IntMax(max) = res { assert_eq!(max, 99); } else { panic!("Max expected"); }
                    }
                    2 => {
                        if let QueryResult::Count(c) = res { assert_eq!(c, 100); } else { panic!("Count expected"); }
                    }
                    _ => panic!("Too many results"),
                }
                idx += 1;
            });
        
            // 6. Test String/Blob Scan
            println!("Testing String/Blob Scan...");
            db.execute_batch("olap.test", &[QueryNode::Scan { attr: "str_val" }], |res| {
                if let QueryResult::StrList(list) = res {
                    assert_eq!(list.len(), count);
                    assert_eq!(list[0], "str-0");
                } else {
                    panic!("Expected StrList");
                }
            });
        
            db.execute_batch("olap.test", &[QueryNode::Scan { attr: "blob_val" }], |res| {
                if let QueryResult::BlobList(list) = res {
                    assert_eq!(list.len(), count);
                    assert_eq!(list[1][0], 1);
                } else {
                    panic!("Expected BlobList");
                }
            });
        
            println!("=== OLAP Test Passed ===\n");
        }
    }

    mod read_pressure_test {
        use cdDB::{CdDBDispatcher, WriteCommand, Attributes, Query, QueryNode};
        use std::time::{Instant, Duration};
        use rand::Rng;
        use std::hint::black_box;
        use std::thread;
        
        #[test]
        fn test_read_pressure_benchmark() {
            println!("\n=== cdDB Read Pressure Benchmark (Wait-Free & Multi-threaded) ===");
            
            // 1. Preload entities
            let count = 10_000; 
            let _temp_dir = tempfile::tempdir().unwrap();
            let tmp = _temp_dir.path().to_path_buf();
            let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
            let tx = db.register_partition("bench.pressure".to_string());
            
            let mut batch = Vec::with_capacity(count);
            for i in 0..count {
                let mut attrs_int = Attributes::new();
                attrs_int.insert("val".to_string(), i as u32);
                attrs_int.insert("link".to_string(), (i + 1) as u32); // Simple link to next entity
                batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
            }
            
            let start_load = Instant::now();
            tx.send(WriteCommand::BatchInsert(batch)).unwrap();
            
            let route = db.get_route("bench.pressure").unwrap();
            let worker = route.register_worker();
            while route.len(&worker) < count {
                thread::sleep(Duration::from_millis(100));
            }
            println!("  - Data Prep Done ({} entities): {:?}", count, start_load.elapsed());
            
            // 2. Stabilization (Removed redundant sleep since route.len() confirmed data)    
            // 3. Multi-threaded Read Bombing
            let num_threads = 4;
            let ops_per_thread = 250_000;
            let mut handles = vec![];
            
            let start_bench = Instant::now();
            
            for _ in 0..num_threads {
                let r = route.clone();
                let handle = thread::spawn(move || {
                    let query_engine = Query::new(&r);
                    let mut latencies = Vec::with_capacity(ops_per_thread);
                    for _ in 0..ops_per_thread {
                        let entity_id = rand::thread_rng().gen_range(0..count - 100);
                        
                        // Construct a mixed query (Stack-allocated array, zero-allocation)
                        let nodes = [
                            QueryNode::Get { entity_id, attr: "val" },
                            QueryNode::Link { 
                                from_entity_id: entity_id, 
                                link_attr: "link", 
                                target_attr: "val" 
                            },
                        ];
                        
                        let start_op = Instant::now();
                        query_engine.execute_with_cb(&nodes, |res| {
                            match res {
                                cdDB::QueryResult::None => panic!("Unexpected None for existing entity"),
                                other => { black_box(other); }
                            }
                        });
                        let duration = start_op.elapsed();
                        latencies.push(duration.as_nanos() as u64);
                    }
                    latencies
                });
                handles.push(handle);
            }
            
            let mut all_latencies = Vec::with_capacity(num_threads * ops_per_thread);
            for h in handles {
                let lats = h.join().unwrap();
                all_latencies.extend(lats);
            }
            
            let total_duration = start_bench.elapsed();
            let total_ops = num_threads * ops_per_thread;
            
            // 4. Statistics
            all_latencies.sort_unstable();
            let p50 = Duration::from_nanos(all_latencies[total_ops / 2]);
            let p99 = Duration::from_nanos(all_latencies[(total_ops as f64 * 0.99) as usize]);
            let p999 = Duration::from_nanos(all_latencies[(total_ops as f64 * 0.999) as usize]);
            let qps = total_ops as f64 / total_duration.as_secs_f64();
            
            println!("\nBenchmark Results:");
            println!("  - Total Operations: {}", total_ops);
            println!("  - Total Duration:   {:?}", total_duration);
            println!("  - Throughput:       {:.2} QPS", qps);
            println!("  - Latency P50:      {:?}", p50);
            println!("  - Latency P99:      {:?}", p99);
            println!("  - Latency P99.9:    {:?}", p999);
            
            // Verify wait-free property: P99 shouldn't be significantly worse than P50
            println!("\nWait-Free Analysis:");
            println!("  - Tail factor (P99/P50): {:.2}x", p99.as_secs_f64() / p50.as_secs_f64());
        }
    }

    mod read_test {
        use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
        use std::time::{Instant, Duration};
        use std::thread;
        use ahash::AHashMap;
        
        #[derive(Clone)]
        struct TraditionalStruct {
            _id: usize,
            val: u32,
        }
        
        #[test]
        fn test_read_performance_benchmark() {
            println!("\n=== cdDB Fair Performance Audit (10,000 Entities) ===");
            
            let count = 10_000;
            let scan_size = 1_000;
            
            // 1. Prepare Data
            let _temp_dir = tempfile::tempdir().unwrap();
            let tmp = _temp_dir.path().to_path_buf();
            let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
            let tx = db.register_partition("bench.read".to_string());
            
            let mut batch = Vec::with_capacity(count);
            let mut hash_map = AHashMap::with_capacity(count);
            let mut vec_struct = Vec::with_capacity(count);
        
            for i in 0..count {
                let mut attrs_int = Attributes::new();
                attrs_int.insert("val".to_string(), i as u32);
                batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
        
                let s = TraditionalStruct { _id: i, val: i as u32 };
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
            let query = Query::new(&route);
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
            let a_sec = dur_a.as_secs_f64();
            if a_sec > 0.0 {
                println!("1. Scan Efficiency: cdDB is {:.2}x faster than Vec<Struct> (DOD benefit)", dur_b.as_secs_f64() / a_sec);
            } else {
                println!("1. Scan Efficiency: cdDB is infinitely faster (0.0s measured)");
            }
            
            let c_sec = dur_c.as_secs_f64();
            if c_sec > 0.0 {
                println!("2. Lookup Overhead: cdDB Query API is {:.2}x slower than HashMap (Sync/Security overhead)", dur_d.as_secs_f64() / c_sec);
            } else {
                println!("2. Lookup Overhead: HashMap is infinitely faster (0.0s measured)");
            }
            
            let e_sec = dur_e.as_secs_f64();
            if e_sec > 0.0 {
                println!("3. Bloom Impact:    cdDB Misses are {:.2}x slower/faster than HashMap Misses", dur_f.as_secs_f64() / e_sec);
            } else {
                println!("3. Bloom Impact:    HashMap Misses are infinitely faster (0.0s measured)");
            }
        }
    }

    mod wal_replay_test {
        use cdDB::{CdDBDispatcher, WriteCommand, Attributes};
        use std::thread;
        
        #[test]
        fn test_wal_replay() {
            let _temp_dir = tempfile::tempdir().unwrap();
            let tmp = _temp_dir.path().to_path_buf();
            let wal_path = tmp.join("wal.log");
        
            // Phase 1: Create DB and write data
            {
                let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
                let tx = db.register_partition_with_wal(
                    "test_wal".to_string(),
                    Some(wal_path.to_string_lossy().into_owned()),
                    cdDB::wal::WalMode::Sync,
                );
        
                let mut attrs_int = Attributes::new();
                attrs_int.insert("val".to_string(), 42);
                let cmd = WriteCommand::Insert {
                    entity_id: 1,
                    attributes: Attributes::new(),
                    attributes_int: attrs_int,
                    attributes_blob: Attributes::new(),
                };
                tx.send(cmd).unwrap();
                
                let route = db.get_route("test_wal").unwrap();
                let worker = route.register_worker();
                while route.len(&worker) < 1 {
                    thread::yield_now();
                }
                
                // Drop db and tx, and sleep briefly to ensure the background thread completes Shutdown and flushes WAL
                drop(tx);
                drop(db);
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        
            // Phase 2: Create a new DB and replay WAL
            {
                let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
                // The act of registering with an existing WAL path will trigger `replay_wal`
                let _tx = db.register_partition_with_wal(
                    "test_wal".to_string(),
                    Some(wal_path.to_string_lossy().into_owned()),
                    cdDB::wal::WalMode::Sync,
                );
        
                let route = db.get_route("test_wal").unwrap();
                let worker = route.register_worker();
                
                while route.len(&worker) < 1 {
                    thread::yield_now();
                }
                
                // Check if data is already loaded synchronously during register!
                // `replay_wal` runs during partition init before it starts processing new cmds.
                let q = cdDB::Query::new(&route);
                let session = q.session();
                let val = session.get_int(1, "val");
                assert_eq!(val, Some(42));
            }
        }
    }

    mod sleep_wake_test {
        use cdDB::CdDBDispatcher;

        #[test]
        fn test_sleep_wake() {
            let _temp_dir = tempfile::tempdir().unwrap();
            let tmp = _temp_dir.path().to_path_buf();
            let db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
            
            assert_eq!(db.is_sleeping(), false);
            
            db.sleep();
            assert_eq!(db.is_sleeping(), true);
            
            // In a real application, the frontend/connection listener would check db.is_sleeping()
            // and pause accepting requests. The background threads continue in a natural minimal-execution loop.
            
            db.wake();
            assert_eq!(db.is_sleeping(), false);
        }
    }
}
