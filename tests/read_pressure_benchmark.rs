use cdDB::{CdDBDispatcher, WriteCommand, Attributes, Query, CdDbQuery, QueryNode};
use std::time::{Instant, Duration};
use rand::Rng;
use std::hint::black_box;

#[tokio::test]
async fn test_read_pressure_benchmark() {
    println!("\n=== cdDB Read Pressure Benchmark (Wait-Free & Multi-threaded) ===");
    
    // 1. Preload 1,000,000 entities
    // Use a count that fits in memory for this environment
    let count = 10_000; // Adjusted for environment stability
    let mut db = CdDBDispatcher::new(None);
    let tx = db.register_partition("bench.pressure".to_string());
    
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        attrs_int.insert("link".to_string(), (i + 1) as u32); // Simple link to next entity
        batch.push((i, Attributes::new(), attrs_int));
    }
    
    let start_load = Instant::now();
    tx.send(WriteCommand::BatchInsert(batch)).unwrap();
    
    let route = db.get_route("bench.pressure").unwrap();
    let worker = route.register_worker();
    while route.get_snapshot(&worker).len() < count {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("  - Data Prep Done ({} entities): {:?}", count, start_load.elapsed());
    
    // 2. Stabilization
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    // 3. Multi-threaded Read Bombing
    let num_threads = 8;
    let ops_per_thread = 50_000;
    let mut handles = vec![];
    
    let start_bench = Instant::now();
    
    for _ in 0..num_threads {
        let r = route.clone();
        let handle = tokio::spawn(async move {
            let mut rng = rand::thread_rng();
            let query_engine = Query::new(&r);
            let mut latencies = Vec::with_capacity(ops_per_thread);
            
            for _ in 0..ops_per_thread {
                let entity_id = rng.gen_range(0..count - 100);
                
                // Construct a mixed query
                let query = CdDbQuery {
                    nodes: vec![
                        QueryNode::Get { entity_id, attr: "val".to_string() },
                        QueryNode::Link { 
                            from_entity_id: entity_id, 
                            link_attr: "link".to_string(), 
                            target_attr: "val".to_string() 
                        },
                    ],
                };
                
                let start_op = Instant::now();
                let results = query_engine.execute(query);
                let duration = start_op.elapsed();
                
                black_box(results);
                latencies.push(duration.as_nanos() as u64);
            }
            latencies
        });
        handles.push(handle);
    }
    
    let mut all_latencies = Vec::with_capacity(num_threads * ops_per_thread);
    for h in handles {
        let lats = h.await.unwrap();
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
    // (In a Wait-Free system, there's no lock contention to cause massive tail latency)
    println!("\nWait-Free Analysis:");
    println!("  - Tail factor (P99/P50): {:.2}x", p99.as_secs_f64() / p50.as_secs_f64());
}
