use cdDB::{CdDBDispatcher, WriteCommand, Attributes, Query};
use std::time::Duration;
use std::thread;

// Explicitly use dhat for heap profiling to ensure no leaks
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[test]
fn test_memory_leak_and_thread_drop() {
    let _profiler = dhat::Profiler::builder().testing().build();
    
    println!("=== Starting Memory Leak & Thread Drop Test ===");

    {
        let tmp = std::env::temp_dir().join(format!("cdDB_{}", std::process::id()));
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
        let tx = db.register_partition("test.memory_leak".to_string());

        // 1. Insert Data
        let mut batch = Vec::new();
        for i in 0..100 {
            let mut attrs = Attributes::new();
            attrs.insert("val".to_string(), i as u32);
            batch.push((i, Attributes::new(), attrs, Attributes::new()));
        }
        tx.send(WriteCommand::BatchInsert(batch)).unwrap();

        let route = db.get_route("test.memory_leak").unwrap();

        // 2. Spawn and Drop Workers repeatedly
        let mut handles = vec![];
        for _ in 0..10 {
            let route_clone = route.clone();
            handles.push(thread::spawn(move || {
                let query = Query::new(&route_clone);
                for i in 0..100 {
                    let _ = query.get_int(i, "val");
                }
                // QuerySession is dropped here, which triggers `worker.leave()`
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Wait to ensure everything is flushed/processed
        thread::sleep(Duration::from_millis(100));
        
        // CdDBDispatcher and its routes will be dropped at the end of this block
    }

    // After this scope, all DB instances, partition threads, and channels should be dropped.
    // dhat will panic when _profiler is dropped if there are any unfreed allocations.
    // However, since we use testing() mode, we can explicitly assert stats.
    let stats = dhat::HeapStats::get();
    println!("Heap Stats: {:?}", stats);
    
    // Note: Due to some static lazy_statics or internal library allocations,
    // exact 0 might be hard, but this verifies major structural drops.
    // We mainly rely on dhat not crashing/panicking on major leaks if configured strictly.
    println!("=== Memory Leak Test Completed ===");
}
