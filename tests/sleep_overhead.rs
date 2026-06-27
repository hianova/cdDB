use std::time::Instant;
use dualcache_ff::{Config, DualCacheFF};

#[test]
fn test_sleep_overhead() {
    let cache_config = Config::with_memory_budget(100, 60);

    // Measure spawning overhead
    let start = Instant::now();
    for _ in 0..100 {
        let (global_cache, daemon) = DualCacheFF::<u32, u32>::new_headless(cache_config.clone());
        let handle = std::thread::spawn(move || {
            daemon.run();
        });
        global_cache.shutdown_gracefully(None);
        let _ = handle.join();
    }
    let elapsed = start.elapsed();
    println!("Daemon spawn/shutdown overhead (per thread): {:?}", elapsed / 100);

    // Compare with a running daemon
    let (global_cache, daemon) = DualCacheFF::<u32, u32>::new_headless(cache_config);
    let handle = std::thread::spawn(move || {
        daemon.run();
    });
    
    let start = Instant::now();
    for _ in 0..10_000 {
        let _ = global_cache.cmd_tx.try_send(dualcache_ff::daemon::Command::InsertT1(1, 1, 1));
    }
    let elapsed = start.elapsed();
    println!("Sending 10_000 commands overhead: {:?}", elapsed);

    global_cache.shutdown_gracefully(None);
    let _ = handle.join();
}
