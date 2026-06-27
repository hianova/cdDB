use cdDB::{
    CdDBStore, CdDBStrStore, CdDBBlobStore, CdDBManagedCache, CdDBDispatcher, Config, DaemonStatus
};
use std::thread;

#[test]
fn test_managed_cache_ops() {
    let config = Config::with_memory_budget(10, 100);
    
    // Create a new CdDBManagedCache
    let cache = CdDBManagedCache::new(config);
    
    // Perform inserts, gets, and removes
    cache.insert(42, "hello".to_string());
    cache.inner.sync();
    assert_eq!(cache.get(&42), Some("hello".to_string()));
    
    cache.remove(&42);
    
    // Test pin_to_t1
    cache.pin_to_t1(2, "two".to_string());
    cache.inner.sync();
    assert_eq!(cache.get(&2), Some("two".to_string()));
    
    // Test suspend/resume
    cache.suspend();
    cache.resume();
    
    // Check daemon health
    let health = cache.daemon_health();
    // DaemonStatus should be stopped or running depending on feature flag
    #[cfg(feature = "dualcache-ff")]
    {
        // Under std and dualcache-ff, it starts.
        assert_eq!(health, DaemonStatus::Running);
    }
    #[cfg(not(feature = "dualcache-ff"))]
    {
        assert_eq!(health, DaemonStatus::Stopped);
    }

    // Test shutdown_gracefully
    cache.shutdown_gracefully(Some(std::time::Duration::from_millis(10)));
}

#[test]
fn test_partition_facade_ops() {
    let _temp_dir = tempfile::tempdir().unwrap();
    let tmp = _temp_dir.path().to_path_buf();
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    let _tx = db.register_partition("facade_test".to_string());
    
    // Get partition
    let partition = db.get_partition("facade_test").expect("Partition should exist");
    
    // Write and read string
    partition.write_str(1, "name", "alice").unwrap();
    
    // Let dispatcher process background commands
    db.sync_cache();
    let route = db.get_route("facade_test").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < 1 {
        thread::yield_now();
    }
    
    let read_val = partition.read_str(1, "name");
    assert_eq!(read_val, Some("alice".to_string()));
    
    // Write and read blob
    let blob = vec![1, 2, 3, 4];
    partition.write_blob(2, "data", blob.clone()).unwrap(); // Entity ID 2
    
    db.sync_cache();
    while route.len(&worker) < 2 {
        thread::yield_now();
    }
    
    let read_blob = partition.read_blob(2, "data");
    assert_eq!(read_blob, Some(blob));
    
    // Test delete
    partition.delete(1).unwrap();
    db.sync_cache();
    
    // Test epoch snapshot
    partition.write_epoch_snapshot(3, 100, 1, vec![9, 9, 9]).unwrap();
}

#[test]
fn test_store_traits() {
    let _temp_dir = tempfile::tempdir().unwrap();
    let tmp = _temp_dir.path().to_path_buf();
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    let _tx = db.register_partition("store_test".to_string());
    let partition = db.get_partition("store_test").expect("Partition should exist");
    
    // String Store
    let str_store = CdDBStrStore::new(partition);
    str_store.put(1, "key1", "val1".to_string()).unwrap();
    
    db.sync_cache();
    let route = db.get_route("store_test").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < 1 {
        thread::yield_now();
    }
    
    assert_eq!(str_store.get(1, "key1"), Some("val1".to_string()));
    
    // Blob Store
    let partition2 = db.get_partition("store_test").expect("Partition should exist");
    let blob_store = CdDBBlobStore::new(partition2);
    blob_store.put(2, "key2", vec![5, 6, 7]).unwrap();
    
    db.sync_cache();
    while route.len(&worker) < 2 {
        thread::yield_now();
    }
    
    assert_eq!(blob_store.get(2, "key2"), Some(vec![5, 6, 7]));
    
    // Test delete
    str_store.delete(1).unwrap();
    blob_store.delete(2).unwrap();
}
