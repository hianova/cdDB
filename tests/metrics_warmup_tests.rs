use cdDB::{CdDBDispatcher, WriteCommand, Attributes};
use std::thread;

#[test]
fn test_metrics_and_warmup() {
    let _temp_dir = tempfile::tempdir().unwrap();
    let tmp = _temp_dir.path().to_path_buf();
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    let tx = db.register_partition("metrics_test".to_string());

    // 1. Initial metrics should be clean
    let initial_metrics = db.metrics();
    assert_eq!(initial_metrics.is_sleeping, false);
    assert_eq!(initial_metrics.partitions.len(), 1);
    assert_eq!(initial_metrics.partitions[0].memory_entities, 0);

    // 2. Prewarm partition (injecting items into T1)
    let prewarm_entities = vec![101, 102, 103, 104, 105];
    db.prewarm_partition("metrics_test", prewarm_entities).unwrap();
    db.sync_cache();

    // 3. Verify Cache metrics reflect the prewarm
    let metrics = db.metrics();
    
    #[cfg(feature = "dualcache-ff")]
    {
        assert!(metrics.cache_enabled);
        assert!(metrics.cache_is_cold_start);
        assert_eq!(metrics.cache_pending_commands, 0); // Should be 0 since we called sync_cache
        // At least 5 items should be in T1 because we prewarmed them
        assert!(metrics.cache_t1_count >= 5);
    }
    
    // 4. Insert data
    let mut attrs_int = Attributes::new();
    attrs_int.insert("age".to_string(), 30);
    let cmd = WriteCommand::Insert {
        entity_id: 1,
        attributes: Attributes::new(),
        attributes_int: attrs_int,
        attributes_blob: Attributes::new(),
    };
    tx.send(cmd).unwrap();

    let route = db.get_route("metrics_test").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < 1 {
        thread::yield_now();
    }

    // 5. Verify Bloom filter saturation and memory entities
    let final_metrics = db.metrics();
    assert_eq!(final_metrics.partitions[0].memory_entities, 1);
    assert!(final_metrics.partitions[0].bloom_saturation > 0.0);
    
    // 6. Test Sleep mode toggling
    db.sleep();
    assert_eq!(db.metrics().is_sleeping, true);
    
    db.wake();
    assert_eq!(db.metrics().is_sleeping, false);
}
