use cdDB::{
    CdDBDispatcher, WriteCommand, Attributes, WalMode, DurabilityMode, FlushConfigBuilder
};
use std::thread;
use std::time::Duration;

#[test]
fn test_default_wal_modes() {
    let _temp_dir = tempfile::tempdir().unwrap();
    let tmp = _temp_dir.path().to_path_buf();
    
    // Test Sync mode
    {
        let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
        let _tx = db.register_partition_with_wal(
            "sync_test".to_string(),
            None,
            WalMode::Sync,
        );
    }
    
    // Test Async100ms mode
    {
        let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
        let _tx = db.register_partition_with_wal(
            "async_test".to_string(),
            None,
            WalMode::Async100ms,
        );
    }
}

#[test]
fn test_custom_strict_durability() {
    let _temp_dir = tempfile::tempdir().unwrap();
    let tmp = _temp_dir.path().to_path_buf();
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    
    let mode = WalMode::Custom {
        durability: DurabilityMode::Strict,
        flush: FlushConfigBuilder::new().build(),
    };
    
    let tx = db.register_partition_with_wal(
        "custom_strict".to_string(),
        None,
        mode,
    );
    
    let cmd = WriteCommand::Insert {
        entity_id: 1,
        attributes: Attributes::new(),
        attributes_int: Attributes::new(),
        attributes_blob: Attributes::new(),
    };
    tx.send(cmd).unwrap();
    
    db.sync_cache();
    let route = db.get_route("custom_strict").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < 1 {
        thread::yield_now();
    }
}

#[test]
fn test_custom_relaxed_durability() {
    let _temp_dir = tempfile::tempdir().unwrap();
    let tmp = _temp_dir.path().to_path_buf();
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    
    let flush_config = FlushConfigBuilder::new()
        .with_batch_size(1024)
        .with_ttl_micros(5000) // 5ms
        .build();
    let mode = WalMode::Custom {
        durability: DurabilityMode::Relaxed(Duration::from_millis(5)),
        flush: flush_config,
    };
    
    let tx = db.register_partition_with_wal(
        "custom_relaxed".to_string(),
        None,
        mode,
    );
    
    let cmd = WriteCommand::Insert {
        entity_id: 2,
        attributes: Attributes::new(),
        attributes_int: Attributes::new(),
        attributes_blob: Attributes::new(),
    };
    tx.send(cmd).unwrap();
    
    db.sync_cache();
    let route = db.get_route("custom_relaxed").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < 1 {
        thread::yield_now();
    }
}

#[test]
#[should_panic(expected = "TTL 500 µs is below the physical SSD and OS timer limit")]
fn test_flush_config_builder_panic() {
    // Should panic because ttl_micros < 1000 and expert_mode is false
    let _ = FlushConfigBuilder::new()
        .with_ttl_micros(500)
        .build();
}

#[test]
fn test_flush_config_builder_expert_override() {
    // Should NOT panic because expert_mode is unlocked
    let config = FlushConfigBuilder::new()
        .with_ttl_micros(500)
        .unlock_expert_danger_zone()
        .build();
    assert_eq!(config.ttl_micros, 500);
    assert!(config.expert_mode);
}
