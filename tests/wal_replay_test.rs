use cdDB::{CdDBDispatcher, WriteCommand, Attributes};
use std::time::Duration;
use std::thread;

#[test]
fn test_wal_replay() {
    let _temp_dir = tempfile::tempdir().unwrap();
    let tmp = _temp_dir.path().to_path_buf();
    let wal_path = tmp.join("wal.log");

    // Phase 1: Create DB and write data
    {
        let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(None);
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
        let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(None);
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
