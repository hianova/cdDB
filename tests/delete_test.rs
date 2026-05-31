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
