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
