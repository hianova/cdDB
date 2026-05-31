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
