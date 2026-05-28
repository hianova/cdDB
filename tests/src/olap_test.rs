use cdDB::{
    AggregateOp, Attributes, CdDBDispatcher, QueryNode, QueryResult, WriteCommand,
};
use std::time::Duration;
use std::thread;

#[test]
fn test_olap_vectorized_queries() {
    println!("\n=== cdDB OLAP Vectorized Queries Test ===");

    let tmp = std::env::temp_dir().join(format!("cdDB_{}", std::process::id()));
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(tmp.to_string_lossy().into_owned()));
    let tx = db.register_partition("olap.test".to_string());

    // 1. Insert some data
    let count = 100;
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        attrs_int.insert("even".to_string(), (i % 2 == 0) as u32);
        batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
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
    let mut scan_results = Vec::new();
    db.execute_batch("olap.test", &[QueryNode::Scan { attr: "val" }], |res| scan_results.push(res));
    if let QueryResult::IntList(list) = &scan_results[0] {
        assert_eq!(list.len(), count);
        assert_eq!(list[0], 0);
        assert_eq!(list[99], 99);
        println!("  - Scan success: collected {} elements", list.len());
    } else {
        panic!("Scan failed to return IntList");
    }

    // 3. Test Aggregate Sum
    println!("Testing Aggregate Sum...");
    let mut sum_results = Vec::new();
    db.execute_batch("olap.test", &[QueryNode::Aggregate { attr: "val", op: AggregateOp::Sum }], |res| sum_results.push(res));
    if let QueryResult::IntSum(sum) = sum_results[0] {
        let expected_sum = (0..count as u64).sum::<u64>();
        assert_eq!(sum, expected_sum);
        println!("  - Sum success: {} (expected {})", sum, expected_sum);
    } else {
        panic!("Sum failed to return IntSum");
    }

    // 4. Test Aggregate Avg
    println!("Testing Aggregate Avg...");
    let mut avg_results = Vec::new();
    db.execute_batch("olap.test", &[QueryNode::Aggregate { attr: "val", op: AggregateOp::Avg }], |res| avg_results.push(res));
    if let QueryResult::IntAvg(avg) = avg_results[0] {
        let expected_avg = (count - 1) as f64 / 2.0;
        assert_eq!(avg, expected_avg);
        println!("  - Avg success: {} (expected {})", avg, expected_avg);
    } else {
        panic!("Avg failed to return IntAvg");
    }

    // 5. Test Aggregate Min/Max/Count
    println!("Testing Aggregate Min/Max/Count...");
    let mut mix_results = Vec::new();
    let nodes = [
        QueryNode::Aggregate { attr: "val", op: AggregateOp::Min },
        QueryNode::Aggregate { attr: "val", op: AggregateOp::Max },
        QueryNode::Aggregate { attr: "val", op: AggregateOp::Count },
    ];
    db.execute_batch("olap.test", &nodes, |res| mix_results.push(res));
    
    match (&mix_results[0], &mix_results[1], &mix_results[2]) {
        (QueryResult::IntMin(min), QueryResult::IntMax(max), QueryResult::Count(c)) => {
            assert_eq!(*min, 0);
            assert_eq!(*max, 99);
            assert_eq!(*c, 100);
            println!(
                "  - Min/Max/Count success: min={}, max={}, count={}",
                min, max, c
            );
        }
        _ => panic!("Mixed aggregate failed"),
    }

    println!("=== OLAP Test Passed ===\n");
}
