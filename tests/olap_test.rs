use cdDB::{
    AggregateOp, Attributes, CdDBDispatcher, CdDbQuery, Query, QueryNode, QueryResult, WriteCommand,
};
use std::time::Duration;

#[tokio::test]
async fn test_olap_vectorized_queries() {
    println!("\n=== cdDB OLAP Vectorized Queries Test ===");

    let mut db = CdDBDispatcher::new(None);
    let tx = db.register_partition("olap.test".to_string());

    // 1. Insert some data
    let count = 100;
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        attrs_int.insert("even".to_string(), (i % 2 == 0) as u32);
        batch.push((i, Attributes::new(), attrs_int));
    }
    tx.send(WriteCommand::BatchInsert(batch)).await.unwrap();

    let route = db.get_route("olap.test").unwrap();
    let worker = route.register_worker();

    // Wait for data to be applied
    while route.len(&worker) < count {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let query_engine = Query::new(route);

    // 2. Test Scan
    println!("Testing Scan...");
    let scan_query = CdDbQuery {
        nodes: vec![QueryNode::Scan {
            attr: "val".to_string(),
        }],
    };
    let results = query_engine.execute(scan_query).await;
    if let QueryResult::IntList(list) = &results[0] {
        assert_eq!(list.len(), count);
        assert_eq!(list[0], 0);
        assert_eq!(list[99], 99);
        println!("  - Scan success: collected {} elements", list.len());
    } else {
        panic!("Scan failed to return IntList");
    }

    // 3. Test Aggregate Sum
    println!("Testing Aggregate Sum...");
    let sum_query = CdDbQuery {
        nodes: vec![QueryNode::Aggregate {
            attr: "val".to_string(),
            op: AggregateOp::Sum,
        }],
    };
    let results = query_engine.execute(sum_query).await;
    if let QueryResult::IntSum(sum) = results[0] {
        let expected_sum = (0..count as u64).sum::<u64>();
        assert_eq!(sum, expected_sum);
        println!("  - Sum success: {} (expected {})", sum, expected_sum);
    } else {
        panic!("Sum failed to return IntSum");
    }

    // 4. Test Aggregate Avg
    println!("Testing Aggregate Avg...");
    let avg_query = CdDbQuery {
        nodes: vec![QueryNode::Aggregate {
            attr: "val".to_string(),
            op: AggregateOp::Avg,
        }],
    };
    let results = query_engine.execute(avg_query).await;
    if let QueryResult::IntAvg(avg) = results[0] {
        let expected_avg = (count - 1) as f64 / 2.0;
        assert_eq!(avg, expected_avg);
        println!("  - Avg success: {} (expected {})", avg, expected_avg);
    } else {
        panic!("Avg failed to return IntAvg");
    }

    // 5. Test Aggregate Min/Max/Count
    println!("Testing Aggregate Min/Max/Count...");
    let mix_query = CdDbQuery {
        nodes: vec![
            QueryNode::Aggregate {
                attr: "val".to_string(),
                op: AggregateOp::Min,
            },
            QueryNode::Aggregate {
                attr: "val".to_string(),
                op: AggregateOp::Max,
            },
            QueryNode::Aggregate {
                attr: "val".to_string(),
                op: AggregateOp::Count,
            },
        ],
    };
    let results = query_engine.execute(mix_query).await;
    if let (QueryResult::IntMin(min), QueryResult::IntMax(max), QueryResult::Count(c)) =
        (&results[0], &results[1], &results[2])
    {
        assert_eq!(*min, 0);
        assert_eq!(*max, 99);
        assert_eq!(*c, 100);
        println!(
            "  - Min/Max/Count success: min={}, max={}, count={}",
            min, max, c
        );
    } else {
        panic!("Mixed aggregate failed");
    }

    println!("=== OLAP Test Passed ===\n");
}
