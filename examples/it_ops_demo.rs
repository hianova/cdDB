use cdDB::{CdDBDispatcher, WriteCommand, Query, ITOpsRecord, LogLevel};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    println!("=== cdDB IT Operations Interface Demo ===");
    
    // Initialize dispatcher
    let mut db = CdDBDispatcher::new(Some("ops_data".into()));

    // 1. Register a partition for system metrics
    let ops_tx = db.register_partition("system.metrics".to_string());

    // 2. Create a structured IT Ops record
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let record = ITOpsRecord {
        timestamp: now,
        service: "api-gateway".to_string(),
        node: "node-01".to_string(),
        level: LogLevel::Info,
        message: "High traffic detected, scaling up...".to_string(),
        cpu_usage: 0.75, // 75%
        mem_usage: 0.42, // 42%
        response_time_ms: 120,
    };

    println!("Ingesting IT Ops record: {:?}", record);

    // 3. Convert and send to cdDB
    let (attrs, attrs_int) = record.to_cd_db_params();
    ops_tx.send(WriteCommand::Insert {
        entity_id: 1001,
        attributes: attrs,
        attributes_int: attrs_int,
    }).unwrap();

    // Wait for async processing
    thread::sleep(Duration::from_millis(300));

    // 4. Query the ingested data
    if let Some(route) = db.get_route("system.metrics") {
        let query = Query::new(route);
        println!("\nQuerying back Ops data for Entity 1001:");
        
        if let Some(service) = query.get_str(1001, "service") {
            println!("  - Service: {}", service);
        }
        
        if let Some(cpu) = query.get_int(1001, "cpu_milli") {
            println!("  - CPU Usage: {}%", cpu as f32 / 10.0);
        }

        if let Some(msg) = query.get_str(1001, "message") {
            println!("  - Message: {}", msg);
        }
    }

    println!("\nDemo completed successfully.");
}
