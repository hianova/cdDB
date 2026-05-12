use cdDB::{CdDBDispatcher, WriteCommand, Query};
use ahash::AHashMap;
use std::thread;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut db = CdDBDispatcher::new(Some("data".into()));

    // 1. Create a partition for "Food.Apple"
    let writer_tx = db.register_partition("Food.Apple".to_string());

    // 2. Insert some data
    let mut attrs = AHashMap::new();
    attrs.insert("Country".to_string(), "Japan".to_string());
    
    let mut attrs_int = AHashMap::new();
    attrs_int.insert("Price".to_string(), 150);

    println!("--- Step 1: Inserting Entity 1 ---");
    writer_tx.send(WriteCommand::Insert {
        entity_id: 1,
        attributes: attrs.into(),
        attributes_int: attrs_int.into(),
    }).await.unwrap();

    // Wait a bit for the background thread to process
    thread::sleep(Duration::from_millis(300));

    // 3. Read data using the new Query interface
    if let Some(route) = db.get_route("Food.Apple") {
        let query = Query::new(route);
        println!("Entity 1 access via Query:");
        
        if let Some(country) = query.get_str(1, "Country").await {
            println!("  - Country: {}", country);
        }
        
        if let Some(price) = query.get_int(1, "Price").await {
            println!("  - Price: {}", price);
        }
    }

    // 4. Delete data
    println!("\n--- Step 2: Deleting Entity 1 ---");
    writer_tx.send(WriteCommand::Delete { entity_id: 1 }).await.unwrap();
    
    thread::sleep(Duration::from_millis(100));

    if let Some(route) = db.get_route("Food.Apple") {
        let query = Query::new(route);
        if query.get_str(1, "Country").await.is_none() {
            println!("Entity 1 successfully deleted (verified via Query).");
        }
    }

    println!("\n--- Step 3: Recycling Index ---");
    let mut attrs2 = AHashMap::new();
    attrs2.insert("Country".to_string(), "Taiwan".to_string());
    writer_tx.send(WriteCommand::Insert {
        entity_id: 2,
        attributes: attrs2.into(),
        attributes_int: AHashMap::new().into(),
    }).await.unwrap();

    thread::sleep(Duration::from_millis(100));
    
    if let Some(route) = db.get_route("Food.Apple") {
        let query = Query::new(route);
        if let Some(country) = query.get_str(2, "Country").await {
            println!("Entity 2: Country = {}", country);
        }
        
        // Check waitlist via internal access if needed (just for demo)
        let worker = route.register_worker();
        if let Some(col) = route.get_column_str("Country", &worker) {
            println!("  - Column waitlist size: {}", col.get_waitlist_snapshot(&worker).len());
            println!("  - Column data size: {}", col.data_len(&worker));
        }
    }
}
