use cdDB::{CdDBDispatcher, WriteCommand};
use ahash::AHashMap;
use std::thread;
use std::time::Duration;

fn main() {
    let mut db = CdDBDispatcher::new();

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
        attributes: attrs,
        attributes_int: attrs_int,
    }).unwrap();

    // Wait a bit for the background thread to process
    thread::sleep(Duration::from_millis(100));

    // 3. Read data using snapshot and ColumnArray access
    if let Some(route) = db.get_route("Food.Apple") {
        let snapshot = route.get_snapshot();
        if let Some(ptr) = snapshot.get(&1) {
            println!("Entity 1 found in snapshot: {:?}", ptr);
            
            // Look up "Country"
            if let Some(idx) = ptr.attribute_indices.get("Country") {
                if let Some(col) = route.get_column_str("Country") {
                    let data = col.data.read();
                    if let Some(val) = &data[*idx] {
                        println!("  - Country: {}", val);
                    }
                }
            }

            // Look up "Price"
            if let Some(idx) = ptr.attribute_indices.get("Price") {
                if let Some(col) = route.get_column_int("Price") {
                    let data = col.data.read();
                    if let Some(val) = &data[*idx] {
                        println!("  - Price: {}", val);
                    }
                }
            }
        }
    }

    // 4. Delete data
    println!("\n--- Step 2: Deleting Entity 1 ---");
    writer_tx.send(WriteCommand::Delete { entity_id: 1 }).unwrap();
    
    thread::sleep(Duration::from_millis(100));

    if let Some(route) = db.get_route("Food.Apple") {
        let snapshot = route.get_snapshot();
        if snapshot.get(&1).is_none() {
            println!("Entity 1 successfully deleted from snapshot.");
            
            // Check if it's None in the ColumnArray too
            if let Some(col) = route.get_column_str("Country") {
                let _data = col.data.read();
                // Since we don't know the index anymore (removed from snapshot), 
                // we'd normally trust the snapshot. But for this demo, let's just 
                // check if the waitlist was populated.
                println!("  - Column waitlist size: {}", col.waitlist.read().len());
            }
        }
    }

    println!("\n--- Step 3: Recycling Index ---");
    let mut attrs2 = AHashMap::new();
    attrs2.insert("Country".to_string(), "Taiwan".to_string());
    writer_tx.send(WriteCommand::Insert {
        entity_id: 2,
        attributes: attrs2,
        attributes_int: AHashMap::new(),
    }).unwrap();

    thread::sleep(Duration::from_millis(100));
    
    if let Some(route) = db.get_route("Food.Apple") {
        if let Some(col) = route.get_column_str("Country") {
            println!("  - Column waitlist size after re-insert: {}", col.waitlist.read().len());
            println!("  - Column data size: {}", col.data.read().len());
        }
    }
}
