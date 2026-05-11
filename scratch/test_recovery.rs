use cdDB::{CdDBDispatcher, WriteCommand, Attributes, Query};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use ahash::AHashMap;

fn main() {
    let base_path = PathBuf::from("test_recovery_data");
    if base_path.exists() {
        let _ = std::fs::remove_dir_all(&base_path);
    }

    println!("--- Phase 1: Write data and stop ---");
    {
        let mut db = CdDBDispatcher::new(Some(base_path.clone()));
        let tx = db.register_partition("test.recovery".to_string());
        
        let mut attrs_int = AHashMap::new();
        attrs_int.insert("val".to_string(), 123);
        
        tx.send(WriteCommand::Insert {
            entity_id: 1,
            attributes: AHashMap::new().into(),
            attributes_int: attrs_int.into(),
        }).unwrap();
        
        thread::sleep(Duration::from_millis(200));
        println!("Data written to WAL.");
    } // db dropped, background thread stops

    println!("\n--- Phase 2: Restart and Recover ---");
    {
        let mut db = CdDBDispatcher::new(Some(base_path.clone()));
        // register_partition should automatically replay WAL
        let _tx = db.register_partition("test.recovery".to_string());
        
        // Wait for replay if needed (though it happens synchronously in register_partition)
        if let Some(route) = db.get_route("test.recovery") {
            let query = Query::new(route);
            if let Some(val) = query.get_int(1, "val") {
                println!("Recovered Value: {}", val);
                if val == 123 {
                    println!("SUCCESS: Data recovered correctly!");
                } else {
                    println!("FAILURE: Wrong value recovered.");
                }
            } else {
                println!("FAILURE: Data not found after recovery.");
            }
        }
    }

    let _ = std::fs::remove_dir_all(&base_path);
}
