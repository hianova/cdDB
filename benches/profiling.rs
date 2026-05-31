use cdDB::{CdDBDispatcher, QueryNode, CdDbQuery, Query, AHashMap};
use cdDB::commands::{WriteCommand, ColumnValue};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    let _profiler = dhat::Profiler::new_heap();

    let _temp_dir = tempfile::tempdir().unwrap();
    let tmp = _temp_dir.path().to_path_buf();
    let mut dispatcher = CdDBDispatcher::<1024>::new_std(Some(tmp.to_string_lossy().into_owned()));
    let writer = dispatcher.register_partition("perf_part".to_string());
    
    if let Some(route) = dispatcher.get_route("perf_part") {
        // Warm up and allocate
        for i in 0..10_000 {
            let mut attrs = AHashMap::default();
            attrs.insert("name".to_string(), ColumnValue::Str(format!("Entity {}", i)));
            attrs.insert("age".to_string(), ColumnValue::Int((20 + (i % 50)) as u32));
            
            writer.send(WriteCommand::insert(i, attrs)).unwrap();
        }
        
        let query = Query::new(&route);
        let q = CdDbQuery {
            nodes: vec![QueryNode::Get { entity_id: 5000, attr: "name" }]
        };
        let _ = query.execute_with_cb(&q.nodes, |_res| {});
    }
}
