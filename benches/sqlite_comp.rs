use cdDB::{CdDBDispatcher, WriteCommand, Query, Attributes};
use criterion::{criterion_group, criterion_main, Criterion, black_box};
use rusqlite::Connection;
use std::thread;
use std::time::Duration;

fn sqlite_comp_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("cdDB vs SQLite");

    // ==========================================================
    // Setup cdDB
    // ==========================================================
    let cddb_temp_dir = tempfile::tempdir().unwrap();
    let cddb_path = cddb_temp_dir.path().to_path_buf();
    let mut db: CdDBDispatcher<1024> = CdDBDispatcher::new_std(Some(cddb_path.to_string_lossy().into_owned()));
    let tx = db.register_partition("bench.sqlite".to_string());
    
    // ==========================================================
    // Setup SQLite (On-disk and In-memory)
    // ==========================================================
    let sqlite_temp_dir = tempfile::tempdir().unwrap();
    let sqlite_db_path = sqlite_temp_dir.path().join("sqlite_disk.db");
    
    let sqlite_disk = Connection::open(&sqlite_db_path).unwrap();
    sqlite_disk.execute("CREATE TABLE kv (id INTEGER PRIMARY KEY, value INTEGER)", []).unwrap();
    
    let sqlite_mem = Connection::open_in_memory().unwrap();
    sqlite_mem.execute("CREATE TABLE kv (id INTEGER PRIMARY KEY, value INTEGER)", []).unwrap();

    // Prepare prepared statements
    let mut stmt_disk_insert = sqlite_disk.prepare("INSERT OR REPLACE INTO kv (id, value) VALUES (?, ?)").unwrap();
    let mut stmt_mem_insert = sqlite_mem.prepare("INSERT OR REPLACE INTO kv (id, value) VALUES (?, ?)").unwrap();

    // ==========================================================
    // 1. Write Benchmark
    // ==========================================================
    let mut insert_id = 0;
    let mut pre_allocated_attrs = Attributes::new();
    pre_allocated_attrs.insert("val".to_string(), 1);
    
    group.bench_function("cdDB Async WAL TrySend (Wait-Free Enqueue)", |b| {
        b.iter(|| {
            let cmd = WriteCommand::Insert {
                entity_id: black_box(insert_id),
                attributes: Attributes::new(),
                attributes_int: pre_allocated_attrs.clone(),
                attributes_blob: Attributes::new(),
            };
            // Measure pure wait-free enqueue overhead (fails instantly if queue is full, avoiding background thread blocking)
            let _ = tx.try_send(cmd);
            insert_id += 1;
        });
    });

    group.bench_function("SQLite In-Memory Write (Prepared Stmt)", |b| {
        let mut id = 0;
        b.iter(|| {
            stmt_mem_insert.execute(black_box((id, id))).unwrap();
            id += 1;
        });
    });

    group.bench_function("SQLite On-Disk Write (Prepared Stmt)", |b| {
        let mut id = 0;
        b.iter(|| {
            stmt_disk_insert.execute(black_box((id, id))).unwrap();
            id += 1;
        });
    });

    // Populate data for query benchmarks
    let populate_count = 1000;
    
    // Populate cdDB
    let mut batch = Vec::with_capacity(populate_count);
    for i in 0..populate_count {
        let mut attrs_int = Attributes::new();
        attrs_int.insert("val".to_string(), i as u32);
        batch.push((i, Attributes::new(), attrs_int, Attributes::new()));
    }
    tx.send(WriteCommand::BatchInsert(batch)).unwrap();
    
    let route = db.get_route("bench.sqlite").unwrap();
    let worker = route.register_worker();
    while route.len(&worker) < populate_count {
        thread::sleep(Duration::from_millis(5));
    }
    let cddb_query = Query::new(&route);

    // Populate SQLite (Mem & Disk)
    sqlite_mem.execute("BEGIN TRANSACTION", []).unwrap();
    for i in 0..populate_count {
        sqlite_mem.execute("INSERT OR REPLACE INTO kv (id, value) VALUES (?, ?)", [i, i]).unwrap();
    }
    sqlite_mem.execute("COMMIT", []).unwrap();

    sqlite_disk.execute("BEGIN TRANSACTION", []).unwrap();
    for i in 0..populate_count {
        sqlite_disk.execute("INSERT OR REPLACE INTO kv (id, value) VALUES (?, ?)", [i, i]).unwrap();
    }
    sqlite_disk.execute("COMMIT", []).unwrap();

    let mut stmt_mem_select = sqlite_mem.prepare("SELECT value FROM kv WHERE id = ?").unwrap();
    let mut stmt_disk_select = sqlite_disk.prepare("SELECT value FROM kv WHERE id = ?").unwrap();

    // ==========================================================
    // 2. Point Query Benchmark
    // ==========================================================
    let mut rng = rand::thread_rng();
    let mut read_indices = Vec::with_capacity(10_000);
    use rand::Rng;
    for _ in 0..10_000 {
        read_indices.push(rng.gen_range(0..populate_count));
    }

    group.bench_function("cdDB Point Query (Wait-Free RCU)", |b| {
        let mut i = 0;
        b.iter(|| {
            let res = cddb_query.get_int(black_box(read_indices[i % 10_000]), black_box("val"));
            black_box(res);
            i += 1;
        });
    });

    group.bench_function("SQLite In-Memory Point Query", |b| {
        let mut i = 0;
        b.iter(|| {
            let res: Result<i32, _> = stmt_mem_select.query_row(black_box([read_indices[i % 10_000]]), |row| row.get(0));
            black_box(res.ok());
            i += 1;
        });
    });

    group.bench_function("SQLite On-Disk Point Query", |b| {
        let mut i = 0;
        b.iter(|| {
            let res: Result<i32, _> = stmt_disk_select.query_row(black_box([read_indices[i % 10_000]]), |row| row.get(0));
            black_box(res.ok());
            i += 1;
        });
    });

    // ==========================================================
    // 3. Scan Range Sum Benchmark
    // ==========================================================
    group.bench_function("cdDB Columnar Scan Sum Range (100 elements)", |b| {
        let query_engine = Query::new(&route);
        b.iter(|| {
            // Wait-free, we acquire the RCU session once outside the loop
            let session = query_engine.session();
            let start = 100;
            let end = 200;
            let mut sum = 0;
            // Iterate over range using the zero-copy session pointer
            for id in start..end {
                if let Some(val) = session.get_int(id, "val") {
                    sum += val;
                }
            }
            black_box(sum);
        });
    });

    group.bench_function("SQLite In-Memory Scan Sum Range (100 elements)", |b| {
        let mut stmt = sqlite_mem.prepare("SELECT SUM(value) FROM kv WHERE id >= ? AND id < ?").unwrap();
        b.iter(|| {
            let sum: i32 = stmt.query_row(black_box([100, 200]), |row| row.get(0)).unwrap_or(0);
            black_box(sum);
        });
    });

    group.bench_function("SQLite On-Disk Scan Sum Range (100 elements)", |b| {
        let mut stmt = sqlite_disk.prepare("SELECT SUM(value) FROM kv WHERE id >= ? AND id < ?").unwrap();
        b.iter(|| {
            let sum: i32 = stmt.query_row(black_box([100, 200]), |row| row.get(0)).unwrap_or(0);
            black_box(sum);
        });
    });

    group.finish();
}

criterion_group!(benches, sqlite_comp_benchmark);
criterion_main!(benches);
