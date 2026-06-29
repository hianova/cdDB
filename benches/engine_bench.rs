use crossbeam_utils::thread;
use hdrhistogram::Histogram;
use rand::Rng;
use rand::distributions::Uniform;
use rand::prelude::Distribution;
use rand_distr::Zipf;
use std::sync::{Arc, Barrier};
use std::time::Instant;

use cdDB::core::commands::{ColumnValue, WriteCommand};
use cdDB::io::wal::NoopWal;
use cdDB::{CdDBDispatcher, Query};

const THREAD_COUNT: usize = 4;
const TOTAL_OPS: usize = 100_000;
const OPS_PER_THREAD: usize = TOTAL_OPS / THREAD_COUNT;
const DATASET_SIZE: u64 = 100_000;

#[derive(Clone, Copy)]
enum AccessPattern {
    Uniform,
    Zipf,
    Scan,
}

struct BenchResult {
    throughput: f64,
    p50: u64,
    p90: u64,
    p99: u64,
}

fn run_workload(pattern: AccessPattern, read_ratio_percent: u8) -> BenchResult {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().to_path_buf().to_string_lossy().into_owned();

    let mut db = CdDBDispatcher::<512>::new_std(Some(path.clone()));
    let writer =
        db.register_partition_with_wal_provider("bench_partition".to_string(), Arc::new(NoopWal));
    let route = db.get_route("bench_partition").unwrap();

    let mut all_ops_data = Vec::new();
    for thread_id in 0..THREAD_COUNT {
        let mut rng = rand::thread_rng();
        let uniform = Uniform::new(0, DATASET_SIZE);
        let zipf = Zipf::new(DATASET_SIZE, 0.99).unwrap();
        let mut ops_data = Vec::with_capacity(OPS_PER_THREAD);
        for i in 0..OPS_PER_THREAD {
            let key = match pattern {
                AccessPattern::Uniform => uniform.sample(&mut rng),
                AccessPattern::Zipf => zipf.sample(&mut rng) as u64,
                AccessPattern::Scan => ((i + thread_id * OPS_PER_THREAD) as u64) % DATASET_SIZE,
            };
            let is_read = rng.gen_range(0..100) < read_ratio_percent;
            ops_data.push((key, is_read));
        }
        all_ops_data.push(ops_data);
    }

    let barrier = Arc::new(Barrier::new(THREAD_COUNT));
    let start_time = Instant::now();

    let mut total_ops = 0;
    let mut total_reads = 0;
    let mut merged_hist = Histogram::<u64>::new(3).unwrap();

    thread::scope(|s| {
        let mut handles = vec![];

        for thread_id in 0..THREAD_COUNT {
            let barrier_clone = barrier.clone();
            let ops_data = all_ops_data[thread_id].clone();
            let writer_clone = writer.clone();
            let route_ref = &route;

            handles.push(s.spawn(move |_| {
                let query = Query::new(route_ref);

                let mut hist = Histogram::<u64>::new(3).unwrap();
                let mut reads = 0;
                let mut local_ops = 0;

                barrier_clone.wait();

                for (i, &(key, is_read)) in ops_data.iter().enumerate() {
                    let measure_latency = i % 100 == 0;
                    if is_read {
                        let op_start = if measure_latency {
                            Some(Instant::now())
                        } else {
                            None
                        };
                        reads += 1;
                        let _ = query.get_int(key as usize, "v");
                        if let Some(start) = op_start {
                            let elapsed = start.elapsed().as_nanos() as u64;
                            let _ = hist.record(elapsed);
                        }
                    } else {
                        let mut attrs = cdDB::core::AHashMap::default();
                        attrs.insert("v".to_string(), ColumnValue::Int(key as u32));
                        let cmd = WriteCommand::insert(key as usize, attrs);

                        let op_start = if measure_latency {
                            Some(Instant::now())
                        } else {
                            None
                        };
                        let _ = writer_clone.send(cmd);
                        if let Some(start) = op_start {
                            let elapsed = start.elapsed().as_nanos() as u64;
                            let _ = hist.record(elapsed);
                        }
                    }

                    local_ops += 1;
                }

                (reads, local_ops, hist)
            }));
        }

        for handle in handles {
            let (reads_done, ops, hist) = handle.join().unwrap();
            total_reads += reads_done;
            total_ops += ops;
            let _ = merged_hist.add(hist);
        }
    })
    .unwrap();

    let duration = start_time.elapsed();
    let throughput = (total_ops as f64) / duration.as_secs_f64();

    BenchResult {
        throughput,
        p50: merged_hist.value_at_quantile(0.50),
        p90: merged_hist.value_at_quantile(0.90),
        p99: merged_hist.value_at_quantile(0.99),
    }
}

fn main() {
    // macOS default stack size is too small for 8MB DualCacheFF initialization.
    // Spawn a thread with 32MB stack to run the benchmark.
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            println!("# cdDB High-Performance Benchmark");
            println!("* **Threads**: {}", THREAD_COUNT);
            println!("* **Dataset Size**: {}", DATASET_SIZE);
            println!("* **Operations per test**: {}", TOTAL_OPS);
            println!();

            let configs = vec![
                (AccessPattern::Zipf, 99, "Zipf (99:1)"),
                (AccessPattern::Zipf, 90, "Zipf (90:10)"),
                (AccessPattern::Zipf, 50, "Zipf (50:50)"),
                (AccessPattern::Zipf, 10, "Zipf (10:90)"),
                (AccessPattern::Uniform, 90, "Uniform (90:10)"),
                (AccessPattern::Scan, 90, "Scan (90:10)"),
            ];

            println!(
                "| Pattern | R/W Ratio | Throughput (ops/s) | P50 (ns) | P90 (ns) | P99 (ns) |"
            );
            println!(
                "|---------|-----------|-------------------|----------|----------|----------|"
            );

            for (pattern, read_ratio, name) in configs {
                let result = run_workload(pattern, read_ratio);
                println!(
                    "| {:<15} | {:>2}:{:>2} | {:>17.0} | {:>8} | {:>8} | {:>8} |",
                    name,
                    read_ratio,
                    100 - read_ratio,
                    result.throughput,
                    result.p50,
                    result.p90,
                    result.p99
                );
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
