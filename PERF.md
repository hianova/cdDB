# Performance Guide - cdDB

## Benchmark Results

```text
running 87 tests
test bloom::tests::test_bloom_filter ... ignored
test column::tests::test_column_array_data_len ... ignored
test column::tests::test_column_array_default ... ignored
test column::tests::test_column_array_double_lock_panics - should panic ... ignored
test column::tests::test_column_array_get_data_snapshot ... ignored
test column::tests::test_column_array_get_element_pinned ... ignored
test column::tests::test_column_array_get_element_with_worker ... ignored
test column::tests::test_column_array_get_waitlist_snapshot ... ignored
test column::tests::test_column_array_insertion ... ignored
test column::tests::test_column_array_with_data ... ignored
test column::tests::test_column_array_with_data_pinned ... ignored
test column::tests::test_column_array_with_element ... ignored
test column::tests::test_column_array_with_element_pinned ... ignored
test column::tests::test_column_data_basics ... ignored
test column::tests::test_column_data_default ... ignored
test column::tests::test_column_data_iter_empty ... ignored
test column::tests::test_column_data_iter_skips_invalid ... ignored
test column::tests::test_column_data_set ... ignored
test column::tests::test_column_data_set_valid ... ignored
test column::tests::test_columns_new_and_default ... ignored
test commands::tests::test_attributes ... ignored
test commands::tests::test_batch_and_fast_insert ... ignored
test commands::tests::test_decode_invalid ... ignored
test commands::tests::test_it_ops ... ignored
test commands::tests::test_partition_command_debug ... ignored
test commands::tests::test_write_command_encode_decode ... ignored
test commands::tests::test_write_command_extra_variants ... ignored
test commands::tests::test_write_command_insert_helper ... ignored
test dispatcher::tests::test_dispatcher_register_with_budget ... ignored
test dispatcher::tests::test_route_getters_and_execute ... ignored
test dispatcher::tests::test_user_writer_send_backoff ... ignored
test dispatcher::tests::test_user_writer_try_send_full_and_drop ... ignored
test partition::tests::test_partition_apply_commands ... ignored
test platform::tests::test_backoff ... ignored
test platform::tests::test_filesystem_default_impls ... ignored
test platform::tests::test_std_executor_and_queue ... ignored
test platform::tests::test_std_file_system ... ignored
test platform::tests::test_std_filesystem_errors ... ignored
test platform::tests::test_std_message_sender_backoff ... ignored
test query::tests::test_aggregate_op_debug ... ignored
test query::tests::test_bump_allocator ... ignored
test query::tests::test_bump_allocator_multiple ... ignored
test query::tests::test_cddb_query_struct ... ignored
test query::tests::test_query_execute_batch_multiple_nodes ... ignored
test query::tests::test_query_execute_range_none ... ignored
test query::tests::test_query_execute_range_success ... ignored
test query::tests::test_query_link_blob ... ignored
test query::tests::test_query_node_debug ... ignored
test query::tests::test_query_result_debug ... ignored
test query::tests::test_query_seed_bloom_filter ... ignored
test query::tests::test_query_session_entities_iter ... ignored
test query::tests::test_query_session_execute_aggregate_avg ... ignored
test query::tests::test_query_session_execute_aggregate_count ... ignored
test query::tests::test_query_session_execute_aggregate_empty_avg ... ignored
test query::tests::test_query_session_execute_aggregate_min_max ... ignored
test query::tests::test_query_session_execute_aggregate_nonexistent ... ignored
test query::tests::test_query_session_execute_aggregate_sum ... ignored
test query::tests::test_query_session_execute_get_blob_via_get_node ... ignored
test query::tests::test_query_session_execute_get_int ... ignored
test query::tests::test_query_session_execute_get_none ... ignored
test query::tests::test_query_session_execute_get_str_via_get_node ... ignored
test query::tests::test_query_session_execute_link ... ignored
test query::tests::test_query_session_execute_link_none ... ignored
test query::tests::test_query_session_execute_scan ... ignored
test query::tests::test_query_session_execute_scan_blob ... ignored
test query::tests::test_query_session_execute_scan_nonexistent ... ignored
test query::tests::test_query_session_execute_scan_str ... ignored
test query::tests::test_query_session_get_blob ... ignored
test query::tests::test_query_session_get_int ... ignored
test query::tests::test_query_session_get_signed_record ... ignored
test query::tests::test_query_session_get_str ... ignored
test query::tests::test_query_session_with_blob ... ignored
test query::tests::test_query_session_with_str ... ignored
test query::tests::test_query_sum_int_range ... ignored
test query::tests::test_unsafe_transmute_lifetime ... ignored
test storage::tests::test_entity_data_encode_decode ... ignored
test storage::tests::test_storage_fallback_write ... ignored
test storage::tests::test_storage_flush_none ... ignored
test storage::tests::test_storage_read_write ... ignored
test storage::tests::test_storage_rebuild_and_corrupt ... ignored
test sync::map::tests::test_ahashmap_all_apis ... ignored
test sync::map::tests::test_ahashmap_deleted_buckets ... ignored
test wal::tests::test_noop_wal ... ignored
test wal::tests::test_std_wal_async ... ignored
test wal::tests::test_std_wal_custom_relaxed ... ignored
test wal::tests::test_std_wal_fallback ... ignored
test wal::tests::test_std_wal_sync ... ignored

test result: ok. 0 passed; 0 failed; 87 ignored; 0 measured; 0 filtered out; finished in 0.00s

Efficiency Index (Throughput/Resource)/u32 Scan Efficiency
                        time:   [34.849 µs 37.779 µs 41.855 µs]
                        thrpt:  [4.4502 GiB/s 4.9304 GiB/s 5.3449 GiB/s]
                 change:
                        time:   [+2.3720% +6.1242% +10.394%] (p = 0.00 < 0.05)
                        thrpt:  [-9.4151% -5.7707% -2.3171%]
                        Performance has regressed.
Found 15 outliers among 100 measurements (15.00%)
  2 (2.00%) high mild
  13 (13.00%) high severe

Access Latency/Hot Path Get Int (Wait-Free RCU)
                        time:   [71.916 ns 79.057 ns 88.278 ns]
                        change: [+176.90% +212.75% +251.03%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 2 outliers among 100 measurements (2.00%)
  1 (1.00%) high mild
  1 (1.00%) high severe
Access Latency/Bloom Filter Miss
                        time:   [26.382 ns 28.788 ns 32.182 ns]
                        change: [+230.92% +256.30% +288.86%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe

Memory Ops/ColumnArray String Allocation (1000 items)
                        time:   [35.282 µs 36.268 µs 37.862 µs]
                        change: [-4.4868% -1.6097% +1.3346%] (p = 0.33 > 0.05)
                        No change in performance detected.
Found 7 outliers among 100 measurements (7.00%)
  4 (4.00%) high mild
  3 (3.00%) high severe

cdDB vs SQLite/cdDB Async WAL TrySend (Wait-Free Enqueue)
                        time:   [100.03 ns 110.53 ns 125.95 ns]
                        change: [-42.916% -34.161% -23.053%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 9 outliers among 100 measurements (9.00%)
  6 (6.00%) high mild
  3 (3.00%) high severe
cdDB vs SQLite/SQLite In-Memory Write (Prepared Stmt)
                        time:   [850.61 ns 855.98 ns 862.61 ns]
                        change: [-6.7975% -1.1650% +5.2249%] (p = 0.73 > 0.05)
                        No change in performance detected.
Found 10 outliers among 100 measurements (10.00%)
  3 (3.00%) low mild
  4 (4.00%) high mild
  3 (3.00%) high severe
cdDB vs SQLite/SQLite On-Disk Write (Prepared Stmt)
                        time:   [281.29 µs 288.82 µs 298.76 µs]
                        change: [-29.403% -25.806% -22.169%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 4 outliers among 100 measurements (4.00%)
  1 (1.00%) high mild
  3 (3.00%) high severe
cdDB vs SQLite/cdDB Point Query (Wait-Free RCU)
                        time:   [48.414 ns 49.704 ns 51.486 ns]
                        change: [-82.228% -78.852% -74.649%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 4 outliers among 100 measurements (4.00%)
  1 (1.00%) high mild
  3 (3.00%) high severe
cdDB vs SQLite/SQLite In-Memory Point Query
                        time:   [315.45 ns 316.65 ns 317.96 ns]
                        change: [-23.631% -20.697% -17.736%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 6 outliers among 100 measurements (6.00%)
  4 (4.00%) high mild
  2 (2.00%) high severe
cdDB vs SQLite/SQLite On-Disk Point Query
                        time:   [5.1347 µs 5.2480 µs 5.4011 µs]
                        change: [-24.557% -22.210% -19.579%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild
  2 (2.00%) high severe
cdDB vs SQLite/cdDB Columnar Scan Sum Range (100 elements)
                        time:   [3.8614 µs 4.0692 µs 4.3240 µs]
                        change: [-1.3166% +4.3673% +10.395%] (p = 0.14 > 0.05)
                        No change in performance detected.
Found 28 outliers among 100 measurements (28.00%)
  22 (22.00%) low severe
  3 (3.00%) high mild
  3 (3.00%) high severe
cdDB vs SQLite/SQLite In-Memory Scan Sum Range (100 elements)
                        time:   [3.2267 µs 3.2400 µs 3.2537 µs]
                        change: [-57.901% -53.163% -47.469%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 4 outliers among 100 measurements (4.00%)
  3 (3.00%) low mild
  1 (1.00%) high mild
cdDB vs SQLite/SQLite On-Disk Scan Sum Range (100 elements)
                        time:   [7.9394 µs 8.0486 µs 8.2518 µs]
                        change: [-8.2905% -7.5295% -6.4309%] (p = 0.00 < 0.05)
                        Performance has improved.
Found 8 outliers among 100 measurements (8.00%)
  3 (3.00%) low mild
  4 (4.00%) high mild
  1 (1.00%) high severe

Read Throughput/Single Thread Get Int
                        time:   [134.91 ns 136.40 ns 138.68 ns]
                        thrpt:  [7.2108 Melem/s 7.3316 Melem/s 7.4122 Melem/s]
                 change:
                        time:   [+45.234% +52.199% +64.214%] (p = 0.00 < 0.05)
                        thrpt:  [-39.104% -34.297% -31.146%]
                        Performance has regressed.
Found 13 outliers among 100 measurements (13.00%)
  5 (5.00%) low severe
  1 (1.00%) low mild
  3 (3.00%) high mild
  4 (4.00%) high severe
Read Throughput/Multi-Thread (4 Readers) Stress
                        time:   [254.68 ns 257.43 ns 260.31 ns]
                        thrpt:  [15.366 Melem/s 15.538 Melem/s 15.706 Melem/s]
                 change:
                        time:   [+37.979% +40.112% +42.222%] (p = 0.00 < 0.05)
                        thrpt:  [-29.688% -28.629% -27.525%]
                        Performance has regressed.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild
Read Throughput/Multi-Thread (4 Readers) Columnar Read
                        time:   [2.1869 ns 2.2005 ns 2.2149 ns]
                        thrpt:  [1.8060 Gelem/s 1.8178 Gelem/s 1.8291 Gelem/s]
                 change:
                        time:   [-13.896% -11.127% -8.5473%] (p = 0.00 < 0.05)
                        thrpt:  [+9.3462% +12.520% +16.139%]
                        Performance has improved.
Found 8 outliers among 100 measurements (8.00%)
  4 (4.00%) high mild
  4 (4.00%) high severe

Write Throughput/Batch Insert (1000 items)
                        time:   [1.3507 ms 1.3910 ms 1.4315 ms]
                        thrpt:  [698.58 Kelem/s 718.91 Kelem/s 740.38 Kelem/s]
                 change:
                        time:   [+39.382% +45.621% +52.172%] (p = 0.00 < 0.05)
                        thrpt:  [-34.285% -31.329% -28.255%]
                        Performance has regressed.
```
