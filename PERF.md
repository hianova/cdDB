# Performance Guide - cdDB

## Benchmark Results

```text
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
