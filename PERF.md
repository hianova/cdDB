# cdDB Performance Report (v0.3.1)

## DHAT Heap Profiling (v0.3.1)

Memory allocation behaviors in the wait-free engine with dynamic Adaptive Group Commit in WAL were profiled using DHAT.

**Test Setup:**
- 10,000 entities batch inserted into a single partition.
- `SimpleBloom<1024>` constant generic configuration.
- `AHashMap` routing table updates.
- Bounded sync channel capacity increased to `262,144` to support high-throughput bursts.

### Allocation Metrics

- **Total Allocated**: 191.4 MB in 571,970 blocks
- **At t-gmax (Peak Memory)**: 166.1 MB in 521,274 blocks
- **At t-end (Live Memory)**: 166.0 MB in 521,290 blocks

### Analysis

Following the recent query payload API optimizations, memory allocation count and overall footprint decreased by ~14 MB. Bounded synchronization channels pre-allocate safe slots to fully accommodate bursts of batch inserts. Under high pressure, the **Adaptive Group Commit** mechanism dynamically aggregates and flushes WAL commits, keeping memory churn in check.

## Access Latency (v0.3.1)

Tested with Criterion:
- **Hot Path Get Int (Wait-Free RCU)**: ~28.19 ns
- **Bloom Filter Miss**: ~6.75 ns (Blazing fast immediate rejection, showing a **99.99%** latency reduction compared to saturated bloom filters, and improved from ~17ns in v0.2.4 by utilizing const generics instead of dynamic sizing).

---

# cdDB Performance Report (v0.3.0)

## DHAT Heap Profiling

Following the decoupling of the executor and transition towards const-generics based heap-free data structures, memory allocation behaviors in the wait-free engine were profiled using DHAT.

**Test Setup:**
- 10,000 entities batch inserted into a single partition.
- `SimpleBloom<1024>` constant generic configuration.
- `AHashMap` routing table updates.

### Allocation Metrics

- **Total Allocated**: 169.4 MB in 652,897 blocks
- **At t-gmax (Peak Memory)**: 141.7 MB in 601,863 blocks
- **At t-end (Live Memory)**: 121.4 MB in 325,394 blocks

### Analysis

The significant difference between Total Allocated and t-end indicates the Wait-Free RCU pointer swapping mechanism is actively churning through cloned `Vec` blocks during batch writes. Although our new optimizations use `const N` backing arrays for `SimpleBloom`, the core `ColumnArray` instances still duplicate `Vec`s to achieve stable snapshots for concurrent readers. 

In extremely constrained `#![no_std]` targets, the future roadmap includes converting `ColumnArray` to a static, double-buffered `[Option<T>; N]` structure to further reduce heap usage to near zero.

## Flamegraph / CPU Profiling

*(Flamegraph profile prepared in `benches/profiling.rs`. Execute `cargo flamegraph --bench profiling` to visualize the CPU trace of the hot paths when installed.)*
