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
