# cdDB

**cdDB** is an extreme-performance, DOD (Data-Oriented Design) columnar storage engine engineered for modern hardware architectures, emphasizing L1/L2 cache locality and Zero CPU Copy.

> [!WARNING]
> **Alpha Status Notice**
> cdDB is currently in Alpha stage. Many unit and integration tests are marked as `#[ignore]` due to ongoing architectural refactoring of the DOD columnar execution engine. Some features might be unstable or incomplete. Use with caution in production environments.

## Tech Stack
- **DOD Columnar Storage**: Data is laid out in contiguous column arrays rather than arrays of structures (AoS) to maximize cache locality and SIMD vectorization capabilities.
- **Mmap Zero-Copy**: Maps disk files directly into the virtual memory space, bypassing the kernel page cache overhead during hot-path reads.
- **Write-Ahead Log (WAL)**: Includes tunable group-commit (e.g., `Async100ms`) for massive write throughput without blocking foreground execution.
- **QSBR Pointer Tagging**: Utilizes Wait-Free concurrent dispatch combined with Quiescent State Based Reclamation for memory safety without read locks.
- **DualCache-FF Integration**: Seamlessly embeds the world-class `DualCache-FF` to act as a static global cache layer.

## Example

```rust
use cddb::io::storage::Storage;
use cddb::core::column::ColumnArray;

fn main() {
    // 1. Initialize Mmap-backed storage
    let storage = Storage::open("/path/to/cddb/data").unwrap();
    
    // 2. Initialize DOD Column Array for high-speed columnar access
    // This allows vectorized SIMD scans over the exact fields you need
    let mut ages = ColumnArray::<u32>::new("user_ages", &storage);
    let mut active_flags = ColumnArray::<u8>::new("user_active", &storage);
    
    // 3. Batch insertions (appended linearly into Mmap with WAL group-commit)
    ages.push(25);
    active_flags.push(1);
    
    // 4. Ultra-fast columnar scans (highly cache localized)
    let active_users: usize = active_flags.iter()
        .zip(ages.iter())
        .filter(|(active, age)| **active == 1 && **age >= 18)
        .count();
        
    println!("Active adults: {}", active_users);
}
```
