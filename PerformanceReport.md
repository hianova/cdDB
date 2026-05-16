# cdDB 效能測試報告 (Performance Audit Report)

## 1. 測試環境 (Test Environment)

| 項目 | 規格 |
|------|------|
| **硬體** | Mac (Apple Silicon) |
| **軟體** | Rust 2024 Edition, cdDB v0.2.0 |
| **優化狀態** | Release Profile (`-C opt-level=3`) |
| **併發配置** | 4 Reader Threads (Physical Cores) |
| **Benchmark 框架** | Criterion.rs v0.5 |
| **公允性保證** | 所有讀取結果以 `std::hint::black_box()` 包裝，防止 LLVM Dead Code Elimination |
| **記憶體屏障** | Reader: `Ordering::Acquire`；Writer Swap: `Ordering::AcqRel`（已驗證） |

---

## 2. 核心指標測試 (Core Benchmarks)

### 2.1 單執行緒讀取延遲 (Single-Thread Access Latency)

> Benchmark: `latancy` — Criterion 精密量測，每個樣本約 116M 次迭代

| 測試項目 | 中位時間 | 說明 |
|----------|----------|------|
| **Hot Path Get Int (Wait-Free RCU)** | **~44 ns** | 命中記憶體 Index，完整走過 AHashMap + QSBR 路徑 |
| **Bloom Filter Miss** | **~17 ns** | 未命中 Bloom Filter，提前返回，無磁碟 I/O |

> [!NOTE]
> 以上延遲經 `black_box` 保護，為實際 RCU 讀取路徑開銷，非空轉數字。

---

### 2.2 讀取吞吐量 (Read Throughput)

> Benchmark: `throughput` — Criterion 精密量測

| 測試項目 | 中位時間/iter | 吞吐量（元素/秒） | 說明 |
|----------|--------------|-----------------|------|
| **Single Thread Get Int** | ~116 ns/op | **~8.6M QPS** | 單核連續隨機讀取 |
| **Multi-Thread 4 Readers (4000 ops/iter)** | ~249 µs/iter | **~16M QPS** | 4 執行緒並行，每次 iter 共 4000 次讀取 |

> [!NOTE]
> Multi-Thread 測試每個 iter = 4 threads × 1000 reads = 4000 ops，Criterion 以此計算吞吐率，數字公允。

---

### 2.3 複合查詢壓力測試 (Multi-Thread Pressure Benchmark)

> Benchmark: `read_pressure_benchmark` — 手動計時，1,000,000 次複合操作（Get + Link）

| 指標 | 數值 |
|------|------|
| **總操作數** | 1,000,000 |
| **總耗時** | 175.3 ms |
| **複合查詢 QPS** | **5,703,939 QPS** |
| **P50 延遲** | **541 ns** |
| **P99 延遲** | **2,167 ns** |
| **P99.9 延遲** | **2,958 ns** |
| **尾部係數 (P99/P50)** | **4.01x** (Wait-Free 穩定性標誌) |

> [!IMPORTANT]
> 此 benchmark 所有查詢結果以 `black_box(res)` 包裝，確保 LLVM 不會消除查詢路徑。尾部係數 4.01x 遠優於傳統 Mutex-based 系統（通常 >50x），證明 Wait-Free 架構的延遲穩定性。

---

### 2.4 列式掃描效率 (Columnar Scan Efficiency)

> Benchmark: `capex` — 50,000 個 u32 元素的全量加總

| 測試項目 | 中位時間 | 吞吐量 |
|----------|----------|--------|
| **u32 Columnar Sum (50k items)** | **~16.4 µs** | ~234 KiB/s 有效資料帶寬 |

---

### 2.5 寫入效能 (Write Throughput)

> Benchmark: `throughput` — 批次寫入（含 WAL 落盤 + 記憶體索引更新）

| 測試項目 | 中位時間/iter | 吞吐量 |
|----------|--------------|--------|
| **Batch Insert (1000 items)** | ~397 µs | **~2.5M items/s** (中位) |

> [!NOTE]
> 寫入吞吐量受制於單文件同步 WAL 寫入（每個 commit 一次 `append` 系統調用）。實際應用中可透過更大批次（10k+ items）提升寫入吞吐量。

--- 

## 3. 版本演進與里程碑 (Evolution & Milestones)

| 版本 | 架構特點 | 讀取 QPS | 依賴 |
|------|---------|---------|------|
| **v0.1.0** | 基礎架構，tokio/serde/bincode | ~900k | 重依賴 |
| **v0.2.0** | Wait-Free RCU + 零分配 + NoStd | **8.6M (1T) / 16M (4T)** | ahash + dualcache-ff |

