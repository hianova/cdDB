# cdDB 技術規格書 (Technical Specification)

## 1. 核心設計原則 (Design Principles)

cdDB 旨在打造一個極致效能的「全記憶體加速層」，同時具備處理海量冷資料的能力。其核心設計環繞以下四點：

*   **資料模型 (Columnar / DOD)**：採用行式儲存 (Column-oriented)，將同類別、同屬性的資料儲存在連續的記憶體陣列 (`Vec<Option<T>>`) 中。這能最大化 CPU L1/L2 Cache 命中率，並支援極速的範圍掃描。
*   **併發模型 (Single-Writer + Custom QSBR)**：
    *   **寫入**：採用 Single-Writer 模式，由單一執行緒負責。透過 **Group Commit (微批次)** 優化，將多個寫入指令合併為單次 RCU Swap 與 WAL 寫入。
    *   **讀取**：基於自定義 **QSBR (Quiescent State Based Reclamation)** 實現 **Wait-Free** 讀取。讀取端絕對零打工，延遲低至 200ns。
*   **分層儲存 (Tiered Storage 2.0)**：
    *   整合 **DualCache-FF (v0.1.0)** 引擎，實現 O(1) 的超高速熱度追蹤。
    *   **I/O 硬化 (Storage Hardening)**：
        *   **Page Cache & Block Pre-fetching**：磁碟讀取時自動預取下一個 Block (Double Fetching)，解決 I/O 放大並隱藏磁碟延遲。
        *   **Async I/O**：使用非同步讀取避免 Worker 執行緒阻塞。
        *   **Dynamic Bloom Filter**：動態擴縮容的布隆過濾器。當飽和度達到 70% 時自動翻倍並從磁碟重建，有效防止快照穿透。

---

## 2. 系統架構與模組 (System Architecture)

cdDB 已完成模組化拆分，以提升可維護性：

- **`column.rs`**: 核心列式儲存資料結構 (`Columns`, `ColumnArray`)。
- **`commands.rs`**: 內部指令與寫入協議 (`WriteCommand`, `PartitionCommand`)。
- **`query.rs`**: 非同步查詢引擎與多維指標跳轉邏輯。
- **`partition.rs`**: 分區 Actor 核心，負責 RCU 狀態維護與 RCU Swap。
- **`dispatcher.rs`**: 全域分發器與路由管理。
- **`ops.rs`**: **IT 運營資訊處理介面**。提供結構化的監控數據與日誌注入介面。
- **`unsafe_core.rs`**: **安全性封裝層**。所有 `unsafe` 操作集中於此，對外提供安全封裝 API。

### 2.1 核心資料結構

#### ColumnArray
```rust
pub struct ColumnArray<T> {
    pub data: AtomicPtr<Vec<Option<T>>>,    // 核心資料陣列
    pub waitlist: AtomicPtr<Vec<usize>>,    // Tombstone 空間回收
    pub(crate) write_guard: AtomicBool,      // 單寫入者安全檢查
}
```

#### Partition
```rust
pub struct Partition {
    pub columns: Arc<AtomicPtr<Columns>>,
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    pub storage: Arc<AsyncStorage>,
    pub hot_index: DualCacheFF<usize, ()>,
    pub bloom_filter: Arc<Mutex<BloomFilter>>,
    pub wal_file: Option<File>,
}
```

---

## 3. 關鍵流程分析 (Key Workflows)

### 3.1 寫入優化：Batch WAL & Group Commit
1.  **指令收集**：Partition 執行緒批次獲取 Channel 中排隊的所有指令 (一次最多 1000 條)。
2.  **批次 WAL**：將所有寫入指令序列化後合併為單次 `write_all` 調用，最後調用單次 `flush()`，顯著降低系統調用開銷。
3.  **單次 RCU Swap**：在複製快照並套用批次變更後，執行一次 `swap_ptr`，並由 `QsbrManager` 延後回收舊指標。

### 3.2 讀取路徑：Wait-Free + Pre-fetching
1.  **Bloom Check**：快速過濾不存在的實體。
2.  **Memory Index**：透過 Wait-Free RCU 指標快速查找。若命中熱資料，延遲 < 300ns。
3.  **Page Fault & Pre-fetch**：若未命中，觸發非同步磁碟讀取。載入當前實體時，自動預取下一個 32 實體大小的 Block，優化連續掃描效能。

### 3.3 動態布隆過濾器重建
*   當 `bloom_count > (bits * 0.7)` 時，系統會自動啟動重建流程。
*   容量翻倍 (`bits *= 2`)，並掃描資料目錄下的所有實體文件重新填入。

---

## 4. 安全性與封裝 (Safety & Encapsulation)

cdDB 遵循 **Edition 2024** 的嚴格安全規範：
*   **Unsafe Archive**：所有底層指標操作、RCU 載入與釋放邏輯均歸檔於 `unsafe_core.rs`。
*   **Safe Wrappers**：提供 `load_ref` 與 `load_clone` 等安全封裝，確保上層邏輯 (Partition, Query) 無須編寫 `unsafe` 代碼。
*   **Encapsulated GC**：`GarbageEntry` 內部實作 `Drop` 封裝 `Box::from_raw`，確保垃圾回收過程對管理層透明且安全。

---

## 5. 效能指標總結 (Performance Benchmarks)

*   **讀取輸送量 (Throughput)**：~2.5 Million QPS (8 Reader Threads)
*   **冷資料提升效能**：~28x (Disk to Memory Promotion benefit)
*   **列式掃描優勢**：~17x (Columnar vs Traditional Struct Scan)
*   **尾部延遲穩定性**：P99 延遲穩定，證明 Wait-Free 架構在 Batch 寫入壓力下依然保持極速響應。

---

## 6. IT 運營資訊處理 (IT Operations Processing)

cdDB 專為 IT 運營監控設計了結構化介面：
- **`ITOpsRecord`**: 支援自動化的 CPU/Memory 佔用率縮放存儲 (Scaled u32)。
- **時序屬性**: 自動映射時間戳記與服務節點資訊。
- **高併發注入**: 透過 `ITOpsIngest` 擴展，實現秒級百萬級數據點的無鎖注入。