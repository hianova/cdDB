# cdDB 技術規格書 (Technical Specification)

## 1. 核心設計原則 (Design Principles)

cdDB 旨在打造一個極致效能的「全記憶體加速層」，同時具備處理海量冷資料的能力。其核心設計環繞以下四點：

*   **資料模型 (Columnar / DOD)**：採用行式儲存 (Column-oriented)，將同類別、同屬性的資料儲存在連續的記憶體陣列 (`Vec<Option<T>>`) 中。這能最大化 CPU L1/L2 Cache 命中率，並支援極速的範圍掃描。
*   **併發模型 (Single-Writer + Custom QSBR)**：
    *   **寫入**：採用 Single-Writer 模式，由單一執行緒負責。透過 **Group Commit (微批次)** 優化，將多個寫入指令合併為單次 RCU Swap 與 WAL 寫入。
    *   **讀取**：基於自定義 **QSBR (Quiescent State Based Reclamation)** 實現 **Wait-Free** 讀取。讀取端絕對零打工，延遲低至 200ns。
*   **分層儲存 (Tiered Storage 2.0)**：
    *   整合 **DualCache-FF** 引擎取代鐘擺驅逐，實現 O(1) 的超高速熱度追蹤。
    *   **I/O 硬化 (Storage Hardening)**：
        *   **Page Cache & Block Fetching**：磁碟讀取以 Block 為單位，解決 I/O 放大問題。
        *   **Async I/O**：使用非同步讀取避免 Worker 執行緒阻塞。
        *   **Bloom Filter**：記憶體過濾無效查詢，防止快照穿透。

---

## 2. 核心資料結構 (Core Data Structures)

### 2.1 基礎列陣列 (ColumnArray)
```rust
pub struct ColumnArray<T> {
    pub data: Atomic<Vec<Option<T>>>,    // 核心資料陣列
    pub waitlist: Atomic<Vec<usize>>,    // Tombstone 空間回收
    pub(crate) write_guard: AtomicBool,  // 單寫入者安全檢查
}
```

### 2.2 查詢引擎 IR (Query Engine IR)
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryNode {
    Get { entity_id: usize, attr: String },                      // 精準取值
    Link { from_id: usize, link_attr: String, target: String },  // 多重指標跳轉
    Range { entity_id: usize, attr: String, len: usize },        // 範圍掃描
}
```

### 2.3 分區與管理 (Partition & Management)
```rust
pub struct Partition {
    pub columns: Arc<AtomicPtr<Columns>>, // 包含 Str/Int 列
    pub hot_index: DualCacheFF<usize, MultiVectorPointer>, // 熱資料索引
    pub bloom_filter: BloomFilter,       // 快照穿透過濾
    pub storage: AsyncStorage,           // 非同步持久層
}
```

---

## 3. 關鍵流程分析 (Key Workflows)

### 3.1 寫入優化：Group Commit (微批次提交)
1.  **收集**：Partition 執行緒使用 `try_iter()` 快速收集 Channel 中排隊的所有指令。
2.  **批次 WAL**：將所有指令一次性序列化並寫入 WAL 文件，調用單次 `flush()`。
3.  **單次 RCU Swap**：在複製快照並套用所有變更後，僅執行一次 `Atomic::swap`，最小化 RCU 頻繁切換帶來的開銷。

### 3.2 讀取路徑：Wait-Free + Bloom Filter
1.  **Bloom Check**：首先通過 Bloom Filter，若不存在則直接返回 None (20ns)。
2.  **QSBR 打卡**：Reader 透過 `WorkerState` 在 `GLOBAL_EPOCH` 註冊。
3.  **Cache Hit**：查詢 `DualCache-FF`。若命中，直接從記憶體獲取 `MultiVectorPointer`。
4.  **Async Load**：若快取未命中，觸發非同步磁碟讀取。Worker 不阻塞，將請求掛起或返回 `Pending`。
5.  **Block Fetch**：載入目標實體時，一併將同 Page 的鄰近資料載入快取。
6.  **QSBR 登出**：讀取結束。

### 3.3 空間回收：Tombstone + Waitlist
*   刪除資料時將 `ColumnArray` 中的索引設為 `None`。
*   將該索引回收至 `waitlist`。
*   下次寫入時優先從 `waitlist` 提取索引重複使用，避免陣列不斷增長。

---

## 4. 效能指標總結 (Performance Benchmarks)

*   **讀取輸送量 (Throughput)**：~2.4 Million QPS (8 Reader Threads)
*   **讀取延遲 (Latency P50)**：~208 ns
*   **冷資料延遲 (Page Fault)**：~1.2 ms (模擬 SSD)
*   **尾部延遲穩定性 (Wait-Free)**：P99 延遲僅為 P50 的 4 倍，證明自定義 QSBR 在高壓下依然穩定。