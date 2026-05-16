# cdDB 技術規格書 (Technical Specification)

## 1. 核心設計原則 (Design Principles)

cdDB 旨在打造一個極致效能的「全記憶體加速層」，同時具備處理海量冷資料的能力。其核心設計環繞以下四點：

*   **資料模型 (Columnar / DOD)**：採用行式儲存 (Column-oriented)，將同屬性的資料儲存在連續的記憶體陣列中。這能最大化 CPU Cache 命中率，並支援極速的向量化範圍掃描。
*   **併發模型 (Synchronous Wait-Free)**：
    *   **寫入**：採用 Single-Writer 模式，由專屬的原生執行緒負責。透過 **Group Commit** 優化，將多個寫入指令合併為單次 RCU Swap 與 WAL 寫入。
    *   **讀取**：基於自定義 **QSBR (Quiescent State Based Reclamation)** 實現 **Wait-Free** 讀取。移除了 Asynchronous Runtime 的排程開銷，讀取延遲低至 **115ns**。
*   **分層儲存 (Tiered Storage 2.0)**：
    *   整合 **DualCache-FF (v0.1.0)** 引擎，實現 O(1) 的超高速熱度追蹤。
    *   **I/O 優化 (Storage Hardening)**：
        *   **Synchronous I/O**：對於冷資料採用同步阻塞 I/O，在大規模掃描場景下比非同步 I/O 具有更穩定的吞吐量與更低的狀態機開銷。
        *   **Block Pre-fetching**：讀取時自動預取下一個 Block，隱藏磁碟延遲。
        *   **Dynamic Bloom Filter**：動態擴縮容。當飽和度達 70% 時自動翻倍並重建，有效防止快照穿透。

*   **NoStd 架構 (Embedded Ready)**：全面解耦 `std` 依賴。透過 `platform.rs` 提供的抽象介面，cdDB 可以在無作業系統的嵌入式環境或自定義核心中運行。
*   **平台抽象層 (Platform Abstraction Layer)**：定義了 `FileSystem`、`ThreadManager` 與 `MessageQueue` Trait，將 I/O 與執行緒管理從具體系統實現中分離。

---

## 2. 系統架構與模組 (System Architecture)

專案採用 Workspace 結構解耦核心、測試與跑分：

- **`src/` (Core Library)**:
    - **`column.rs`**: 核心列式儲存資料結構。
    - **`query.rs`**: 同步查詢引擎與多維指標跳轉邏輯。
    - **`storage.rs`**: 同步磁碟儲存層與 Payload 編碼。
    - **`unsafe_core.rs`**: 安全性封裝層，管理 RCU 指標操作。
- **`tests/`**: 專注於邊界測試與功能驗證。
- **`benches/`**: 專注於效能指標審計。

### 2.1 核心資料結構

#### ColumnArray
```rust
pub struct ColumnArray<T> {
    pub data: AtomicPtr<Vec<Option<T>>>,    // 核心資料陣列
    pub waitlist: AtomicPtr<Vec<usize>>,    // 空間回收
    pub(crate) write_guard: AtomicBool,      // 單寫入者鎖
}
```

---

## 3. 關鍵流程分析 (Key Workflows)

### 3.1 寫入優化：Group Commit
1.  **指令收集**：Worker 執行緒批次獲取 `crossbeam-channel` 中的所有指令。
2.  **批次 WAL**：合併為單次系統調用，顯著降低 I/O 放大。
3.  **單次 RCU Swap**：在複製快照並套用變更後執行一次指標交換。

### 3.2 讀取路徑：Wait-Free + Promotion
1.  **Bloom Check**：快速過濾不存在的實體。
2.  **Memory Index**：透過 Wait-Free RCU 指標快速查找。延遲 ~115ns。
3.  **Disk Load & Promotion**：若未命中記憶體，觸發磁碟讀取並透過 `DualCache-FF` 評估是否提升至熱快取。

---

## 4. 安全性與封裝 (Safety & Encapsulation)

*   **Unsafe Archive**：所有指標操作與原子邏輯歸檔於 `unsafe_core.rs`。
*   **Safe Wrappers**：外部模組僅能透過安全封裝存取數據，確保符合 Rust 安全哲學。

---

## 5. 效能指標總結 (Performance Benchmarks)

*   **讀取輸送量 (Throughput)**：**~32 Million QPS** (4 Reader Threads)
*   **冷資料提升效能**：**~330x** (Disk to Memory Promotion)
*   **查詢 API 開銷**：與 AHashMap 相比僅 **1.45x** 額外開銷。
*   **尾部延遲**：P99 與 P50 極度接近，證明了無鎖架構的穩定性。

---

## 6. IT 運營資訊處理 (IT Operations Processing)

cdDB 專為 IT 監控設計：
- **`ITOpsRecord`**: 支援 CPU/Memory 佔用率的縮放存儲 (Scaled u32)。
- **時序屬性**: 自動映射時間戳記與節點資訊。