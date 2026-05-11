### 1. cdDB 核心規格與設計原則 (Specification)

*   **資料模型 (Columnar / DOD)**：捨棄傳統列式 (Row-based) KV，採用完全的行式 (Columnar) 連續陣列儲存。同類別、同屬性的資料儲存在同一個 `Vec<Option<T>>` 中，最大化 CPU Cache 命中率。
*   **併發控制 (Single-Writer + Epoch-based RCU)**：
    *   **寫入**：利用 Hash 路由確保每個「資料分區 (Partition/Category)」在同一時間**只有一個執行緒 (Thread)** 負責寫入，免除複雜的多寫入者鎖競爭。
    *   **讀取**：採用基於 `crossbeam-epoch` 的 Wait-Free RCU 模式。讀取者透過 `Atomic` 指標獲取當前「多向量指針」與「資料陣列」的快照（Snapshot），無鎖且無需等待寫入者；寫入者在背景修改後，透過 Atomic 操作切換指標，並由 Epoch 管理舊記憶體的安全釋放。
*   **空間回收 (Waitlist + Tombstone)**：刪除資料時不挪動陣列，僅標記為 `None`（墓碑），並將該 Index 推入該類別專屬的 `waitlist`。新資料優先從 `waitlist` 拿取 Index 填入，空了才 `append`。
*   **安全防線 (Safety Net)**：底層陣列寫入區強制加上輕量級的 `AtomicBool` 鎖，防範分散式路由層發生 Split-brain（腦裂）導致的非預期多重寫入。

---

### 2. 核心結構定義 (Rust Implementation)

以下是 cdDB 目前採用的高效能資料結構：

```rust
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use ahash::AHashMap; 
use crossbeam::channel::{Sender, Receiver}; 
use crossbeam::epoch::{Atomic, Guard};

/// 1. 最底層的連續資料陣列 (Column / DOD 結構)
/// 使用 Atomic 配合 Epoch 管理，支援 Wait-Free 讀取
pub struct ColumnArray<T> {
    pub data: Atomic<Vec<Option<T>>>,    // 核心連續陣列
    pub waitlist: Atomic<Vec<usize>>,    // 空間回收站
    write_guard: AtomicBool,             // 安全鎖
}

/// 2. 多向量指針快照 (RCU Snapshot)
/// 記錄實體 (Entity) 在各個屬性陣列中的 Index
pub struct MultiVectorPointer {
    pub entity_id: usize,
    pub attribute_indices: AHashMap<String, usize>, 
}

/// 3. 分區/群組 (Partition / Group) - 實踐 Single-Writer 原則
pub struct Partition {
    // 該類別下的所有屬性陣列，支援動態增加屬性
    pub columns_str: Arc<Atomic<AHashMap<String, Arc<ColumnArray<String>>>>>, 
    pub columns_int: Arc<Atomic<AHashMap<String, Arc<ColumnArray<u32>>>>>, 

    // RCU 發布點：最新且有效的實體索引表
    pub shared_pointers: Arc<Atomic<AHashMap<usize, MultiVectorPointer>>>, 

    // 單一寫入者的任務接收通道
    writer_rx: Receiver<WriteCommand>, 
}

/// 4. 路由與快照提供者 (PartitionRoute)
/// 讓 Reader 能夠快速獲取分區的唯讀視圖
pub struct PartitionRoute {
    pub writer_tx: Sender<WriteCommand>,
    pub reader_snapshot_root: Arc<Atomic<AHashMap<usize, MultiVectorPointer>>>,
    pub columns_str: Arc<Atomic<AHashMap<String, Arc<ColumnArray<String>>>>>,
    pub columns_int: Arc<Atomic<AHashMap<String, Arc<ColumnArray<u32>>>>>,
}

/// 寫入指令
pub enum WriteCommand {
    Insert { 
        entity_id: usize, 
        attributes: Attributes<String>,
        attributes_int: Attributes<u32>,
    },
    BatchInsert(Vec<(usize, Attributes<String>, Attributes<u32>)>),
    Delete { entity_id: usize },
}
```

---

### 3. 核心流程 (Workflow)

#### A. 寫入流程 (Insert / Update)
1. **路由**：`CdDBDispatcher` 透過類別名稱找到對應的 `PartitionRoute`。
2. **派發**：指令放入 `writer_tx`。
3. **執行 (Single-Writer Thread)**：
   * Writer 執行緒持有該分區的獨佔寫入權。
   * **安全檢查**：嘗試 `write_guard` 上鎖。
   * **取得 Index**：從 `waitlist` 拿取舊 Index 或從 `data.len()` 取得新 Index。
   * **RCU 更新**：複製一份目前的資料陣列/索引表，修改後透過 `Atomic::swap` 原子替換，並使用 `guard.defer_destroy` 確保舊資料在沒有 Reader 使用時才釋放。

#### B. 讀取流程 (Wait-Free Read)
1. 客戶端從 `PartitionRoute` 取得 `reader_snapshot_root` 的當前 Snapshot。
2. 透過 `entity_id` 查表得到 `MultiVectorPointer`。
3. 根據 `attribute_indices` 直接定位到對應 `ColumnArray` 的特定 Index。
4. **極速訪問**：由於是連續陣列且讀取完全不加鎖 (Wait-Free)，讀取效能接近原生記憶體存取。

#### C. 刪除與空間回收
1. Writer 將目標 Index 的資料設為 `None`。
2. 將該 Index 加入 `waitlist` 以待下次 Insert 複用。
3. 更新 `shared_pointers` 移除該實體。

---

### 4. 設計亮點總結

1. **Wait-Free 讀取**：基於 Crossbeam Epoch，讀取者永遠不會被寫入者阻塞，且不需要昂貴的互斥鎖。
2. **真正的 Columnar**：屬性分開儲存，Scan 特定屬性時具有極高的 Cache Locality。
3. **自動空間管理**：Tombstone + Waitlist 機制實現了高效的空間複用，避免頻繁的記憶體分配。
4. **RCU 安全性**：確保讀取者拿到的是一致性快照，不會讀到修改一半的髒資料。