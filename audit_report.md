# cdDB Test & Benchmark 審計報告

## 摘要

| 類別 | 檔案數 | 問題總數 | 嚴重 | 中等 | 低 |
|------|--------|----------|------|------|-----|
| `tests/` 整合測試 | 6 | 14 | 4 | 7 | 3 |
| `benches/` Criterion 基準 | 5 | 8 | 2 | 4 | 2 |
| `src/` 內嵌單元測試 | 0 | 1 | 1 | — | — |
| **合計** | **11** | **23** | **7** | **11** | **5** |

---

## 一、Tests 整合測試審計

### 1.1 [cold_data_benchmark.rs](file:///Users/kuangtalin/Documents/cdDB/tests/cold_data_benchmark.rs)

> [!WARNING]
> **[嚴重] 硬編碼路徑 + 無清理保證 (L7-10)**
> 使用 `current_dir().join("test_cold_data")` 作為測試路徑，若測試 panic，`remove_dir_all` 永遠不會執行。應改用 `tempfile::tempdir()` 以確保 RAII 清理。

```diff
-let base_path = std::env::current_dir().unwrap().join("test_cold_data");
-if base_path.exists() {
-    let _ = std::fs::remove_dir_all(&base_path);
-}
+let _temp_dir = tempfile::tempdir().unwrap();
+let base_path = _temp_dir.path().to_path_buf();
```

> [!IMPORTANT]
> **[中等] `thread::sleep(1000ms)` 等待持久化 (L33)**
> 使用固定 1 秒 sleep 等待持久化完成，在 CI 或高負載下不可靠。應改用輪詢 `route.len()` 或明確同步機制。

> [!NOTE]
> **[低] 測試名稱誤導**
> 文件名為 `benchmark` 但實際是 `#[test]`，不是 Criterion benchmark。建議重命名為 `cold_data_test.rs` 以避免混淆。

---

### 1.2 [loom_tests.rs](file:///Users/kuangtalin/Documents/cdDB/tests/loom_tests.rs)

> [!WARNING]
> **[嚴重] 記憶體洩漏 — 裸指標未釋放 (L15-25, L31-41)**
> `Box::into_raw(Box::new(WorkerNode {...}))` 創建的裸指標在測試結束時從未被 `Box::from_raw` 回收。在 loom 模型下，這會導致 2 個 `WorkerNode` 洩漏（每次模型執行）。

```diff
 // 驗證後應清理:
 assert_eq!(count, 2);
+
+// 清理：將所有節點回收
+let mut curr = workers.load(loom::sync::atomic::Ordering::Acquire);
+while !curr.is_null() {
+    let next = unsafe { (*curr).next.load(loom::sync::atomic::Ordering::Acquire) };
+    unsafe { drop(Box::from_raw(curr)); }
+    curr = next;
+}
```

> [!IMPORTANT]
> **[中等] 只測試了 QSBR registration，缺少 enter/leave/epoch 進階驗證**
> Loom 測試僅覆蓋 WorkerNode 的 CAS 註冊流程。以下核心並行場景完全未測試：
> - `WorkerState::enter()` / `leave()` 的 epoch 推進
> - RCU pointer swap（`swap_ptr`）在讀者持有快照時的安全性
> - ColumnArray 的 writer race detection（`acquire_lock`/`release_lock`）

> [!NOTE]
> **[低] 測試使用 `#![cfg(feature = "loom")]`，一般 `cargo test` 不會執行**
> 需要 `cargo test --features loom` 才能觸發。README 中未提及此命令。

---

### 1.3 [memory_leak_test.rs](file:///Users/kuangtalin/Documents/cdDB/tests/memory_leak_test.rs)

> [!IMPORTANT]
> **[中等] dhat 斷言不完整 (L66-72)**
> 獲取了 `HeapStats` 但只是打印，沒有任何 `assert!` 驗證。即使有大規模洩漏，測試依然 PASS。

```diff
 let stats = dhat::HeapStats::get();
 println!("Heap Stats: {:?}", stats);
+// 驗證：活躍分配數應少於合理閾值
+assert!(stats.curr_blocks < 50, "Potential leak: {} blocks still alive", stats.curr_blocks);
```

> [!IMPORTANT]
> **[中等] 雙重 tempdir guard (L23-25)**
> 同時使用 `tempfile::tempdir()` 和自定義 `TempDirGuard`，造成冗餘。`_temp_dir` 的 Drop 已經會清理目錄。

> [!NOTE]
> **[低] `#[global_allocator]` 衝突風險**
> 此測試定義 `#[global_allocator] = dhat::Alloc`。與其他測試並行執行時（`cargo test` 預設多線程），若另一個測試也定義全局分配器，會導致編譯失敗。目前安全（各測試是獨立 binary），但需注意。

---

### 1.4 [olap_test.rs](file:///Users/kuangtalin/Documents/cdDB/tests/olap_test.rs)

> [!IMPORTANT]
> **[中等] `execute_batch` callback 中的 `idx` 計數器不正確 (L87-102)**
> `idx` 被宣告為 `let mut idx = 0;` 但 `execute_batch` 的 callback 是 `FnMut`，且 `idx` 在 closure 外部。
> 如果 `execute_batch` 的實現改為不是逐個回調（例如一次返回所有結果），`idx` 的遞增邏輯會失效。
> 更健壯的做法是在 closure 內使用 `Cell<usize>` 或收集所有結果後再驗證。

> [!IMPORTANT]
> **[中等] 缺少字串/Blob 欄位的 Scan 和 Aggregate 測試**
> 只測試了 `u32` 類型的 Scan/Aggregate。`QuerySession::execute_with_cb` 對 `Scan` 有三條路徑（int/str/blob），但只有 int 被覆蓋。

> [!IMPORTANT]
> **[中等] 缺少空資料集和邊界條件測試**
> 未測試：count=0 的聚合行為、不存在的 attr name、entity_id 衝突等。

---

### 1.5 [read_benchmark.rs](file:///Users/kuangtalin/Documents/cdDB/tests/read_benchmark.rs)

> [!WARNING]
> **[嚴重] 名稱不實：宣稱「100,000 Entities」但實際只有 10,000 (L14-16)**
> `println!` 輸出 "100,000 Entities" 但 `count = 10_000`。這會在文檔/日誌中產生誤導性數據。

```diff
-println!("\n=== cdDB Fair Performance Audit (100,000 Entities) ===");
-let count = 10_000;
+println!("\n=== cdDB Fair Performance Audit (10,000 Entities) ===");
+let count = 10_000;
```

> [!IMPORTANT]
> **[中等] 效能比較結論可能反轉 (L118-120)**
> 當 `dur_a` < `dur_b` 時，`dur_b / dur_a` 顯示 "cdDB 快 Nx"。但若 Vec 掃描因編譯器自動向量化而更快，除數可能為 0 或產出 `inf`。應加入 guard。

---

### 1.6 [read_pressure_benchmark.rs](file:///Users/kuangtalin/Documents/cdDB/tests/read_pressure_benchmark.rs)

> [!WARNING]
> **[嚴重] 缺少正確性斷言 (全檔)**
> 整個測試只測量延遲統計，但沒有任何 `assert!` 驗證讀取值的正確性。作為 `#[test]`，即使讀取返回全部錯誤值也會 PASS。

```diff
 query_engine.execute_with_cb(&nodes, |res| {
-    black_box(res);
+    // 至少驗證 Get 結果不為 None
+    match res {
+        cdDB::QueryResult::None => panic!("Unexpected None for existing entity"),
+        other => { black_box(other); }
+    }
 });
```

> [!NOTE]
> **[低] 過長穩定化等待 (L37)**
> `thread::sleep(Duration::from_secs(1))` 在每次測試執行時增加 1 秒固定延遲，但沒有解釋為何需要額外穩定化（資料已通過 `route.len()` 確認載入完成）。

---

## 二、Benches Criterion 基準測試審計

### 2.1 [throughput.rs](file:///Users/kuangtalin/Documents/cdDB/benches/throughput.rs)

> [!WARNING]
> **[嚴重] Write Throughput 基準被完全註釋掉 (L146-164)**
> 寫入吞吐量基準因 OOM 問題被禁用。文件中的 WARNING 註解說明了原因，但沒有替代實現。**這意味著寫入效能完全沒有被追蹤。**

建議使用 `iter_custom` 方案替代：
```rust
group.bench_function("Batch Insert (1000 items)", |b| {
    b.iter_custom(|iters| {
        // 為每次迭代重建 DB 以避免 OOM
        let tmp = tempfile::tempdir().unwrap();
        let mut db = CdDBDispatcher::<1024>::new_std(Some(tmp.path().to_string_lossy().into()));
        let tx = db.register_partition("bench.write".into());
        let start = std::time::Instant::now();
        for _ in 0..iters {
            // ... batch insert logic
        }
        start.elapsed()
    });
});
```

> [!IMPORTANT]
> **[中等] 多線程基準的 Throughput 標記不一致 (L29 vs L45)**
> 單線程設定 `Throughput::Elements(1)`，多線程設定 `Throughput::Elements(4)`。這使得 Criterion 報告的 throughput 數字（如 "21M elem/s"）實際上已經是 4x 放大的值。如果意圖是「4 個線程各做 iters 次操作」，則正確的 throughput 應該是 `Elements(4)` 且 `iter_custom` 返回 wall-clock 時間，但這需要明確記錄。

---

### 2.2 [latancy.rs](file:///Users/kuangtalin/Documents/cdDB/benches/latancy.rs)

> [!NOTE]
> **[低] 檔名拼寫錯誤**
> `latancy.rs` → 應為 `latency.rs`。Cargo.toml 中也需同步修改。

> [!IMPORTANT]
> **[中等] Bloom Filter Miss 測試的 entity_id 單調遞增 (L41-46)**
> `i` 從 `count + 1000` 開始不斷遞增，永遠不會被重用。Bloom filter 的 hash 分佈均勻性未被正確壓測。建議使用隨機 ID。

---

### 2.3 [profiling.rs](file:///Users/kuangtalin/Documents/cdDB/benches/profiling.rs)

> [!WARNING]
> **[嚴重] 不是 Criterion 基準，但 Cargo.toml 設定 `harness = false` (L1-31)**
> 這是一個使用 `dhat` 的 `fn main()` 堆分析程式，不產出任何基準數據。混入 `benches/` 容易與真正的 benchmark 混淆。

> [!IMPORTANT]
> **[中等] 使用了 `WriteCommand::insert()` 但只查詢一個 entity (L26-28)**
> 插入了 10,000 個 entity 但只查詢 `entity_id: 5000`。作為 profiling，應測試更多路徑以獲得代表性分析。

---

### 2.4 [memory.rs](file:///Users/kuangtalin/Documents/cdDB/benches/memory.rs)

> [!IMPORTANT]
> **[中等] 基準完全不使用 cdDB 的任何類型 (L9-17)**
> 這個 "Memory Ops" 基準測量的是 `Vec<Option<String>>` 的分配速度，與 cdDB 的 `ColumnArray` 毫無關係。名稱嚴重誤導。

```rust
// 當前測量的是：
let mut col = Vec::with_capacity(1000);
for i in 0..1000 { col.push(Some(format!("item-{}", i))); }
// 這只是標準庫 Vec + String 分配，不涉及 ColumnArray、AtomicPtr、QSBR 等
```

建議改為測量 cdDB 的實際 `ColumnArray<String, 1024>` 寫入分配行為。

---

### 2.5 [capex.rs](file:///Users/kuangtalin/Documents/cdDB/benches/capex.rs)

> [!NOTE]
> **[低] Throughput 單位不準確 (L32)**
> `Throughput::Bytes(4)` 表示「每次迭代處理 4 bytes」。但 `sum_int_range` 掃描 50,000 個 `u32` = 200,000 bytes。Criterion 報告的 throughput (如 "237 KiB/s") 完全不反映實際吞吐量。

```diff
-group.throughput(Throughput::Bytes(4)); // u32 size
+group.throughput(Throughput::Bytes((count * 4) as u64)); // 總掃描 bytes
```

---

## 三、嚴重缺失：零內嵌單元測試

> [!CAUTION]
> **`src/` 目錄下的 15 個原始碼檔案中，沒有任何 `#[cfg(test)]` 模組或 `#[test]` 函式。**

這意味著以下關鍵模組完全缺乏單元測試：

| 模組 | 行數 | 風險評估 |
|------|------|----------|
| [partition.rs](file:///Users/kuangtalin/Documents/cdDB/src/partition.rs) | 407 | 🔴 核心寫入路徑，Group Commit、RCU swap |
| [query.rs](file:///Users/kuangtalin/Documents/cdDB/src/query.rs) | 429 | 🔴 查詢引擎，含 unsafe transmute (L219, L226) |
| [storage.rs](file:///Users/kuangtalin/Documents/cdDB/src/storage.rs) | ~400 | 🔴 磁碟持久化，序列化/反序列化 |
| [dispatcher.rs](file:///Users/kuangtalin/Documents/cdDB/src/dispatcher.rs) | 407 | 🟡 調度器，多分區路由 |
| [commands.rs](file:///Users/kuangtalin/Documents/cdDB/src/commands.rs) | 297 | 🟡 encode/decode 二進制協議 |
| [wal.rs](file:///Users/kuangtalin/Documents/cdDB/src/wal.rs) | 215 | 🟡 WAL 持久化，含 async 線程 |
| [unsafe_core.rs](file:///Users/kuangtalin/Documents/cdDB/src/unsafe_core.rs) | ~100 | 🔴 所有 unsafe 操作集中處 |
| [column.rs](file:///Users/kuangtalin/Documents/cdDB/src/column.rs) | 227 | 🟡 ColumnData 位圖邏輯 |
| [bloom.rs](file:///Users/kuangtalin/Documents/cdDB/src/bloom.rs) | ~50 | 🟡 Bloom filter |
| [qsbr.rs](file:///Users/kuangtalin/Documents/cdDB/src/qsbr.rs) | ~80 | 🔴 QSBR 記憶體回收 |

---

## 四、已發現的 API 測試覆蓋矩陣

| 公開 API | 測試覆蓋 | 基準覆蓋 |
|----------|----------|----------|
| `CdDBDispatcher::new_std` | ✅ 全部測試 | ✅ 全部基準 |
| `register_partition` | ✅ | ✅ |
| `register_partition_with_wal` | ❌ | ❌ |
| `register_partition_with_budget` | ❌ | ❌ |
| `execute_batch` (sync) | ✅ olap_test | ❌ |
| `execute_batch_async` | ❌ | ❌ |
| `Query::get_int` | ✅ 多個 | ✅ latency/throughput |
| `Query::get_str` | ❌ | ❌ |
| `Query::get_blob` | ❌ | ❌ |
| `QuerySession::with_str` | ❌ | ❌ |
| `QuerySession::with_blob` | ❌ | ❌ |
| `QuerySession::get_signed_record` | ❌ | ❌ |
| `QuerySession::entities_iter` | ❌ | ❌ |
| `Query::execute` | ❌ (且為 `unimplemented!()`) | ❌ |
| `WriteCommand::Insert` (單筆) | ❌ (profiling.rs only) | ❌ |
| `WriteCommand::BatchInsert` | ✅ 全部 | ✅ |
| `WriteCommand::Delete` | ❌ | ❌ |
| `WriteCommand::InsertFast` | ❌ | ❌ |
| `WriteCommand::encode/decode` | ❌ | ❌ |
| `ColumnArray::with_data` | ✅ read_benchmark | ✅ throughput |
| `ColumnArray::get_element_pinned` | ❌ (僅 throughput bench) | ✅ |
| `PartitionRoute::execute_batch` | ❌ | ❌ |
| `Query::sum_int_range` | ❌ | ✅ capex |
| `WalMode::Async100ms` | ❌ | ❌ |
| `StdWal` 生命週期 | ❌ | ❌ |
| Bloom Filter 飽和重建 | ❌ | ❌ |
| `no_std` 建置 | ❌ | ❌ |

**覆蓋率估計：約 25-30% 的公開 API 有至少一個測試路徑。**

---

## 五、優先修復建議

### P0 — 阻塞品質 (立即修復)

1. **為 `cold_data_benchmark.rs` 改用 `tempfile::tempdir()`** — 避免測試殘留和 CI 汙染
2. **為 `read_pressure_benchmark.rs` 加入正確性斷言** — 否則是無效測試
3. **修復 `read_benchmark.rs` 中 "100,000" vs 10,000 的不一致** — 數據誤導
4. **修復 `loom_tests.rs` 中的記憶體洩漏** — 裸指標未回收

### P1 — 高優先 (本迭代)

5. **實現 Write Throughput benchmark** — 寫入效能完全未追蹤
6. **為 `memory_leak_test.rs` 加入 dhat 斷言** — 否則洩漏不可發現
7. **修復 `memory.rs` bench 使其實際測量 cdDB 類型**
8. **加入 `WriteCommand::encode/decode` 往返測試** — 二進制協議正確性
9. **加入 `Delete` 操作的整合測試**

### P2 — 中等優先 (下一迭代)

10. **為 `query.rs` 中的 `unsafe transmute` (L219, L226) 加入安全性測試**
11. **加入字串/Blob 欄位的完整 OLAP 測試**
12. **加入 WAL replay 正確性測試**
13. **加入 `execute_batch_async` 的異步整合測試**
14. **修正 `latancy.rs` → `latency.rs` 拼寫**
15. **加入 `no_std` 建置驗證** (`cargo build --no-default-features`)

### P3 — 低優先 (技術債)

16. **為核心模組加入 `#[cfg(test)]` 單元測試**（優先：`commands.rs`、`column.rs`、`bloom.rs`）
17. **統一測試中的 `TempDirGuard` 使用模式** — 移除冗餘 guard
18. **在 `throughput.rs` 中記錄 `Throughput::Elements` 語義**
19. **修正 `capex.rs` 的 Throughput::Bytes 計算**

---

## 六、代碼品質觀察

### 6.1 `Query::execute()` 是死代碼
[query.rs:L110-114](file:///Users/kuangtalin/Documents/cdDB/src/query.rs#L110-L114) — `unimplemented!()` 且為 public API。呼叫者會 panic。建議標記 `#[deprecated]` 或移除。

### 6.2 `Query::entities()` 永遠返回空 Vec
[query.rs:L412-414](file:///Users/kuangtalin/Documents/cdDB/src/query.rs#L412-L414) — `pub fn entities(&self) -> Vec<usize> { Vec::new() }`。完全沒有實現但暴露為 API。

### 6.3 Bloom Filter `BITS_PER_WORD` 未使用
`src/bloom.rs:6` 中定義了 `BITS_PER_WORD` 常量但從未使用（見 PERF.md 中的編譯警告）。
