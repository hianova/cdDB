# cdDB 效能測試報告 (Performance Audit Report)

## 1. 測試環境 (Test Environment)
- **硬體**: Mac (Apple Silicon)
- **軟體**: Rust 2024 Edition, cdDB v0.1.0
- **數據量**: 10,000 ~ 100,000 實體 (Entities)

---

## 2. 核心指標測試 (Core Benchmarks)

### 2.1 冷/熱數據切換效能 (Cold vs Hot Data)
此測試模擬數據從磁碟（冷數據）加載至記憶體（熱數據）後的效能提升。
- **冷啟動掃描 (1,000 條)**: 55.06 ms
- **熱啟動掃描 (1,000 條)**: 2.07 ms
- **效能提升**: **26.57x** 🚀

> [!TIP]
> cdDB 的分層存儲引擎能有效識別熱點數據並自動提升至記憶體列式快取。

### 2.2 列式存儲優勢 (DOD vs Traditional Struct)
比較 cdDB 的列式存儲 (Columnar) 與傳統的結構體數組 (Vec<Struct>) 在範圍掃描上的差異。
- **cdDB 列式掃描 (10,000 條)**: 156.29 µs
- **傳統結構體掃描 (10,000 條)**: 2.66 ms
- **效能提升**: **17.06x** 🚀

> [!IMPORTANT]
> 數據導向設計 (DOD) 大幅提升了 CPU L1/L2 快取的命中率，減少了緩存失效產生的延遲。

### 2.3 隨機查詢開銷 (Lookup Overhead)
- **HashMap 查找 (10,000 條)**: 18.45 ms
- **cdDB Query API (10,000 條)**: 80.46 ms
- **差異分析**: cdDB 的查詢介面因包含非同步支持 (Async)、分區路由與安全邊界封裝，其開銷約為原生 HashMap 的 4.36 倍。

---

## 3. OLAP 向量化查詢能力
cdDB 目前已初步具備向量化聚合計算能力 (Vectorized Aggregation)：
- **聚合操作**: Sum, Avg, Min, Max, Count
- **測試結果**: 100% 準確率，支持零拷貝掃描。

---

## 4. 結論 (Conclusion)
cdDB 在**大規模掃描**與**分層存儲**場景下表現優異，特別是 17x 的列式掃描優勢與 26x 的冷熱提升，使其非常適合處理 IoT 監控數據與 IT 運營分析。

---
報告生成時間: 2026-05-15
