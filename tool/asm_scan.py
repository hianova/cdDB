import os
import sys
import statistics
from pathlib import Path

# 排除不需要掃描的目錄
EXCLUDE_DIRS = {'target', '.git', 'node_modules'}

def count_real_loc(filepath):
    """計算真實代碼行數（扣除空白行與單行註解）"""
    loc = 0
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                stripped = line.strip()
                # 忽略空白行和以 // 開頭的註解行
                if stripped and not stripped.startswith('//'):
                    loc += 1
    except Exception as e:
        print(f"無法讀取 {filepath}: {e}")
    return loc

def get_rust_files(root_dir):
    """遞迴找出所有 .rs 檔案"""
    rust_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # 過濾掉排除的目錄
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]

        for f in filenames:
            if f.endswith('.rs'):
                rust_files.append(os.path.join(dirpath, f))
    return rust_files

def main():
    # 如果有傳入路徑參數就用傳入的，否則預設掃描當前目錄
    target_dir = sys.argv[1] if len(sys.argv) > 1 else "."

    print(f"🔍 開始掃描目錄: {os.path.abspath(target_dir)}")
    rust_files = get_rust_files(target_dir)

    if not rust_files:
        print("❌ 找不到任何 .rs 檔案！")
        return

    # 計算每個檔案的 LOC
    file_stats = []
    for path in rust_files:
        loc = count_real_loc(path)
        file_stats.append({'path': path, 'loc': loc})

    # 依照 LOC 由小到大排序
    file_stats.sort(key=lambda x: x['loc'])

    # 提取所有 LOC 數值來算統計數據
    loc_values = [x['loc'] for x in file_stats]
    median_loc = statistics.median(loc_values)
    mean_loc = statistics.mean(loc_values)

    print("\n📊 專案宏觀數據:")
    print(f"  - 總檔案數: {len(file_stats)} 個")
    print(f"  - 平均行數: {mean_loc:.1f} 行")
    print(f"  - 中位數行數: {median_loc:.1f} 行")
    print("-" * 50)

    # 制定過濾條件：找出「小於中位數」且「小於 100 行」的碎片檔案
    # 如果中位數很小（例如 40），我們以中位數為準；如果中位數很大，我們以 100 行為觀察閾值
    threshold = min(median_loc, 100)

    candidates = [f for f in file_stats if f['loc'] <= threshold]

    if candidates:
        print(f"🚨 發現 {len(candidates)} 個極小部件 (LOC <= {threshold:.0f})，強烈建議合併：\n")
        for f in candidates:
            # 這裡可以過濾掉像是 mod.rs 這種本來就用來做路由的檔案
            filename = os.path.basename(f['path'])
            if filename == 'mod.rs' or filename == 'lib.rs' or filename == 'main.rs':
                print(f"  [略過路由/入口] {f['path']} ({f['loc']} 行)")
            else:
                print(f"  👉 [建議合併] {f['path']} ({f['loc']} 行)")
    else:
        print("✅ 您的專案很健康，沒有發現過度解耦的碎片檔案！")

    print("-" * 50)

if __name__ == "__main__":
    main()
