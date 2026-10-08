import os
import time
import numpy as np
import pandas as pd

# ---------------------------------------------------------
# 0. Налаштування шляхів та директорій
# ---------------------------------------------------------
DATA_DIR = "lab1_data_storage/data"
os.makedirs(DATA_DIR, exist_ok=True)

csv_path = os.path.join(DATA_DIR, "transactions.csv")
parquet_none_path = os.path.join(DATA_DIR, "transactions_none.parquet")
parquet_snappy_path = os.path.join(DATA_DIR, "transactions_snappy.parquet")
parquet_gzip_path = os.path.join(DATA_DIR, "transactions_gzip.parquet")

# ---------------------------------------------------------
# 1. Генерація тестових даних (1 000 000 рядків)
# ---------------------------------------------------------
print("1. Генерація тестових даних...")
NUM_ROWS = 1_000_000

np.random.seed(42)
data = {
    'transaction_id': [f"TXN_{i:08d}" for i in range(NUM_ROWS)],
    'user_id': np.random.randint(1000, 9999, size=NUM_ROWS),
    'amount': np.round(np.random.uniform(5.0, 5000.0, size=NUM_ROWS), 2),
    'category': np.random.choice(['Groceries', 'Electronics', 'Clothing', 'Travel', 'Entertainment'], size=NUM_ROWS),
    'status': np.random.choice(['COMPLETED', 'PENDING', 'FAILED', 'REFUNDED'], size=NUM_ROWS),
    'timestamp': pd.date_range(start='2025-01-01', periods=NUM_ROWS, freq='s')
}

df = pd.DataFrame(data)

print(f"Збереження у CSV ({NUM_ROWS:,} рядків)...")
df.to_csv(csv_path, index=False)

# ---------------------------------------------------------
# 2. Конвертація в Parquet (різні алгоритми стиснення)
# ---------------------------------------------------------
print("\n2. Конвертація в Parquet з різними алгоритмами стиснення...")
df.to_parquet(parquet_none_path, compression=None, index=False)
df.to_parquet(parquet_snappy_path, compression='snappy', index=False)
df.to_parquet(parquet_gzip_path, compression='gzip', index=False)

# ---------------------------------------------------------
# 3. Порівняння розмірів файлів
# ---------------------------------------------------------
print("\n=== ПОРІВНЯННЯ РОЗМІРІВ ФАЙЛІВ ===")
files = {
    "CSV (Uncompressed)": csv_path,
    "Parquet (None)": parquet_none_path,
    "Parquet (Snappy)": parquet_snappy_path,
    "Parquet (GZIP)": parquet_gzip_path,
}

csv_size = os.path.getsize(csv_path) / (1024 * 1024)

for name, filepath in files.items():
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    compression_ratio = (1 - size_mb / csv_size) * 100
    print(f"{name:<20}: {size_mb:>6.2f} MB (стиснення на {compression_ratio:>5.1f}%)")

# ---------------------------------------------------------
# 4. Вимірювання продуктивності запитів (Benchmarking)
# ---------------------------------------------------------
print("\n=== ПОРІВНЯННЯ ШВИДКОДІЇ ЗЧИТУВАННЯ ТА АГРЕГАЦІЇ ===")

# Опис аналітичних функцій
def query_csv(path):
    # CSV змушений зчитувати текстовий потік
    df_sub = pd.read_csv(path, usecols=['category', 'amount'])
    return df_sub.groupby('category')['amount'].sum()

def query_parquet(path):
    # Parquet використовує Column Projection (читає лише потрібні 2 колонки)
    df_sub = pd.read_parquet(path, columns=['category', 'amount'])
    return df_sub.groupby('category')['amount'].sum()

# Етап А: Підігрів (Warm-up) - прибирає затримку від першої ініціалізації C++ модулів
print("Виконання підігріву (warm-up)...")
_ = query_csv(csv_path)
_ = query_parquet(parquet_snappy_path)

# Етап Б: Вимірювання середнього часу за 5 запусків
def measure_average_time(func, filepath, iterations=5):
    times = []
    for _ in range(iterations):
        start_time = time.time()
        func(filepath)
        times.append(time.time() - start_time)
    return sum(times) / len(times)

print("Замір середнього часу за 5 повторень...")
avg_time_csv = measure_average_time(query_csv, csv_path, iterations=5)
avg_time_parquet = measure_average_time(query_parquet, parquet_snappy_path, iterations=5)

print(f"\nСередній час обробки CSV            : {avg_time_csv:.4f} сек")
print(f"Середній час обробки Parquet (Snappy): {avg_time_parquet:.4f} сек")

if avg_time_parquet > 0:
    speedup = avg_time_csv / avg_time_parquet
    print(f"Реальне прискорення Parquet         : {speedup:.2f}x")