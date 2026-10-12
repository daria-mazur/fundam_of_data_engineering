import math
import os
import time
import numpy as np
import pandas as pd

# Шлях відносно файлу скрипта, щоб запуск працював з будь-якої робочої директорії
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lab1_data_storage", "data")
os.makedirs(DATA_DIR, exist_ok=True)

NUM_ROWS = 1_000_000
ITERATIONS = 5

csv_path = os.path.join(DATA_DIR, "transactions.csv")
parquet_paths = {
    "Parquet (None)": os.path.join(DATA_DIR, "transactions_none.parquet"),
    "Parquet (Snappy)": os.path.join(DATA_DIR, "transactions_snappy.parquet"),
    "Parquet (GZIP)": os.path.join(DATA_DIR, "transactions_gzip.parquet"),
}
parquet_codecs = {
    "Parquet (None)": None,
    "Parquet (Snappy)": "snappy",
    "Parquet (GZIP)": "gzip",
}


def size_mb(path):
    return os.path.getsize(path) / (1024 * 1024)

print("1. Генерація тестових даних та запис у CSV")

rng = np.random.default_rng(42)
source_df = pd.DataFrame({
    "transaction_id": [f"TXN_{i:08d}" for i in range(NUM_ROWS)],
    "user_id": rng.integers(1000, 9999, size=NUM_ROWS),
    "amount": np.round(rng.uniform(5.0, 5000.0, size=NUM_ROWS), 2),
    "category": rng.choice(
        ["Groceries", "Electronics", "Clothing", "Travel", "Entertainment"],
        size=NUM_ROWS),
    "status": rng.choice(
        ["COMPLETED", "PENDING", "FAILED", "REFUNDED"], size=NUM_ROWS),
    "timestamp": pd.date_range(start="2025-01-01", periods=NUM_ROWS, freq="s"),
})

start = time.perf_counter()
source_df.to_csv(csv_path, index=False)
print(f"CSV записано: {NUM_ROWS:,} рядків за {time.perf_counter() - start:.2f} сек")
del source_df  # далі працюємо лише з файлом, як у реальній задачі

print("\n2. Конвертація CSV у Parquet з різними алгоритмами стиснення")

csv_df = pd.read_csv(csv_path, parse_dates=["timestamp"])

conversion_times = {}
for name, codec in parquet_codecs.items():
    start = time.perf_counter()
    csv_df.to_parquet(parquet_paths[name], compression=codec, index=False)
    conversion_times[name] = time.perf_counter() - start
    print(f"{name:<18}: записано за {conversion_times[name]:.2f} сек")

del csv_df

print("\nПОРІВНЯННЯ РОЗМІРІВ ФАЙЛІВ")
all_files = {"CSV (Uncompressed)": csv_path, **parquet_paths}
csv_size = size_mb(csv_path)

for name, path in all_files.items():
    s = size_mb(path)
    saved = (1 - s / csv_size) * 100
    print(f"{name:<20}: {s:>6.2f} MB (менше за CSV на {saved:>5.1f}%)")

# Запит 1: сума транзакцій за категоріями (агрегація)
def query1_csv(path):
    df = pd.read_csv(path, usecols=["category", "amount"])
    return df.groupby("category")["amount"].sum()


def query1_parquet(path):
    # Column projection: читаються лише 2 потрібні колонки
    df = pd.read_parquet(path, columns=["category", "amount"])
    return df.groupby("category")["amount"].sum()


# Запит 2: кількість та середня сума успішних транзакцій з amount > 1000 (фільтрація)
def query2_csv(path):
    df = pd.read_csv(path, usecols=["status", "amount"])
    sub = df[(df["status"] == "COMPLETED") & (df["amount"] > 1000)]
    return {"count": int(len(sub)), "mean_amount": float(sub["amount"].mean())}


def query2_parquet(path):
    # Column projection + predicate pushdown (фільтри передаються рушію pyarrow)
    df = pd.read_parquet(
        path,
        columns=["status", "amount"],
        filters=[("status", "==", "COMPLETED"), ("amount", ">", 1000)],
    )
    return {"count": int(len(df)), "mean_amount": float(df["amount"].mean())}


queries = {
    "Запит 1: SUM(amount) GROUP BY category": (query1_csv, query1_parquet),
    "Запит 2: COUNT/AVG для COMPLETED та amount > 1000": (query2_csv, query2_parquet),
}


def results_equal(a, b):
    if isinstance(a, pd.Series):
        pd.testing.assert_series_equal(a, b, check_names=False, rtol=1e-9)
        return True
    return a["count"] == b["count"] and math.isclose(
        a["mean_amount"], b["mean_amount"], rel_tol=1e-9)


# 4.1 Перевірка коректності: усі формати мають давати однаковий результат
print("\nПЕРЕВІРКА КОРЕКТНОСТІ РЕЗУЛЬТАТІВ")
for q_name, (q_csv, q_parquet) in queries.items():
    reference = q_csv(csv_path)
    for name, path in parquet_paths.items():
        ok = results_equal(reference, q_parquet(path))
        print(f"{q_name} | {name:<17}: {'OK' if ok else 'РОЗБІЖНІСТЬ!'}")
        assert ok, f"Результати CSV і {name} не збігаються"

# 4.2 Вимірювання продуктивності
def measure(func, path, iterations=ITERATIONS):
    func(path)  # прогрів (warm-up): ініціалізація модулів, кеш файлової системи
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func(path)
        times.append(time.perf_counter() - start)
    return sum(times) / len(times)


print(f"\nПОРІВНЯННЯ ШВИДКОДІЇ (середнє за {ITERATIONS} запусків)")
for q_name, (q_csv, q_parquet) in queries.items():
    print(f"\n{q_name}")
    t_csv = measure(q_csv, csv_path)
    print(f"  {'CSV':<18}: {t_csv:.4f} сек")
    for name, path in parquet_paths.items():
        t = measure(q_parquet, path)
        print(f"  {name:<18}: {t:.4f} сек  (прискорення {t_csv / t:.2f}x)")