import os
import sqlite3
import statistics
import time

import matplotlib.pyplot as plt
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
db_path = os.path.join(DATA_DIR, "dw_lab2.db")

ITERATIONS = 5

conn = sqlite3.connect(db_path)

print("Розпочинаємо порівняльне тестування продуктивності запитів\n")

# Кожна пара запитів повертає ОДНАКОВИЙ бізнес-результат, різниця лише у схемі.
# Тільки так порівняння продуктивності є коректним.
QUERIES = {
    "Q1: Category & Status": (
        """
        SELECT c.category_name, s.status_name, COUNT(*) AS total_txns, SUM(f.amount) AS total_amount
        FROM star_fact_transactions f
        JOIN star_dim_category c ON f.category_id = c.category_id
        JOIN star_dim_status s ON f.status_id = s.status_id
        GROUP BY c.category_name, s.status_name
        ORDER BY 1, 2;
        """,
        """
        SELECT c.category_name, s.status_name, COUNT(*) AS total_txns, SUM(f.amount) AS total_amount
        FROM snow_fact_transactions f
        JOIN snow_dim_category c ON f.category_id = c.category_id
        JOIN snow_dim_status s ON f.status_id = s.status_id
        GROUP BY c.category_name, s.status_name
        ORDER BY 1, 2;
        """,
    ),
    # Запит по ієрархіях: у «зірці» група і рік лежать прямо у вимірах,
    # у «сніжинці» потрібні додаткові JOIN-и (category_group, month, year)
    "Q2: Category group & Year": (
        """
        SELECT c.category_group AS group_name, d.year, SUM(f.amount) AS total_amount
        FROM star_fact_transactions f
        JOIN star_dim_category c ON f.category_id = c.category_id
        JOIN star_dim_date d ON f.date_id = d.date_id
        GROUP BY c.category_group, d.year
        ORDER BY 1, 2;
        """,
        """
        SELECT cg.group_name, y.year_val AS year, SUM(f.amount) AS total_amount
        FROM snow_fact_transactions f
        JOIN snow_dim_category c ON f.category_id = c.category_id
        JOIN snow_dim_category_group cg ON c.group_id = cg.group_id
        JOIN snow_dim_date d ON f.date_id = d.date_id
        JOIN snow_dim_month m ON d.month_id = m.month_id
        JOIN snow_dim_year y ON m.year_id = y.year_id
        GROUP BY cg.group_name, y.year_val
        ORDER BY 1, 2;
        """,
    ),
    # Типовий ad-hoc запит аналітика: фільтр + поденна динаміка
    "Q3: Daily COMPLETED revenue by group": (
        """
        SELECT c.category_group AS group_name, d.year, d.month, d.day, COUNT(*) AS total_txns, SUM(f.amount) AS total_amount
        FROM star_fact_transactions f
        JOIN star_dim_category c ON f.category_id = c.category_id
        JOIN star_dim_status s ON f.status_id = s.status_id
        JOIN star_dim_date d ON f.date_id = d.date_id
        WHERE s.status_name = 'COMPLETED'
        GROUP BY c.category_group, d.year, d.month, d.day
        ORDER BY 1, 2, 3, 4;
        """,
        """
        SELECT cg.group_name, y.year_val AS year, m.month_val AS month, d.day, COUNT(*) AS total_txns, SUM(f.amount) AS total_amount
        FROM snow_fact_transactions f
        JOIN snow_dim_category c ON f.category_id = c.category_id
        JOIN snow_dim_category_group cg ON c.group_id = cg.group_id
        JOIN snow_dim_status s ON f.status_id = s.status_id
        JOIN snow_dim_date d ON f.date_id = d.date_id
        JOIN snow_dim_month m ON d.month_id = m.month_id
        JOIN snow_dim_year y ON m.year_id = y.year_id
        WHERE s.status_name = 'COMPLETED'
        GROUP BY cg.group_name, y.year_val, m.month_val, d.day
        ORDER BY 1, 2, 3, 4;
        """,
    ),
}


def run_benchmark(query, name, iterations=ITERATIONS):
    # Прогрів: перший запуск читає сторінки з диска у кеш, тому його не враховуємо
    result = pd.read_sql(query, conn)
    times = []
    for _ in range(iterations):
        start_time = time.perf_counter()
        pd.read_sql(query, conn)
        times.append((time.perf_counter() - start_time) * 1000)  # у мілісекундах
    median_ms = statistics.median(times)
    print(f"[{name:<9}] медіана {median_ms:8.2f} ms "
          f"(min {min(times):.2f}, max {max(times):.2f}; отримано {len(result)} рядків)")
    return median_ms, result


labels, star_times, snow_times = [], [], []
for q_name, (q_star, q_snow) in QUERIES.items():
    print(q_name)
    t_star, res_star = run_benchmark(q_star, "Star")
    t_snow, res_snow = run_benchmark(q_snow, "Snowflake")

    # Перевірка коректності: обидві схеми мають давати однаковий результат
    pd.testing.assert_frame_equal(res_star, res_snow, check_dtype=False, rtol=1e-9)
    print(f"  Результати збігаються. Snowflake / Star = {t_snow / t_star:.2f}x\n")

    labels.append(q_name)
    star_times.append(t_star)
    snow_times.append(t_snow)

conn.close()

x = range(len(labels))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 5))
rects1 = ax.bar([i - width / 2 for i in x], star_times, width, label="Star Schema", color="#2b5c8f")
rects2 = ax.bar([i + width / 2 for i in x], snow_times, width, label="Snowflake Schema", color="#d95f02")

ax.set_ylabel("Час виконання, медіана (мілісекунди)")
ax.set_title(f"Порівняння продуктивності запитів: Star vs Snowflake (медіана {ITERATIONS} запусків)")
ax.set_xticks(list(x))
ax.set_xticklabels(labels)
ax.legend()

for rect in rects1 + rects2:
    height = rect.get_height()
    ax.annotate(f"{height:.1f} ms",
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center", va="bottom")

plt.tight_layout()
output_png = os.path.join(DATA_DIR, "benchmark_result.png")
plt.savefig(output_png)
print(f"Графік порівняння збережено у {output_png}")
