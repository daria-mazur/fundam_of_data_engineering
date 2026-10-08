import sqlite3
import time
import pandas as pd
import matplotlib.pyplot as plt

db_path = "data/dw_lab2.db"
conn = sqlite3.connect(db_path)

print("Розпочинаємо порівняльне тестування продуктивності запитів\n")

query_star_1 = """
SELECT c.category_name, s.status_name, COUNT(*) AS total_txns, SUM(f.amount) AS total_amount
FROM star_fact_transactions f
JOIN star_dim_category c ON f.category_id = c.category_id
JOIN star_dim_status s ON f.status_id = s.status_id
GROUP BY c.category_name, s.status_name;
"""

query_snow_1 = """
SELECT c.category_name, s.status_name, COUNT(*) AS total_txns, SUM(f.amount) AS total_amount
FROM snow_fact_transactions f
JOIN snow_dim_category c ON f.category_id = c.category_id
JOIN star_dim_status s ON f.status_id = s.status_id
GROUP BY c.category_name, s.status_name;
"""

query_star_2 = """
SELECT c.category_name, d.year, SUM(f.amount) AS total_amount
FROM star_fact_transactions f
JOIN star_dim_category c ON f.category_id = c.category_id
JOIN star_dim_date d ON f.date_id = d.date_id
GROUP BY c.category_name, d.year;
"""

query_snow_2 = """
SELECT cg.group_name, y.year_val, SUM(f.amount) AS total_amount
FROM snow_fact_transactions f
JOIN snow_dim_category c ON f.category_id = c.category_id
JOIN snow_dim_category_group cg ON c.group_id = cg.group_id
JOIN snow_dim_date d ON f.date_id = d.date_id
JOIN snow_dim_month m ON d.month_id = m.month_id
JOIN snow_dim_year y ON m.year_id = y.year_id
GROUP BY cg.group_name, y.year_val;
"""

def run_benchmark(query, name):
    start_time = time.perf_counter()
    res = pd.read_sql(query, conn)
    end_time = time.perf_counter()
    execution_time = (end_time - start_time) * 1000  # у мілісекундах
    print(f"[{name}] Виконано за {execution_time:.2f} ms (отримано {len(res)} рядків)")
    return execution_time

# Виконуємо тестування
t_star_1 = run_benchmark(query_star_1, "Star Schema - Query 1")
t_snow_1 = run_benchmark(query_snow_1, "Snowflake Schema - Query 1")

t_star_2 = run_benchmark(query_star_2, "Star Schema - Query 2")
t_snow_2 = run_benchmark(query_snow_2, "Snowflake Schema - Query 2")

conn.close()

labels = ['Q1: Category & Status', 'Q2: Group/Cat & Year (Deep Join)']
star_times = [t_star_1, t_star_2]
snow_times = [t_snow_1, t_snow_2]

x = range(len(labels))
width = 0.35

fig, ax = plt.subplots(figsize=(9, 5))
rects1 = ax.bar([i - width/2 for i in x], star_times, width, label='Star Schema', color='#2b5c8f')
rects2 = ax.bar([i + width/2 for i in x], snow_times, width, label='Snowflake Schema', color='#d95f02')

ax.set_ylabel('Час виконання (мілісекунди)')
ax.set_title('Порівняння продуктивності запитів: Star vs Snowflake Schema')
ax.set_xticks(list(x))
ax.set_xticklabels(labels)
ax.legend()

for rect in rects1 + rects2:
    height = rect.get_height()
    ax.annotate(f'{height:.1f} ms',
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),  
                textcoords="offset points",
                ha='center', va='bottom')

plt.tight_layout()
plt.savefig("data/benchmark_result.png")
print("\n Графік порівняння збережено у data/benchmark_result.png")