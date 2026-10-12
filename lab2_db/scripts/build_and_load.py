import os
import sqlite3
import sys

import pandas as pd

# Шляхи будуються відносно папки lab2_db, тому скрипт можна запускати з будь-якої директорії
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

raw_csv_path = os.path.join(DATA_DIR, "raw_transactions.csv")
# Якщо власного CSV немає — беремо файл, згенерований у Лабораторній №1
lab1_csv_path = os.path.join(os.path.dirname(BASE_DIR), "lab1_db", "lab1_data_storage", "data", "transactions.csv")
if not os.path.exists(raw_csv_path) and os.path.exists(lab1_csv_path):
    raw_csv_path = lab1_csv_path
if not os.path.exists(raw_csv_path):
    print(f"Помилка: Файл {raw_csv_path} не знайдено! Спочатку запустіть lab1_db/main.py.")
    sys.exit(1)

df = pd.read_csv(raw_csv_path)
print(f"Успішно зчитано {len(df):,} рядків з {raw_csv_path}")

df["timestamp"] = pd.to_datetime(df["timestamp"])
df["transaction_ts"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
# Зерно (grain) виміру дат — година: так вимір значно менший за таблицю фактів
df["date_hour"] = df["timestamp"].dt.floor("h")

# Ієрархія категорій: категорія -> група категорій
CATEGORY_GROUPS = {
    "Clothing": "Goods / Retail",
    "Electronics": "Goods / Retail",
    "Groceries": "Goods / Retail",
    "Travel": "Services / Leisure",
    "Entertainment": "Services / Leisure",
}

db_path = os.path.join(DATA_DIR, "dw_lab2.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# ---------------------------------------------------------------------------
# Спільна підготовка значень вимірів (сурогатні ключі генеруються тут,
# щоб обидві схеми містили ідентичні дані і порівняння було чесним)
# ---------------------------------------------------------------------------
categories = pd.DataFrame({"category_name": sorted(df["category"].dropna().unique())})
categories["category_id"] = range(1, len(categories) + 1)
categories["group_name"] = categories["category_name"].map(CATEGORY_GROUPS).fillna("Other")

statuses = pd.DataFrame({"status_name": sorted(df["status"].dropna().unique())})
statuses["status_id"] = range(1, len(statuses) + 1)

hours = pd.DataFrame({"date_hour": sorted(df["date_hour"].unique())})
hours["date_id"] = range(1, len(hours) + 1)
hours["year"] = hours["date_hour"].dt.year
hours["month"] = hours["date_hour"].dt.month
hours["day"] = hours["date_hour"].dt.day
hours["hour"] = hours["date_hour"].dt.hour
hours["date_hour_str"] = hours["date_hour"].dt.strftime("%Y-%m-%d %H:00")

# Таблиця фактів однакова для обох схем (відрізняються лише виміри)
fact = df.merge(categories[["category_name", "category_id"]], left_on="category", right_on="category_name") \
         .merge(statuses, left_on="status", right_on="status_name") \
         .merge(hours[["date_hour", "date_id"]], on="date_hour")
fact = fact[["transaction_id", "user_id", "category_id", "status_id", "date_id", "transaction_ts", "amount"]]
assert len(fact) == len(df), "Частина рядків втратилась під час зіставлення з вимірами"

FACT_DDL = """
CREATE TABLE {prefix}_fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id INTEGER,
    category_id INTEGER,
    status_id INTEGER,
    date_id INTEGER,
    transaction_ts TEXT,
    amount REAL,
    FOREIGN KEY (category_id) REFERENCES {prefix}_dim_category(category_id),
    FOREIGN KEY (status_id) REFERENCES {prefix}_dim_status(status_id),
    FOREIGN KEY (date_id) REFERENCES {prefix}_dim_date(date_id)
);
"""

print("\nСтворення та заповнення Схеми «Зірка»")

# «Зірка»: кожен вимір — одна денормалізована таблиця (ієрархії зберігаються всередині)
cursor.executescript("""
DROP TABLE IF EXISTS star_fact_transactions;
DROP TABLE IF EXISTS star_dim_category;
DROP TABLE IF EXISTS star_dim_status;
DROP TABLE IF EXISTS star_dim_date;

CREATE TABLE star_dim_category (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT UNIQUE,
    category_group TEXT          -- денормалізований атрибут ієрархії
);

CREATE TABLE star_dim_status (
    status_id INTEGER PRIMARY KEY,
    status_name TEXT UNIQUE
);

CREATE TABLE star_dim_date (
    date_id INTEGER PRIMARY KEY,
    date_hour TEXT UNIQUE,
    year INTEGER,                -- рік, місяць, день, година в одній таблиці
    month INTEGER,
    day INTEGER,
    hour INTEGER
);
""" + FACT_DDL.format(prefix="star"))

categories.rename(columns={"group_name": "category_group"})[["category_id", "category_name", "category_group"]] \
    .to_sql("star_dim_category", conn, if_exists="append", index=False)
statuses[["status_id", "status_name"]].to_sql("star_dim_status", conn, if_exists="append", index=False)
hours[["date_id", "date_hour_str", "year", "month", "day", "hour"]] \
    .rename(columns={"date_hour_str": "date_hour"}) \
    .to_sql("star_dim_date", conn, if_exists="append", index=False)
fact.to_sql("star_fact_transactions", conn, if_exists="append", index=False)

print("Схема «Зірка» успішно створена та заповнена!")

print("\nСтворення та заповнення Схеми «Сніжинка»")

# «Сніжинка»: ієрархії вимірів нормалізовані в окремі таблиці
cursor.executescript("""
DROP TABLE IF EXISTS snow_fact_transactions;
DROP TABLE IF EXISTS snow_dim_category;
DROP TABLE IF EXISTS snow_dim_category_group;
DROP TABLE IF EXISTS snow_dim_status;
DROP TABLE IF EXISTS snow_dim_date;
DROP TABLE IF EXISTS snow_dim_month;
DROP TABLE IF EXISTS snow_dim_year;

CREATE TABLE snow_dim_category_group (
    group_id INTEGER PRIMARY KEY,
    group_name TEXT UNIQUE
);

CREATE TABLE snow_dim_category (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT UNIQUE,
    group_id INTEGER,
    FOREIGN KEY (group_id) REFERENCES snow_dim_category_group(group_id)
);

CREATE TABLE snow_dim_status (
    status_id INTEGER PRIMARY KEY,
    status_name TEXT UNIQUE
);

CREATE TABLE snow_dim_year (
    year_id INTEGER PRIMARY KEY,
    year_val INTEGER UNIQUE
);

CREATE TABLE snow_dim_month (
    month_id INTEGER PRIMARY KEY,
    month_val INTEGER,
    year_id INTEGER,
    UNIQUE (month_val, year_id),
    FOREIGN KEY (year_id) REFERENCES snow_dim_year(year_id)
);

CREATE TABLE snow_dim_date (
    date_id INTEGER PRIMARY KEY,
    date_hour TEXT UNIQUE,
    day INTEGER,
    hour INTEGER,
    month_id INTEGER,
    FOREIGN KEY (month_id) REFERENCES snow_dim_month(month_id)
);
""" + FACT_DDL.format(prefix="snow"))

# 1. Категорія -> група категорій
groups = pd.DataFrame({"group_name": sorted(categories["group_name"].unique())})
groups["group_id"] = range(1, len(groups) + 1)
groups.to_sql("snow_dim_category_group", conn, if_exists="append", index=False)
categories.merge(groups, on="group_name")[["category_id", "category_name", "group_id"]] \
    .to_sql("snow_dim_category", conn, if_exists="append", index=False)

# 2. Статус
statuses[["status_id", "status_name"]].to_sql("snow_dim_status", conn, if_exists="append", index=False)

# 3. Дата -> місяць -> рік
years = pd.DataFrame({"year_val": sorted(hours["year"].unique())})
years["year_id"] = range(1, len(years) + 1)
years.to_sql("snow_dim_year", conn, if_exists="append", index=False)

months = hours[["year", "month"]].drop_duplicates().sort_values(["year", "month"]) \
    .merge(years, left_on="year", right_on="year_val")
months["month_id"] = range(1, len(months) + 1)
months.rename(columns={"month": "month_val"})[["month_id", "month_val", "year_id"]] \
    .to_sql("snow_dim_month", conn, if_exists="append", index=False)

hours.merge(months[["year", "month", "month_id"]], on=["year", "month"]) \
    [["date_id", "date_hour_str", "day", "hour", "month_id"]] \
    .rename(columns={"date_hour_str": "date_hour"}) \
    .to_sql("snow_dim_date", conn, if_exists="append", index=False)

fact.to_sql("snow_fact_transactions", conn, if_exists="append", index=False)

print("Схема «Сніжинка» успішно створена та заповнена!")

conn.commit()

# Короткий підсумок розмірів таблиць для звіту
print("\nКількість рядків у таблицях:")
for (table,) in cursor.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall():
    count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table:<26}: {count:>9,}")

conn.execute("VACUUM")
conn.close()

print(f"\nВсі дані успішно збережені у базі даних SQLite: {db_path}")
