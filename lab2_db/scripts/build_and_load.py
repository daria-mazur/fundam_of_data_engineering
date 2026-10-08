import sqlite3
import pandas as pd
import os

# Переконуємось, що папка data існує
os.makedirs("data", exist_ok=True)

# Зчитуємо початковий CSV файл з транзакціями
raw_csv_path = "data/raw_transactions.csv"
if not os.path.exists(raw_csv_path):
    print(f"Помилка: Файл {raw_csv_path} не знайдено! Перевірте шлях до CSV файлу.")
    exit(1)

df = pd.read_csv(raw_csv_path)
print(f"Успішно зчитано {len(df)} рядків з {raw_csv_path}")

# Створюємо / підключаємось до локальної бази даних SQLite
db_path = "data/dw_lab2.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("\nСтворення та заповнення Схеми «Зірка»")

cursor.executescript("""
DROP TABLE IF EXISTS star_fact_transactions;
DROP TABLE IF EXISTS star_dim_category;
DROP TABLE IF EXISTS star_dim_status;
DROP TABLE IF EXISTS star_dim_date;

CREATE TABLE star_dim_category (
    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name TEXT UNIQUE
);

CREATE TABLE star_dim_status (
    status_id INTEGER PRIMARY KEY AUTOINCREMENT,
    status_name TEXT UNIQUE
);

CREATE TABLE star_dim_date (
    date_id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_timestamp TEXT UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER
);

CREATE TABLE star_fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id INTEGER,
    category_id INTEGER,
    status_id INTEGER,
    date_id INTEGER,
    amount REAL,
    FOREIGN KEY (category_id) REFERENCES star_dim_category(category_id),
    FOREIGN KEY (status_id) REFERENCES star_dim_status(status_id),
    FOREIGN KEY (date_id) REFERENCES star_dim_date(date_id)
);
""")

# Конвертуємо колонку timestamp у зручний формат
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['full_timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Заповнюємо вимір Категорій (Star)
categories = pd.DataFrame({'category_name': df['category'].dropna().unique()})
categories.to_sql('star_dim_category', conn, if_exists='append', index=False)

# Заповнюємо вимір Статусів (Star)
statuses = pd.DataFrame({'status_name': df['status'].dropna().unique()})
statuses.to_sql('star_dim_status', conn, if_exists='append', index=False)

# Заповнюємо вимір Дат (Star)
dates = pd.DataFrame({
    'full_timestamp': df['full_timestamp'],
    'year': df['timestamp'].dt.year,
    'month': df['timestamp'].dt.month,
    'day': df['timestamp'].dt.day,
    'hour': df['timestamp'].dt.hour
}).drop_duplicates(subset=['full_timestamp'])
dates.to_sql('star_dim_date', conn, if_exists='append', index=False)

# Отримуємо автоматично згенеровані ID із заповнених таблиць вимірів
cat_df = pd.read_sql("SELECT category_id, category_name FROM star_dim_category", conn)
stat_df = pd.read_sql("SELECT status_id, status_name FROM star_dim_status", conn)
date_df = pd.read_sql("SELECT date_id, full_timestamp FROM star_dim_date", conn)

# Об'єднуємо ключі (Foreign Keys) з таблицею фактів
fact_star = df.merge(cat_df, left_on='category', right_on='category_name', how='left') \
              .merge(stat_df, left_on='status', right_on='status_name', how='left') \
              .merge(date_df, on='full_timestamp', how='left')

fact_star = fact_star[['transaction_id', 'user_id', 'category_id', 'status_id', 'date_id', 'amount']]
fact_star.to_sql('star_fact_transactions', conn, if_exists='append', index=False)

print("Схема «Зірка» успішно створена та заповнена!")


print("\nСтворення та заповнення Схеми «Сніжинка»")

cursor.executescript("""
DROP TABLE IF EXISTS snow_fact_transactions;
DROP TABLE IF EXISTS snow_dim_category;
DROP TABLE IF EXISTS snow_dim_category_group;
DROP TABLE IF EXISTS snow_dim_date;
DROP TABLE IF EXISTS snow_dim_month;
DROP TABLE IF EXISTS snow_dim_year;

CREATE TABLE snow_dim_category_group (
    group_id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT UNIQUE
);

CREATE TABLE snow_dim_category (
    category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name TEXT UNIQUE,
    group_id INTEGER,
    FOREIGN KEY (group_id) REFERENCES snow_dim_category_group(group_id)
);

CREATE TABLE snow_dim_year (
    year_id INTEGER PRIMARY KEY AUTOINCREMENT,
    year_val INTEGER UNIQUE
);

CREATE TABLE snow_dim_month (
    month_id INTEGER PRIMARY KEY AUTOINCREMENT,
    month_val INTEGER,
    year_id INTEGER,
    FOREIGN KEY (year_id) REFERENCES snow_dim_year(year_id)
);

CREATE TABLE snow_dim_date (
    date_id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_timestamp TEXT UNIQUE,
    day INTEGER,
    hour INTEGER,
    month_id INTEGER,
    FOREIGN KEY (month_id) REFERENCES snow_dim_month(month_id)
);

CREATE TABLE snow_fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    user_id INTEGER,
    category_id INTEGER,
    status_id INTEGER,
    date_id INTEGER,
    amount REAL,
    FOREIGN KEY (category_id) REFERENCES snow_dim_category(category_id),
    FOREIGN KEY (status_id) REFERENCES star_dim_status(status_id),
    FOREIGN KEY (date_id) REFERENCES snow_dim_date(date_id)
);
""")

# Заповнюємо нормалізовані виміри Сніжинки

# 1. Групи категорій (Нормалізація категорії)
cat_groups = pd.DataFrame({'group_name': ['Goods / Retail', 'Services / Travel']})
cat_groups.to_sql('snow_dim_category_group', conn, if_exists='append', index=False)
group_df = pd.read_sql("SELECT group_id, group_name FROM snow_dim_category_group", conn)

# Прив'язуємо категорії до груп
def assign_group(cat):
    if cat in ['Clothing', 'Electronics', 'Groceries']:
        return group_df.loc[group_df['group_name'] == 'Goods / Retail', 'group_id'].values[0]
    else:
        return group_df.loc[group_df['group_name'] == 'Services / Travel', 'group_id'].values[0]

cat_snow = categories.copy()
cat_snow['group_id'] = cat_snow['category_name'].apply(assign_group)
cat_snow.to_sql('snow_dim_category', conn, if_exists='append', index=False)

# 2. Роки, Місяці, Дати (Нормалізація часу)
years_df = pd.DataFrame({'year_val': dates['year'].unique()})
years_df.to_sql('snow_dim_year', conn, if_exists='append', index=False)
snow_year_df = pd.read_sql("SELECT year_id, year_val FROM snow_dim_year", conn)

unique_months = dates[['month', 'year']].drop_duplicates().merge(snow_year_df, left_on='year', right_on='year_val')
unique_months = unique_months[['month', 'year_id']].rename(columns={'month': 'month_val'})
unique_months.to_sql('snow_dim_month', conn, if_exists='append', index=False)
snow_month_df = pd.read_sql("SELECT month_id, month_val, year_id FROM snow_dim_month", conn)

dates_snow = dates.merge(snow_year_df, left_on='year', right_on='year_val') \
                  .merge(snow_month_df, left_on=['month', 'year_id'], right_on=['month_val', 'year_id'])
dates_snow = dates_snow[['full_timestamp', 'day', 'hour', 'month_id']]
dates_snow.to_sql('snow_dim_date', conn, if_exists='append', index=False)

# Формуємо факт для сніжинки
snow_cat_df = pd.read_sql("SELECT category_id, category_name FROM snow_dim_category", conn)
snow_date_df = pd.read_sql("SELECT date_id, full_timestamp FROM snow_dim_date", conn)

fact_snow = df.merge(snow_cat_df, left_on='category', right_on='category_name', how='left') \
              .merge(stat_df, left_on='status', right_on='status_name', how='left') \
              .merge(snow_date_df, on='full_timestamp', how='left')

fact_snow = fact_snow[['transaction_id', 'user_id', 'category_id', 'status_id', 'date_id', 'amount']]
fact_snow.to_sql('snow_fact_transactions', conn, if_exists='append', index=False)

print("Схема «Сніжинка» успішно створена та заповнена!")

conn.commit()
conn.close()

print(f"\n Всі дані успішно збережені у базі даних SQLite: {db_path}")