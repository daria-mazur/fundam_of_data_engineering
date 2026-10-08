import psycopg2
from faker import Faker
import random

# Налаштування підключення до бази даних PostgreSQL
DB_CONFIG = {
    "dbname": "lab0_db",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

fake = Faker('uk_UA')  # Генератор україномовних даних

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# Створення таблиці
def init_db():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS students (
        id SERIAL PRIMARY KEY,
        full_name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        gpa NUMERIC(5, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            print("[+] Таблицю 'students' успішно створено/перевірено.")

# Генерація та додавання даних (Insert)
def seed_data(count=5):
    insert_query = """
    INSERT INTO students (full_name, email, gpa)
    VALUES (%s, %s, %s);
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for _ in range(count):
                name = fake.name()
                email = fake.email()
                gpa = round(random.uniform(60.0, 100.0), 2)
                cursor.execute(insert_query, (name, email, gpa))
            conn.commit()
            print(f"[+] Додано {count} згенерованих записів.")

# Вибірка даних (Select / Read)
def fetch_all_students():
    select_query = "SELECT id, full_name, email, gpa FROM students ORDER BY id;"
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(select_query)
            records = cursor.fetchall()
            print("\n--- Список студентів у БД ---")
            for row in records:
                print(f"ID: {row[0]} | ПІБ: {row[1]} | Email: {row[2]} | GPA: {row[3]}")
            return records

# Зміна даних (Update)
def update_student_gpa(student_id, new_gpa):
    update_query = "UPDATE students SET gpa = %s WHERE id = %s;"
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update_query, (new_gpa, student_id))
            conn.commit()
            print(f"\n[+] Оновлено GPA для студента з ID={student_id} на {new_gpa}.")

# Видалення даних (Delete)
def delete_student(student_id):
    delete_query = "DELETE FROM students WHERE id = %s;"
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(delete_query, (student_id,))
            conn.commit()
            print(f"\n[-] Видалено студента з ID={student_id}.")

if __name__ == "__main__":
    try:
        init_db()
        seed_data(5)
        
        students = fetch_all_students()
        
        if students:
            first_id = students[0][0]
            
            # Демонстрація оновлення
            update_student_gpa(first_id, 99.50)
            fetch_all_students()
            
            # Демонстрація видалення
            delete_student(first_id)
            fetch_all_students()
            
    except Exception as e:
        print(f"[!] Помилка виконання: {e}")