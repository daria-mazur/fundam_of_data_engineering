import os
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

def generate_logs():
    print("Ініціалізація Spark Session")
    spark = SparkSession.builder \
        .appName("Lab4_DataGenerator") \
        .master("local[*]") \
        .getOrCreate()

    # Параметри генерації даних
    NUM_RECORDS = 2_000_000
    NUM_USERS = 50_000
    NUM_SESSIONS = 200_000

    print(f"Генерація {NUM_RECORDS:,} записів")
    
    base_timestamp = 1700000000000  # Фіксований початковий timestamp у мс
    data = []

    for _ in range(NUM_RECORDS):
        u_id = f"user_{random.randint(1, NUM_USERS)}"
        s_id = f"sess_{random.randint(1, NUM_SESSIONS)}"
        # Часовий зсув у межах сесії (до 2 годин = 7 200 000 мс)
        ts = base_timestamp + random.randint(0, 7_200_000)
        data.append((u_id, s_id, ts))

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_timestamp", LongType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    output_path = "data/logs.parquet"
    print(f"Збереження даних у Parquet за шляхом '{output_path}'")
    
    # Репартиціюємо на 4 файли для оптимального збереження
    df.repartition(4).write.mode("overwrite").parquet(output_path)

    print("Дані згенеровано та збережено")
    spark.stop()

if __name__ == "__main__":
    generate_logs()