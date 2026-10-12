import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Параметри генерації даних
NUM_SESSIONS = 400_000           # ~4.4 млн подій (у середньому 11 подій на сесію)
NUM_USERS = 50_000
MIN_EVENTS, MAX_EVENTS = 2, 20   # кількість подій у сесії
MAX_SESSION_MS = 2 * 60 * 60 * 1000   # максимальна тривалість сесії — 2 години
PERIOD_MS = 30 * 24 * 60 * 60 * 1000  # сесії розкидані по 30 днях
BASE_TIMESTAMP = 1_700_000_000_000    # фіксований початковий timestamp у мс
OUTPUT_FILES = 16

SKEWED_USER = "user_whale"
SKEW_SHARE = 0.9                 # частка сесій, що належать одному користувачу


def build_logs(spark, skewed=False):
    # 1. Сесії: кожна сесія належить рівно одному користувачу (як у реальних логах)
    regular_user = F.concat(F.lit("user_"), (F.rand(seed=1) * NUM_USERS).cast("long").cast("string"))
    user_id = F.when(F.rand(seed=7) < SKEW_SHARE, F.lit(SKEWED_USER)).otherwise(regular_user) \
        if skewed else regular_user

    sessions = spark.range(NUM_SESSIONS) \
        .withColumn("user_id", user_id) \
        .withColumn("session_id", F.concat(F.lit("sess_"), F.col("id").cast("string"))) \
        .withColumn("n_events", (F.rand(seed=2) * (MAX_EVENTS - MIN_EVENTS + 1)).cast("int") + MIN_EVENTS) \
        .withColumn("start_ts", F.lit(BASE_TIMESTAMP) + (F.rand(seed=3) * PERIOD_MS).cast("long")) \
        .withColumn("session_len", (F.rand(seed=4) * MAX_SESSION_MS).cast("long"))

    # 2. Події: n_events подій у межах [start_ts, start_ts + session_len]
    events = sessions \
        .withColumn("k", F.explode(F.sequence(F.lit(1), F.col("n_events")))) \
        .withColumn("event_timestamp", F.col("start_ts") + (F.rand(seed=5) * F.col("session_len")).cast("long")) \
        .select("user_id", "session_id", "event_timestamp")

    # 3. Round-robin розкидає події однієї сесії по різним файлам,
    #    як це буває в реальних логах, що пишуться в міру надходження подій
    return events.repartition(OUTPUT_FILES)


def generate_logs():
    print("Ініціалізація Spark Session")
    spark = SparkSession.builder \
        .appName("Lab4_DataGenerator") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

    datasets = {
        "logs.parquet": False,         # рівномірний розподіл користувачів
        "logs_skewed.parquet": True,   # 90% сесій належать одному користувачу
    }
    for name, skewed in datasets.items():
        output_path = os.path.join(DATA_DIR, name)
        print(f"Генерація {NUM_SESSIONS:,} сесій -> '{output_path}'")
        build_logs(spark, skewed).write.mode("overwrite").parquet(output_path)

        df = spark.read.parquet(output_path)
        print(f"  Збережено подій: {df.count():,}, схема: {df.schema.simpleString()}")

    print("Дані згенеровано та збережено")
    spark.stop()


if __name__ == "__main__":
    generate_logs()
