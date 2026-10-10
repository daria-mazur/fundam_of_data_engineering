import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_lab4():
    print("1. ІНІЦІАЛІЗАЦІЯ SPARK КЛАСТЕРА")
    
    # Симуляція кластера: local[4] означає 4 обчислювальні ядра
    spark = SparkSession.builder \
        .appName("Lab4_Performance_Analysis") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    # Зчитуємо згенеровані Parquet-дані
    data_path = "data/logs.parquet"
    df = spark.read.parquet(data_path)
    print(f"Завантажено записів: {df.count():,}")
    df.printSchema()

    print("2. СТРАТЕГІЯ A: 'ВИСОКЕ ПЕРЕТАШУВАННЯ' (High Shuffle)")
    
    # Робимо штучний repartition на 200 партицій без врахування ключів
    df_strat_a = df.repartition(200)

    start_a = time.time()

    # Агрегація тривалості сесій (MAX - MIN)
    session_durations_a = df_strat_a.groupBy("user_id", "session_id") \
        .agg((F.max("event_timestamp") - F.min("event_timestamp")).alias("duration_ms"))

    # Фінальна агрегація - середнє значення по всіх сесіях
    avg_duration_a = session_durations_a.select(F.avg("duration_ms")).collect()[0][0]

    time_a = time.time() - start_a

    print(f"Середня тривалість сесії (Стратегія A): {avg_duration_a:.2f} мс")
    print(f"Час виконання Стратегії A: {time_a:.4f} сек")

    print("3. СТРАТЕГІЯ B: 'НИЗЬКЕ ПЕРЕТАШУВАННЯ' (Low Shuffle)")
    
    # Оптимізуємо кількість партицій під кількість ядер (4) і групуємо за ключем
    df_strat_b = df.repartition(4, "user_id", "session_id")

    start_b = time.time()

    session_durations_b = df_strat_b.groupBy("user_id", "session_id") \
        .agg((F.max("event_timestamp") - F.min("event_timestamp")).alias("duration_ms"))

    avg_duration_b = session_durations_b.select(F.avg("duration_ms")).collect()[0][0]

    time_b = time.time() - start_b

    print(f"Середня тривалість сесії (Стратегія B): {avg_duration_b:.2f} мс")
    print(f"Час виконання Стратегії B: {time_b:.4f} сек")

    print("4. ПОРІВНЯННЯ ТА МЕТРИКИ ПРИСКОРЕННЯ")
    speedup = time_a / time_b if time_b > 0 else 0
    print(f"Час A: {time_a:.4f}s | Час B: {time_b:.4f}s")
    print(f"Прискорення Оптимізованої Стратегії B відносно A: {speedup:.2f}x")

    print("5. АНАЛІЗ ТА ПОМ'ЯКШЕННЯ DATA SKEW (SALTING TECHNIQUE)")
    
    # Симуляція перекосу: додаємо випадкову "сіль" (0..9) до user_id
    SALT_BUCKETS = 10
    df_salted = df.withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int")) \
                  .withColumn("salted_user_id", F.concat(F.col("user_id"), F.lit("_"), F.col("salt")))

    start_skew = time.time()

    # Крок 1 Salting: Часткова агрегація з урахуванням "солі"
    partial_agg = df_salted.groupBy("salted_user_id", "user_id", "session_id") \
                           .agg(F.max("event_timestamp").alias("max_ts"),
                                F.min("event_timestamp").alias("min_ts"))

    # Крок 2 Salting: Фінальна агрегація за чистим user_id
    final_agg = partial_agg.groupBy("user_id", "session_id") \
                           .agg((F.max("max_ts") - F.min("min_ts")).alias("duration_ms"))

    avg_duration_skew = final_agg.select(F.avg("duration_ms")).collect()[0][0]
    time_skew = time.time() - start_skew

    print(f"Середня тривалість (із Salting): {avg_duration_skew:.2f} мс")
    print(f"Час виконання алгоритму Salting: {time_skew:.4f} сек")

    spark.stop()

if __name__ == "__main__":
    run_lab4()