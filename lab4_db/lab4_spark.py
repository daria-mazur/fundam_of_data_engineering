import json
import os
import statistics
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "logs.parquet")
SKEWED_DATA_PATH = os.path.join(BASE_DIR, "data", "logs_skewed.parquet")
RESULTS_PATH = os.path.join(BASE_DIR, "data", "lab4_results.json")

# Конфігурація "кластера": X виконавців по Y ядер.
# У локальному режимі local[Y] — це один виконавець (у JVM драйвера) з Y ядрами (X = 1).
# На справжньому кластері: --num-executors X --executor-cores Y (spark.executor.instances / spark.executor.cores).
NUM_EXECUTORS = 1                                  # X
CORES_PER_EXECUTOR = 4                             # Y
TOTAL_CORES = NUM_EXECUTORS * CORES_PER_EXECUTOR

HIGH_SHUFFLE_PARTITIONS = 200   # N для стратегії A
RUNS = 3                        # кількість вимірювань (після прогріву), беремо медіану
AMDAHL_CORES = [1, 2, 4, 8]     # конфігурації для оцінки масштабованості
SALT_BUCKETS = TOTAL_CORES * 8


def create_spark(cores):
    return SparkSession.builder \
        .appName(f"Lab4_Performance_Analysis_{cores}_cores") \
        .master(f"local[{cores}]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()


def load_cached(spark, path):
    # Кешуємо дані в пам'яті, щоб у вимірюваний час НЕ входило читання з диска
    df = spark.read.parquet(path).cache()
    df.count()
    return df


# ---------------------------------------------------------------------------
# Кроки 2-4: groupBy(user_id, session_id) -> MAX-MIN -> загальне середнє
# ---------------------------------------------------------------------------
def session_durations(df):
    # Якщо дані розподілені з сіллю, додаємо її до ключа групування: сіль — функція від
    # session_id, тож результат той самий, але Spark не робить повторного перетасування
    keys = ["user_id", "salt", "session_id"] if "salt" in df.columns else ["user_id", "session_id"]
    return df.groupBy(*keys) \
        .agg((F.max("event_timestamp") - F.min("event_timestamp")).alias("duration_ms"))


def average_session_duration(df):
    return session_durations(df).agg(F.avg("duration_ms")).collect()[0][0]


# ---------------------------------------------------------------------------
# Метрики перетасування з REST API Spark UI (ті самі цифри, що у вкладці Stages)
# ---------------------------------------------------------------------------
def _rest(spark, path):
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def shuffle_metrics(spark, job_group):
    try:
        # Слухач подій асинхронний — чекаємо, доки завершені задачі з'являться у статусі
        jobs = []
        for _ in range(50):
            jobs = [j for j in _rest(spark, "jobs") if j.get("jobGroup") == job_group]
            if jobs and all(j["status"] != "RUNNING" for j in jobs):
                break
            time.sleep(0.1)
        stage_ids = {s for j in jobs for s in j["stageIds"]}
        stages = [s for s in _rest(spark, "stages?status=complete") if s["stageId"] in stage_ids]
        return {
            "shuffle_write_mb": sum(s["shuffleWriteBytes"] for s in stages) / 1024 ** 2,
            "shuffle_read_mb": sum(s["shuffleReadBytes"] for s in stages) / 1024 ** 2,
            "shuffle_write_records": sum(s["shuffleWriteRecords"] for s in stages),
            "stages": len(stages),
            "tasks": sum(s["numCompleteTasks"] for s in stages),
        }
    except Exception as e:  # Spark UI вимкнений або недоступний — час усе одно виміряно
        print(f"  (метрики перетасування недоступні: {e})")
        return {}


def measure(spark, df, prepare, label, runs=RUNS):
    """Вимірює час обчислення (без читання даних): 1 прогрів + runs запусків, медіана."""
    average_session_duration(prepare(df))  # прогрів JVM / JIT / кодогенерації
    times, metrics, result = [], {}, None
    for i in range(runs):
        job_group = f"{label}#{i}#{time.time_ns()}"
        spark.sparkContext.setJobGroup(job_group, label)
        start = time.perf_counter()
        result = average_session_duration(prepare(df))
        times.append(time.perf_counter() - start)
        metrics = shuffle_metrics(spark, job_group)
    spark.sparkContext.setJobGroup("", "")
    return {"label": label, "avg_duration_ms": result, "time_s": statistics.median(times),
            "times_s": times, **metrics}


def print_measurement(m):
    line = f"  {m['label']:<52} час {m['time_s']:.3f} с"
    if "shuffle_write_mb" in m:
        line += (f" | shuffle write {m['shuffle_write_mb']:7.1f} MB"
                 f" ({m['shuffle_write_records']:,} записів), stages {m['stages']}, tasks {m['tasks']}")
    print(line)


def partition_stats(df):
    """Розмір кожної партиції — основний інструмент виявлення перекосу."""
    sizes = sorted(r["count"] for r in df.groupBy(F.spark_partition_id().alias("pid")).count().collect())
    total = sum(sizes)
    return {"partitions": len(sizes), "max_rows": sizes[-1], "median_rows": statistics.median(sizes),
            "max_share": sizes[-1] / total, "max_to_median": sizes[-1] / statistics.median(sizes)}


# ---------------------------------------------------------------------------
# Розділ 2: порівняння стратегій розподілу
# ---------------------------------------------------------------------------
def compare_strategies(spark, df):
    print("\n2. ПОРІВНЯННЯ СТРАТЕГІЙ РОЗПОДІЛУ")
    print(f"   spark.sql.shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}, "
          f"AQE = {spark.conf.get('spark.sql.adaptive.enabled')}")

    strategies = {
        # Алгоритм 1 / Стратегія A: налаштування за замовчуванням + repartition(N) без ключа.
        # Round-robin перемішує ВСІ сирі події, а потім groupBy робить ще одне перетасування
        # за ключем; часткова агрегація майже не зменшує дані, бо події сесії розкидані.
        f"A: High shuffle (repartition({HIGH_SHUFFLE_PARTITIONS}) + groupBy)":
            lambda d: d.repartition(HIGH_SHUFFLE_PARTITIONS),
        # Базовий варіант: просто groupBy з налаштуваннями за замовчуванням (200 shuffle-партицій)
        "Default (groupBy, 200 shuffle partitions)":
            lambda d: d,
        # Алгоритм 2 / Стратегія B: hash-розподіл за ключем групування, кількість партицій = ядрам.
        # Дані перетасовуються ОДИН раз, і groupBy вже не потребує додаткового Exchange.
        f"B: Low shuffle (repartition({TOTAL_CORES}, user_id, session_id))":
            lambda d: d.repartition(TOTAL_CORES, "user_id", "session_id"),
    }

    results = []
    for label, prepare in strategies.items():
        m = measure(spark, df, prepare, label)
        print_measurement(m)
        results.append(m)

    # Кількість операцій Exchange (перетасувань) у фізичному плані
    print("\n   Кількість Exchange у фізичному плані:")
    for (label, prepare), m in zip(strategies.items(), results):
        plan = session_durations(prepare(df))._jdf.queryExecution().executedPlan().toString()
        m["exchanges"] = plan.count("Exchange ")
        print(f"   {label:<52} {m['exchanges']}")

    averages = {round(m["avg_duration_ms"], 6) for m in results}
    assert len(averages) == 1, "Стратегії дали різні результати!"
    print(f"\n   Середня тривалість сесії (усі стратегії однакові): "
          f"{results[0]['avg_duration_ms']:.0f} мс = {results[0]['avg_duration_ms'] / 60000:.2f} хв")

    a, b = results[0], results[-1]
    print(f"   Прискорення B відносно A: {a['time_s'] / b['time_s']:.2f}x")
    if a.get("shuffle_write_mb") and b.get("shuffle_write_mb"):
        print(f"   Обсяг shuffle write: A = {a['shuffle_write_mb']:.1f} MB, B = {b['shuffle_write_mb']:.1f} MB "
              f"(у {a['shuffle_write_mb'] / b['shuffle_write_mb']:.2f} раза менше)")
    return results


# ---------------------------------------------------------------------------
# Розділ 3: закон Амдала
# ---------------------------------------------------------------------------
def amdahl_analysis(spark):
    print("\n3. МОДЕЛЮВАННЯ ПРОДУКТИВНОСТІ: ЗАКОН АМДАЛА")
    print("   Вимірюємо Стратегію B на різній кількості ядер (n), партицій = n")

    timings = {}
    for cores in AMDAHL_CORES:
        spark.stop()
        spark = create_spark(cores)
        df = load_cached(spark, DATA_PATH)
        m = measure(spark, df, lambda d, n=cores: d.repartition(n, "user_id", "session_id"),
                    f"n = {cores}")
        timings[cores] = m["time_s"]
        print_measurement(m)
        df.unpersist()

    # Модель Амдала: T(n) = T_seq + T_par / n — лінійна відносно 1/n.
    # Метод найменших квадратів дає T_seq (послідовна частина) і T_par (паралельна частина).
    xs = [1 / n for n in AMDAHL_CORES]
    ys = [timings[n] for n in AMDAHL_CORES]
    x_mean, y_mean = statistics.mean(xs), statistics.mean(ys)
    t_par = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / sum((x - x_mean) ** 2 for x in xs)
    t_seq = y_mean - t_par * x_mean
    t_seq, t_par = max(t_seq, 0.0), max(t_par, 0.0)
    p = t_par / (t_seq + t_par)
    s_max = 1 / (1 - p) if p < 1 else float("inf")

    print(f"\n   Апроксимація: T(n) = {t_seq:.3f} + {t_par:.3f} / n  (сек)")
    print(f"   Паралельна частка p = {p:.3f}, послідовна частка (1 - p) = {1 - p:.3f}")
    print(f"   Максимально досяжне прискорення S_max = 1 / (1 - p) = {s_max:.2f}x")

    t1 = timings[AMDAHL_CORES[0]]
    print(f"\n   {'n':>3} | {'T(n), с':>8} | {'S вимір.':>8} | {'S Амдал':>8} | {'Ефект.':>6} | {'Karp-Flatt e':>12}")
    table = []
    for n in AMDAHL_CORES:
        s_meas = t1 / timings[n]
        s_model = 1 / ((1 - p) + p / n)
        efficiency = s_meas / n
        karp_flatt = (1 / s_meas - 1 / n) / (1 - 1 / n) if n > 1 else None
        kf = f"{karp_flatt:12.3f}" if karp_flatt is not None else f"{'-':>12}"
        print(f"   {n:>3} | {timings[n]:8.3f} | {s_meas:8.2f} | {s_model:8.2f} | {efficiency:6.2f} | {kf}")
        table.append({"cores": n, "time_s": timings[n], "speedup_measured": s_meas,
                      "speedup_amdahl": s_model, "efficiency": efficiency, "karp_flatt": karp_flatt})

    print("""
   Послідовні компоненти (не масштабуються з ядрами):
     - планування запиту та кодогенерація на драйвері, планування задач (task scheduling);
     - фінальна агрегація AVG в одній партиції та collect() результату на драйвер;
     - накладні витрати на запуск stages і бар'єри синхронізації між ними.
   Паралельні компоненти:
     - сканування кешованих партицій, часткова агрегація MAX/MIN у кожній задачі;
     - запис/читання shuffle-блоків і фінальна агрегація за ключем (user_id, session_id).
   Примітка: на одній машині ядра ділять пам'ять і диск, тому реальний ріст прискорення
   сповільнюється сильніше, ніж передбачає закон Амдала (див. зростання Karp-Flatt e).""")

    return spark, {"t_seq": t_seq, "t_par": t_par, "p": p, "s_max": s_max, "table": table}


# ---------------------------------------------------------------------------
# Розділ 4: асиметрія (skew) у стовпці user_id
# ---------------------------------------------------------------------------
def skew_analysis(spark):
    print("\n4. АНАЛІЗ АСИМЕТРІЇ ДАНИХ (90% сесій належать одному користувачу)")
    df = load_cached(spark, SKEWED_DATA_PATH)

    # Виявлення 1: розподіл кількості сесій за користувачами
    sessions_per_user = df.select("user_id", "session_id").distinct().groupBy("user_id").count()
    total_sessions = sessions_per_user.agg(F.sum("count")).collect()[0][0]
    top = sessions_per_user.orderBy(F.desc("count")).limit(3).collect()
    print("   Топ користувачів за кількістю сесій:")
    for row in top:
        print(f"     {row['user_id']:<14} {row['count']:>9,} сесій ({row['count'] / total_sessions:.1%})")

    # Виявлення 2: розміри партицій при розподілі за різними ключами
    variants = {
        "Naive: repartition(user_id)":
            lambda d: d.repartition(TOTAL_CORES, "user_id"),
        "Fix 1: repartition(user_id, session_id)":
            lambda d: d.repartition(TOTAL_CORES, "user_id", "session_id"),
        # Сіль обчислюється з session_id, тому всі події однієї сесії мають однакову сіль
        # і агрегацію MAX-MIN можна робити за один крок без перерахунку
        f"Fix 2: salting (user_id, salt[{SALT_BUCKETS}])":
            lambda d: d.withColumn("salt", F.pmod(F.hash("session_id"), F.lit(SALT_BUCKETS)))
                       .repartition(TOTAL_CORES, "user_id", "salt"),
    }

    print(f"\n   Розподіл рядків по {TOTAL_CORES} партиціях:")
    results = []
    for label, prepare in variants.items():
        stats = partition_stats(prepare(df))
        print(f"   {label:<52} найбільша партиція = {stats['max_share']:.1%} рядків, "
              f"max/median = {stats['max_to_median']:.1f}")
        results.append({"label": label, **stats})

    print("\n   Час обчислення середньої тривалості сесії:")
    for r, (label, prepare) in zip(results, variants.items()):
        m = measure(spark, df, prepare, label)
        print_measurement(m)
        r.update(m)

    print("""
   Висновки щодо skew:
     - Виявлення: розподіл кількості подій/сесій за user_id (top-N, частка лідера), розміри
       партицій (spark_partition_id), а у Spark UI — задача-«відстаючий» (straggler), у якої
       тривалість і Shuffle Read значно більші за медіану по stage.
     - Якщо розподіляти дані лише за user_id, 90% рядків потрапляють в одну партицію: одне
       ядро виконує майже всю роботу, решта простоюють — паралелізм зводиться нанівець
       (за Амдалом це як збільшення послідовної частки до ~0.9).
     - Пом'якшення: (1) розподіл за складеним ключем (user_id, session_id), який і так є
       ключем групування; (2) salting — додавання солі до «гарячого» ключа з подальшою
       дво-етапною агрегацією для метрик на рівні користувача; (3) AQE
       (spark.sql.adaptive.skewJoin.enabled) автоматично ділить перекошені партиції в JOIN;
       (4) окрема обробка «гарячих» ключів (isolate hot keys) або broadcast для малих таблиць.""")

    df.unpersist()
    return results


def run_lab4():
    print("1. ІНІЦІАЛІЗАЦІЯ SPARK")
    print(f"   Конфігурація: X = {NUM_EXECUTORS} виконавець(ів) x Y = {CORES_PER_EXECUTOR} ядер "
          f"= {TOTAL_CORES} паралельних задач (master local[{TOTAL_CORES}])")
    spark = create_spark(TOTAL_CORES)
    spark.sparkContext.setLogLevel("ERROR")

    df = load_cached(spark, DATA_PATH)
    print(f"   Завантажено та закешовано записів: {df.count():,}")
    df.printSchema()

    strategies = compare_strategies(spark, df)
    df.unpersist()

    spark, amdahl = amdahl_analysis(spark)

    spark.stop()
    spark = create_spark(TOTAL_CORES)
    skew = skew_analysis(spark)
    spark.stop()

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump({"config": {"executors": NUM_EXECUTORS, "cores_per_executor": CORES_PER_EXECUTOR},
                   "strategies": strategies, "amdahl": amdahl, "skew": skew},
                  f, indent=2, ensure_ascii=False)
    print(f"\nРезультати для звіту збережено у {RESULTS_PATH}")


if __name__ == "__main__":
    run_lab4()
