import logging
import os
import shutil
import sys
import time

from data_generator import SourceUnavailableError, UnreliableDataGenerator
from pipeline import STORAGE_DIR, FaultTolerantPipeline

MAX_RETRIES = 4
BASE_BACKOFF_SEC = 0.2


def fetch_with_retry(generator, pipeline, batch_size):
    """Отримання батча з повторними спробами та експоненційною затримкою."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return generator.generate_stream(batch_size=batch_size)
        except SourceUnavailableError as e:
            if attempt == MAX_RETRIES:
                pipeline.record_fetch_failure()
                logging.error(f"[Отримання] Вичерпано {MAX_RETRIES} спроб: {e}. Батч пропущено.")
                return None
            delay = BASE_BACKOFF_SEC * 2 ** (attempt - 1)
            pipeline.record_fetch_retry()
            logging.warning(f"[Отримання] Спроба {attempt} невдала: {e}. Повтор через {delay:.1f} с")
            time.sleep(delay)


def main(iterations=10, batch_size=10):
    generator = UnreliableDataGenerator(failure_rate=0.4, duplicate_rate=0.2, outage_rate=0.3)
    pipeline = FaultTolerantPipeline()

    print("Запуск відмовостійкого конвеєра даних")

    for iteration in range(1, iterations + 1):
        print(f"\n--- Ітерація {iteration} ---")
        stream_batch = fetch_with_retry(generator, pipeline, batch_size)
        if stream_batch is None:
            continue

        print(f"Отримано {len(stream_batch)} записів з джерела.")
        pipeline.process_batch(stream_batch)

    # Демонстрація ідемпотентності: повторна доставка вже обробленого батча
    # (наприклад, після перезапуску споживача) не створює дублікатів у сховищі
    print("\n--- Повторна доставка (replay) перших 5 записів ---")
    pipeline.process_batch([dict(r) for r in generator.sent_records[:5]])

    print("\nОбробка завершена. Перевірте папку storage/ і запустіть metrics.py")


if __name__ == "__main__":
    # python process.py --reset  — очистити сховище перед запуском
    if "--reset" in sys.argv and os.path.isdir(STORAGE_DIR):
        shutil.rmtree(STORAGE_DIR)
    main()
