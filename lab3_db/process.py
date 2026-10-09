import time
from data_generator import UnreliableDataGenerator
from pipeline import FaultTolerantPipeline


def main():
    generator = UnreliableDataGenerator(failure_rate=0.4)
    pipeline = FaultTolerantPipeline()

    print("Запуск відмовостійкого конвеєра даних")

    for iteration in range(1, 4):
        print(f"\n--- Ітерація {iteration} ---")
        stream_batch = generator.generate_stream(batch_size=5)

        print(f"Отримано {len(stream_batch)} записів з джерела.")
        pipeline.process_batch(stream_batch)

        time.sleep(1)

    print("\n Обробка завершена. Перевірте папку storage/")


if __name__ == "__main__":
    main()