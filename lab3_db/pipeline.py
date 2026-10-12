import json
import logging
import os
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage")

REQUIRED_KEYS = ["record_id", "timestamp", "sensor_id", "temperature", "humidity"]
METRIC_KEYS = ["received", "clean", "dlq", "duplicates", "fetch_retries", "fetch_failures"]


def _is_number(value):
    # bool є підкласом int у Python, тому виключаємо його явно
    return isinstance(value, (int, float)) and not isinstance(value, bool)


class FaultTolerantPipeline:

    def __init__(self, storage_dir=STORAGE_DIR):
        self.output_path = os.path.join(storage_dir, "clean_data.json")
        self.dlq_path = os.path.join(storage_dir, "dlq_data.json")
        self.state_path = os.path.join(storage_dir, "processed_ids.txt")
        self.metrics_path = os.path.join(storage_dir, "pipeline_metrics.json")

        os.makedirs(storage_dir, exist_ok=True)
        self.processed_ids = self._load_state()
        self.metrics = self._load_metrics()

    def _load_state(self):
        """Зчитує ID вже оброблених записів для забезпечення ідемпотентності."""
        if os.path.exists(self.state_path):
            with open(self.state_path, "r") as f:
                return set(line.strip() for line in f if line.strip())
        return set()

    def _save_state_ids(self, record_ids):
        """Зберігає оброблені ID у стан (лише ПІСЛЯ успішного запису даних)."""
        if not record_ids:
            return
        with open(self.state_path, "a") as f:
            f.writelines(f"{record_id}\n" for record_id in record_ids)
            f.flush()
            os.fsync(f.fileno())

    def _load_metrics(self):
        metrics = {key: 0 for key in METRIC_KEYS}
        if os.path.exists(self.metrics_path):
            with open(self.metrics_path, "r", encoding="utf-8") as f:
                metrics.update(json.load(f))
        return metrics

    def record_fetch_retry(self):
        self.metrics["fetch_retries"] += 1
        self._atomic_write_json(self.metrics_path, self.metrics)

    def record_fetch_failure(self):
        self.metrics["fetch_failures"] += 1
        self._atomic_write_json(self.metrics_path, self.metrics)

    def validate_record(self, record):
        """Перевіряє цілісність та допустимість даних."""
        if not isinstance(record, dict):
            return False, "Формат запису не є словником"

        for key in REQUIRED_KEYS:
            if key not in record:
                return False, f"Відсутнє обов'язкове поле: {key}"

        if not isinstance(record["record_id"], str) or not record["record_id"]:
            return False, "Некоректний record_id"

        if not isinstance(record["sensor_id"], str) or not record["sensor_id"]:
            return False, f"Некоректний sensor_id: {record['sensor_id']}"

        try:
            datetime.fromisoformat(record["timestamp"])
        except (TypeError, ValueError):
            return False, f"Некоректний timestamp: {record['timestamp']}"

        temp = record["temperature"]
        hum = record["humidity"]

        if temp is None or hum is None:
            return False, "Значення temperature або humidity є None"

        if not _is_number(temp) or not _is_number(hum):
            return False, f"Некоректний тип даних для атрибутів: temp={temp}, hum={hum}"

        if not (-50.0 <= temp <= 100.0):
            return False, f"Температура виходить за межі норми: {temp}"

        if not (0.0 <= hum <= 100.0):
            return False, f"Вологість виходить за межі норми: {hum}"

        return True, "OK"

    def process_batch(self, batch):
        clean_records = []
        dlq_records = []
        handled_ids = []

        for record in batch:
            self.metrics["received"] += 1
            record_id = record.get("record_id") if isinstance(record, dict) else None

            # Перевірка на дублікати (Ідемпотентність): кожен record_id обробляється
            # рівно один раз — і для чистих записів, і для записів у DLQ
            if record_id in self.processed_ids:
                self.metrics["duplicates"] += 1
                logging.warning(
                    f"[Дублікат] Запис {record_id} вже оброблено. Пропускаємо."
                )
                continue

            # Валідація даних
            is_valid, reason = self.validate_record(record)

            if is_valid:
                clean_records.append(record)
            else:
                logging.error(f"[Помилка валідації] ID {record_id}: {reason}")
                dlq_records.append({
                    "record": record,
                    "reason": reason,
                    "failed_at": datetime.now().isoformat(),
                })

            if record_id is not None:
                self.processed_ids.add(record_id)
                handled_ids.append(record_id)

        # Спочатку надійно записуємо дані, і лише потім фіксуємо стан.
        # Якщо процес впаде між цими кроками, при повторному запуску запис
        # буде оброблено ще раз, а не втрачено (at-least-once + дедуплікація).
        self._append_records(self.output_path, clean_records)
        self._append_records(self.dlq_path, dlq_records)
        self._save_state_ids(handled_ids)

        self.metrics["clean"] += len(clean_records)
        self.metrics["dlq"] += len(dlq_records)
        self._atomic_write_json(self.metrics_path, self.metrics)

        logging.info(
            f"Батч оброблено: чистих={len(clean_records)}, DLQ={len(dlq_records)}, "
            f"дублікатів={len(batch) - len(clean_records) - len(dlq_records)}"
        )

    def _append_records(self, file_path, records):
        if not records:
            return

        existing_data = []
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    # Не затираємо пошкоджений файл мовчки — зберігаємо його копію для аналізу
                    backup_path = f"{file_path}.corrupted.{int(time.time())}"
                    os.replace(file_path, backup_path)
                    logging.critical(f"Файл {file_path} пошкоджено, копію збережено як {backup_path}")

        existing_data.extend(records)
        self._atomic_write_json(file_path, existing_data)

    @staticmethod
    def _atomic_write_json(file_path, data):
        # Атомарний запис: спочатку у тимчасовий файл, потім os.replace.
        # Збій посеред запису не залишить напівзаписаний (пошкоджений) JSON.
        tmp_path = f"{file_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, file_path)
