import json
import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class FaultTolerantPipeline:

    def __init__(
        self,
        output_path="storage/clean_data.json",
        dlq_path="storage/dlq_data.json",
        state_path="storage/processed_ids.txt",
    ):
        self.output_path = output_path
        self.dlq_path = dlq_path
        self.state_path = state_path

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        self.processed_ids = self._load_state()

    def _load_state(self):
        """Зчитує ID вже оброблених записів для забезпечення ідемпотентності."""
        if os.path.exists(self.state_path):
            with open(self.state_path, "r") as f:
                return set(line.strip() for line in f if line.strip())
        return set()

    def _save_state_id(self, record_id):
        """Зберігає оброблений ID у стан."""
        self.processed_ids.add(record_id)
        with open(self.state_path, "a") as f:
            f.write(f"{record_id}\n")

    def validate_record(self, record):
        """Перевіряє цілісність та допустимість даних."""
        if not isinstance(record, dict):
            return False, "Формат запису не є словником"

        required_keys = [
            "record_id",
            "timestamp",
            "sensor_id",
            "temperature",
            "humidity",
        ]
        for key in required_keys:
            if key not in record:
                return False, f"Відсутнє обов'язкове поле: {key}"

        temp = record["temperature"]
        hum = record["humidity"]

        if temp is None or hum is None:
            return False, "Значення temperature або humidity є None"

        if not isinstance(temp, (int, float)) or not isinstance(
            hum, (int, float)
        ):
            return False, f"Некоректний тип даних для атрибутів: temp={temp}"

        if not (-50.0 <= temp <= 100.0):
            return False, f"Температура виходить за межі норми: {temp}"

        if not (0.0 <= hum <= 100.0):
            return False, f"Вологість виходить за межі норми: {hum}"

        return True, "OK"

    def process_batch(self, batch):
        clean_records = []
        dlq_records = []

        for record in batch:
            record_id = record.get("record_id")

            # Перевірка на дублікати (Ідемпотентність)
            if record_id in self.processed_ids:
                logging.warning(
                    f"[Дублікат] Запис {record_id} вже оброблено. Пропускаємо."
                )
                continue

            # Валідація даних
            is_valid, reason = self.validate_record(record)

            if is_valid:
                clean_records.append(record)
                self._save_state_id(record_id)
            else:
                logging.error(f"[Помилка валідації] ID {record_id}: {reason}")
                dlq_records.append({"record": record, "reason": reason})

        # Запис у сховище
        self._write_to_file(self.output_path, clean_records)
        self._write_to_file(self.dlq_path, dlq_records)

    def _write_to_file(self, file_path, records):
        if not records:
            return

        existing_data = []
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []

        existing_data.extend(records)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=4, ensure_ascii=False)