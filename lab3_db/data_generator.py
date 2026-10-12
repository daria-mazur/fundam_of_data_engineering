import copy
import random
import uuid
from datetime import datetime


class SourceUnavailableError(ConnectionError):
    """Тимчасова відмова джерела (мережа, таймаут) — запит можна повторити."""


class UnreliableDataGenerator:

    def __init__(self, failure_rate=0.3, duplicate_rate=0.15, outage_rate=0.3, seed=None):
        self.failure_rate = failure_rate      # частка пошкоджених записів
        self.duplicate_rate = duplicate_rate  # частка повторно доставлених записів
        self.outage_rate = outage_rate        # ймовірність тимчасової відмови джерела
        self.random = random.Random(seed)
        self.sent_records = []

    def _new_record(self):
        record = {
            "record_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "sensor_id": f"sensor_{self.random.randint(1, 5)}",
            "temperature": round(self.random.uniform(15.0, 30.0), 2),
            "humidity": round(self.random.uniform(40.0, 80.0), 2),
        }

        if self.random.random() < self.failure_rate:
            error_type = self.random.choice(
                ["missing_field", "null_value", "corrupted_value", "out_of_bounds", "bad_timestamp"]
            )

            if error_type == "missing_field":
                del record[self.random.choice(["sensor_id", "temperature", "humidity"])]
            elif error_type == "null_value":
                record["temperature"] = None
            elif error_type == "corrupted_value":
                record["temperature"] = "INVALID_TEMP"
            elif error_type == "out_of_bounds":
                record["temperature"] = 999.99
            elif error_type == "bad_timestamp":
                record["timestamp"] = "not-a-date"

        return record

    def generate_record(self):
        # Дублікат — це повторна доставка вже надісланого запису (семантика at-least-once):
        # той самий record_id і ті самі значення
        if self.sent_records and self.random.random() < self.duplicate_rate:
            record = copy.deepcopy(self.random.choice(self.sent_records))
        else:
            record = self._new_record()
            self.sent_records.append(copy.deepcopy(record))
        return record

    def generate_stream(self, batch_size=10):
        # Імітація тимчасової недоступності джерела
        if self.random.random() < self.outage_rate:
            raise SourceUnavailableError("Джерело даних тимчасово недоступне")
        return [self.generate_record() for _ in range(batch_size)]
