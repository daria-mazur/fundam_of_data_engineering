import random
import time
import uuid
from datetime import datetime


class UnreliableDataGenerator:

    def __init__(self, failure_rate=0.3):
        self.failure_rate = failure_rate
        self.generated_ids = []

    def generate_record(self):
        # З ймовірністю failure_rate повертаємо дублікат попереднього запису
        if self.generated_ids and random.random() < (self.failure_rate / 2):
            record_id = random.choice(self.generated_ids)
        else:
            record_id = str(uuid.uuid4())
            self.generated_ids.append(record_id)

        timestamp = datetime.now().isoformat()
        sensor_id = f"sensor_{random.randint(1, 5)}"
        temperature = round(random.uniform(15.0, 30.0), 2)
        humidity = round(random.uniform(40.0, 80.0), 2)

        if random.random() < self.failure_rate:
            error_type = random.choice(
                ["missing_field", "corrupted_value", "out_of_bounds"]
            )

            if error_type == "missing_field":
                temperature = None
            elif error_type == "corrupted_value":
                temperature = "INVALID_TEMP"
            elif error_type == "out_of_bounds":
                temperature = 999.99

        return {
            "record_id": record_id,
            "timestamp": timestamp,
            "sensor_id": sensor_id,
            "temperature": temperature,
            "humidity": humidity,
        }

    def generate_stream(self, batch_size=10):
        return [self.generate_record() for _ in range(batch_size)]