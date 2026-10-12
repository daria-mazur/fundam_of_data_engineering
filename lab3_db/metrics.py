import json
import os

import matplotlib.pyplot as plt

from pipeline import STORAGE_DIR


def load_json_count(file_path):
    if not os.path.exists(file_path):
        return 0
    with open(file_path, "r", encoding="utf-8") as f:
        return len(json.load(f))


def calculate_metrics(storage_dir=STORAGE_DIR):
    # Лічильники, які веде сам конвеєр під час обробки
    with open(os.path.join(storage_dir, "pipeline_metrics.json"), "r", encoding="utf-8") as f:
        counters = json.load(f)

    # Звірка лічильників із фактичним вмістом сховища (контроль цілісності)
    clean_count = load_json_count(os.path.join(storage_dir, "clean_data.json"))
    dlq_count = load_json_count(os.path.join(storage_dir, "dlq_data.json"))
    assert clean_count == counters["clean"], "Кількість чистих записів не збігається з метриками"
    assert dlq_count == counters["dlq"], "Кількість записів DLQ не збігається з метриками"
    assert counters["received"] == counters["clean"] + counters["dlq"] + counters["duplicates"], \
        "Отримані записи не дорівнюють сумі чистих, DLQ і дублікатів"

    unique = counters["clean"] + counters["dlq"]
    return {
        "Clean Records": counters["clean"],
        "Validation Errors (DLQ)": counters["dlq"],
        "Duplicates Skipped": counters["duplicates"],
        "Fetch Retries": counters["fetch_retries"],
    }, {
        "Отримано записів": counters["received"],
        "Частка валідних (з унікальних)": f"{counters['clean'] / unique:.1%}" if unique else "n/a",
        "Частка дублікатів (з отриманих)":
            f"{counters['duplicates'] / counters['received']:.1%}" if counters["received"] else "n/a",
        "Втрачених батчів (після всіх повторів)": counters["fetch_failures"],
    }


def plot_metrics(metrics):
    labels = list(metrics.keys())
    values = list(metrics.values())
    colors = ["#4CAF50", "#F44336", "#FF9800", "#2196F3"]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, values, color=colors, width=0.5)

    ax.set_ylabel("Кількість")
    ax.set_title("Результати обробки конвеєра даних")
    ax.grid(axis="y", linestyle="--", alpha=0.7)

    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2.0, yval + 0.1, int(yval), ha="center", va="bottom",
                fontweight="bold")

    plt.tight_layout()

    output_img = os.path.join(STORAGE_DIR, "pipeline_metrics.png")
    plt.savefig(output_img, dpi=300)
    print(f"Графік успішно збережено у {output_img}")
    plt.show()


if __name__ == "__main__":
    metrics, summary = calculate_metrics()

    print("=== Метрики роботи конвеєра ===")
    for key, val in {**metrics, **summary}.items():
        print(f"{key}: {val}")

    plot_metrics(metrics)
