import json
import os
import matplotlib.pyplot as plt

def load_json_count(file_path):
    if not os.path.exists(file_path):
        return 0
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return len(data)
    except Exception:
        return 0

def calculate_metrics(clean_path="storage/clean_data.json", dlq_path="storage/dlq_data.json", total_received=15):
    # Кількість успішних записів
    clean_count = load_json_count(clean_path)
    
    # Кількість пошкоджених записів
    dlq_count = load_json_count(dlq_path)

    processed_count = clean_count + dlq_count
    duplicate_count = max(0, total_received - processed_count)
    
    return {
        "Clean Records": clean_count,
        "Validation Errors (DLQ)": dlq_count,
        "Duplicates Skipped": duplicate_count
    }

def plot_metrics(metrics):
    labels = list(metrics.keys())
    values = list(metrics.values())
    colors = ['#4CAF50', '#F44336', '#FF9800']

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(labels, values, color=colors, width=0.5)

    ax.set_ylabel('Кількість записів')
    ax.set_title('Співвідношення результатів обробки конвеєра даних')
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 0.1, int(yval), ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    
    output_img = "storage/pipeline_metrics.png"
    plt.savefig(output_img, dpi=300)
    print(f"Графік успішно збережено у {output_img}")
    plt.show()

if __name__ == "__main__":
    metrics = calculate_metrics(total_received=15)
    
    print("=== Метрики роботи конвеєра ===")
    for key, val in metrics.items():
        print(f"{key}: {val}")
        
    plot_metrics(metrics)