"""Побудова графіків для звітів з результатів запусків лабораторних робіт."""
import json
import os
import shutil

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPORTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(REPORTS_DIR)
FIG_DIR = os.path.join(REPORTS_DIR, "figures")
os.makedirs(FIG_DIR, exist_ok=True)

GREY, ACCENT = "#9aa0a6", "#2b5c8f"
plt.rcParams.update({"font.size": 11, "axes.spines.top": False, "axes.spines.right": False})


def annotate(ax, bars, fmt):
    for b in bars:
        ax.annotate(fmt.format(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                    xytext=(0, 3), textcoords="offset points", ha="center", va="bottom", fontsize=9)


# --- Лаб 1: результати з виводу lab1_db/main.py ---------------------------------
formats = ["CSV", "Parquet\n(None)", "Parquet\n(Snappy)", "Parquet\n(GZIP)"]
sizes = [61.59, 33.42, 18.65, 12.16]
q1 = [0.1749, 0.0494, 0.0492, 0.0503]
q2 = [0.1746, 0.0164, 0.0165, 0.0176]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
bars = ax1.bar(formats, sizes, color=[GREY, ACCENT, ACCENT, ACCENT])
annotate(ax1, bars, "{:.1f} MB")
ax1.set_title("Розмір файлу, MB")
x = range(len(formats))
b1 = ax2.bar([i - 0.2 for i in x], [t * 1000 for t in q1], 0.4, label="Запит 1 (агрегація)", color=GREY)
b2 = ax2.bar([i + 0.2 for i in x], [t * 1000 for t in q2], 0.4, label="Запит 2 (фільтрація)", color=ACCENT)
annotate(ax2, list(b1) + list(b2), "{:.0f}")
ax2.set_xticks(list(x))
ax2.set_xticklabels(formats)
ax2.set_title("Час виконання запиту, мс (середнє з 5)")
ax2.legend(frameon=False)
plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "lab1_results.png"), dpi=200)
plt.close()

# --- Лаб 2, 3: готові графіки зі скриптів лабораторних -------------------------
shutil.copy(os.path.join(ROOT, "lab2_db", "data", "benchmark_result.png"), os.path.join(FIG_DIR, "lab2_benchmark.png"))
shutil.copy(os.path.join(ROOT, "lab3_db", "storage", "pipeline_metrics.png"), os.path.join(FIG_DIR, "lab3_metrics.png"))

# --- Лаб 4: результати з lab4_db/data/lab4_results.json --------------------------
with open(os.path.join(ROOT, "lab4_db", "data", "lab4_results.json"), encoding="utf-8") as f:
    res = json.load(f)

names = ["A: repartition(200)", "Default groupBy", "B: repartition(4, keys)"]
strat = res["strategies"]
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
bars = ax1.bar(names, [s["time_s"] for s in strat], color=[GREY, GREY, ACCENT])
annotate(ax1, bars, "{:.2f} с")
ax1.set_title("Час обчислення, с (медіана з 3)")
bars = ax2.bar(names, [s["shuffle_write_mb"] for s in strat], color=[GREY, GREY, ACCENT])
annotate(ax2, bars, "{:.0f} MB")
ax2.set_title("Shuffle write, MB")
for ax in (ax1, ax2):
    ax.tick_params(axis="x", labelsize=9)
plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "lab4_strategies.png"), dpi=200)
plt.close()

amd = res["amdahl"]
cores = [r["cores"] for r in amd["table"]]
fig, ax = plt.subplots(figsize=(7, 4.2))
ax.plot(cores, cores, linestyle=":", color=GREY, label="Ідеальне (лінійне) прискорення")
ax.plot(cores, [r["speedup_amdahl"] for r in amd["table"]], color=GREY, marker="s",
        label=f"Модель Амдала, p = {amd['p']:.2f}")
ax.plot(cores, [r["speedup_measured"] for r in amd["table"]], color=ACCENT, marker="o", linewidth=2,
        label="Виміряне прискорення")
ax.axhline(amd["s_max"], color=GREY, linestyle="--", linewidth=1)
ax.text(1.1, amd["s_max"] - 0.6, f"S_max = 1/(1-p) = {amd['s_max']:.1f}", color="#555", fontsize=9)
ax.set_xticks(cores)
ax.set_xlabel("Кількість ядер n")
ax.set_ylabel("Прискорення S(n) = T(1) / T(n)")
ax.legend(frameon=False, loc="center right")
plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "lab4_amdahl.png"), dpi=200)
plt.close()

skew = res["skew"]
labels = ["Наївний:\nrepartition(user_id)", "Fix 1:\n(user_id, session_id)", "Fix 2:\nsalting"]
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))
bars = ax1.bar(labels, [s["max_share"] * 100 for s in skew], color=[GREY, ACCENT, ACCENT])
annotate(ax1, bars, "{:.0f}%")
ax1.axhline(25, color=GREY, linestyle="--", linewidth=1, label="Рівномірний розподіл: 25%")
ax1.legend(frameon=False, loc="upper right")
ax1.set_title("Частка рядків у найбільшій партиції (з 4)")
bars = ax2.bar(labels, [s["time_s"] for s in skew], color=[GREY, ACCENT, ACCENT])
annotate(ax2, bars, "{:.2f} с")
ax2.set_title("Час обчислення, с")
plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "lab4_skew.png"), dpi=200)
plt.close()

print("Графіки збережено у", FIG_DIR)
