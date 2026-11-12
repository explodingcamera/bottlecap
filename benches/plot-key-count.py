#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib>=3.10", "seaborn>=0.13"]
# ///
"""Plot Criterion's key-count baseline: ./benches/plot-key-count.py."""

import argparse
import json
import math
import tomllib
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--results", type=Path, default=root / "target/criterion")
parser.add_argument("--baseline", default="key-count")
parser.add_argument("--workers", type=int, choices=[1, 8], default=8)
parser.add_argument("--output", type=Path)
args = parser.parse_args()
group = "ip_count_single_thread" if args.workers == 1 else "ip_count"
if args.output is None:
    filename = "key-count-single-thread.svg" if args.workers == 1 else "key-count.svg"
    args.output = root / "benches" / filename

keys = [1 << exponent for exponent in range(10, 22)]
versions = {package["name"]: package["version"] for package in
            tomllib.loads((root / "Cargo.lock").read_text())["package"]}
series = []
for library, label, color in [
    ("bottlecap", f"Bottlecap {versions['bottlecap']}", "#58a6ff"),
    ("governor", f"Governor {versions['governor']}", "#3fb950"),
]:
    rates = []
    for key in keys:
        directory = args.results / group / library / str(key) / args.baseline
        try:
            benchmark = json.loads((directory / "benchmark.json").read_text())
            estimates = json.loads((directory / "estimates.json").read_text())
        except FileNotFoundError:
            parser.error(f"missing {directory}, run {group}/ with --save-baseline {args.baseline}")
        estimate = estimates.get("slope") or estimates["mean"]
        # Criterion records nanoseconds per iteration and checks per iteration.
        rates.append(benchmark["throughput"]["Elements"] * 1_000 / estimate["point_estimate"])
    series.append((label, color, rates))

background, foreground = "#161b22", "#e6edf3"
sns.set_theme(style="darkgrid", rc={
    "figure.facecolor": background,
    "axes.facecolor": background,
    "axes.edgecolor": "#6e7681",
    "axes.labelcolor": foreground,
    "text.color": foreground,
    "xtick.color": foreground,
    "ytick.color": foreground,
    "grid.color": "#30363d",
    "grid.linewidth": 0.7,
    "svg.fonttype": "none",
    "svg.hashsalt": f"bottlecap-key-count-{args.workers}",
})
figure, axis = plt.subplots(figsize=(11, 5.4))
for label, color, rates in series:
    axis.plot(keys, rates, label=label, color=color, marker="o", markersize=4.5, linewidth=2)

maximum = max(rate for _, _, rates in series for rate in rates)
for index, key in enumerate(keys):
    order = sorted(range(len(series)), key=lambda item: series[item][2][index])
    for rank, item in enumerate(order):
        _, color, rates = series[item]
        if rank == 0:
            if rates[index] < maximum * 0.08:
                offset, horizontal, vertical = (0, 6), "center", "bottom"
            else:
                offset, horizontal, vertical = (0, -10), "center", "top"
        elif rank == len(series) - 1:
            offset, horizontal, vertical = (0, 9), "center", "bottom"
        else:
            offset, horizontal, vertical = (7, 0), "left", "center"
        axis.annotate(
            f"{rates[index]:,.0f}", (key, rates[index]), xytext=offset,
            textcoords="offset points", ha=horizontal, va=vertical,
            color=color, fontsize=8.5,
            bbox={"facecolor": background, "edgecolor": "none", "pad": 0.8, "alpha": 0.9},
        )

axis.set_xscale("log", base=2)
axis.set_xticks(keys, [f"{key // 1024}K" if key < 1_048_576 else f"{key // 1_048_576}M" for key in keys])
axis.xaxis.set_minor_locator(ticker.NullLocator())
axis.set_xlim(keys[0] / 1.3, keys[-1] * 1.4)
axis.set_ylim(0, math.ceil(maximum * 1.1 / 25) * 25)
axis.yaxis.set_major_locator(ticker.MaxNLocator(6, integer=True))
axis.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}"))
axis.set_xlabel("Active IPs", labelpad=12)
axis.set_ylabel("Throughput · million checks/s", labelpad=12)
threads = "1 thread" if args.workers == 1 else f"{args.workers} threads"
axis.set_title(f"Per-IP rate limiting · {threads}", loc="left", fontsize=14, weight="bold", pad=16)
axis.tick_params(labelsize=10)
axis.legend(loc="upper right", frameon=True, facecolor=background,
            edgecolor="none", framealpha=1, fontsize=10)
sns.despine(ax=axis)
figure.tight_layout()
figure.savefig(args.output, format="svg", bbox_inches="tight", pad_inches=0.18, metadata={
    "Date": None,
    "Title": f"Per-IP rate-limit throughput · {threads}",
    "Description": "Criterion measurements. Logarithmic key counts, zero-based linear throughput. "
                   "Throughput is in millions of checks per second. Higher is better. "
                   "Mixed IPv4/IPv6 clients with 20% shared hot-client traffic. "
                    "Bottlecap and Governor use fixed-seed rapidhash and their default clocks.",
})
plt.close(figure)

# Keep the asset embeddable directly in rustdoc, without an XML declaration or DTD.
svg = args.output.read_text()
svg = svg[svg.index("<svg "):].replace(
    "<svg ", '<svg role="img" aria-label="Per-IP rate-limit throughput" '
    'style="max-width:100%;height:auto" ', 1,
)
# Inherit the active rustdoc theme, with standalone dark-theme fallbacks.
for color, themed in [
    (background, "var(--code-block-background-color, #2a2a2a)"),
    (foreground, "var(--main-color, #dddddd)"),
    ("#30363d", "var(--border-color, #555555)"),
    ("#6e7681", "var(--main-color, #dddddd)"),
]:
    svg = svg.replace(color, themed)
args.output.write_text("\n".join(line.rstrip() for line in svg.splitlines()) + "\n")
print(args.output)
