"""
CS 223 – Plot Generator
Run:  python3 plot_results.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()
BASE = os.path.dirname(os.path.abspath(__file__))
RES  = f"{BASE}/results"
DIST = f"{RES}/dist"
OUT  = f"{RES}/plots"
os.makedirs(OUT, exist_ok=True)

w1 = pd.read_csv(f"{RES}/workload1.csv")
w2 = pd.read_csv(f"{RES}/workload2.csv")

OCC_COLOR  = "#2196F3"
TPL_COLOR  = "#F44336"
FIGSIZE    = (7, 4.5)

plt.rcParams.update({
    "font.family": "sans-serif",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "lines.linewidth": 2,
    "lines.markersize": 7,
})

def save(fig, name):
    path = f"{OUT}/{name}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved → {path}")

def prep_proto(df, protocol, x_col, y_col):
    subset = df[df.protocol == protocol].copy()
    subset = subset.groupby(x_col, as_index=False)[y_col].mean()
    subset = subset.sort_values(x_col)
    return subset

# ─────────────────────────────────────────────────────────────────────────────
# 6.1 – Retry rate vs contention
# ─────────────────────────────────────────────────────────────────────────────
def plot_retries_vs_contention(df, title, fname):
    data = df[df.threads == 4]
    occ = prep_proto(data, "occ", "hotset_prob", "retry_pct")
    tpl = prep_proto(data, "2pl", "hotset_prob", "retry_pct")
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.plot(occ["hotset_prob"], occ["retry_pct"], color=OCC_COLOR, marker="o", label="OCC")
    ax.plot(tpl["hotset_prob"], tpl["retry_pct"], color=TPL_COLOR, marker="s", label="Conservative 2PL")
    ax.set_xlabel("Contention Level (hotset probability)")
    ax.set_ylabel("Transactions Retried (%)")
    ax.set_title(f"{title}\nRetry Rate vs. Contention  (threads=4)")
    ax.legend()
    save(fig, fname)

print("── 6.1 ──")
plot_retries_vs_contention(w1, "Workload 1 (Transfer)",         "6_1_w1_retries_vs_contention")
plot_retries_vs_contention(w2, "Workload 2 (NewOrder+Payment)", "6_1_w2_retries_vs_contention")

# ─────────────────────────────────────────────────────────────────────────────
# 6.2 – Throughput
# ─────────────────────────────────────────────────────────────────────────────
def plot_throughput_vs_threads(df, title, fname):
    data = df[df.hotset_prob == 0.5]
    occ = prep_proto(data, "occ", "threads", "throughput_tps")
    tpl = prep_proto(data, "2pl", "threads", "throughput_tps")
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.plot(occ["threads"], occ["throughput_tps"], color=OCC_COLOR, marker="o", label="OCC")
    ax.plot(tpl["threads"], tpl["throughput_tps"], color=TPL_COLOR, marker="s", label="Conservative 2PL")
    ax.set_xlabel("Number of Threads")
    ax.set_ylabel("Throughput (transactions/sec)")
    ax.set_title(f"{title}\nThroughput vs. Threads  (hotset_prob=0.5)")
    ax.xaxis.set_major_locator(ticker.FixedLocator(sorted(occ["threads"].tolist())))
    ax.legend()
    save(fig, fname)


def plot_throughput_vs_contention(df, title, fname):
    data = df[df.threads == 4]
    occ = prep_proto(data, "occ", "hotset_prob", "throughput_tps")
    tpl = prep_proto(data, "2pl", "hotset_prob", "throughput_tps")
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.plot(occ["hotset_prob"], occ["throughput_tps"], color=OCC_COLOR, marker="o", label="OCC")
    ax.plot(tpl["hotset_prob"], tpl["throughput_tps"], color=TPL_COLOR, marker="s", label="Conservative 2PL")
    ax.set_xlabel("Contention Level (hotset probability)")
    ax.set_ylabel("Throughput (transactions/sec)")
    ax.set_title(f"{title}\nThroughput vs. Contention  (threads=4)")
    ax.legend()
    save(fig, fname)

def plot_throughput_occ_vs_2pl_bar(df, title, fname):
    """
    Extra graph: grouped bar chart comparing OCC vs 2PL throughput
    at each thread count. Cleaner than the broken high/low graph.
    """
    data = df[df.hotset_prob == 0.5]

    occ = prep_proto(data, "occ", "threads", "throughput_tps")
    tpl = prep_proto(data, "2pl", "threads", "throughput_tps")

    threads = sorted(occ["threads"].tolist())
    x = np.arange(len(threads))
    w = 0.35

    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.bar(x - w/2, occ["throughput_tps"], w, color=OCC_COLOR, alpha=0.85, label="OCC")
    ax.bar(x + w/2, tpl["throughput_tps"], w, color=TPL_COLOR, alpha=0.85, label="Conservative 2PL")
    ax.set_xticks(x)
    ax.set_xticklabels([str(t) for t in threads])
    ax.set_xlabel("Number of Threads")
    ax.set_ylabel("Throughput (transactions/sec)")
    ax.set_title(f"{title}\nOCC vs. 2PL Throughput by Thread Count  (hotset_prob=0.5)")
    ax.legend()
    save(fig, fname)

print("── 6.2 ──")
plot_throughput_vs_threads(      w1, "Workload 1 (Transfer)",         "6_2_w1_throughput_vs_threads")
plot_throughput_vs_contention(   w1, "Workload 1 (Transfer)",         "6_2_w1_throughput_vs_contention")
plot_throughput_occ_vs_2pl_bar(  w1, "Workload 1 (Transfer)",         "6_2_w1_throughput_bar")

plot_throughput_vs_threads(      w2, "Workload 2 (NewOrder+Payment)", "6_2_w2_throughput_vs_threads")
plot_throughput_vs_contention(   w2, "Workload 2 (NewOrder+Payment)", "6_2_w2_throughput_vs_contention")
plot_throughput_occ_vs_2pl_bar(  w2, "Workload 2 (NewOrder+Payment)", "6_2_w2_throughput_bar")

# ─────────────────────────────────────────────────────────────────────────────
# 6.3 – Response time
# ─────────────────────────────────────────────────────────────────────────────
def plot_rt_vs_threads(df, title, fname):
    data = df[df.hotset_prob == 0.5]

    occ = prep_proto(data, "occ", "threads", "avg_rt_us")
    tpl = prep_proto(data, "2pl", "threads", "avg_rt_us")

    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.plot(occ["threads"], occ["avg_rt_us"], color=OCC_COLOR, marker="o", label="OCC")
    ax.plot(tpl["threads"], tpl["avg_rt_us"], color=TPL_COLOR, marker="s", label="Conservative 2PL")
    ax.set_xlabel("Number of Threads")
    ax.set_ylabel("Avg Response Time (µs)")
    ax.set_title(f"{title}\nAvg Response Time vs. Threads  (hotset_prob=0.5)")
    ax.xaxis.set_major_locator(ticker.FixedLocator(sorted(occ["threads"].tolist())))
    ax.legend()
    save(fig, fname)

def plot_rt_vs_contention(df, title, fname):
    data = df[df.threads == 4]

    occ = prep_proto(data, "occ", "hotset_prob", "avg_rt_us")
    tpl = prep_proto(data, "2pl", "hotset_prob", "avg_rt_us")

    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.plot(occ["hotset_prob"], occ["avg_rt_us"], color=OCC_COLOR, marker="o", label="OCC")
    ax.plot(tpl["hotset_prob"], tpl["avg_rt_us"], color=TPL_COLOR, marker="s", label="Conservative 2PL")
    ax.set_xlabel("Contention Level (hotset probability)")
    ax.set_ylabel("Avg Response Time (µs)")
    ax.set_title(f"{title}\nAvg Response Time vs. Contention  (threads=4)")
    ax.legend()
    save(fig, fname)

def plot_rt_distribution(dist_csv_occ, dist_csv_tpl, title, fname):
    """
    CDF of response times per tx_type, clipped at 95th percentile
    so the bulk of the distribution is visible.
    """
    occ_df = pd.read_csv(dist_csv_occ)
    tpl_df = pd.read_csv(dist_csv_tpl)
    tx_types = sorted(occ_df["tx_type"].unique())

    fig, axes = plt.subplots(1, len(tx_types), figsize=(6 * len(tx_types), 4.5), squeeze=False)

    for i, tx_type in enumerate(tx_types):
        ax = axes[0][i]

        occ_rt = occ_df[occ_df.tx_type == tx_type]["response_time_us"].sort_values().values
        tpl_rt = tpl_df[tpl_df.tx_type == tx_type]["response_time_us"].sort_values().values

        # Clip x-axis at 95th percentile to avoid outlier distortion
        x_max = np.percentile(np.concatenate([occ_rt, tpl_rt]), 95)

        ax.plot(occ_rt, np.arange(len(occ_rt)) / len(occ_rt),
                color=OCC_COLOR, linewidth=1.5, label="OCC")
        ax.plot(tpl_rt, np.arange(len(tpl_rt)) / len(tpl_rt),
                color=TPL_COLOR, linewidth=1.5, label="Conservative 2PL")

        ax.set_xlim(0, x_max)
        ax.set_xlabel("Response Time (µs)")
        ax.set_ylabel("CDF")
        ax.set_title(f"{tx_type} – Response Time Distribution\n(95th percentile cutoff)")
        ax.legend()

    fig.suptitle(f"{title} – Response Time Distributions", fontweight="bold")
    fig.tight_layout()
    save(fig, fname)

print("── 6.3 ──")
plot_rt_vs_threads(   w1, "Workload 1 (Transfer)",         "6_3_w1_rt_vs_threads")
plot_rt_vs_contention(w1, "Workload 1 (Transfer)",         "6_3_w1_rt_vs_contention")
plot_rt_distribution(
    f"{DIST}/w1_occ_dist.csv", f"{DIST}/w1_2pl_dist.csv",
    "Workload 1 (Transfer)", "6_3_w1_rt_distribution")

plot_rt_vs_threads(   w2, "Workload 2 (NewOrder+Payment)", "6_3_w2_rt_vs_threads")
plot_rt_vs_contention(w2, "Workload 2 (NewOrder+Payment)", "6_3_w2_rt_vs_contention")
plot_rt_distribution(
    f"{DIST}/w2_occ_dist.csv", f"{DIST}/w2_2pl_dist.csv",
    "Workload 2 (NewOrder+Payment)", "6_3_w2_rt_distribution")

print(f"\nAll plots saved to {OUT}/")