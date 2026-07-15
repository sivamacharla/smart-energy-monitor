"""
Data cleaning + visualization workflow (the MATLAB-equivalent analytics
step, reimplemented in Python/pandas so it runs without a MATLAB license).

Loads raw ingested telemetry, cleans it, flags anomalies, and produces the
plots/summary a MATLAB workflow would have generated: per-device time
series with anomalies highlighted, an hour-of-day peak-usage heatmap, and
a summary CSV of consumption stats per device.
"""
from __future__ import annotations

import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import yaml


def load_raw(path: str) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["timestamp", "ingested_at"])


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["device_id", "seq"]).copy()
    df = df.sort_values(["device_id", "timestamp"])

    # interpolate small gaps in power per device (missing samples); genuine
    # spikes are kept intact -- outlier *flagging* happens separately below
    df["power_w"] = df.groupby("device_id")["power_w"].transform(lambda s: s.interpolate(limit=3))
    df = df.dropna(subset=["power_w"])

    # IQR-based anomaly flag per device, used to separate sensor noise from
    # genuine consumption anomalies worth surfacing to analysts
    def flag_outliers(s: pd.Series) -> pd.Series:
        q1, q3 = s.quantile([0.25, 0.75])
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        return (s < lower) | (s > upper)

    df["is_anomaly"] = df.groupby("device_id")["power_w"].transform(flag_outliers)
    df["hour"] = df["timestamp"].dt.hour
    return df


def plot_timeseries(df: pd.DataFrame, out_dir: str):
    for device_id, g in df.groupby("device_id"):
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(g["timestamp"], g["power_w"], linewidth=1, label="power (W)")
        anomalies = g[g["is_anomaly"]]
        ax.scatter(anomalies["timestamp"], anomalies["power_w"], color="red", s=18, zorder=5, label="anomaly")
        ax.set_title(f"{device_id} power consumption")
        ax.set_xlabel("time")
        ax.set_ylabel("watts")
        ax.legend()
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, f"{device_id}_timeseries.png"), dpi=130)
        plt.close(fig)


def plot_peak_hour_heatmap(df: pd.DataFrame, out_dir: str):
    pivot = df.pivot_table(index="device_id", columns="hour", values="power_w", aggfunc="mean")
    pivot = pivot.reindex(columns=range(24))
    fig, ax = plt.subplots(figsize=(12, 3 + 0.4 * len(pivot)))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(24))
    ax.set_xticklabels(range(24))
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_xlabel("hour of day")
    ax.set_title("Average power by hour of day (peak-usage identification)")
    fig.colorbar(im, ax=ax, label="avg watts")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "peak_hour_heatmap.png"), dpi=130)
    plt.close(fig)


def write_summary(df: pd.DataFrame, path: str) -> pd.DataFrame:
    summary = df.groupby("device_id").agg(
        samples=("power_w", "size"),
        mean_w=("power_w", "mean"),
        max_w=("power_w", "max"),
        anomalies=("is_anomaly", "sum"),
    )
    peak_hour = df.groupby(["device_id", "hour"])["power_w"].mean().reset_index()
    peak_hour = peak_hour.loc[peak_hour.groupby("device_id")["power_w"].idxmax()].set_index("device_id")["hour"]
    summary["peak_hour"] = peak_hour
    os.makedirs(os.path.dirname(path), exist_ok=True)
    summary.to_csv(path)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Clean telemetry and generate consumption analytics/plots")
    parser.add_argument("--config", default="config/settings.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)["analytics"]

    os.makedirs(config["output_dir"], exist_ok=True)

    df = load_raw(config["raw_data_path"])
    print(f"loaded {len(df)} raw samples")
    df = clean(df)
    print(f"{len(df)} samples after cleaning, {int(df['is_anomaly'].sum())} anomalies flagged")

    plot_timeseries(df, config["output_dir"])
    plot_peak_hour_heatmap(df, config["output_dir"])
    summary = write_summary(df, config["summary_csv"])
    print(summary)


if __name__ == "__main__":
    main()
