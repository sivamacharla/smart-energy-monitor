import pandas as pd

from src.analytics.clean_and_visualize import clean


def test_clean_dedups_and_flags_outliers():
    df = pd.DataFrame([
        {"device_id": "d1", "seq": 1, "timestamp": "2026-01-01T00:00:00", "power_w": 100},
        {"device_id": "d1", "seq": 1, "timestamp": "2026-01-01T00:00:00", "power_w": 100},  # duplicate seq
        {"device_id": "d1", "seq": 2, "timestamp": "2026-01-01T00:01:00", "power_w": 105},
        {"device_id": "d1", "seq": 3, "timestamp": "2026-01-01T00:02:00", "power_w": 98},
        {"device_id": "d1", "seq": 4, "timestamp": "2026-01-01T00:03:00", "power_w": 102},
        {"device_id": "d1", "seq": 5, "timestamp": "2026-01-01T00:04:00", "power_w": 2000},  # anomaly
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ingested_at"] = df["timestamp"]

    cleaned = clean(df)

    assert len(cleaned) == 5  # duplicate seq removed
    assert cleaned["is_anomaly"].sum() == 1


def test_clean_interpolates_small_gaps():
    df = pd.DataFrame([
        {"device_id": "d1", "seq": i, "timestamp": f"2026-01-01T00:0{i}:00", "power_w": v}
        for i, v in enumerate([100, None, 104, None, 108, 110], start=1)
    ])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["ingested_at"] = df["timestamp"]

    cleaned = clean(df)

    assert cleaned["power_w"].isna().sum() == 0
    assert len(cleaned) == 6
