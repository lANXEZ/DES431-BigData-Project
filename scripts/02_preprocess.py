"""
TASK 2 - DATA PREPROCESSING
===========================
Cleaning, transformation and feature engineering. Streams the 12 raw monthly
CSVs one at a time (never loads 5.5 M rows at once) and writes:

  outputs/clean/year=YYYY/month=MM/*.parquet   analysis-ready trip fact table
  outputs/rejects/*.parquet                    every dropped row + reject_reason
  outputs/handoff/station_dim.{parquet,csv}    station dimension (name-keyed)
  outputs/handoff/station_hourly_flow.parquet  departures/arrivals per station-hour
  outputs/stats/11_cleaning_audit.csv          row accounting, rule by rule

Every rule is imported from common.py so the PySpark port cannot drift.

Run:  python scripts/02_preprocess.py
"""
from __future__ import annotations
import os
import sys
import shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
C.ensure_dirs()
for d in (C.CLEAN_DIR, C.REJ_DIR):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)

OUT_COLS = [
    "ride_id", "rideable_type", "member_casual",
    "started_at", "ended_at",
    "start_station_name", "start_station_id", "start_station_key",
    "end_station_name", "end_station_id", "end_station_key",
    "start_lat", "start_lng", "end_lat", "end_lng",
    "start_geohash7", "end_geohash7",
    "duration_min", "haversine_km", "avg_speed_kmh",
    "trip_type", "is_round_trip",
    "trip_date", "year", "month", "day", "hour",
    "day_of_week", "day_name", "is_weekend", "season", "time_of_day", "is_rush_hour",
]

audit_rows, station_parts, flow_parts = [], [], []
n_raw_total = n_clean_total = 0
rule_named = [r for r, _ in C.RULES]


def time_of_day(h: pd.Series) -> pd.Series:
    return pd.cut(h, bins=[-1, 5, 11, 17, 23],
                  labels=["Night", "Morning", "Afternoon", "Evening"]).astype("string")


print("Preprocessing %d monthly files ...\n" % len(C.monthly_files()))
print("  %-28s %>9s" % ("file", "") if False else "")

for path in C.monthly_files():
    fname = os.path.basename(path)
    df = C.read_raw(path)
    n_raw = len(df)
    n_raw_total += n_raw

    # ---- 1. STRUCTURAL CLEANING -------------------------------------
    # 1a. de-duplicate on the natural key (0 expected, kept for defensibility)
    before = len(df)
    df = df.drop_duplicates(subset="ride_id", keep="first")
    n_dupes = before - len(df)

    # 1b. whitespace defects in station names (13 names affected dataset-wide)
    for c in ("start_station_name", "end_station_name"):
        df[c] = C.clean_name(df[c])

    # ---- 2. DERIVED QUANTITIES (needed before the rules can be judged)
    dur_sec = (df.ended_at - df.started_at).dt.total_seconds()
    dist_km = C.haversine_km(df.start_lat, df.start_lng, df.end_lat, df.end_lng)
    with np.errstate(divide="ignore", invalid="ignore"):
        speed = np.where(dur_sec > 0, dist_km / (dur_sec / 3600.0), np.nan)

    # ---- 3. QUALITY RULES -------------------------------------------
    oob = (~df.start_lat.between(C.CHI_LAT_MIN, C.CHI_LAT_MAX)
           | ~df.start_lng.between(C.CHI_LNG_MIN, C.CHI_LNG_MAX)
           | (df.end_lat.notna() & ~df.end_lat.between(C.CHI_LAT_MIN, C.CHI_LAT_MAX))
           | (df.end_lng.notna() & ~df.end_lng.between(C.CHI_LNG_MIN, C.CHI_LNG_MAX)))
    masks = {
        "R1_missing_end_coords":     df.end_lat.isna() | df.end_lng.isna(),
        "R2_negative_duration":      dur_sec < 0,
        "R3_duration_under_60s":     (dur_sec >= 0) & (dur_sec < C.MIN_DURATION_SEC),
        "R4_duration_over_24h":      dur_sec > C.MAX_DURATION_SEC,
        "R5_coords_outside_chicago": oob,
        "R6_implausible_speed":      pd.Series(speed > C.MAX_SPEED_KMH, index=df.index).fillna(False),
    }
    # priority tagging: first matching rule owns the row
    reason = pd.Series(pd.NA, index=df.index, dtype="string")
    for r in rule_named:
        m = masks[r].fillna(False) & reason.isna()
        reason[m] = r
    drop = reason.notna()

    audit_rows.append(dict(
        file=fname, raw_rows=n_raw, duplicate_ride_id=n_dupes,
        **{r: int(masks[r].fillna(False).sum()) for r in rule_named},
        **{r + "_owned": int((reason == r).sum()) for r in rule_named},
        removed=int(drop.sum()), clean_rows=int((~drop).sum()),
    ))

    # ---- 4. PERSIST REJECTS (full audit trail) -----------------------
    rej = df[drop].copy()
    rej["reject_reason"] = reason[drop]
    rej["duration_min"] = (dur_sec[drop] / 60.0).round(4)
    rej["avg_speed_kmh"] = np.round(speed[drop.to_numpy()], 3)
    rej.to_parquet(os.path.join(C.REJ_DIR, fname.replace(".csv", ".parquet")),
                   index=False, compression="snappy")

    # ---- 5. KEEP THE CLEAN ROWS --------------------------------------
    keep = ~drop
    df = df[keep].copy()
    dur_sec, dist_km, speed = dur_sec[keep], dist_km[keep.to_numpy()], speed[keep.to_numpy()]
    n_clean_total += len(df)

    # ---- 6. FEATURE ENGINEERING --------------------------------------
    st = df.started_at
    df["duration_min"]  = (dur_sec / 60.0).astype("float32").round(4)
    df["haversine_km"]  = pd.Series(dist_km, index=df.index).astype("float32").round(4)
    df["avg_speed_kmh"] = pd.Series(speed, index=df.index).astype("float32").round(3)

    df["start_station_key"] = C.station_key(df.start_station_name)
    df["end_station_key"]   = C.station_key(df.end_station_name)

    # Spatial binning: a ~153 m geohash cell exists for EVERY trip, including the
    # 20 % of dockless trips that carry no station name. This is what lets the
    # analytics team do spatial work on the full population rather than 80 % of it.
    df["start_geohash7"] = C.geohash7(df.start_lat.to_numpy(), df.start_lng.to_numpy())
    df["end_geohash7"]   = C.geohash7(df.end_lat.to_numpy(),   df.end_lng.to_numpy())

    sm, em = df.start_station_name.isna(), df.end_station_name.isna()
    df["trip_type"] = np.select(
        [~sm & ~em, sm & ~em, ~sm & em],
        ["docked", "dockless_start", "dockless_end"],
        default="fully_dockless").astype("object")
    df["trip_type"] = df["trip_type"].astype("string")

    # Only meaningful where both endpoints are docked; NA elsewhere by design.
    rt = (df.start_station_key == df.end_station_key)
    df["is_round_trip"] = rt.astype("boolean").where(~sm & ~em, pd.NA)

    df["trip_date"]   = st.dt.date.astype("string")
    df["year"]        = st.dt.year.astype("int16")
    df["month"]       = st.dt.month.astype("int8")
    df["day"]         = st.dt.day.astype("int8")
    df["hour"]        = st.dt.hour.astype("int8")
    df["day_of_week"] = st.dt.dayofweek.astype("int8")          # 0 = Monday
    df["day_name"]    = df.day_of_week.map(dict(enumerate(C.DAY_NAMES))).astype("string")
    df["is_weekend"]  = (df.day_of_week >= 5)
    df["season"]      = df.month.map(C.SEASONS).astype("string")
    df["time_of_day"] = time_of_day(df.hour)
    df["is_rush_hour"] = (~df.is_weekend
                          & (df.hour.between(*C.RUSH_AM) | df.hour.between(*C.RUSH_PM)))

    # ---- 7. WRITE PARTITIONED PARQUET --------------------------------
    out = df[OUT_COLS]
    pq.write_to_dataset(pa.Table.from_pandas(out, preserve_index=False),
                        root_path=C.CLEAN_DIR, partition_cols=["year", "month"],
                        compression="snappy",
                        basename_template=fname.replace(".csv", "") + "-{i}.parquet",
                        existing_data_behavior="overwrite_or_ignore")

    # ---- 8. HANDOFF AGGREGATES ---------------------------------------
    ends = pd.concat([
        df.loc[~sm, ["start_station_key", "start_station_name", "start_station_id",
                     "start_lat", "start_lng"]].set_axis(
            ["station_key", "station_name", "station_id", "lat", "lng"], axis=1),
        df.loc[~em, ["end_station_key", "end_station_name", "end_station_id",
                     "end_lat", "end_lng"]].set_axis(
            ["station_key", "station_name", "station_id", "lat", "lng"], axis=1),
    ], ignore_index=True)
    g = ends.groupby("station_key", observed=True)
    station_parts.append(pd.DataFrame({
        "station_name": g.station_name.first(),
        "n_obs":        g.size(),
        "lat":          g.lat.median(),
        "lng":          g.lng.median(),
        "ids":          g.station_id.apply(lambda s: set(s.dropna())),
    }).reset_index())

    dep = (df.loc[~sm].groupby(["start_station_key", "trip_date", "hour"], observed=True)
             .size().rename("departures").reset_index()
             .rename(columns={"start_station_key": "station_key"}))
    arr = (df.loc[~em].groupby(["end_station_key", "trip_date", "hour"], observed=True)
             .size().rename("arrivals").reset_index()
             .rename(columns={"end_station_key": "station_key"}))
    flow_parts.append(dep.merge(arr, on=["station_key", "trip_date", "hour"], how="outer"))

    print("  %-32s raw=%9s  removed=%7s (%.2f%%)  clean=%9s"
          % (fname, "{:,}".format(n_raw), "{:,}".format(int(drop.sum())),
             100.0 * drop.sum() / n_raw, "{:,}".format(len(df))))
    del df, out, rej

# ==========================================================================
# Station dimension
# ==========================================================================
sd = pd.concat(station_parts, ignore_index=True)
sd["w_lat"], sd["w_lng"] = sd.lat * sd.n_obs, sd.lng * sd.n_obs
agg = sd.groupby("station_key", observed=True).agg(
    station_name=("station_name", "first"),
    n_trip_endpoints=("n_obs", "sum"),
    w_lat=("w_lat", "sum"), w_lng=("w_lng", "sum"),
    ids=("ids", lambda s: sorted(set().union(*s))),
).reset_index()
agg["lat"] = (agg.w_lat / agg.n_trip_endpoints).round(6)
agg["lng"] = (agg.w_lng / agg.n_trip_endpoints).round(6)
agg["n_legacy_ids"] = agg.ids.str.len()
agg["legacy_station_ids"] = agg.ids.apply(lambda x: " | ".join(x))
station_dim = (agg[["station_key", "station_name", "lat", "lng",
                    "n_trip_endpoints", "n_legacy_ids", "legacy_station_ids"]]
               .sort_values("n_trip_endpoints", ascending=False).reset_index(drop=True))
station_dim.to_parquet(os.path.join(C.HAND_DIR, "station_dim.parquet"),
                       index=False, compression="snappy")
station_dim.to_csv(os.path.join(C.HAND_DIR, "station_dim.csv"), index=False)

# ==========================================================================
# Station x date x hour flow  (feeds the station-imbalance analysis)
# ==========================================================================
flow = (pd.concat(flow_parts, ignore_index=True)
          .groupby(["station_key", "trip_date", "hour"], observed=True)[["departures", "arrivals"]]
          .sum().reset_index())
flow["departures"] = flow.departures.fillna(0).astype("int32")
flow["arrivals"]   = flow.arrivals.fillna(0).astype("int32")
flow["net_flow"]   = flow.arrivals - flow.departures     # +ve = bikes pile up here
flow.to_parquet(os.path.join(C.HAND_DIR, "station_hourly_flow.parquet"),
                index=False, compression="snappy")

# ==========================================================================
# Audit
# ==========================================================================
audit = pd.DataFrame(audit_rows)
audit.to_csv(os.path.join(C.STAT_DIR, "11_cleaning_audit.csv"), index=False)

reg = pd.DataFrame([{
    "rule": r, "description": d,
    "rows_matched": int(audit[r].sum()),
    "rows_removed_by_this_rule": int(audit[r + "_owned"].sum()),
    "pct_of_raw": round(100.0 * audit[r + "_owned"].sum() / n_raw_total, 4),
} for r, d in C.RULES])
reg.loc[len(reg)] = ["TOTAL", "All rules combined (rows are counted once)",
                     int(audit[[r for r in rule_named]].sum().sum()),
                     int(audit.removed.sum()),
                     round(100.0 * audit.removed.sum() / n_raw_total, 4)]
reg.to_csv(os.path.join(C.STAT_DIR, "12_quality_register.csv"), index=False)

size = sum(os.path.getsize(os.path.join(dp, f))
           for dp, _, fs in os.walk(C.CLEAN_DIR) for f in fs)
raw_size = sum(os.path.getsize(p) for p in C.monthly_files())

print("\n" + "=" * 78)
print("RAW ROWS      : {:,}".format(n_raw_total))
print("REMOVED       : {:,}  ({:.2f}%)".format(n_raw_total - n_clean_total,
                                               100.0 * (n_raw_total - n_clean_total) / n_raw_total))
print("CLEAN ROWS    : {:,}  ({:.2f}%)".format(n_clean_total, 100.0 * n_clean_total / n_raw_total))
print("STATIONS      : {:,}".format(len(station_dim)))
print("FLOW ROWS     : {:,}".format(len(flow)))
print("SIZE  raw CSV : {:,.1f} MB   ->   clean Parquet: {:,.1f} MB   ({:.1f}x smaller)"
      .format(raw_size / 1e6, size / 1e6, raw_size / size))
print("\nQUALITY REGISTER")
print(reg.to_string(index=False))
