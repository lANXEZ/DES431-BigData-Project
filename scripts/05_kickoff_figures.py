"""
KICK-OFF DOCUMENT - SECTIONS 3 AND 4
====================================
Everything the kick-off document needs for "3 Data Description" and
"4 General Statistics of the Data": the per-month record counts, the feature
table (type / range / example), the per-column x per-month missing-value
cross-tab, and the sample distributions.

Like 01_profile.py this is a measure-only pass - it never mutates the raw data.
It streams one monthly CSV at a time, so peak memory stays near the size of the
largest single month (~790k rows) rather than the full 5.5 M.

  outputs/stats/kickoff/*.csv     tables that go straight into the document
  outputs/figures/kickoff/*.png   figures K01-K21

Run:  python scripts/05_kickoff_figures.py            # re-render from cache if present
      python scripts/05_kickoff_figures.py --refresh  # force a full pass over the CSVs

The streaming pass takes a few minutes, so its accumulators are cached. Figure
styling can then be iterated on without touching the raw data again.

The raw CSVs are gitignored (1.1 GB). This script looks for them in, in order:
  1. archive/CSV_Files/ inside the repo
  2. $DIVVY_RAW_DIR
"""
from __future__ import annotations
import os
import sys
import json
import pickle
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.colors import LinearSegmentedColormap

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ==========================================================================
# Locate the raw CSVs
# ==========================================================================
def resolve_raw_dir() -> str:
    candidates = [C.RAW_DIR, os.environ.get("DIVVY_RAW_DIR")]
    for d in candidates:
        if d and os.path.isdir(d) and any(f.endswith(".csv") for f in os.listdir(d)):
            return d
    raise SystemExit(
        "No raw CSVs found. Put the 12 monthly Divvy files in archive/CSV_Files/\n"
        "or point $DIVVY_RAW_DIR at them - see archive/README.md."
    )


FIG = os.path.join(C.FIG_DIR, "kickoff")
STA = os.path.join(C.STAT_DIR, "kickoff")
for d in (FIG, STA):
    os.makedirs(d, exist_ok=True)
CACHE = os.path.join(STA, "_accumulators.pkl")
REFRESH = "--refresh" in sys.argv or not os.path.exists(CACHE)


# ==========================================================================
# Accumulators
# ==========================================================================
# Numeric features carried into the correlation matrix, in display order.
NUMERIC = ["duration_min", "distance_km", "speed_kmh",
           "start_lat", "start_lng", "end_lat", "end_lng",
           "hour", "day_of_week", "month", "is_member", "is_electric"]
K = len(NUMERIC)

# Fixed histogram bins, so counts from each month can simply be added up.
BINS = {
    "duration_min": np.logspace(np.log10(0.05), np.log10(2000), 70),
    "distance_km":  np.linspace(0, 15, 61),
    "speed_kmh":    np.linspace(0, 40, 61),
    "start_lat":    np.linspace(41.60, 42.10, 61),
    "start_lng":    np.linspace(-87.90, -87.50, 61),
    "end_lat":      np.linspace(41.60, 42.10, 61),
    "end_lng":      np.linspace(-87.90, -87.50, 61),
}
hist = {k: np.zeros(len(v) - 1, dtype="int64") for k, v in BINS.items()}

rows_by_month   = {}                       # ym -> row count
missing_cm      = {}                       # (col, ym) -> missing count
cat_counts      = {"rideable_type": {}, "member_casual": {}}
hour_counts     = np.zeros(24, dtype="int64")
dow_counts      = np.zeros(7, dtype="int64")
day_counts      = {}                       # date -> trips
start_station   = {}                       # cleaned name -> trips
end_station     = {}                       # cleaned name -> trips
month_stations  = {}                       # ym -> set(cleaned names)
station_day     = {}                       # (name, date) -> trips
name_to_ids     = {}                       # cleaned name -> set(station_id)
col_minmax      = {}                       # col -> [min, max] for numeric / timestamp
col_examples    = {}                       # col -> first non-null value seen
col_nunique     = {"start_station_name": set(), "end_station_name": set(),
                   "start_station_id": set(), "end_station_id": set()}
file_sizes      = {}                       # ym -> size in bytes

# Streaming co-moments for the correlation matrix (complete cases only).
corr_n = 0
corr_s = np.zeros(K, dtype="float64")
corr_S = np.zeros((K, K), dtype="float64")

if REFRESH:
    C.RAW_DIR = resolve_raw_dir()
    print("Raw data: %s" % C.RAW_DIR)
    files = sorted([os.path.join(C.RAW_DIR, f) for f in os.listdir(C.RAW_DIR)
                    if f.endswith(".csv")])
    print("Streaming %d monthly files ...\n" % len(files))
else:
    print("Loading cached accumulators (pass --refresh to re-read the CSVs) ...")
    with open(CACHE, "rb") as f:
        globals().update(pickle.load(f))

# Skipped entirely when the cache is being reused.
for path in (files if REFRESH else []):
    fname = os.path.basename(path)
    df = C.read_raw(path)
    ym = fname[:4] + "-" + fname[4:6]
    n = len(df)
    rows_by_month[ym] = rows_by_month.get(ym, 0) + n
    file_sizes[ym] = os.path.getsize(path)
    print("  %s  %7d rows" % (fname, n))

    # ---- completeness, per column per month ---------------------------
    miss = df.isna().sum()
    for c in C.RAW_COLUMNS:
        missing_cm[(c, ym)] = missing_cm.get((c, ym), 0) + int(miss[c])

    # ---- derived quantities -------------------------------------------
    st = df.started_at
    dur_min = (df.ended_at - st).dt.total_seconds() / 60.0
    dist_km = C.haversine_km(df.start_lat, df.start_lng, df.end_lat, df.end_lng)
    with np.errstate(divide="ignore", invalid="ignore"):
        speed = np.where(dur_min > 0, dist_km / (dur_min / 60.0), np.nan)

    feat = pd.DataFrame({
        "duration_min": dur_min,
        "distance_km":  dist_km,
        "speed_kmh":    speed,
        "start_lat":    df.start_lat,
        "start_lng":    df.start_lng,
        "end_lat":      df.end_lat,
        "end_lng":      df.end_lng,
        "hour":         st.dt.hour,
        "day_of_week":  st.dt.dayofweek,
        "month":        st.dt.month,
        "is_member":    (df.member_casual == "member").astype("int8"),
        "is_electric":  (df.rideable_type == "electric_bike").astype("int8"),
    })

    # ---- histograms ----------------------------------------------------
    for k, edges in BINS.items():
        v = feat[k].to_numpy(dtype="float64")
        v = v[np.isfinite(v)]
        hist[k] += np.histogram(v, bins=edges)[0]

    # ---- correlation co-moments (complete cases) ------------------------
    X = feat[NUMERIC].to_numpy(dtype="float64")
    X = X[np.isfinite(X).all(axis=1)]
    corr_n += X.shape[0]
    corr_s += X.sum(axis=0)
    corr_S += X.T @ X

    # ---- categorical / temporal distributions ---------------------------
    for c in cat_counts:
        for k, v in df[c].value_counts().items():
            cat_counts[c][k] = cat_counts[c].get(k, 0) + int(v)
    hour_counts += np.bincount(st.dt.hour.to_numpy(), minlength=24).astype("int64")
    dow_counts  += np.bincount(st.dt.dayofweek.to_numpy(), minlength=7).astype("int64")

    dates = st.dt.strftime("%Y-%m-%d")
    for k, v in dates.value_counts().items():
        day_counts[k] = day_counts.get(k, 0) + int(v)

    # ---- stations --------------------------------------------------------
    s_nm = C.clean_name(df.start_station_name).dropna()
    e_nm = C.clean_name(df.end_station_name).dropna()
    for k, v in s_nm.value_counts().items():
        start_station[k] = start_station.get(k, 0) + int(v)
    for k, v in e_nm.value_counts().items():
        end_station[k] = end_station.get(k, 0) + int(v)
    month_stations.setdefault(ym, set()).update(s_nm.unique())
    month_stations[ym].update(e_nm.unique())

    sd = pd.DataFrame({"nm": s_nm, "d": dates.loc[s_nm.index]}).value_counts()
    for (nm, d), v in sd.items():
        station_day[(nm, d)] = station_day.get((nm, d), 0) + int(v)

    for nm_col, id_col in (("start_station_name", "start_station_id"),
                           ("end_station_name", "end_station_id")):
        sub = df[[nm_col, id_col]].dropna().drop_duplicates()
        for nm, sid in zip(C.clean_name(sub[nm_col]), sub[id_col]):
            name_to_ids.setdefault(nm, set()).add(sid)

    for c in col_nunique:
        col_nunique[c].update(df[c].dropna().unique())

    # ---- ranges and example values ----------------------------------------
    for c in C.RAW_COLUMNS:
        s = df[c].dropna()
        if s.empty:
            continue
        if c not in col_examples:
            col_examples[c] = s.iloc[0]
        if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_datetime64_any_dtype(s):
            lo, hi = s.min(), s.max()
            if c in col_minmax:
                col_minmax[c][0] = min(col_minmax[c][0], lo)
                col_minmax[c][1] = max(col_minmax[c][1], hi)
            else:
                col_minmax[c] = [lo, hi]

    del df, feat, X

if REFRESH:
    with open(CACHE, "wb") as f:
        pickle.dump({k: globals()[k] for k in (
            "files", "hist", "rows_by_month", "missing_cm", "cat_counts",
            "hour_counts", "dow_counts", "day_counts", "start_station",
            "end_station", "month_stations", "station_day", "name_to_ids",
            "col_minmax", "col_examples", "col_nunique", "file_sizes",
            "corr_n", "corr_s", "corr_S")}, f, protocol=4)
    print("  cached accumulators -> %s" % os.path.relpath(CACHE, C.ROOT))

total_rows = sum(rows_by_month.values())
months = sorted(rows_by_month)
print("\nTotal rows: {:,}".format(total_rows))


# ==========================================================================
# Tables
# ==========================================================================
# 4.1 records per month
rec = pd.DataFrame({
    "year_month": months,
    "records": [rows_by_month[m] for m in months],
    "file_size_mb": [round(file_sizes[m] / 1e6, 1) for m in months],
})
rec["pct_of_total"] = (rec.records / total_rows * 100).round(2)
rec.to_csv(os.path.join(STA, "K1_records_by_month.csv"), index=False)

# 4.2 feature table
DESC = {
    "ride_id":            "Unique trip identifier (16-character hex)",
    "rideable_type":      "Bike class used for the trip",
    "started_at":         "Trip start timestamp, local Chicago time",
    "ended_at":           "Trip end timestamp, local Chicago time",
    "start_station_name": "Docking station where the trip began",
    "start_station_id":   "Divvy internal station code for the origin",
    "end_station_name":   "Docking station where the trip ended",
    "end_station_id":     "Divvy internal station code for the destination",
    "start_lat":          "Origin latitude, WGS84 decimal degrees",
    "start_lng":          "Origin longitude, WGS84 decimal degrees",
    "end_lat":            "Destination latitude, WGS84 decimal degrees",
    "end_lng":            "Destination longitude, WGS84 decimal degrees",
    "member_casual":      "Rider subscription type",
}
TYPE = {"ride_id": "string", "rideable_type": "string", "started_at": "timestamp",
        "ended_at": "timestamp", "start_station_name": "string",
        "start_station_id": "string", "end_station_name": "string",
        "end_station_id": "string", "start_lat": "double", "start_lng": "double",
        "end_lat": "double", "end_lng": "double", "member_casual": "string"}


def fmt_range(c):
    if c in ("rideable_type", "member_casual"):
        return " | ".join(sorted(cat_counts[c]))
    if c == "ride_id":
        return "-"
    if c in col_minmax:
        lo, hi = col_minmax[c]
        if TYPE[c] == "timestamp":
            return "%s  to  %s" % (pd.Timestamp(lo).strftime("%Y-%m-%d %H:%M"),
                                   pd.Timestamp(hi).strftime("%Y-%m-%d %H:%M"))
        return "%.4f  to  %.4f" % (lo, hi)
    if c in col_nunique:
        return "%d distinct values" % len(col_nunique[c])
    return "-"


feat_rows = []
for i, c in enumerate(C.RAW_COLUMNS, 1):
    total_missing = sum(missing_cm[(c, m)] for m in months)
    ex = col_examples.get(c, "")
    if isinstance(ex, pd.Timestamp):
        ex = ex.strftime("%Y-%m-%d %H:%M:%S")
    feat_rows.append({
        "#": i, "feature": c, "data_type": TYPE[c], "description": DESC[c],
        "range": fmt_range(c), "example": str(ex),
        "missing": total_missing,
        "pct_missing": round(total_missing / total_rows * 100, 2),
    })
feat_tbl = pd.DataFrame(feat_rows)
feat_tbl.to_csv(os.path.join(STA, "K2_feature_table.csv"), index=False)

# 4.3 missing cross-tab
cm = pd.DataFrame(
    [[missing_cm[(c, m)] for m in months] for c in C.RAW_COLUMNS],
    index=C.RAW_COLUMNS, columns=months)
cm["Total"] = cm.sum(axis=1)
cm.to_csv(os.path.join(STA, "K3_missing_by_column_by_month.csv"))

# stations
top_start = pd.Series(start_station).sort_values(ascending=False)
top_end = pd.Series(end_station).sort_values(ascending=False)
pd.DataFrame({"station": top_start.index[:30], "trips": top_start.values[:30]}).to_csv(
    os.path.join(STA, "K4_top30_start_stations.csv"), index=False)
pd.DataFrame({"station": top_end.index[:30], "trips": top_end.values[:30]}).to_csv(
    os.path.join(STA, "K5_top30_end_stations.csv"), index=False)

uniq = pd.DataFrame({"year_month": months,
                     "unique_stations": [len(month_stations[m]) for m in months]})
uniq.to_csv(os.path.join(STA, "K6_unique_stations_per_month.csv"), index=False)

# correlation
mean = corr_s / corr_n
cov = corr_S / corr_n - np.outer(mean, mean)
sd = np.sqrt(np.clip(np.diag(cov), 1e-12, None))
corr = cov / np.outer(sd, sd)
corr_df = pd.DataFrame(corr, index=NUMERIC, columns=NUMERIC).round(3)
corr_df.to_csv(os.path.join(STA, "K7_correlation_matrix.csv"))

summary = {
    "total_rows": int(total_rows),
    "n_files": len(files),
    "n_columns": len(C.RAW_COLUMNS),
    "raw_size_mb": round(sum(file_sizes.values()) / 1e6, 1),
    "date_min": str(col_minmax["started_at"][0]),
    "date_max": str(col_minmax["ended_at"][1]),
    "distinct_start_stations": len(col_nunique["start_station_name"]),
    "distinct_station_names": len(set(start_station) | set(end_station)),
    "distinct_station_ids": len(col_nunique["start_station_id"] | col_nunique["end_station_id"]),
    "names_with_multiple_ids": sum(1 for v in name_to_ids.values() if len(v) > 1),
    "correlation_complete_cases": int(corr_n),
    "bike_type_counts": {k: int(v) for k, v in cat_counts["rideable_type"].items()},
    "user_type_counts": {k: int(v) for k, v in cat_counts["member_casual"].items()},
}
with open(os.path.join(STA, "K0_summary.json"), "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)
print(json.dumps(summary, indent=2))


# ==========================================================================
# Figures - same house style as F1-F10
# ==========================================================================
plt.rcParams.update({
    "figure.facecolor": C.SURFACE, "axes.facecolor": C.SURFACE,
    "savefig.facecolor": C.SURFACE, "text.color": C.INK,
    "axes.labelcolor": C.INK_2, "xtick.color": C.INK_2, "ytick.color": C.INK_2,
    "font.size": 10, "axes.titlesize": 12, "axes.titleweight": "bold",
    "figure.dpi": 150, "savefig.bbox": "tight",
})
THOUSANDS = FuncFormatter(lambda v, p: "{:,.0f}".format(v))
BLUES = LinearSegmentedColormap.from_list("blues", C.SEQ_BLUE)


def subtitle(ax, sub, size=9):
    """Offset in POINTS, not axes fractions, so tall axes do not collide."""
    ax.annotate(sub, xy=(0, 1), xycoords="axes fraction", xytext=(0, 7),
                textcoords="offset points", fontsize=size, color=C.INK_2,
                va="bottom", ha="left", annotation_clip=False)


def style(ax, title, sub=None, ylab=None, xlab=None, ygrid=True, scale=1.0):
    """`scale` enlarges every text element. Figures that are wide in inches get
    scaled down hard when placed in the document, so they need bigger source
    type to stay legible on the page."""
    ax.set_title(title, loc="left", pad=24 * scale if sub else 10,
                 color=C.INK, fontsize=12 * scale)
    if sub:
        subtitle(ax, sub, size=9 * scale)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(C.GRID)
    if ygrid:
        ax.grid(axis="y", color=C.GRID, lw=0.8)
        ax.set_axisbelow(True)
    if ylab:
        ax.set_ylabel(ylab, fontsize=10 * scale)
    if xlab:
        ax.set_xlabel(xlab, fontsize=10 * scale)


def save(fig, name):
    fig.savefig(os.path.join(FIG, name))
    plt.close(fig)
    print("  figure -> kickoff/%s" % name)


def centers(edges):
    return (edges[:-1] + edges[1:]) / 2


print("\nRendering figures ...")

# ---- K01 records per month ------------------------------------------------
fig, ax = plt.subplots(figsize=(9, 4.2))
ax.bar(rec.year_month, rec.records, color=C.SERIES[0], width=0.68)
for x, y in zip(rec.year_month, rec.records):
    ax.annotate("{:,.0f}k".format(y / 1000), (x, y), textcoords="offset points",
                xytext=(0, 4), ha="center", fontsize=8, color=C.INK_2)
style(ax, "Trip volume swings 5.7x between the winter trough and the summer peak",
      "Records per monthly CSV, Oct 2024 - Sep 2025 (n = {:,})".format(total_rows), "Records")
ax.yaxis.set_major_formatter(THOUSANDS)
ax.tick_params(axis="x", rotation=45)
ax.margins(y=0.14)
save(fig, "K01_records_per_month.png")

# ---- K02 trips per day ----------------------------------------------------
ds = pd.Series(day_counts).sort_index()
ds.index = pd.to_datetime(ds.index)
fig, ax = plt.subplots(figsize=(9.6, 4.0))
ax.plot(ds.index, ds.values, color=C.SERIES[0], lw=0.8)
ax.plot(ds.index, ds.rolling(7, center=True).mean(), color=C.SERIES[1], lw=1.8)
style(ax, "Daily volume is strongly seasonal with a sharp weekly rhythm",
      "Trips per calendar day; orange line is the 7-day centred mean", "Trips")
ax.yaxis.set_major_formatter(THOUSANDS)
save(fig, "K02_trips_per_day.png")

# ---- K03 / K04 categorical shares -----------------------------------------
for name, col, title, sub in [
        ("K03_bike_type_share.png", "rideable_type",
         "Electric bikes account for nearly two thirds of all trips",
         "Share of records by rideable_type"),
        ("K04_user_type_share.png", "member_casual",
         "Members make roughly two out of every three trips",
         "Share of records by member_casual")]:
    s = pd.Series(cat_counts[col]).sort_values(ascending=False)
    fig, ax = plt.subplots(figsize=(6.4, 4.4))
    w, _, at = ax.pie(s.values, labels=[k.replace("_", " ") for k in s.index],
                      autopct=lambda p: "%.1f%%" % p, startangle=90,
                      colors=C.SERIES[:len(s)],
                      wedgeprops={"edgecolor": C.SURFACE, "linewidth": 2},
                      textprops={"color": C.INK})
    for t in at:
        t.set_color("white")
        t.set_fontweight("bold")
    ax.set_title(title, loc="left", pad=24, color=C.INK)
    subtitle(ax, sub)
    save(fig, name)

# ---- K05 hour of day ------------------------------------------------------
fig, ax = plt.subplots(figsize=(9, 4.0))
ax.bar(range(24), hour_counts, color=C.SERIES[0], width=0.72)
style(ax, "Demand is bimodal: a morning commute peak and a larger evening one",
      "All trips by hour of start time", "Trips", "Hour of day")
ax.yaxis.set_major_formatter(THOUSANDS)
ax.set_xticks(range(0, 24, 2))
save(fig, "K05_hour_distribution.png")

# ---- K06 day of week ------------------------------------------------------
fig, ax = plt.subplots(figsize=(8, 4.0))
ax.bar(C.DAY_NAMES, dow_counts, color=C.SERIES[0], width=0.6)
style(ax, "Weekday and weekend volumes are close, but their shape differs",
      "All trips by day of week of start time", "Trips")
ax.yaxis.set_major_formatter(THOUSANDS)
ax.tick_params(axis="x", rotation=30)
save(fig, "K06_dayofweek_distribution.png")

# ---- K07 duration ---------------------------------------------------------
fig, ax = plt.subplots(figsize=(9, 4.2))
e = BINS["duration_min"]
ax.bar(centers(e), hist["duration_min"], width=np.diff(e), color=C.SERIES[0])
ax.set_xscale("log")
for x, lab in ((1.0, "60 s"), (1440.0, "24 h")):
    ax.axvline(x, color=C.SERIES[1], ls="--", lw=1.6)
    ax.annotate(lab, (x, ax.get_ylim()[1] * 0.92), color=C.SERIES[1],
                fontsize=9, fontweight="bold", ha="right", rotation=90)
style(ax, "Trip duration is long-tailed with junk at both extremes",
      "All trips, log-scaled minutes; dashed lines mark the preprocessing cut-offs",
      "Trips", "Duration (minutes, log scale)")
ax.yaxis.set_major_formatter(THOUSANDS)
save(fig, "K07_duration_distribution.png")

# ---- K08 distance / K09 speed ---------------------------------------------
for name, key, title, sub, xlab in [
        ("K08_distance_distribution.png", "distance_km",
         "Most trips are short: half cover under 1.6 km straight-line",
         "Haversine distance between endpoints, trips beyond 15 km omitted",
         "Straight-line distance (km)"),
        ("K09_speed_distribution.png", "speed_kmh",
         "Average speed clusters tightly around 11-12 km/h",
         "Straight-line distance divided by elapsed time; a lower bound on true speed",
         "Average speed (km/h)")]:
    fig, ax = plt.subplots(figsize=(9, 4.0))
    e = BINS[key]
    ax.bar(centers(e), hist[key], width=np.diff(e) * 0.95, color=C.SERIES[0])
    style(ax, title, sub, "Trips", xlab)
    ax.yaxis.set_major_formatter(THOUSANDS)
    save(fig, name)

# ---- K10 coordinate histograms --------------------------------------------
fig, axes = plt.subplots(2, 2, figsize=(10, 6.2))
for ax, key in zip(axes.ravel(), ["start_lat", "start_lng", "end_lat", "end_lng"]):
    e = BINS[key]
    ax.bar(centers(e), hist[key], width=np.diff(e) * 0.95, color=C.SERIES[0])
    style(ax, key, None, "Trips")
    ax.yaxis.set_major_formatter(THOUSANDS)
    ax.tick_params(labelsize=8)
fig.suptitle("Coordinates concentrate on the Chicago lakefront and near North Side",
             x=0.005, ha="left", fontsize=12, fontweight="bold", color=C.INK)
fig.tight_layout(rect=(0, 0, 1, 0.96))
save(fig, "K10_coordinate_distributions.png")

# ---- K11 spatial density ---------------------------------------------------
sd_df = pd.DataFrame({"nm": list(start_station), "trips": list(start_station.values())})
dim_path = os.path.join(C.HAND_DIR, "station_dim.csv")
if os.path.exists(dim_path):
    dim = pd.read_csv(dim_path)
    fig, ax = plt.subplots(figsize=(7.6, 7.4))
    sc = ax.scatter(dim.lng, dim.lat, s=np.sqrt(dim.n_trip_endpoints) / 3.2,
                    c=dim.n_trip_endpoints, cmap=BLUES, alpha=0.85,
                    edgecolors="none")
    ax.set_aspect(1 / np.cos(np.radians(41.88)))
    style(ax, "Station demand is concentrated downtown and along the lake",
          "%d stations, sized and coloured by trip endpoints" % len(dim),
          "Latitude", "Longitude", ygrid=False)
    ax.grid(color=C.GRID, lw=0.6)
    ax.set_axisbelow(True)
    ax.xaxis.set_major_locator(plt.MultipleLocator(0.1))
    ax.tick_params(axis="x", labelsize=9)
    cb = fig.colorbar(sc, ax=ax, shrink=0.5, pad=0.04, location="right")
    cb.set_label("Trip endpoints", color=C.INK_2)
    cb.ax.yaxis.set_major_formatter(THOUSANDS)
    cb.outline.set_visible(False)
    save(fig, "K11_station_map.png")

# ---- K12 missing by column -------------------------------------------------
pm_ = feat_tbl.sort_values("pct_missing")
fig, ax = plt.subplots(figsize=(8, 5.0))
bars = ax.barh(pm_.feature, pm_.pct_missing, color=C.SERIES[0], height=0.68)
for b, v in zip(bars, pm_.pct_missing):
    ax.annotate("%.2f%%" % v, (b.get_width(), b.get_y() + b.get_height() / 2),
                xytext=(4, 0), textcoords="offset points", va="center",
                fontsize=8, color=C.INK_2)
style(ax, "Only the station and end-coordinate columns have missing values",
      "Percentage of the 5,539,521 raw records with a null in each column",
      None, "% of records missing", ygrid=False)
ax.grid(axis="x", color=C.GRID, lw=0.8)
ax.set_axisbelow(True)
ax.margins(x=0.12)
save(fig, "K12_missing_by_column.png")

# ---- K13 missing heatmap ----------------------------------------------------
hm = cm.drop(columns="Total")
pct = hm.div(pd.Series(rows_by_month), axis=1) * 100
keep = pct[pct.max(axis=1) > 0]
fig, ax = plt.subplots(figsize=(9.5, 3.4))
im = ax.imshow(keep.values, cmap=BLUES, aspect="auto", vmin=0)
ax.set_xticks(range(len(months)), months, rotation=45, ha="right", fontsize=8)
ax.set_yticks(range(len(keep)), keep.index, fontsize=9)
for i in range(keep.shape[0]):
    for j in range(keep.shape[1]):
        v = keep.values[i, j]
        ax.text(j, i, "%.1f" % v, ha="center", va="center", fontsize=7,
                color="white" if v > keep.values.max() * 0.55 else C.INK)
ax.set_title("Missingness is stable month to month - it is structural, not an incident",
             loc="left", pad=24, color=C.INK)
subtitle(ax, "% of that month's records that are null; columns with any missing data only")
for s in ax.spines.values():
    s.set_visible(False)
save(fig, "K13_missing_heatmap.png")

# ---- K14 missing by bike type ------------------------------------------------
mb = pd.read_csv(os.path.join(C.STAT_DIR, "03_missing_by_biketype.csv"))
mb = mb[mb.rideable_type.isin(["classic_bike", "electric_bike"])].sort_values("pct_missing_start")
x = np.arange(len(mb))
fig, ax = plt.subplots(figsize=(7, 4.2))
ax.bar(x - 0.18, mb.pct_missing_start, 0.34, label="start station", color=C.SERIES[0])
ax.bar(x + 0.18, mb.pct_missing_end, 0.34, label="end station", color=C.SERIES[1])
for xi, a, b in zip(x, mb.pct_missing_start, mb.pct_missing_end):
    ax.annotate("%.2f%%" % a, (xi - 0.18, a), xytext=(0, 4), textcoords="offset points",
                ha="center", fontsize=9, color=C.INK)
    ax.annotate("%.2f%%" % b, (xi + 0.18, b), xytext=(0, 4), textcoords="offset points",
                ha="center", fontsize=9, color=C.INK)
ax.set_xticks(x, [t.replace("_", " ") for t in mb.rideable_type])
style(ax, "Every missing station name belongs to a dockless e-bike trip",
      "Share of trips with no station name, split by bike type",
      "% of trips missing a station")
ax.legend(frameon=False, fontsize=9)
ax.margins(y=0.18)
save(fig, "K14_missing_by_biketype.png")

# ---- K15 / K16 top 30 stations -------------------------------------------------
for name, s, title, sub in [
        ("K15_top30_start_stations.png", top_start[:30],
         "Top 30 origin stations are lakefront and downtown landmarks",
         "Trips STARTING at each station over the 12 months"),
        ("K16_top30_end_stations.png", top_end[:30],
         "Destination ranking mirrors the origin ranking almost exactly",
         "Trips ENDING at each station over the 12 months")]:
    fig, ax = plt.subplots(figsize=(9.2, 7.0))
    o = s[::-1]
    ax.barh(range(len(o)), o.values, color=C.SERIES[0], height=0.72)
    ax.set_yticks(range(len(o)), o.index, fontsize=12)
    for i, v in enumerate(o.values):
        ax.annotate("{:,.0f}".format(v), (v, i), xytext=(5, 0),
                    textcoords="offset points", va="center", fontsize=10, color=C.INK_2)
    style(ax, title, sub, None, "Trips", ygrid=False, scale=1.55)
    ax.tick_params(axis="x", labelsize=12)
    ax.grid(axis="x", color=C.GRID, lw=0.8)
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(THOUSANDS)
    ax.margins(x=0.13)
    save(fig, name)

# ---- K17 unique stations per month ----------------------------------------------
fig, ax = plt.subplots(figsize=(9, 4.0))
ax.plot(uniq.year_month, uniq.unique_stations, marker="o", color=C.SERIES[0], lw=2)
for xx, yy in zip(uniq.year_month, uniq.unique_stations):
    ax.annotate(str(yy), (xx, yy), xytext=(0, 7), textcoords="offset points",
                ha="center", fontsize=8, color=C.INK_2)
style(ax, "Stations 'disappear' in winter - the count tracks demand, not the network",
      "Distinct station names seen as a trip endpoint that month; a station with no "
      "trips simply does not appear", "Distinct stations")
ax.tick_params(axis="x", rotation=45)
ax.margins(y=0.18)
save(fig, "K17_unique_stations_per_month.png")

# ---- K18 station long tail ---------------------------------------------------
fig, ax = plt.subplots(figsize=(9, 4.0))
vals = top_start.values
ax.fill_between(range(len(vals)), vals, color=C.SERIES[0], alpha=0.85)
share = np.cumsum(vals) / vals.sum()
i50 = int(np.searchsorted(share, 0.5)) + 1
ax.axvline(i50, color=C.SERIES[1], ls="--", lw=1.6)
ax.annotate("%d stations carry 50%% of trips" % i50, (i50, vals.max() * 0.7),
            xytext=(10, 0), textcoords="offset points", color=C.SERIES[1],
            fontsize=9, fontweight="bold")
style(ax, "Station demand follows a long tail",
      "Stations ranked by departures, %d stations in total" % len(vals),
      "Departures", "Station rank")
ax.yaxis.set_major_formatter(THOUSANDS)
save(fig, "K18_station_long_tail.png")

# ---- K19 trips per station per day --------------------------------------------
spd = np.fromiter(station_day.values(), dtype="int64")
fig, ax = plt.subplots(figsize=(9, 4.0))
ax.hist(spd, bins=np.arange(0.5, 60.5, 1), color=C.SERIES[0])
style(ax, "A typical station sees only a handful of departures a day",
      "Departures per station per calendar day (median %d, capped at 60 for display)"
      % int(np.median(spd)), "Station-days", "Departures in the day")
ax.yaxis.set_major_formatter(THOUSANDS)
save(fig, "K19_trips_per_station_per_day.png")

# ---- K20 correlation matrix -----------------------------------------------------
fig, ax = plt.subplots(figsize=(8.6, 7.4))
im = ax.imshow(corr, cmap="RdBu_r", vmin=-1, vmax=1)
ax.set_xticks(range(K), NUMERIC, rotation=45, ha="right", fontsize=8)
ax.set_yticks(range(K), NUMERIC, fontsize=8)
for i in range(K):
    for j in range(K):
        v = corr[i, j]
        ax.text(j, i, "%.2f" % v, ha="center", va="center", fontsize=7,
                color="white" if abs(v) > 0.55 else C.INK)
ax.set_title("Only geographically coupled features correlate strongly",
             loc="left", pad=24, color=C.INK)
subtitle(ax, "Pearson correlation, {:,} complete cases".format(corr_n))
for s in ax.spines.values():
    s.set_visible(False)
cb = fig.colorbar(im, ax=ax, shrink=0.6, pad=0.02)
cb.outline.set_visible(False)
save(fig, "K20_correlation_matrix.png")

# ---- K21 station_id integrity ---------------------------------------------------
nid = pd.Series({k: len(v) for k, v in name_to_ids.items()}).value_counts().sort_index()
fig, ax = plt.subplots(figsize=(7.4, 4.2))
ax.bar(nid.index.astype(str), nid.values, color=C.SERIES[0], width=0.6)
for xx, yy in zip(nid.index.astype(str), nid.values):
    ax.annotate("{:,}".format(yy), (xx, yy), xytext=(0, 4),
                textcoords="offset points", ha="center", fontsize=9, color=C.INK_2)
style(ax, "Most station names carry more than one station_id",
      "Number of distinct station_id values observed per station name",
      "Station names", "Distinct station_id values for that name")
ax.margins(y=0.18)
save(fig, "K21_ids_per_station_name.png")

print("\nDone. %d figures in %s" % (len(os.listdir(FIG)), FIG))
