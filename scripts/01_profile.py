"""
TASK 1 - DATA UNDERSTANDING
===========================
Single streaming pass over the 12 raw monthly CSVs. Nothing is modified here;
this script only measures. It emits:

  outputs/stats/*.csv     machine-readable profiling tables
  outputs/figures/*.png   figures for the kick-off deck and the final report

Run:  python scripts/01_profile.py
"""
from __future__ import annotations
import os
import sys
import json
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
C.ensure_dirs()

# ==========================================================================
# Accumulators
# ==========================================================================
per_month      = []
missing_cells  = {c: 0 for c in C.RAW_COLUMNS}
miss_by_bike   = {}                      # bike -> [n_rows, n_missing_start, n_missing_end]
hour_x_user    = {}                      # (hour, user) -> count
dow_x_user     = {}                      # (dow, user)  -> count
month_x_user   = {}                      # (ym, user)   -> count
month_x_bike   = {}                      # (ym, bike)   -> count
station_ids    = {}                      # cleaned name -> set(ids)
id_names       = {}                      # id -> set(cleaned names)
raw_name_defect = set()                  # names with whitespace defects
durations      = []                      # float32 seconds, whole dataset
total_rows     = 0
both_station_missing = one_station_missing = 0
seen_ids, dup_within, dup_across = set(), 0, 0
geo_outliers   = 0
rule_hits      = {r: 0 for r, _ in C.RULES}
lat_rng = [np.inf, -np.inf]
lng_rng = [np.inf, -np.inf]
ts_rng  = [pd.Timestamp.max, pd.Timestamp.min]

files = C.monthly_files()
print("Profiling %d monthly files ...\n" % len(files))

for path in files:
    fname = os.path.basename(path)
    df = C.read_raw(path)
    n = len(df)
    total_rows += n

    # ---- completeness ------------------------------------------------
    miss = df.isna().sum()
    for c in C.RAW_COLUMNS:
        missing_cells[c] += int(miss[c])

    sm, em = df.start_station_name.isna(), df.end_station_name.isna()
    both_station_missing += int((sm & em).sum())
    one_station_missing  += int((sm ^ em).sum())

    # ---- uniqueness --------------------------------------------------
    ids = set(df.ride_id.dropna())
    dup_within += n - len(ids)
    dup_across += len(seen_ids & ids)
    seen_ids |= ids

    # ---- duration ----------------------------------------------------
    dur = (df.ended_at - df.started_at).dt.total_seconds()
    durations.append(dur.to_numpy(dtype="float32"))

    # ---- rule pre-count (independent masks, for the quality register) --
    dist  = C.haversine_km(df.start_lat, df.start_lng, df.end_lat, df.end_lng)
    speed = np.where(dur > 0, dist / (dur / 3600.0), np.nan)
    oob = (~df.start_lat.between(C.CHI_LAT_MIN, C.CHI_LAT_MAX)
           | ~df.start_lng.between(C.CHI_LNG_MIN, C.CHI_LNG_MAX)
           | (df.end_lat.notna() & ~df.end_lat.between(C.CHI_LAT_MIN, C.CHI_LAT_MAX))
           | (df.end_lng.notna() & ~df.end_lng.between(C.CHI_LNG_MIN, C.CHI_LNG_MAX)))
    geo_outliers += int(oob.sum())
    rule_hits["R1_missing_end_coords"]     += int(df.end_lat.isna().sum())
    rule_hits["R2_negative_duration"]      += int((dur < 0).sum())
    rule_hits["R3_duration_under_60s"]     += int(((dur >= 0) & (dur < C.MIN_DURATION_SEC)).sum())
    rule_hits["R4_duration_over_24h"]      += int((dur > C.MAX_DURATION_SEC).sum())
    rule_hits["R5_coords_outside_chicago"] += int(oob.sum())
    rule_hits["R6_implausible_speed"]      += int(np.nansum(speed > C.MAX_SPEED_KMH))

    # ---- distributions ------------------------------------------------
    st = df.started_at
    ym = st.dt.strftime("%Y-%m")
    for (k, u), v in df.groupby([st.dt.hour, "member_casual"], observed=True).size().items():
        hour_x_user[(int(k), u)] = hour_x_user.get((int(k), u), 0) + int(v)
    for (k, u), v in df.groupby([st.dt.dayofweek, "member_casual"], observed=True).size().items():
        dow_x_user[(int(k), u)] = dow_x_user.get((int(k), u), 0) + int(v)
    for (k, u), v in df.groupby([ym, "member_casual"], observed=True).size().items():
        month_x_user[(k, u)] = month_x_user.get((k, u), 0) + int(v)
    for (k, b), v in df.groupby([ym, "rideable_type"], observed=True).size().items():
        month_x_bike[(k, b)] = month_x_bike.get((k, b), 0) + int(v)

    for b, g in df.groupby("rideable_type", observed=True):
        acc = miss_by_bike.setdefault(b, [0, 0, 0])
        acc[0] += len(g)
        acc[1] += int(g.start_station_name.isna().sum())
        acc[2] += int(g.end_station_name.isna().sum())

    # ---- station identity integrity ------------------------------------
    for nm_col, id_col in (("start_station_name", "start_station_id"),
                           ("end_station_name", "end_station_id")):
        sub = df[[nm_col, id_col]].dropna().drop_duplicates()
        raw_name_defect.update(x for x in sub[nm_col].unique()
                               if x != x.strip() or "  " in x)
        cn = C.clean_name(sub[nm_col])
        for nm, sid in zip(cn, sub[id_col]):
            station_ids.setdefault(nm, set()).add(sid)
            id_names.setdefault(sid, set()).add(nm)

    # ---- ranges ---------------------------------------------------------
    for c in ("start_lat", "end_lat"):
        v = df[c].dropna()
        if len(v):
            lat_rng[0] = min(lat_rng[0], float(v.min())); lat_rng[1] = max(lat_rng[1], float(v.max()))
    for c in ("start_lng", "end_lng"):
        v = df[c].dropna()
        if len(v):
            lng_rng[0] = min(lng_rng[0], float(v.min())); lng_rng[1] = max(lng_rng[1], float(v.max()))
    ts_rng[0] = min(ts_rng[0], st.min()); ts_rng[1] = max(ts_rng[1], df.ended_at.max())

    per_month.append({
        "file": fname,
        "year_month": ym.mode().iat[0],
        "rows": n,
        "missing_cells": int(miss.sum()),
        "pct_cells_missing": round(100.0 * miss.sum() / (n * len(C.RAW_COLUMNS)), 2),
        "member": int((df.member_casual == "member").sum()),
        "casual": int((df.member_casual == "casual").sum()),
        "electric_bike": int((df.rideable_type == "electric_bike").sum()),
        "classic_bike": int((df.rideable_type == "classic_bike").sum()),
        "median_duration_min": round(float(dur.median() / 60.0), 2),
    })
    print("  %-32s rows=%9s  missing_cells=%9s" % (fname, "{:,}".format(n), "{:,}".format(int(miss.sum()))))
    del df

dur_all = np.concatenate(durations)
del durations

# ==========================================================================
# Stats tables
# ==========================================================================
pm = pd.DataFrame(per_month)
pm.to_csv(os.path.join(C.STAT_DIR, "01_volume_by_month.csv"), index=False)

miss_tbl = pd.DataFrame({
    "column":       C.RAW_COLUMNS,
    "dtype":        [str(C.RAW_DTYPES.get(c, "datetime64[ns]")) for c in C.RAW_COLUMNS],
    "missing":      [missing_cells[c] for c in C.RAW_COLUMNS],
}).assign(pct_missing=lambda d: (100.0 * d.missing / total_rows).round(2))
miss_tbl.to_csv(os.path.join(C.STAT_DIR, "02_missing_by_column.csv"), index=False)

mb = pd.DataFrame([{"rideable_type": k, "rows": v[0],
                    "missing_start_station": v[1], "missing_end_station": v[2],
                    "pct_missing_start": round(100.0 * v[1] / v[0], 2),
                    "pct_missing_end":   round(100.0 * v[2] / v[0], 2)}
                   for k, v in miss_by_bike.items()]).sort_values("rows", ascending=False)
mb.to_csv(os.path.join(C.STAT_DIR, "03_missing_by_biketype.csv"), index=False)

pcts = [0.1, 1, 5, 10, 25, 50, 75, 90, 95, 99, 99.9]
dmin = dur_all / 60.0
dur_tbl = pd.DataFrame({"statistic": ["count", "mean", "std", "min", "max"] + ["p%s" % p for p in pcts],
                        "minutes": [len(dmin), dmin.mean(), dmin.std(), dmin.min(), dmin.max()]
                                   + [float(np.percentile(dmin, p)) for p in pcts]})
dur_tbl["minutes"] = dur_tbl.minutes.round(3)
dur_tbl.to_csv(os.path.join(C.STAT_DIR, "04_duration_stats.csv"), index=False)

reg = pd.DataFrame([{"rule": r, "description": d, "rows_matched": rule_hits[r],
                     "pct_of_dataset": round(100.0 * rule_hits[r] / total_rows, 4)}
                    for r, d in C.RULES])
reg.to_csv(os.path.join(C.STAT_DIR, "05_quality_rule_hits.csv"), index=False)

multi_id = {n: s for n, s in station_ids.items() if len(s) > 1}
multi_nm = {i: s for i, s in id_names.items() if len(s) > 1}
integrity = pd.DataFrame([
    {"check": "distinct cleaned station names",          "value": len(station_ids)},
    {"check": "distinct station_id values",              "value": len(id_names)},
    {"check": "names mapping to >1 station_id",          "value": len(multi_id)},
    {"check": "station_ids mapping to >1 name",          "value": len(multi_nm)},
    {"check": "raw names with whitespace defects",       "value": len(raw_name_defect)},
])
integrity.to_csv(os.path.join(C.STAT_DIR, "06_station_identity_integrity.csv"), index=False)
pd.DataFrame([{"station_name": n, "station_ids": " | ".join(sorted(s))}
              for n, s in sorted(multi_id.items())]
             ).to_csv(os.path.join(C.STAT_DIR, "06b_names_with_multiple_ids.csv"), index=False)
pd.DataFrame([{"station_id": i, "station_names": " | ".join(sorted(s))}
              for i, s in sorted(multi_nm.items())]
             ).to_csv(os.path.join(C.STAT_DIR, "06c_ids_with_multiple_names.csv"), index=False)

hx = (pd.Series(hour_x_user).rename_axis(["hour", "member_casual"]).rename("trips")
        .reset_index().pivot(index="hour", columns="member_casual", values="trips").fillna(0).astype(int))
hx.to_csv(os.path.join(C.STAT_DIR, "07_trips_by_hour_usertype.csv"))
dx = (pd.Series(dow_x_user).rename_axis(["dow", "member_casual"]).rename("trips")
        .reset_index().pivot(index="dow", columns="member_casual", values="trips").fillna(0).astype(int))
dx.index = [C.DAY_NAMES[i] for i in dx.index]
dx.to_csv(os.path.join(C.STAT_DIR, "08_trips_by_dayofweek_usertype.csv"))
mx = (pd.Series(month_x_user).rename_axis(["year_month", "member_casual"]).rename("trips")
        .reset_index().pivot(index="year_month", columns="member_casual", values="trips").fillna(0).astype(int))
mx.to_csv(os.path.join(C.STAT_DIR, "09_trips_by_month_usertype.csv"))
bx = (pd.Series(month_x_bike).rename_axis(["year_month", "rideable_type"]).rename("trips")
        .reset_index().pivot(index="year_month", columns="rideable_type", values="trips").fillna(0).astype(int))
bx.to_csv(os.path.join(C.STAT_DIR, "10_trips_by_month_biketype.csv"))

summary = {
    "total_rows": int(total_rows),
    "n_columns": len(C.RAW_COLUMNS),
    "n_files": len(files),
    "date_range": [str(ts_rng[0]), str(ts_rng[1])],
    "duplicate_ride_id_within_file": int(dup_within),
    "duplicate_ride_id_across_files": int(dup_across),
    "distinct_station_names": len(station_ids),
    "both_stations_missing": int(both_station_missing),
    "exactly_one_station_missing": int(one_station_missing),
    "coord_range_lat": lat_rng,
    "coord_range_lng": lng_rng,
    "coords_outside_chicago_bbox": int(geo_outliers),
    "total_rows_matching_any_quality_rule": None,
}
with open(os.path.join(C.STAT_DIR, "00_summary.json"), "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2)

# ==========================================================================
# Figures
# ==========================================================================
plt.rcParams.update({
    "figure.facecolor": C.SURFACE, "axes.facecolor": C.SURFACE,
    "savefig.facecolor": C.SURFACE, "text.color": C.INK,
    "axes.labelcolor": C.INK_2, "xtick.color": C.INK_2, "ytick.color": C.INK_2,
    "font.size": 10, "axes.titlesize": 12, "axes.titleweight": "bold",
    "figure.dpi": 150, "savefig.bbox": "tight",
})
THOUSANDS = FuncFormatter(lambda v, p: "{:,.0f}".format(v))


def style(ax, title, sub=None, ylab=None, xlab=None, ygrid=True):
    ax.set_title(title, loc="left", pad=18 if sub else 10, color=C.INK)
    if sub:
        ax.text(0, 1.02, sub, transform=ax.transAxes, fontsize=9, color=C.INK_2, va="bottom")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(C.GRID)
    if ygrid:
        ax.grid(axis="y", color=C.GRID, lw=0.8)
        ax.set_axisbelow(True)
    if ylab:
        ax.set_ylabel(ylab)
    if xlab:
        ax.set_xlabel(xlab)


def save(fig, name):
    p = os.path.join(C.FIG_DIR, name)
    fig.savefig(p)
    plt.close(fig)
    print("  figure -> %s" % name)


print("\nRendering figures ...")

# F1 - monthly volume (single series -> single hue, no legend needed)
fig, ax = plt.subplots(figsize=(9, 4.2))
ax.bar(pm.year_month, pm.rows, color=C.SERIES[0], width=0.68)
for x, y in zip(pm.year_month, pm.rows):
    ax.annotate("{:,.0f}k".format(y / 1000), (x, y), textcoords="offset points",
                xytext=(0, 4), ha="center", fontsize=8, color=C.INK_2)
style(ax, "Divvy trip volume collapses in winter and peaks in August",
      "Monthly trips, Oct 2024 - Sep 2025 (n = {:,})".format(total_rows), "Trips")
ax.yaxis.set_major_formatter(THOUSANDS)
ax.tick_params(axis="x", rotation=45)
ax.margins(y=0.14)
save(fig, "F1_monthly_volume.png")

# F2 - the headline finding: missingness is entirely an e-bike phenomenon
fig, ax = plt.subplots(figsize=(6.2, 4.2))
order = mb.sort_values("pct_missing_start")
ax.bar(order.rideable_type, order.pct_missing_start, color=C.SERIES[0], width=0.5)
for x, y in zip(order.rideable_type, order.pct_missing_start):
    ax.annotate("%.2f%%" % y, (x, y), textcoords="offset points", xytext=(0, 4),
                ha="center", fontsize=11, color=C.INK, fontweight="bold")
style(ax, "Missing station names come only from dockless e-bikes",
      "Share of trips with no start-station name, by bike type", "% of trips missing start station")
ax.margins(y=0.18)
save(fig, "F2_missing_by_biketype.png")

# F3 - missingness by column
fig, ax = plt.subplots(figsize=(7.5, 4.6))
mt = miss_tbl.sort_values("pct_missing")
ax.barh(mt.column, mt.pct_missing, color=C.SERIES[0], height=0.62)
for y, v in zip(mt.column, mt.pct_missing):
    if v > 0:
        ax.annotate("%.2f%%" % v, (v, y), textcoords="offset points", xytext=(4, 0),
                    va="center", fontsize=8, color=C.INK_2)
style(ax, "Only station and end-coordinate fields are incomplete",
      "Missing values by column, all 5.5 M rows", xlab="% missing", ygrid=False)
ax.grid(axis="x", color=C.GRID, lw=0.8)
ax.set_axisbelow(True)
ax.margins(x=0.12)
save(fig, "F3_missing_by_column.png")

# F4 - duration distribution, log scale, with cleaning cutoffs
fig, ax = plt.subplots(figsize=(9, 4.4))
pos = dur_all[dur_all > 0]
bins = np.logspace(0, np.log10(max(pos.max(), 10)), 90)
ax.hist(pos, bins=bins, color=C.SERIES[0], edgecolor="none")
for xv, lab in ((C.MIN_DURATION_SEC, "60 s cutoff"), (C.MAX_DURATION_SEC, "24 h cutoff")):
    ax.axvline(xv, color=C.SERIES[1], lw=1.6, ls="--")
    ax.annotate(lab, (xv, ax.get_ylim()[1] * 0.92), xytext=(6, 0), textcoords="offset points",
                fontsize=9, color=C.SERIES[1], fontweight="bold")
ax.set_xscale("log")
style(ax, "Trip duration is long-tailed, with junk at both extremes",
      "All trips, log-scaled seconds (median 9.5 min). Dashed lines mark the rules applied in preprocessing.",
      "Trips", "Duration (seconds, log scale)")
ax.yaxis.set_major_formatter(THOUSANDS)
save(fig, "F4_duration_distribution.png")

# F5 - hour-of-day profile by user type (2 series -> legend + direct labels)
fig, ax = plt.subplots(figsize=(9, 4.4))
for i, col in enumerate(["member", "casual"]):
    ax.plot(hx.index, hx[col], color=C.SERIES[i], lw=2, label=col)
    xm = int(hx[col].idxmax())
    ax.annotate(col, (xm, hx[col].max()), textcoords="offset points", xytext=(6, 6),
                fontsize=10, color=C.SERIES[i], fontweight="bold")
style(ax, "Members ride a commute pattern; casual riders ride the afternoon",
      "Trips by hour of day, whole study period", "Trips", "Hour of day")
ax.yaxis.set_major_formatter(THOUSANDS)
ax.set_xticks(range(0, 24, 2))
ax.legend(frameon=False, loc="upper left", fontsize=9)
save(fig, "F5_hourly_by_usertype.png")

# F6 - day-of-week by user type (grouped bars, 2 px spacer between bars)
fig, ax = plt.subplots(figsize=(9, 4.2))
x = np.arange(len(dx)); w = 0.38
for i, col in enumerate(["member", "casual"]):
    ax.bar(x + (i - 0.5) * (w + 0.02), dx[col], width=w, color=C.SERIES[i], label=col)
style(ax, "Member demand is weekday-led, casual demand is weekend-led",
      "Trips by day of week", "Trips")
ax.set_xticks(x); ax.set_xticklabels([d[:3] for d in dx.index])
ax.yaxis.set_major_formatter(THOUSANDS)
ax.legend(frameon=False, fontsize=9)
save(fig, "F6_dayofweek_by_usertype.png")

# F7 - seasonality by user type
fig, ax = plt.subplots(figsize=(9, 4.2))
for i, col in enumerate(["member", "casual"]):
    ax.plot(mx.index, mx[col], color=C.SERIES[i], lw=2, marker="o", ms=4, label=col)
    ax.annotate(col, (mx.index[-1], mx[col].iloc[-1]), textcoords="offset points",
                xytext=(6, 0), fontsize=10, color=C.SERIES[i], fontweight="bold", va="center")
style(ax, "Casual ridership is far more seasonal than membership ridership",
      "Monthly trips by user type", "Trips")
ax.yaxis.set_major_formatter(THOUSANDS)
ax.tick_params(axis="x", rotation=45)
ax.margins(x=0.08)
ax.legend(frameon=False, fontsize=9)
save(fig, "F7_seasonality_by_usertype.png")

print("\n" + "=" * 68)
print("TOTAL ROWS          : {:,}".format(total_rows))
print("DATE RANGE          : %s -> %s" % (ts_rng[0], ts_rng[1]))
print("DUPLICATE ride_id   : within-file %d | across-file %d" % (dup_within, dup_across))
print("STATION NAMES       : {:,}  ({:,} map to >1 id)".format(len(station_ids), len(multi_id)))
print("COORD RANGE         : lat %s  lng %s" % (lat_rng, lng_rng))
print("OUTSIDE CHICAGO BBOX: {:,}".format(geo_outliers))
print("\nQUALITY RULE HITS (independent masks, may overlap)")
print(reg.to_string(index=False))
print("\nStats -> %s\nFigures -> %s" % (C.STAT_DIR, C.FIG_DIR))
