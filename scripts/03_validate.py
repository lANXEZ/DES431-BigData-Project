"""
TASK 2 - VALIDATION & HANDOFF RECONCILIATION
============================================
Re-reads the cleaned Parquet dataset from disk and proves the cleaning rules
actually hold. Also emits the reconciliation card that the AWS/Spark owner must
reproduce after running the same logic in Glue - if their numbers differ from
this file, the two implementations have drifted.

  outputs/stats/13_validation_checks.csv
  outputs/stats/14_reconciliation_card.csv
  outputs/figures/F8..F10*.png

Run:  python scripts/03_validate.py
"""
from __future__ import annotations
import os
import sys
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

COLS = ["ride_id", "rideable_type", "member_casual", "duration_min", "haversine_km",
        "avg_speed_kmh", "trip_type", "is_round_trip", "start_lat", "start_lng",
        "end_lat", "end_lng", "hour", "day_of_week", "is_weekend", "season",
        "is_rush_hour", "start_geohash7", "start_station_key"]

print("Reading cleaned dataset ...")
df = pd.read_parquet(C.CLEAN_DIR, columns=COLS)
n = len(df)
print("  %s rows x %d columns\n" % ("{:,}".format(n), df.shape[1]))

# ==========================================================================
# Assertions
# ==========================================================================
checks = []


def check(name, ok, detail=""):
    checks.append({"check": name, "result": "PASS" if ok else "FAIL", "detail": detail})
    print("  [%s] %-52s %s" % ("PASS" if ok else "FAIL", name, detail))


dmin, dmax = float(df.duration_min.min()), float(df.duration_min.max())
check("duration >= 60 s for every row", dmin >= 1.0, "min = %.4f min" % dmin)
check("duration <= 24 h for every row", dmax <= 1440.0, "max = %.2f min" % dmax)
check("no negative durations", dmin > 0, "min = %.4f min" % dmin)
check("no duplicate ride_id", df.ride_id.duplicated().sum() == 0,
      "%d duplicates" % df.ride_id.duplicated().sum())
check("no null ride_id / duration / coords",
      int(df[["ride_id", "duration_min", "start_lat", "end_lat"]].isna().sum().sum()) == 0)
check("all start coords inside Chicago bbox",
      bool(df.start_lat.between(C.CHI_LAT_MIN, C.CHI_LAT_MAX).all()
           and df.start_lng.between(C.CHI_LNG_MIN, C.CHI_LNG_MAX).all()),
      "lat [%.4f, %.4f]" % (df.start_lat.min(), df.start_lat.max()))
check("all end coords inside Chicago bbox",
      bool(df.end_lat.between(C.CHI_LAT_MIN, C.CHI_LAT_MAX).all()
           and df.end_lng.between(C.CHI_LNG_MIN, C.CHI_LNG_MAX).all()),
      "lat [%.4f, %.4f]" % (df.end_lat.min(), df.end_lat.max()))
smax = float(df.avg_speed_kmh.max())
check("avg speed <= 60 km/h", smax <= C.MAX_SPEED_KMH, "max = %.2f km/h" % smax)
check("geohash present for every row", int((df.start_geohash7.fillna("") == "").sum()) == 0)
check("trip_type has no nulls and only 4 levels",
      df.trip_type.notna().all() and df.trip_type.nunique() == 4,
      " / ".join(sorted(df.trip_type.unique())))
check("is_round_trip is NA exactly when a station is missing",
      int((df.is_round_trip.isna() != (df.trip_type != "docked")).sum()) == 0)
check("hour in 0..23 and day_of_week in 0..6",
      bool(df.hour.between(0, 23).all() and df.day_of_week.between(0, 6).all()))
check("dockless rows retained (structural missingness preserved)",
      int((df.trip_type != "docked").sum()) > 0,
      "{:,} dockless-endpoint trips kept".format(int((df.trip_type != "docked").sum())))

pd.DataFrame(checks).to_csv(os.path.join(C.STAT_DIR, "13_validation_checks.csv"), index=False)

# ==========================================================================
# Reconciliation card - the numbers the Spark/Glue run must reproduce
# ==========================================================================
tt = df.trip_type.value_counts()
card = pd.DataFrame([
    ("clean_row_count",             n),
    ("distinct_ride_id",            int(df.ride_id.nunique())),
    ("mean_duration_min",           round(float(df.duration_min.mean()), 4)),
    ("median_duration_min",         round(float(df.duration_min.median()), 4)),
    ("mean_haversine_km",           round(float(df.haversine_km.mean()), 4)),
    ("mean_avg_speed_kmh",          round(float(df.avg_speed_kmh.mean()), 4)),
    ("pct_member",                  round(100.0 * (df.member_casual == "member").mean(), 4)),
    ("pct_electric_bike",           round(100.0 * (df.rideable_type == "electric_bike").mean(), 4)),
    ("pct_weekend",                 round(100.0 * df.is_weekend.mean(), 4)),
    ("pct_rush_hour",               round(100.0 * df.is_rush_hour.mean(), 4)),
    ("trips_docked",                int(tt.get("docked", 0))),
    ("trips_dockless_start",        int(tt.get("dockless_start", 0))),
    ("trips_dockless_end",          int(tt.get("dockless_end", 0))),
    ("trips_fully_dockless",        int(tt.get("fully_dockless", 0))),
    ("distinct_start_geohash7",     int(df.start_geohash7.nunique())),
    ("distinct_start_station_key",  int(df.start_station_key.nunique())),
    ("round_trip_share_of_docked",  round(100.0 * df.is_round_trip.mean(skipna=True), 4)),
], columns=["metric", "value"], dtype=object)
card["value"] = card.value.map(lambda v: "{:,}".format(v) if isinstance(v, int) else "{:.4f}".format(v))
card.to_csv(os.path.join(C.STAT_DIR, "14_reconciliation_card.csv"), index=False)
print("\nRECONCILIATION CARD")
print(card.to_string(index=False))

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
        ax.grid(axis="y", color=C.GRID, lw=0.8); ax.set_axisbelow(True)
    if ylab: ax.set_ylabel(ylab)
    if xlab: ax.set_xlabel(xlab)


def save(fig, name):
    fig.savefig(os.path.join(C.FIG_DIR, name)); plt.close(fig)
    print("  figure -> %s" % name)


print("\nRendering figures ...")

# F8 - what the cleaning actually removed
reg = pd.read_csv(os.path.join(C.STAT_DIR, "12_quality_register.csv"))
reg = reg[reg.rule != "TOTAL"].sort_values("rows_removed_by_this_rule")
fig, ax = plt.subplots(figsize=(8.4, 4.2))
ax.barh(reg.rule, reg.rows_removed_by_this_rule, color=C.SERIES[0], height=0.6)
for y, v in zip(reg.rule, reg.rows_removed_by_this_rule):
    ax.annotate("{:,}".format(v), (v, y), xytext=(5, 0), textcoords="offset points",
                va="center", fontsize=9, color=C.INK_2)
style(ax, "One rule accounts for 96% of everything removed",
      "Rows dropped per rule (each row attributed once, in rule order). Total removed: 142,350 of 5,539,521 = 2.57%.",
      xlab="Rows removed", ygrid=False)
ax.set_xscale("log"); ax.grid(axis="x", color=C.GRID, lw=0.8); ax.set_axisbelow(True)
ax.margins(x=0.22)
save(fig, "F8_rows_removed_by_rule.png")

# F9 - trip_type composition: the case for keeping dockless rows
fig, ax = plt.subplots(figsize=(7.6, 4.2))
order = ["docked", "dockless_start", "dockless_end", "fully_dockless"]
vals = [int(tt.get(k, 0)) for k in order]
ax.bar(order, vals, color=C.SERIES[0], width=0.6)
for x, v in zip(order, vals):
    ax.annotate("{:,}\n{:.1f}%".format(v, 100.0 * v / n), (x, v), xytext=(0, 4),
                textcoords="offset points", ha="center", fontsize=9, color=C.INK_2)
style(ax, "A station-only analysis would silently discard 3 trips in 10",
      "Clean trips by endpoint type. Geohash binning keeps all {:,} usable for spatial work.".format(n),
      "Trips")
ax.yaxis.set_major_formatter(THOUSANDS); ax.margins(y=0.20)
save(fig, "F9_trip_type_composition.png")

# F10 - duration distribution, raw vs cleaned
raw = []
for p in C.monthly_files():
    d = pd.read_csv(p, usecols=["started_at", "ended_at"], parse_dates=["started_at", "ended_at"])
    raw.append(((d.ended_at - d.started_at).dt.total_seconds() / 60.0).to_numpy("float32"))
raw = np.concatenate(raw)
fig, ax = plt.subplots(figsize=(9, 4.4))
bins = np.logspace(-2, np.log10(2000), 90)
ax.hist(raw[raw > 0], bins=bins, color=C.SERIES[1], edgecolor="none", label="raw", alpha=0.95)
ax.hist(df.duration_min, bins=bins, color=C.SERIES[0], edgecolor="none", label="cleaned", alpha=0.95)
ax.set_xscale("log")
style(ax, "Cleaning removes the sub-minute spike without touching the real distribution",
      "Trip duration in minutes, log scale. Raw n = {:,}; cleaned n = {:,}.".format(len(raw), n),
      "Trips", "Duration (minutes, log scale)")
ax.yaxis.set_major_formatter(THOUSANDS)
ax.legend(frameon=False, fontsize=9)
save(fig, "F10_duration_raw_vs_clean.png")

fails = [c for c in checks if c["result"] == "FAIL"]
print("\n" + "=" * 70)
print("VALIDATION: %d checks, %d passed, %d failed" % (len(checks), len(checks) - len(fails), len(fails)))
if fails:
    for f in fails:
        print("  FAILED: %s  %s" % (f["check"], f["detail"]))
    sys.exit(1)
print("All checks passed. Dataset is ready for the AWS/Glue handoff.")
