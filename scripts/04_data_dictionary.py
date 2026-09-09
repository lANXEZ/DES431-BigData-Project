"""
TASK 1 - DATA DICTIONARY GENERATOR
==================================
Emits the raw and curated schemas as CSV, reading the real dtypes off disk so
the documentation can never drift from the actual data.

  outputs/stats/15_data_dictionary_raw.csv
  outputs/stats/16_data_dictionary_clean.csv

Run:  python scripts/04_data_dictionary.py
"""
from __future__ import annotations
import os
import sys
import glob
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RAW_DESC = {
    "ride_id":            ("Unique trip identifier, 16-char hex", "Natural primary key. Verified unique across all 12 files."),
    "rideable_type":      ("Bike class: classic_bike | electric_bike", "Determines whether station fields can be present at all."),
    "started_at":         ("Trip start timestamp, local Chicago time, ms precision", "Basis for every temporal feature and for partitioning."),
    "ended_at":           ("Trip end timestamp", "Used with started_at to derive duration."),
    "start_station_name": ("Docking station where the trip began", "NULL for dockless e-bike trips - structural, not an error."),
    "start_station_id":   ("Divvy internal station code", "UNRELIABLE: re-numbered and recycled mid-period. Do not join on this."),
    "end_station_name":   ("Docking station where the trip ended", "NULL for dockless e-bike trips."),
    "end_station_id":     ("Divvy internal station code", "Same reliability problem as start_station_id."),
    "start_lat":          ("Start latitude, WGS84 decimal degrees", "Always populated. Basis for spatial binning."),
    "start_lng":          ("Start longitude, WGS84 decimal degrees", "Always populated."),
    "end_lat":            ("End latitude", "Missing on 5,442 rows where the bike was never properly returned."),
    "end_lng":            ("End longitude", "Missing on the same 5,442 rows."),
    "member_casual":      ("Rider type: member (subscriber) | casual (pay-per-ride)", "Primary segmentation dimension."),
}

CLEAN_DESC = {
    "ride_id":            ("Unique trip identifier", "carried through unchanged"),
    "rideable_type":      ("classic_bike | electric_bike", "carried through unchanged"),
    "member_casual":      ("member | casual", "carried through unchanged"),
    "started_at":         ("Trip start timestamp", "carried through unchanged"),
    "ended_at":           ("Trip end timestamp", "carried through unchanged"),
    "start_station_name": ("Start station, whitespace-normalised", "cleaned (trim + collapse internal spaces)"),
    "start_station_id":   ("Original Divvy code, kept for traceability only", "carried through - DO NOT JOIN ON THIS"),
    "start_station_key":  ("Name-derived surrogate key, e.g. CLARK_ST_ARMITAGE_AVE", "ENGINEERED - the stable join key"),
    "end_station_name":   ("End station, whitespace-normalised", "cleaned"),
    "end_station_id":     ("Original Divvy code, traceability only", "carried through - DO NOT JOIN ON THIS"),
    "end_station_key":    ("Name-derived surrogate key", "ENGINEERED - the stable join key"),
    "start_lat":          ("Start latitude", "validated against Chicago bbox"),
    "start_lng":          ("Start longitude", "validated against Chicago bbox"),
    "end_lat":            ("End latitude", "validated; rows with nulls removed"),
    "end_lng":            ("End longitude", "validated; rows with nulls removed"),
    "start_geohash7":     ("Geohash precision 7 (~153 m cell) of the start point", "ENGINEERED - spatial bin, present on EVERY row"),
    "end_geohash7":       ("Geohash precision 7 of the end point", "ENGINEERED - spatial bin, present on EVERY row"),
    "duration_min":       ("Trip duration in minutes", "ENGINEERED - (ended_at - started_at), bounded to [1, 1440]"),
    "haversine_km":       ("Straight-line start-to-end distance, km", "ENGINEERED - lower bound on distance actually ridden"),
    "avg_speed_kmh":      ("haversine_km / duration hours", "ENGINEERED - also used as an outlier detector"),
    "trip_type":          ("docked | dockless_start | dockless_end | fully_dockless", "ENGINEERED - encodes the structural missingness"),
    "is_round_trip":      ("Start station equals end station; NULL unless trip_type = docked", "ENGINEERED"),
    "trip_date":          ("Calendar date of started_at, yyyy-MM-dd", "ENGINEERED"),
    "year":               ("Year of started_at", "ENGINEERED - Hive partition key"),
    "month":              ("Month of started_at, 1-12", "ENGINEERED - Hive partition key"),
    "day":                ("Day of month", "ENGINEERED"),
    "hour":               ("Hour of started_at, 0-23", "ENGINEERED"),
    "day_of_week":        ("0 = Monday .. 6 = Sunday", "ENGINEERED"),
    "day_name":           ("Monday .. Sunday", "ENGINEERED"),
    "is_weekend":         ("day_of_week >= 5", "ENGINEERED"),
    "season":             ("Winter | Spring | Summer | Fall (meteorological)", "ENGINEERED"),
    "time_of_day":        ("Night 0-5 | Morning 6-11 | Afternoon 12-17 | Evening 18-23", "ENGINEERED"),
    "is_rush_hour":       ("Weekday and hour in 07-09 or 16-18", "ENGINEERED"),
}

# ---- raw -----------------------------------------------------------------
miss = pd.read_csv(os.path.join(C.STAT_DIR, "02_missing_by_column.csv")).set_index("column")
raw = pd.DataFrame([{
    "column": c,
    "type": {"string": "string", "float64": "double"}.get(C.RAW_DTYPES.get(c), "timestamp"),
    "pct_missing": float(miss.loc[c, "pct_missing"]),
    "description": RAW_DESC[c][0],
    "note": RAW_DESC[c][1],
} for c in C.RAW_COLUMNS])
raw.to_csv(os.path.join(C.STAT_DIR, "15_data_dictionary_raw.csv"), index=False)

# ---- curated: report the type ATHENA will see, not Arrow's internal name --
def athena_type(col) -> str:
    lt, pt = str(col.logical_type), col.physical_type
    if lt.startswith("String"):     return "string"
    if lt.startswith("Timestamp"):  return "timestamp"
    if lt.startswith("Int(bitWidth=8"):  return "tinyint"
    if lt.startswith("Int(bitWidth=16"): return "smallint"
    if lt.startswith("Int(bitWidth=32"): return "int"
    return {"DOUBLE": "double", "FLOAT": "float", "BOOLEAN": "boolean",
            "INT32": "int", "INT64": "bigint"}.get(pt, pt.lower())


f = sorted(glob.glob(os.path.join(C.CLEAN_DIR, "**", "*.parquet"), recursive=True))[0]
pf = pq.ParquetFile(f)
fields = {pf.schema.column(i).name: athena_type(pf.schema.column(i))
          for i in range(len(pf.schema))}
fields["year"], fields["month"] = "smallint", "tinyint"

clean = pd.DataFrame([{
    "column": c,
    "athena_type": fields.get(c, "?"),
    "partition_key": c in ("year", "month"),
    "origin": "ENGINEERED" if CLEAN_DESC[c][1].startswith("ENGINEERED") else "source",
    "description": CLEAN_DESC[c][0],
    "note": CLEAN_DESC[c][1],
} for c in CLEAN_DESC])
clean.to_csv(os.path.join(C.STAT_DIR, "16_data_dictionary_clean.csv"), index=False)

# ---- Athena DDL: the handoff contract, in executable form ----------------
body = ",\n".join("  `%s` %s" % (r.column, r.athena_type)
                  for r in clean.itertuples() if not r.partition_key)
ddl = """-- Athena / Glue external table for the curated Divvy trip fact table.
-- Generated by scripts/04_data_dictionary.py - do not hand-edit.
-- After creating the table you MUST load the Hive partitions:
--     MSCK REPAIR TABLE divvy.trips_clean;

CREATE EXTERNAL TABLE IF NOT EXISTS divvy.trips_clean (
{body}
)
PARTITIONED BY (
  `year`  smallint,
  `month` tinyint
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/curated/divvy_trips/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
""".format(body=body)
with open(os.path.join(C.HAND_DIR, "athena_create_table.sql"), "w", encoding="utf-8") as fh:
    fh.write(ddl)

print("RAW SCHEMA (%d columns)" % len(raw))
print(raw[["column", "type", "pct_missing"]].to_string(index=False))
print("\nCURATED SCHEMA (%d columns, %d engineered)"
      % (len(clean), int((clean.origin == "ENGINEERED").sum())))
print(clean[["column", "athena_type", "origin"]].to_string(index=False))
