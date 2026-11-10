"""
TASK 2 - DATA PREPROCESSING  (PySpark)
======================================
Spark implementation of 02_preprocess.py. Same rules, same features, same
outputs, written to the same paths:

  outputs/clean/year=YYYY/month=MM/*.parquet   analysis-ready trip fact table
  outputs/rejects/*.parquet                    every dropped row + reject_reason
  outputs/handoff/station_dim.{parquet,csv}    station dimension (name-keyed)
  outputs/handoff/station_hourly_flow.parquet  departures/arrivals per station-hour
  outputs/stats/11_cleaning_audit.csv          row accounting, rule by rule
  outputs/stats/12_quality_register.csv        rule-level summary

Thresholds, rule order, seasons and rush hours are imported from common.py, so
this file and the pandas version cannot drift apart.

EXACT PARITY WITH THE PANDAS VERSION
Results must be value-identical, not just close, so several pandas/numpy
behaviours are reproduced deliberately:
  * numpy rounding is rint (round-half-even) on x * 10^d, not Spark's
    HALF_UP round(); float32 columns are rounded in float32 arithmetic.
  * the CSV is parsed RFC-4180 style (quote escapes quote) - some station
    names contain a literal backslash, which Spark would otherwise treat as
    an escape character.
  * a missing start coordinate counts as outside the Chicago box (pandas
    between() on NaN is False, so ~between() is True).
  * row order within each raw file is tracked, so "first station name seen"
    and duplicate handling pick the same row pandas does.

Run:  python scripts/02_preprocess_spark.py
      python scripts/02_preprocess_spark.py --out-root <dir>   # write elsewhere

On Windows, Spark needs winutils.exe and hadoop.dll in %HADOOP_HOME%\\bin to
write files - see the README.
"""
from __future__ import annotations
import argparse
import glob
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F, types as T

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ap = argparse.ArgumentParser()
ap.add_argument("--out-root", default=C.OUT,
                help="directory that receives clean/, rejects/, handoff/ and stats/ "
                     "(default: outputs/)")
ap.add_argument("--driver-memory", default="8g")
args = ap.parse_args()

OUT       = os.path.abspath(args.out_root)
CLEAN_DIR = os.path.join(OUT, "clean")
REJ_DIR   = os.path.join(OUT, "rejects")
HAND_DIR  = os.path.join(OUT, "handoff")
STAT_DIR  = os.path.join(OUT, "stats")
for d in (HAND_DIR, STAT_DIR):
    os.makedirs(d, exist_ok=True)

if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
    raise SystemExit(
        "HADOOP_HOME is not set. On Windows, Spark needs winutils.exe and hadoop.dll\n"
        "in %HADOOP_HOME%\\bin to write Parquet - see the README.")

files = C.monthly_files()
if not files:
    raise SystemExit("No raw CSVs found in %s - see archive/README.md." % C.RAW_DIR)

spark = (SparkSession.builder
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .appName("divvy-02-preprocess")
         .config("spark.driver.memory", args.driver_memory)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "32")
         .config("spark.sql.parquet.compression.codec", "snappy")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def spath(p: str) -> str:
    """Hadoop wants forward slashes, including for Windows drive paths."""
    return os.path.abspath(p).replace("\\", "/")


# ==========================================================================
# numpy-compatible helpers
# ==========================================================================
def np_round(col, d: int):
    """numpy.round on float64: rint(x * 10^d) / 10^d, ties to even."""
    s = float(10 ** d)
    return F.rint(col * F.lit(s)) / F.lit(s)


def np_round_f32(col, d: int):
    """pandas .astype("float32").round(d): cast, then round in float32.

    float32 * and / are emulated by doing the op in double and casting back,
    which is exact: a product of two floats fits in a double, and division
    rounded first to double then to float equals float division (53 >= 2*24+2).
    """
    s = float(10 ** d)
    x = col.cast("float").cast("double")
    scaled = (x * F.lit(s)).cast("float").cast("double")
    return (F.rint(scaled) / F.lit(s)).cast("float")


# Python's str.isspace() also counts \x1c-\x1f; (?U) makes \s Unicode-aware.
WS = r"(?U)[\s\x1C-\x1F]"


def clean_name(col):
    """common.clean_name: strip, then collapse internal whitespace runs."""
    stripped = F.regexp_replace(col, r"^%s+|%s+$" % (WS, WS), "")
    return F.regexp_replace(stripped, WS + "+", " ")


def station_key(col):
    """common.station_key: name-derived surrogate key - never the station_id."""
    k = F.regexp_replace(F.upper(clean_name(col)), r"[^A-Z0-9]+", "_")
    return F.regexp_replace(k, r"^_+|_+$", "")


def haversine_km(lat1, lon1, lat2, lon2):
    """common.haversine_km, operation for operation."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi = p2 - p1
    dlmb = F.radians(lon2 - lon1)
    s1, s2 = F.sin(dphi / 2.0), F.sin(dlmb / 2.0)
    a = s1 * s1 + F.cos(p1) * F.cos(p2) * (s2 * s2)
    # np.clip, written with when() because greatest()/least() skip NULLs and
    # would turn a missing coordinate into a distance of 0.
    a = F.when(a < 0.0, 0.0).when(a > 1.0, 1.0).otherwise(a)
    return F.lit(2.0 * 6371.0088) * F.asin(F.sqrt(a))


_B32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def add_geohash7(df, lat: str, lon: str, out: str):
    """common.geohash7 in Spark SQL: 35 bits interleaved lon/lat, 7 base-32 chars.

    Built in stages so each stage references columns rather than re-inlining
    the whole bit-interleaving expression seven times.
    """
    nbits, nlon, nlat = 35, 18, 17
    li, ai, code = "_%s_lon_i" % out, "_%s_lat_i" % out, "_%s_code" % out
    df = df.withColumn(li, F.least(F.greatest(
            ((F.col(lon) + 180.0) / 360.0 * float(1 << nlon)).cast("long"),
            F.lit(0)), F.lit((1 << nlon) - 1)))
    df = df.withColumn(ai, F.least(F.greatest(
            ((F.col(lat) + 90.0) / 180.0 * float(1 << nlat)).cast("long"),
            F.lit(0)), F.lit((1 << nlat) - 1)))
    terms = []
    for k in range(nbits):
        src, bitpos = (li, nlon - 1 - k // 2) if k % 2 == 0 else (ai, nlat - 1 - k // 2)
        bit = F.shiftright(F.col(src), bitpos).bitwiseAND(F.lit(1))
        terms.append(F.shiftleft(bit, nbits - 1 - k))
    acc = terms[0]
    for t in terms[1:]:
        acc = acc.bitwiseOR(t)
    df = df.withColumn(code, acc)
    chars = [F.substr(F.lit(_B32),
                      (F.shiftright(F.col(code), nbits - 5 * (j + 1)).bitwiseAND(F.lit(31)) + 1)
                      .cast("int"),
                      F.lit(1))
             for j in range(7)]
    missing = F.col(lat).isNull() | F.col(lon).isNull()
    return (df.withColumn(out, F.when(missing, F.lit("")).otherwise(F.concat(*chars)))
              .drop(li, ai, code))


# ==========================================================================
# Output helpers
# ==========================================================================
CSV_OPTS = {"header": "true", "quote": '"', "escape": '"', "emptyValue": "",
            "ignoreLeadingWhiteSpace": "false", "ignoreTrailingWhiteSpace": "false",
            "lineSep": "\n"}


def write_single(df, path: str, fmt: str, sort_cols=None) -> None:
    """Write a small DataFrame as ONE file at exactly `path`.

    Spark always writes a directory of part files; downstream scripts expect a
    plain station_dim.csv etc., so write to a temp dir and move the part out.
    """
    tmp = path + ".__spark_tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    out = df.coalesce(1)
    if sort_cols:
        out = out.sortWithinPartitions(*sort_cols)
    w = out.write.mode("overwrite").format(fmt)
    if fmt == "csv":
        w = w.options(**CSV_OPTS)
    w.save(spath(tmp))
    part = [p for p in glob.glob(os.path.join(tmp, "part-*")) if not p.endswith(".crc")]
    assert len(part) == 1, part
    if os.path.exists(path):
        os.remove(path)
    os.replace(part[0], path)
    shutil.rmtree(tmp)


def drop_checksums(root: str) -> None:
    """Hadoop's local filesystem leaves .crc sidecars next to every file."""
    for dp, _, fs in os.walk(root):
        for f in fs:
            if f.endswith(".crc"):
                os.remove(os.path.join(dp, f))


# ==========================================================================
# 1. READ - all columns as strings, then cast, mirroring pd.read_csv
# ==========================================================================
# pandas' default NA markers; none occur in this data today, but a new month
# could contain them and the two implementations must still agree.
PANDAS_NA = ["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan",
             "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "None",
             "n/a", "nan", "null"]

print("Preprocessing %d monthly files with Spark %s ...\n" % (len(files), spark.version))

raw = (spark.read
       .options(header="true", quote='"', escape='"', mode="FAILFAST")
       .schema(T.StructType([T.StructField(c, T.StringType()) for c in C.RAW_COLUMNS]))
       .csv([spath(p) for p in files])
       # Row position within its source file: (file, split offset, row id).
       # monotonically_increasing_id rises with row order inside a split.
       .select("*",
               F.col("_metadata.file_name").alias("source_file"),
               F.col("_metadata.file_block_start").alias("_blk"),
               F.monotonically_increasing_id().alias("_mid")))

cols = []
for c in C.RAW_COLUMNS:
    v = F.when(F.col(c).isin(PANDAS_NA), F.lit(None)).otherwise(F.col(c))
    if c in C.DATE_COLS:
        v = v.cast("timestamp")
    elif C.RAW_DTYPES[c] == "float64":
        v = v.cast("double")
    cols.append(v.alias(c))
raw = raw.select(*cols, "source_file", "_blk", "_mid")

# ==========================================================================
# 2. STRUCTURAL CLEANING
# ==========================================================================
# De-duplicate on ride_id within each file, keeping the first occurrence
# (pandas drop_duplicates(keep="first")). 0 duplicates expected.
first_seen = Window.partitionBy("source_file", "ride_id").orderBy("_blk", "_mid")
df = (raw.withColumn("_dup_rank", F.row_number().over(first_seen))
         .withColumn("start_station_name", clean_name(F.col("start_station_name")))
         .withColumn("end_station_name",   clean_name(F.col("end_station_name"))))

# ==========================================================================
# 3. DERIVED QUANTITIES + QUALITY RULES
# ==========================================================================
dur_sec = (F.unix_micros("ended_at") - F.unix_micros("started_at")) / 1e6
df = (df.withColumn("_dur_sec", dur_sec)
        .withColumn("_dist_km", haversine_km(F.col("start_lat"), F.col("start_lng"),
                                             F.col("end_lat"), F.col("end_lng")))
        .withColumn("_speed", F.when(F.col("_dur_sec") > 0,
                                     F.col("_dist_km") / (F.col("_dur_sec") / 3600.0))))


def inside(col, lo, hi):
    return F.coalesce(col.between(lo, hi), F.lit(False))


oob = (~inside(F.col("start_lat"), C.CHI_LAT_MIN, C.CHI_LAT_MAX)
       | ~inside(F.col("start_lng"), C.CHI_LNG_MIN, C.CHI_LNG_MAX)
       | (F.col("end_lat").isNotNull() & ~inside(F.col("end_lat"), C.CHI_LAT_MIN, C.CHI_LAT_MAX))
       | (F.col("end_lng").isNotNull() & ~inside(F.col("end_lng"), C.CHI_LNG_MIN, C.CHI_LNG_MAX)))
dur = F.col("_dur_sec")
masks = {
    "R1_missing_end_coords":     F.col("end_lat").isNull() | F.col("end_lng").isNull(),
    "R2_negative_duration":      dur < 0,
    "R3_duration_under_60s":     (dur >= 0) & (dur < C.MIN_DURATION_SEC),
    "R4_duration_over_24h":      dur > C.MAX_DURATION_SEC,
    "R5_coords_outside_chicago": oob,
    "R6_implausible_speed":      F.col("_speed") > C.MAX_SPEED_KMH,
}
rule_named = [r for r, _ in C.RULES]
for r in rule_named:
    df = df.withColumn("_m_" + r, F.coalesce(masks[r], F.lit(False)))
# Priority tagging: the first matching rule (in common.RULES order) owns the row.
df = df.withColumn("reject_reason", F.coalesce(
    *[F.when(F.col("_m_" + r), F.lit(r)) for r in rule_named]))

df = df.persist(StorageLevel.MEMORY_AND_DISK)

# ==========================================================================
# 4. AUDIT  (per raw file, same columns as the pandas version)
# ==========================================================================
first = F.col("_dup_rank") == 1
audit = (df.groupBy("source_file").agg(
            F.count("*").alias("raw_rows"),
            F.sum((~first).cast("long")).alias("duplicate_ride_id"),
            *[F.sum((first & F.col("_m_" + r)).cast("long")).alias(r) for r in rule_named],
            *[F.sum((first & (F.col("reject_reason") == r)).cast("long")).alias(r + "_owned")
              for r in rule_named],
            F.sum((first & F.col("reject_reason").isNotNull()).cast("long")).alias("removed"),
            F.sum((first & F.col("reject_reason").isNull()).cast("long")).alias("clean_rows"))
         .withColumnRenamed("source_file", "file"))
audit_rows = [r.asDict() for r in audit.orderBy("file").collect()]

df = df.filter(first).drop("_dup_rank")

# ==========================================================================
# 5. REJECTS  (full audit trail)
# ==========================================================================
rejects = (df.filter(F.col("reject_reason").isNotNull())
             .withColumn("duration_min", np_round(F.col("_dur_sec") / 60.0, 4))
             .withColumn("avg_speed_kmh", np_round(F.col("_speed"), 3))
             .withColumn("started_at", F.col("started_at").cast("timestamp_ntz"))
             .withColumn("ended_at",   F.col("ended_at").cast("timestamp_ntz")))
(rejects.repartition(1)
        .sortWithinPartitions("source_file", "_blk", "_mid")
        .select(*C.RAW_COLUMNS, "reject_reason", "duration_min", "avg_speed_kmh", "source_file")
        .write.mode("overwrite").parquet(spath(REJ_DIR)))

# ==========================================================================
# 6. FEATURE ENGINEERING  (clean rows only)
# ==========================================================================
st = F.col("started_at")
sm, em = F.col("start_station_name").isNull(), F.col("end_station_name").isNull()
season_map = F.create_map(*[F.lit(x) for kv in C.SEASONS.items() for x in kv])
day_names = F.array(*[F.lit(d) for d in C.DAY_NAMES])

clean = (df.filter(F.col("reject_reason").isNull())
    .withColumn("duration_min",  np_round_f32(F.col("_dur_sec") / 60.0, 4))
    .withColumn("haversine_km",  np_round_f32(F.col("_dist_km"), 4))
    .withColumn("avg_speed_kmh", np_round_f32(F.col("_speed"), 3))
    .withColumn("start_station_key", station_key(F.col("start_station_name")))
    .withColumn("end_station_key",   station_key(F.col("end_station_name"))))
clean = add_geohash7(clean, "start_lat", "start_lng", "start_geohash7")
clean = add_geohash7(clean, "end_lat",   "end_lng",   "end_geohash7")
clean = (clean
    .withColumn("trip_type", F.when(~sm & ~em, "docked")
                              .when(sm & ~em,  "dockless_start")
                              .when(~sm & em,  "dockless_end")
                              .otherwise("fully_dockless"))
    # Only meaningful where both endpoints are docked; NULL elsewhere by design.
    .withColumn("is_round_trip", F.when(~sm & ~em,
                                        F.col("start_station_key") == F.col("end_station_key")))
    .withColumn("trip_date",   F.date_format(st, "yyyy-MM-dd"))
    .withColumn("year",        F.year(st).cast("short"))
    .withColumn("month",       F.month(st).cast("byte"))
    .withColumn("day",         F.dayofmonth(st).cast("byte"))
    .withColumn("hour",        F.hour(st).cast("byte"))
    # Spark: 1 = Sunday .. 7 = Saturday.  pandas: 0 = Monday .. 6 = Sunday.
    .withColumn("day_of_week", ((F.dayofweek(st) + 5) % 7).cast("byte"))
    .withColumn("day_name",    F.element_at(day_names, F.col("day_of_week").cast("int") + 1))
    .withColumn("is_weekend",  F.col("day_of_week") >= 5)
    .withColumn("season",      F.element_at(season_map, F.col("month").cast("int")))
    .withColumn("time_of_day", F.when(F.col("hour") <= 5,  "Night")
                                .when(F.col("hour") <= 11, "Morning")
                                .when(F.col("hour") <= 17, "Afternoon")
                                .otherwise("Evening"))
    .withColumn("is_rush_hour", ~F.col("is_weekend")
                & (F.col("hour").between(*C.RUSH_AM) | F.col("hour").between(*C.RUSH_PM)))
    # pandas keeps naive (time-zone-less) timestamps; so does timestamp_ntz.
    .withColumn("started_at", F.col("started_at").cast("timestamp_ntz"))
    .withColumn("ended_at",   F.col("ended_at").cast("timestamp_ntz")))

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
clean = clean.persist(StorageLevel.MEMORY_AND_DISK)

# ==========================================================================
# 7. WRITE PARTITIONED PARQUET
# ==========================================================================
# Sorting by the partition columns first means Spark's writer needs no extra
# sort of its own, so rows keep their raw-file order inside each partition.
(clean.repartition("year", "month")
      .sortWithinPartitions("year", "month", "source_file", "_blk", "_mid")
      .select(*OUT_COLS)
      .write.mode("overwrite").partitionBy("year", "month").parquet(spath(CLEAN_DIR)))

# ==========================================================================
# 8. HANDOFF: station dimension
# ==========================================================================
# pandas builds this per raw file and then combines the files:
#   name = first name seen (file order, start endpoints before end endpoints)
#   lat  = median per file, then the n-weighted mean of those medians
side_cols = ["station_key", "station_name", "station_id", "lat", "lng"]
ends = (clean.filter(~sm).select(
            F.col("start_station_key"), F.col("start_station_name"), F.col("start_station_id"),
            F.col("start_lat"), F.col("start_lng"), "source_file", F.lit(0).alias("side"),
            "_blk", "_mid").toDF(*side_cols, "source_file", "side", "_blk", "_mid")
        .unionByName(clean.filter(~em).select(
            F.col("end_station_key"), F.col("end_station_name"), F.col("end_station_id"),
            F.col("end_lat"), F.col("end_lng"), "source_file", F.lit(1).alias("side"),
            "_blk", "_mid").toDF(*side_cols, "source_file", "side", "_blk", "_mid")))


def exact_median(values):
    """pandas groupby median: middle value, or (lower + upper) / 2."""
    arr = F.array_sort(values)
    n = F.size(arr)
    mid = F.element_at(arr, (n / 2).cast("int") + 1)
    return F.when(n % 2 == 1, mid).otherwise(
        (F.element_at(arr, (n / 2).cast("int")) + mid) / 2.0)


per_file = (ends.groupBy("station_key", "source_file").agg(
                F.count("*").alias("n_obs"),
                exact_median(F.collect_list("lat")).alias("lat"),
                exact_median(F.collect_list("lng")).alias("lng"))
            .withColumn("w_lat", F.col("lat") * F.col("n_obs"))
            .withColumn("w_lng", F.col("lng") * F.col("n_obs")))


def kahan_sum(col):
    """pandas groupby sum uses Kahan summation, in group (file) order."""
    arr = F.transform(F.array_sort(F.collect_list(F.struct("source_file", col.alias("v")))),
                      lambda s: s["v"])
    zero = F.struct(F.lit(0.0).alias("s"), F.lit(0.0).alias("c"))

    def step(acc, x):
        y = x - acc["c"]
        t = acc["s"] + y
        return F.struct(t.alias("s"), ((t - acc["s"]) - y).alias("c"))

    return F.aggregate(arr, zero, step, lambda acc: acc["s"])


first_name = (ends.groupBy("station_key").agg(
    F.min_by("station_name", F.struct("source_file", "side", "_blk", "_mid"))
     .alias("station_name"),
    F.array_sort(F.collect_set("station_id")).alias("ids")))

station_dim = (per_file.groupBy("station_key").agg(
                    F.sum("n_obs").alias("n_trip_endpoints"),
                    kahan_sum(F.col("w_lat")).alias("w_lat"),
                    kahan_sum(F.col("w_lng")).alias("w_lng"))
               .join(first_name, "station_key")
               .select("station_key", "station_name",
                       np_round(F.col("w_lat") / F.col("n_trip_endpoints"), 6).alias("lat"),
                       np_round(F.col("w_lng") / F.col("n_trip_endpoints"), 6).alias("lng"),
                       "n_trip_endpoints",
                       F.size("ids").cast("long").alias("n_legacy_ids"),
                       F.array_join("ids", " | ").alias("legacy_station_ids"))
               .persist())
dim_order = [F.col("n_trip_endpoints").desc(), F.col("station_key")]
write_single(station_dim, os.path.join(HAND_DIR, "station_dim.parquet"), "parquet", dim_order)
write_single(station_dim, os.path.join(HAND_DIR, "station_dim.csv"), "csv", dim_order)

# ==========================================================================
# 9. HANDOFF: station x date x hour flow
# ==========================================================================
# Both counts are bucketed by the trip's START date and hour, as in pandas.
keys = ["station_key", "trip_date", "hour"]
dep = (clean.filter(~sm).groupBy(F.col("start_station_key").alias("station_key"), "trip_date", "hour")
            .agg(F.count("*").alias("departures")))
arr = (clean.filter(~em).groupBy(F.col("end_station_key").alias("station_key"), "trip_date", "hour")
            .agg(F.count("*").alias("arrivals")))
flow = (dep.join(arr, keys, "full_outer")
           .select(*keys,
                   F.coalesce("departures", F.lit(0)).cast("int").alias("departures"),
                   F.coalesce("arrivals", F.lit(0)).cast("int").alias("arrivals"))
           .withColumn("net_flow", F.col("arrivals") - F.col("departures")))  # +ve = bikes pile up
write_single(flow, os.path.join(HAND_DIR, "station_hourly_flow.parquet"), "parquet", keys)

# ==========================================================================
# 10. AUDIT TABLES
# ==========================================================================
audit_cols = (["file", "raw_rows", "duplicate_ride_id"] + rule_named
              + [r + "_owned" for r in rule_named] + ["removed", "clean_rows"])
audit_df = spark.createDataFrame([[row[c] for c in audit_cols] for row in audit_rows],
                                 T.StructType([T.StructField(c, T.StringType() if c == "file"
                                                             else T.LongType())
                                               for c in audit_cols]))
write_single(audit_df, os.path.join(STAT_DIR, "11_cleaning_audit.csv"), "csv", ["file"])

n_raw_total = sum(r["raw_rows"] for r in audit_rows)
n_removed = sum(r["removed"] for r in audit_rows)
n_clean_total = sum(r["clean_rows"] for r in audit_rows)


def pct(n: int) -> str:
    # numpy rounding (ties to even), formatted as Python/pandas would. Passed to
    # Spark as text because Spark's CSV writer prints 0.0008 as 8.0E-4.
    return repr(round(100.0 * n / n_raw_total * 1e4) / 1e4)


reg_rows = [(r, d, sum(a[r] for a in audit_rows), sum(a[r + "_owned"] for a in audit_rows))
            for r, d in C.RULES]
reg_rows.append(("TOTAL", "All rules combined (rows are counted once)",
                 sum(a[r] for a in audit_rows for r in rule_named), n_removed))
reg = spark.createDataFrame([(*row, pct(row[3])) for row in reg_rows],
                            "rule string, description string, rows_matched long, "
                            "rows_removed_by_this_rule long, pct_of_raw string")
write_single(reg, os.path.join(STAT_DIR, "12_quality_register.csv"), "csv")

for d in (CLEAN_DIR, REJ_DIR):
    drop_checksums(d)

# ==========================================================================
# Summary
# ==========================================================================
n_stations = station_dim.count()
n_flow = spark.read.parquet(spath(os.path.join(HAND_DIR, "station_hourly_flow.parquet"))).count()
size = sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(CLEAN_DIR) for f in fs)
raw_size = sum(os.path.getsize(p) for p in files)

for a in audit_rows:
    print("  %-32s raw=%9s  removed=%7s (%.2f%%)  clean=%9s"
          % (a["file"], "{:,}".format(a["raw_rows"]), "{:,}".format(a["removed"]),
             100.0 * a["removed"] / a["raw_rows"], "{:,}".format(a["clean_rows"])))
print("\n" + "=" * 78)
print("RAW ROWS      : {:,}".format(n_raw_total))
print("REMOVED       : {:,}  ({:.2f}%)".format(n_removed, 100.0 * n_removed / n_raw_total))
print("CLEAN ROWS    : {:,}  ({:.2f}%)".format(n_clean_total, 100.0 * n_clean_total / n_raw_total))
print("STATIONS      : {:,}".format(n_stations))
print("FLOW ROWS     : {:,}".format(n_flow))
print("SIZE  raw CSV : {:,.1f} MB   ->   clean Parquet: {:,.1f} MB   ({:.1f}x smaller)"
      .format(raw_size / 1e6, size / 1e6, raw_size / size))
print("\nQUALITY REGISTER")
reg.show(truncate=False)

spark.stop()
